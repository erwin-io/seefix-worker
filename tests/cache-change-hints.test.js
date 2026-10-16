import test from "node:test";
import assert from "node:assert/strict";
import { createHinter, EVENT, readVersions, runOnce } from "../src/jobs/cache-change-hints/index.js";
import { DOMAINS_BY_ROLE } from "../src/jobs/cache-change-hints/domains.js";

const quiet = { log() {}, warn() {} };
const ALL = { reports: 41, work_orders: 17, procurement: 9, action_items: 3, users: 12, reference: 5 };

/** Fake versions table; set `db.versions` between passes. `db.error` makes the read throw. */
function fakeDb(versions = { ...ALL }) {
  const db = {
    versions,
    reads: 0,
    error: null,
    q: async (sql) => {
      db.reads += 1;
      assert.match(sql, /FROM "dbo"\."CacheDomainVersions" WHERE "Domain" <> '\*'/);
      if (db.error) throw db.error;
      return { rows: Object.entries(db.versions).map(([Domain, Version]) => ({ Domain, Version: String(Version) })) };
    },
  };
  return db;
}
function fakePusher() {
  const p = { calls: [], down: false };
  p.publish = async (channels, event, data) => {
    if (p.down) throw new Error("pusher unreachable");
    p.calls.push({ channels, event, data: JSON.parse(JSON.stringify(data)) });
  };
  p.take = () => p.calls.splice(0);
  return p;
}
const byChannel = (calls) => Object.fromEntries(calls.map((c) => [c.channels.join(","), c.data.versions]));

test("first pass (start / restart) publishes current versions once per role, filtered by DOMAINS_BY_ROLE", async () => {
  const db = fakeDb();
  const pusher = fakePusher();
  const r = await createHinter({ q: db.q, publish: pusher.publish, log: quiet }).pass();
  assert.equal(r.status, "ok");
  const calls = pusher.take();
  assert.ok(calls.every((c) => c.event === EVENT && c.channels.length === 1));
  assert.deepEqual(byChannel(calls), {
    "private-role-ADMIN": ALL,
    "private-role-MAINTENANCE_SUPERVISOR": ALL,
    "private-role-MAINTENANCE_STAFF": ALL,
    "private-role-PROCUREMENT": { procurement: 9, reports: 41, reference: 5 },
    "private-role-WORKER": { work_orders: 17, reports: 41, reference: 5 },
  });
  assert.ok(!calls.some((c) => c.channels[0].includes("REPORTER")), "REPORTER has no channel");
});

test("dedupe: unchanged versions publish nothing; a rise reaches only roles that may see that domain", async () => {
  const db = fakeDb();
  const pusher = fakePusher();
  const h = createHinter({ q: db.q, publish: pusher.publish, log: quiet });
  await h.pass();
  pusher.take();
  await h.pass();
  await h.pass();
  assert.deepEqual(pusher.take(), [], "no change, no hint (replayed passes are no-ops)");

  db.versions = { ...ALL, users: 13 };
  await h.pass();
  assert.deepEqual(byChannel(pusher.take()), {
    "private-role-ADMIN": { users: 13 },
    "private-role-MAINTENANCE_SUPERVISOR": { users: 13 },
    "private-role-MAINTENANCE_STAFF": { users: 13 },
  }, "PROCUREMENT and WORKER never hear about users");

  db.versions = { ...ALL, users: 13, work_orders: 18, reports: 42 };
  await h.pass();
  const calls = byChannel(pusher.take());
  assert.deepEqual(calls["private-role-WORKER"], { work_orders: 18, reports: 42 });
  assert.deepEqual(calls["private-role-PROCUREMENT"], { reports: 42 });
  assert.deepEqual(calls["private-role-ADMIN"], { work_orders: 18, reports: 42 });
});

test("hints carry only domain names and integer versions, never record data or unknown domains", async () => {
  const db = fakeDb({ ...ALL, secret_domain: 7, "*": 99 });
  const pusher = fakePusher();
  await createHinter({ q: db.q, publish: pusher.publish, log: quiet }).pass();
  for (const { data } of pusher.take()) {
    assert.deepEqual(Object.keys(data), ["versions"]);
    for (const [domain, version] of Object.entries(data.versions)) {
      assert.ok(Object.values(DOMAINS_BY_ROLE).flat().includes(domain), domain);
      assert.ok(Number.isSafeInteger(version));
    }
    assert.ok(!("secret_domain" in data.versions) && !("*" in data.versions));
  }
});

test("Pusher outage: nothing is marked sent, exponential backoff (capped), then one coalesced hint with the latest numbers", async () => {
  const db = fakeDb();
  const pusher = fakePusher();
  let clock = 1_000_000;
  const warnings = [];
  const h = createHinter({ q: db.q, publish: pusher.publish, log: { log() {}, warn: (m) => warnings.push(m) }, backoffBaseMs: 1000, backoffMaxMs: 4000, now: () => clock });
  await h.pass();
  pusher.take();

  pusher.down = true;
  db.versions = { ...ALL, reports: 50 };
  assert.equal((await h.pass()).status, "pusher-error");
  const reads = db.reads;
  clock += 999;
  assert.equal((await h.pass()).status, "backoff", "waits 1 s");
  assert.equal(db.reads, reads, "no DB read while backing off");
  clock += 1;
  assert.equal((await h.pass()).status, "pusher-error");
  const waits = [];
  for (let i = 0; i < 4; i += 1) {
    let waited = 0;
    while ((await h.pass()).status === "backoff") {
      clock += 500;
      waited += 500;
    }
    waits.push(waited);
  }
  assert.deepEqual(waits, [2000, 4000, 4000, 4000], "1 s, 2 s, 4 s, then capped at 4 s");
  assert.ok(warnings.length >= 1 && warnings.length <= 4, "warnings are rate-limited (1st, 2nd, 4th... failure)");

  // Recovery: versions moved again during the outage; one hint per role with the newest numbers.
  pusher.down = false;
  db.versions = { ...ALL, reports: 52, reference: 6 };
  clock += 10_000;
  assert.equal((await h.pass()).status, "ok");
  const calls = pusher.take();
  assert.equal(calls.length, 5);
  for (const c of calls) assert.deepEqual(c.data.versions, { reports: 52, reference: 6 });
  await h.pass();
  assert.deepEqual(pusher.take(), [], "and not again");
});

test("a failure part-way only re-sends to the roles that did not get the hint", async () => {
  const db = fakeDb();
  const sent = [];
  let failOn = "private-role-PROCUREMENT";
  const publish = async ([channel], _e, data) => {
    if (channel === failOn) throw new Error("timeout");
    sent.push([channel, data.versions]);
  };
  let clock = 0;
  const h = createHinter({ q: db.q, publish, log: quiet, backoffBaseMs: 10, backoffMaxMs: 10, now: () => clock });
  await h.pass();
  assert.deepEqual(sent.map(([c]) => c), ["private-role-ADMIN", "private-role-MAINTENANCE_SUPERVISOR", "private-role-MAINTENANCE_STAFF"]);
  sent.length = 0;
  failOn = null;
  clock += 10;
  await h.pass();
  assert.deepEqual(sent.map(([c]) => c), ["private-role-PROCUREMENT", "private-role-WORKER"]);
});

test("restart safety: a new hinter (fresh process) republishes the current versions exactly once", async () => {
  const db = fakeDb();
  const pusher = fakePusher();
  await createHinter({ q: db.q, publish: pusher.publish, log: quiet }).pass();
  const before = byChannel(pusher.take());
  const restarted = createHinter({ q: db.q, publish: pusher.publish, log: quiet });
  await restarted.pass();
  await restarted.pass();
  assert.deepEqual(byChannel(pusher.take()), before);
  assert.deepEqual((await runOnce({ q: db.q, publish: pusher.publish, log: quiet })).roles.length, 5);
});

test("no versions table yet: idle, logged once, and picks up when the migration lands", async () => {
  const db = fakeDb();
  db.error = Object.assign(new Error('relation "dbo.CacheDomainVersions" does not exist'), { code: "42P01" });
  const pusher = fakePusher();
  const logs = [];
  const log = { log: (m) => logs.push(m), warn: (m) => logs.push(m) };
  const h = createHinter({ q: db.q, publish: pusher.publish, log });
  for (let i = 0; i < 3; i += 1) assert.equal((await h.pass()).status, "unavailable");
  assert.equal(logs.length, 1);
  db.error = null;
  assert.equal((await h.pass()).status, "ok");
  assert.equal(pusher.take().length, 5);
  assert.match(logs[1], /found/);
});

test("database errors propagate (repeat() logs them); readVersions returns null only for a missing table", async () => {
  const db = fakeDb();
  db.error = Object.assign(new Error("connection refused"), { code: "ECONNREFUSED" });
  await assert.rejects(createHinter({ q: db.q, publish: async () => {}, log: quiet }).pass(), /connection refused/);
  db.error = Object.assign(new Error("missing"), { code: "42P01" });
  assert.equal(await readVersions(db.q), null);
});
