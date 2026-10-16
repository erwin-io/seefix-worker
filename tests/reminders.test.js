import test from "node:test";
import assert from "node:assert/strict";
import { createRunner, deliver, dedupeKey } from "../src/jobs/reminders/index.js";
import { validateConfig } from "../src/config.js";

const quiet = { log() {}, warn() {} };
const ROW = { Id: "r1", RecipientUserId: "u1", Title: "Check pump", Message: null, EntityType: null, EntityId: null, Attempts: 1, DueAt: new Date() };

test("reminders migration not applied: idle, logged once, starts by itself once the schema exists", async () => {
  let missing = true;
  const pool = {
    query: async (sql) => {
      assert.match(sql, /ClaimDueAssistantReminders/);
      if (missing) throw Object.assign(new Error('function dbo.ClaimDueAssistantReminders does not exist'), { code: "42883" });
      return { rows: [] };
    },
  };
  const logs = [];
  const runner = createRunner({ pool, log: { log: (m) => logs.push(m), warn: (m) => logs.push(m) } });
  for (let i = 0; i < 3; i += 1) assert.equal((await runner.pass()).available, false);
  assert.equal(logs.length, 1);
  assert.match(logs[0], /does not exist yet/);
  missing = false;
  assert.deepEqual(await runner.pass(), { available: true, claimed: 0, sent: 0, retry: 0, failed: 0, fenced: 0 });
  assert.match(logs[1], /found/);
});

test("other database errors are not swallowed (repeat() logs and retries them)", async () => {
  const pool = { query: async () => { throw Object.assign(new Error("connection refused"), { code: "ECONNREFUSED" }); } };
  await assert.rejects(createRunner({ pool, log: quiet }).pass(), /connection refused/);
});

/** Fake pool recording each statement per transaction. */
function fakePool({ finishOk = true, recipient = true } = {}) {
  const log = [];
  const client = {
    query: async (sql, params) => {
      log.push(sql.trim().split(/\s+/).slice(0, 3).join(" "));
      if (/FROM "dbo"\."Users"/.test(sql)) return { rowCount: recipient ? 1 : 0, rows: recipient ? [{}] : [] };
      if (/FinishAssistantReminder/.test(sql)) return { rows: [{ ok: params[2] ? finishOk : true }] };
      return { rows: [], rowCount: 0 };
    },
    release() {},
  };
  return { log, connect: async () => client, query: client.query };
}

test("delivery is one transaction: notification for the recipient with the reminder's dedupe key, then the fenced finish", async () => {
  const pool = fakePool();
  const calls = [];
  const status = await deliver(ROW, { pool, log: quiet, createNotification: async (_c, n) => (calls.push(n), { id: "n1" }) });
  assert.equal(status, "sent");
  assert.deepEqual(calls, [{ userId: "u1", type: "ASSISTANT_REMINDER", title: "Check pump", message: "Check pump", entityType: null, entityId: null, deduplicationKey: dedupeKey("r1") }]);
  assert.equal(pool.log[0], "BEGIN");
  assert.equal(pool.log.at(-1), "COMMIT");
});

test("finish refused (cancelled / re-claimed meanwhile): the notification is rolled back", async () => {
  const pool = fakePool({ finishOk: false });
  assert.equal(await deliver(ROW, { pool, log: quiet, createNotification: async () => ({ id: "n1" }) }), "fenced");
  assert.equal(pool.log.at(-1), "ROLLBACK");
  assert.ok(!pool.log.includes("COMMIT"));
});

test("a failing delivery rolls back and records the failure; FAILED once the attempt cap is reached", async () => {
  const boom = async () => { throw new Error("db hiccup"); };
  let pool = fakePool();
  assert.equal(await deliver(ROW, { pool, log: quiet, createNotification: boom }), "retry");
  assert.ok(pool.log.includes("ROLLBACK") && !pool.log.includes("COMMIT"));
  assert.ok(pool.log.at(-1).startsWith("SELECT"), "failure recorded after the rollback");
  pool = fakePool();
  assert.equal(await deliver({ ...ROW, Attempts: 5 }, { pool, log: quiet, createNotification: boom }), "failed");
});

test("recipient no longer active at delivery time: nothing is written for them", async () => {
  const pool = fakePool({ recipient: false });
  let wrote = false;
  assert.equal(await deliver(ROW, { pool, log: quiet, createNotification: async () => (wrote = true) }), "retry");
  assert.equal(wrote, false);
});

test("validateConfig: cache-change-hints needs Pusher; reminders needs only the database", () => {
  const base = { databaseUrl: "postgresql://u@db.invalid/x", outboxLeaseSeconds: 60, outboxBatchSize: 10, outboxPublishTimeoutMs: 5000 };
  assert.doesNotThrow(() => validateConfig({ ...base, jobReminders: true }));
  assert.throws(() => validateConfig({ ...base, jobCacheChangeHints: true }), /PUSHER_KEY is required for JOB_CACHE_CHANGE_HINTS/);
  assert.doesNotThrow(() => validateConfig({ ...base, jobCacheChangeHints: true, pusherAppId: "1", pusherKey: "k", pusherSecret: "s", pusherCluster: "ap1" }));
});
