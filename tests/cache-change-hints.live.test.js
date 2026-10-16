/**
 * Opt-in live test (SEEFIX_INTEGRATION_DB=1) of cache-change-hints against a database with seefix-api's
 * cache-domain-versions migration. Use a local/dev database only. Pusher is replaced by a recorder; the only
 * writes are a throw-away test user (created, then deleted) and a rolled-back transaction.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";

const live = process.env.SEEFIX_INTEGRATION_DB === "1";
const quiet = { log() {}, warn() {} };

test("live: cache-change-hints", { skip: !live && "set SEEFIX_INTEGRATION_DB=1" }, async (t) => {
  const { pool, query } = await import("../src/database.js");
  const { createHinter, readVersions } = await import("../src/jobs/cache-change-hints/index.js");
  const sql = async (text, params = []) => (await pool.query(text, params)).rows;
  if (!(await readVersions(query))) {
    await pool.end();
    return t.skip("cache-domain-versions migration not applied to this database");
  }

  const calls = [];
  let down = false;
  const publish = async (channels, event, data) => {
    if (down) throw new Error("pusher unreachable");
    calls.push({ channel: channels[0], event, versions: data.versions, keys: Object.keys(data) });
  };
  const hintsFor = (role) => calls.filter((c) => c.channel === `private-role-${role}`);
  const tag = randomUUID().slice(0, 8);
  let userId = null;
  try {
    let clock = 0;
    const hinter = createHinter({ q: query, publish, log: quiet, backoffBaseMs: 1000, backoffMaxMs: 1000, now: () => clock });

    await t.test("start: current versions once per role; then nothing until a commit", async () => {
      await hinter.pass();
      assert.equal(calls.length, 5);
      assert.ok(calls.every((c) => c.event === "data.changed" && c.keys.join() === "versions"));
      assert.deepEqual(Object.keys(hintsFor("WORKER")[0].versions).sort(), ["reference", "reports", "work_orders"]);
      assert.deepEqual(Object.keys(hintsFor("PROCUREMENT")[0].versions).sort(), ["procurement", "reference", "reports"]);
      calls.length = 0;
      await hinter.pass();
      assert.deepEqual(calls, []);
    });

    await t.test("a rolled-back write publishes nothing", async () => {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        await client.query(
          `INSERT INTO "dbo"."Users" ("FullName","Email","PasswordHash","Role") VALUES ($1,$2,'x','WORKER')`,
          [`Hints rollback ${tag}`, `hints-rb-${tag}@example.invalid`],
        );
        await client.query("ROLLBACK");
      } finally {
        client.release();
      }
      await hinter.pass();
      assert.ok(!calls.some((c) => "users" in c.versions), "rollback bumps nothing");
      calls.length = 0;
    });

    await t.test("a committed write in `users` reaches the staff roles only, with the committed version", async () => {
      const before = (await readVersions(query)).users;
      userId = (await sql(
        `INSERT INTO "dbo"."Users" ("FullName","Email","PasswordHash","Role") VALUES ($1,$2,'x','WORKER') RETURNING "Id"`,
        [`Hints ${tag}`, `hints-${tag}@example.invalid`],
      ))[0].Id;
      const after = (await readVersions(query)).users;
      assert.ok(after > before);
      await hinter.pass();
      for (const role of ["ADMIN", "MAINTENANCE_SUPERVISOR", "MAINTENANCE_STAFF"]) {
        assert.ok(hintsFor(role).some((c) => c.versions.users >= after), role);
      }
      for (const role of ["WORKER", "PROCUREMENT"]) {
        assert.ok(!hintsFor(role).some((c) => "users" in c.versions), `${role} never hears about users`);
      }
      assert.ok(!JSON.stringify(calls).includes(tag) && !JSON.stringify(calls).includes(userId), "no record data in hints");
      calls.length = 0;
    });

    await t.test("Pusher outage: the change is kept and delivered once Pusher is back", async () => {
      down = true;
      await sql(`DELETE FROM "dbo"."Users" WHERE "Id"=$1`, [userId]);
      userId = null;
      const v = (await readVersions(query)).users;
      assert.equal((await hinter.pass()).status, "pusher-error");
      assert.equal((await hinter.pass()).status, "backoff");
      down = false;
      clock += 1000;
      assert.equal((await hinter.pass()).status, "ok");
      assert.ok(hintsFor("ADMIN").some((c) => c.versions.users >= v));
      calls.length = 0;
      await hinter.pass();
      assert.ok(!calls.some((c) => "users" in c.versions), "delivered once");
    });

    await t.test("worker mutations: agent-retry's tables bump versions by trigger; reminder delivery tables are not cache domains", async () => {
      const triggers = await sql(
        `SELECT c.relname AS table FROM pg_trigger tg JOIN pg_class c ON c.oid = tg.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = 'dbo' AND NOT tg.tgisinternal AND tg.tgname ILIKE '%CacheVersion%'`,
      );
      const tables = new Set(triggers.map((r) => r.table));
      for (const table of ["Reports", "WorkOrders"]) assert.ok(tables.has(table), `${table} (agent-retry requeues) is covered`);
      for (const table of ["AssistantReminders", "Notifications", "OutboxEvents"]) assert.ok(!tables.has(table), `${table} is not a cache domain`);
    });
  } finally {
    if (userId) await sql(`DELETE FROM "dbo"."Users" WHERE "Id"=$1`, [userId]).catch(() => {});
    await pool.end();
  }
});
