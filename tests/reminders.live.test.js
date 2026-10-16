/**
 * Opt-in live test (SEEFIX_INTEGRATION_DB=1) of the reminders job against a database with seefix-api's
 * 2026-10-16-v2-workflow-assistant.sql migration. Use a local/dev database only. It creates its own users and
 * reminders and deletes them (and their notifications/outbox rows) afterwards. It refuses to run while other due
 * reminders exist, because the claim function would deliver those too.
 */
import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import pg from "pg";

const live = process.env.SEEFIX_INTEGRATION_DB === "1";
const quiet = { log() {}, warn() {} };

test("live: reminders job", { skip: !live && "set SEEFIX_INTEGRATION_DB=1" }, async (t) => {
  const { pool } = await import("../src/database.js");
  const { createRunner, deliver, dedupeKey } = await import("../src/jobs/reminders/index.js");
  const { resolveChannels } = await import("../src/jobs/realtime-dispatch/channels.js");
  const sql = async (text, params = []) => (await pool.query(text, params)).rows;

  const exists = (await sql(`SELECT to_regprocedure('"dbo"."ClaimDueAssistantReminders"(text,integer,integer)') AS f`))[0].f;
  if (!exists) return t.skip("reminders migration not applied to this database");

  const tag = randomUUID().slice(0, 8);
  const users = {};
  const addUser = async (key, role) => {
    users[key] = (await sql(
      `INSERT INTO "dbo"."Users" ("FullName","Email","PasswordHash","Role") VALUES ($1,$2,'x',$3) RETURNING "Id"`,
      [`Worker3 ${key} ${tag}`, `worker3-${key}-${tag}@example.invalid`, role],
    ))[0].Id;
  };
  await addUser("admin", "ADMIN");
  await addUser("sup", "MAINTENANCE_SUPERVISOR");
  await addUser("workerA", "WORKER");
  await addUser("procB", "PROCUREMENT");
  await addUser("reporter", "REPORTER");
  const ids = () => Object.values(users);

  /** Inserts a reminder the way the API stores it: DueAt/NextRunAt are UTC instants computed by PostgreSQL. */
  const remind = async ({ by = users.sup, to, title = `R ${tag}`, message = null, due = `now() - interval '1 minute'`, tz = "UTC" }) =>
    (await sql(
      `INSERT INTO "dbo"."AssistantReminders" ("CreatedBy","RecipientUserId","Title","Message","DueAt","NextRunAt","TimeZone","IdempotencyKey")
       VALUES ($1,$2,$3,$4,${due},${due},$5,gen_random_uuid()) RETURNING *`,
      [by, to, title, message, tz],
    ))[0];
  const reminder = async (id) => (await sql(`SELECT * FROM "dbo"."AssistantReminders" WHERE "Id"=$1`, [id]))[0];
  const notesFor = async (userId) => sql(`SELECT * FROM "dbo"."Notifications" WHERE "UserId"=$1 ORDER BY "CreatedAt"`, [userId]);
  const notesForReminder = async (id) => sql(`SELECT * FROM "dbo"."Notifications" WHERE "Payload"->>'deduplicationKey'=$1`, [dedupeKey(id)]);
  const runner = (opts = {}) => createRunner({ pool, log: quiet, worker: `live-${tag}`, leaseSeconds: 60, ...opts });
  /** Cancels whatever of ours is still pending, so one subtest never feeds the next. */
  const settle = () => sql(
    `UPDATE "dbo"."AssistantReminders" SET "Status"='CANCELLED' WHERE "CreatedBy" = ANY($1) AND "Status" IN ('SCHEDULED','PROCESSING')`, [ids()]);
  const claimDirect = (worker, lease = 60) => sql(`SELECT * FROM "dbo"."ClaimDueAssistantReminders"($1, 50, $2)`, [worker, lease]);

  try {
    const foreign = await sql(
      `SELECT count(*)::int AS n FROM "dbo"."AssistantReminders"
        WHERE "Status" IN ('SCHEDULED','PROCESSING') AND "NextRunAt" <= now() + interval '15 minutes' AND NOT ("CreatedBy" = ANY($1))`, [ids()]);
    if (foreign[0].n) return t.skip(`${foreign[0].n} other reminder(s) due soon in this database; not delivering them`);

    await t.test("delivers a due reminder to its recipient only, with one notification.created outbox row", async () => {
      const r = await remind({ to: users.workerA, title: `Inspect pump ${tag}` });
      const result = await runner().pass();
      assert.equal(result.sent, 1);
      const row = await reminder(r.Id);
      assert.equal(row.Status, "SENT");
      assert.ok(row.SentAt && row.NotificationId);
      const [n] = await notesFor(users.workerA);
      assert.equal(n.Id, row.NotificationId);
      assert.deepEqual([n.Type, n.Title, n.Message], ["ASSISTANT_REMINDER", `Inspect pump ${tag}`, `Inspect pump ${tag}`], "message falls back to the title");
      assert.deepEqual(await notesFor(users.sup), [], "the creator is not notified");
      const outbox = await sql(`SELECT * FROM "dbo"."OutboxEvents" WHERE "AggregateId"=$1`, [n.Id]);
      assert.equal(outbox.length, 1);
      assert.deepEqual([outbox[0].Transport, outbox[0].EventName, outbox[0].RecipientUserId, outbox[0].Status], ["PUSHER", "notification.created", users.workerA, "PENDING"]);
      assert.deepEqual(Object.keys(outbox[0].Payload).sort(), ["entityId", "entityType", "notificationId", "type"], "ids only");
      assert.deepEqual(await resolveChannels(outbox[0], (q, p) => pool.query(q, p)), [`private-user-${users.workerA}`]);
    });

    await t.test("two accounts and roles: each reminder reaches only its own recipient", async () => {
      const a = await remind({ to: users.workerA, title: `For A ${tag}`, message: "only A" });
      const b = await remind({ by: users.admin, to: users.procB, title: `For B ${tag}`, message: "only B" });
      assert.equal((await runner().pass()).sent, 2);
      const forA = (await notesFor(users.workerA)).filter((n) => n.Title === `For A ${tag}`);
      const forB = (await notesFor(users.procB)).filter((n) => n.Title === `For B ${tag}`);
      assert.equal(forA.length, 1);
      assert.equal(forB.length, 1);
      assert.ok(!(await notesFor(users.workerA)).some((n) => n.Title === `For B ${tag}`));
      assert.ok(!(await notesFor(users.procB)).some((n) => n.Title === `For A ${tag}`));
      for (const [r, user] of [[a, users.workerA], [b, users.procB]]) {
        const ob = await sql(`SELECT o.* FROM "dbo"."OutboxEvents" o JOIN "dbo"."AssistantReminders" r ON r."NotificationId"=o."AggregateId" WHERE r."Id"=$1`, [r.Id]);
        assert.deepEqual(ob.map((o) => o.RecipientUserId), [user]);
      }
      assert.deepEqual((await notesFor(users.admin)).concat(await notesFor(users.sup), await notesFor(users.reporter)), [], "nobody else");
    });

    await t.test("replay idempotency: re-running, or replaying a reminder whose notification exists, never duplicates", async () => {
      const r = await remind({ to: users.workerA, title: `Replay ${tag}` });
      await runner().pass();
      assert.equal((await runner().pass()).claimed, 0, "a SENT reminder is never claimed again");
      const first = await reminder(r.Id);
      // Force a replay as if the SENT mark were lost: the dedupe key returns the existing notification.
      await sql(`UPDATE "dbo"."AssistantReminders" SET "Status"='SCHEDULED',"NextRunAt"=now()-interval '1 second' WHERE "Id"=$1`, [r.Id]);
      assert.equal((await runner().pass()).sent, 1);
      const again = await reminder(r.Id);
      assert.equal(again.Status, "SENT");
      assert.equal(again.NotificationId, first.NotificationId, "same notification re-used");
      assert.equal((await notesForReminder(r.Id)).length, 1);
      assert.equal((await sql(`SELECT 1 FROM "dbo"."OutboxEvents" WHERE "AggregateId"=$1`, [first.NotificationId])).length, 1, "one outbox row");
    });

    await t.test("Pusher outage does not affect reminders: the notification is durable, the outbox row waits for realtime-dispatch", async () => {
      // The reminders job never calls Pusher; realtime-dispatch retries the outbox row with backoff (its own tests).
      const r = await remind({ to: users.procB, title: `Outage ${tag}` });
      assert.equal((await runner().pass()).sent, 1);
      const row = await reminder(r.Id);
      const ob = await sql(`SELECT "Status","DeduplicationKey" FROM "dbo"."OutboxEvents" WHERE "AggregateId"=$1`, [row.NotificationId]);
      assert.deepEqual(ob, [{ Status: "PENDING", DeduplicationKey: `notification:${row.NotificationId}:created` }]);
    });

    await t.test("restart safety: a dead worker's claim is taken over after the lease, its late finish is refused, one notification", async () => {
      const r = await remind({ to: users.workerA, title: `Crash ${tag}` });
      const [claimed] = (await claimDirect("dead-worker")).filter((x) => x.Id === r.Id);
      assert.equal(claimed.Attempts, 1);
      assert.equal((await runner().pass()).claimed, 0, "a live lease is respected");
      await sql(`UPDATE "dbo"."AssistantReminders" SET "ClaimedAt"=now()-interval '5 minutes' WHERE "Id"=$1`, [r.Id]);
      assert.equal((await runner().pass()).sent, 1, "taken over after the lease");
      const row = await reminder(r.Id);
      assert.deepEqual([row.Status, row.Attempts, row.ClaimedBy], ["SENT", 2, `live-${tag}`]);
      // The dead worker comes back and tries to deliver its stale claim.
      assert.equal(await deliver(claimed, { pool, log: quiet }), "fenced");
      assert.equal((await notesForReminder(r.Id)).length, 1);
    });

    await t.test("concurrent workers (SKIP LOCKED): every reminder is delivered exactly once", async () => {
      const made = [];
      for (let i = 0; i < 8; i += 1) made.push(await remind({ to: i % 2 ? users.workerA : users.procB, title: `Conc ${i} ${tag}` }));
      const results = await Promise.all([1, 2, 3].map((n) => runner({ worker: `live-${tag}-${n}`, batchSize: 2 }).pass()));
      assert.equal(results.reduce((s, r) => s + r.sent, 0), 8);
      for (const r of made) assert.equal((await notesForReminder(r.Id)).length, 1, r.Title);
    });

    await t.test("cancelled while PROCESSING: the delivery rolls back, nothing is sent", async () => {
      const r = await remind({ to: users.workerA, title: `Cancel ${tag}` });
      const [claimed] = (await claimDirect("w1")).filter((x) => x.Id === r.Id);
      await sql(`UPDATE "dbo"."AssistantReminders" SET "Status"='CANCELLED',"CancelledAt"=now(),"CancelledBy"=$2 WHERE "Id"=$1`, [r.Id, users.sup]);
      assert.equal(await deliver(claimed, { pool, log: quiet }), "fenced");
      assert.equal((await reminder(r.Id)).Status, "CANCELLED");
      assert.deepEqual(await notesForReminder(r.Id), []);
    });

    await t.test("failure: retry with 2^attempts-minute backoff, FAILED at the attempt cap, never a notification", async () => {
      const r = await remind({ to: users.workerA, title: `Fail ${tag}` });
      const broken = runner({ createNotification: async () => { throw new Error("simulated write failure"); } });
      assert.equal((await broken.pass()).retry, 1);
      let row = (await sql(`SELECT *, extract(epoch FROM "NextRunAt" - now()) AS wait FROM "dbo"."AssistantReminders" WHERE "Id"=$1`, [r.Id]))[0];
      assert.deepEqual([row.Status, row.Attempts, row.LastError], ["SCHEDULED", 1, "simulated write failure"]);
      assert.ok(row.wait > 100 && row.wait <= 120, `backoff 2 min, got ${row.wait}s`);
      assert.equal((await broken.pass()).claimed, 0, "not due during backoff");
      await sql(`UPDATE "dbo"."AssistantReminders" SET "Attempts"=4,"NextRunAt"=now() WHERE "Id"=$1`, [r.Id]);
      assert.equal((await broken.pass()).failed, 1);
      row = await reminder(r.Id);
      assert.deepEqual([row.Status, row.Attempts], ["FAILED", 5]);
      assert.equal((await runner().pass()).claimed, 0, "FAILED is terminal");
      assert.deepEqual(await notesForReminder(r.Id), []);
    });

    await t.test("no unauthorized notification: deactivated or REPORTER recipients are cancelled, never notified", async () => {
      const inactive = await remind({ to: users.procB, title: `Inactive ${tag}` });
      const reporter = await remind({ to: users.reporter, title: `Reporter ${tag}` }); // the API refuses this; defence in depth
      await sql(`UPDATE "dbo"."Users" SET "IsActive"=false WHERE "Id"=$1`, [users.procB]);
      try {
        await runner().pass();
        for (const r of [inactive, reporter]) {
          const row = await reminder(r.Id);
          assert.deepEqual([row.Status, row.LastError], ["CANCELLED", "RECIPIENT_INACTIVE"]);
          assert.deepEqual(await notesForReminder(r.Id), []);
        }
      } finally {
        await sql(`UPDATE "dbo"."Users" SET "IsActive"=true WHERE "Id"=$1`, [users.procB]);
      }
      // Deactivated between the claim and the delivery: refused inside the delivery transaction.
      const late = await remind({ to: users.procB, title: `Late deactivation ${tag}` });
      const [claimed] = (await claimDirect("w2")).filter((x) => x.Id === late.Id);
      await sql(`UPDATE "dbo"."Users" SET "IsActive"=false WHERE "Id"=$1`, [users.procB]);
      try {
        assert.equal(await deliver(claimed, { pool, log: quiet }), "retry");
        assert.deepEqual(await notesForReminder(late.Id), []);
        await sql(`UPDATE "dbo"."AssistantReminders" SET "NextRunAt"=now() WHERE "Id"=$1`, [late.Id]);
        await runner().pass();
        assert.equal((await reminder(late.Id)).Status, "CANCELLED");
      } finally {
        await sql(`UPDATE "dbo"."Users" SET "IsActive"=true WHERE "Id"=$1`, [users.procB]);
      }
    });

    await t.test("time zones: due is a UTC instant; the worker's and the DB session's zone don't matter; DST conversions", async () => {
      // How the API stores local wall-clock input (docs/V2_ASSISTANT.md §5).
      const [dst] = await sql(`SELECT
        to_char(('2027-03-14T02:30'::timestamp AT TIME ZONE 'America/New_York') AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI') AS gap,
        to_char(('2026-11-01T01:30'::timestamp AT TIME ZONE 'America/New_York') AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI') AS ambiguous`);
      assert.deepEqual(dst, { gap: "2027-03-14T07:30", ambiguous: "2026-11-01T06:30" });

      // Local wall-clock "now -1 min" in zones far from UTC, re-read in that zone: due. "now +2 min" in UTC+14: not due,
      // although its local calendar date is already "tomorrow" compared to UTC.
      const local = (zone, offset) => `((to_char((now() + interval '${offset}') AT TIME ZONE '${zone}', 'YYYY-MM-DD"T"HH24:MI:SS'))::timestamp AT TIME ZONE '${zone}')`;
      const manila = await remind({ to: users.workerA, title: `Manila ${tag}`, tz: "Asia/Manila", due: local("Asia/Manila", "-1 minute") });
      const pagoPago = await remind({ to: users.workerA, title: `Pago ${tag}`, tz: "Pacific/Pago_Pago", due: local("Pacific/Pago_Pago", "-1 minute") });
      const kiritimati = await remind({ to: users.workerA, title: `Kiritimati ${tag}`, tz: "Pacific/Kiritimati", due: local("Pacific/Kiritimati", "2 minutes") });
      // A separate pool whose sessions run in yet another zone.
      const tzPool = new pg.Pool({ connectionString: process.env.DATABASE_URL, max: 2, options: "-c TimeZone=Pacific/Kiritimati", ssl: false });
      try {
        await runner({ pool: tzPool }).pass();
      } finally {
        await tzPool.end();
      }
      assert.equal((await reminder(manila.Id)).Status, "SENT");
      assert.equal((await reminder(pagoPago.Id)).Status, "SENT");
      assert.equal((await reminder(kiritimati.Id)).Status, "SCHEDULED", "not before its instant");
      const due = (await sql(`SELECT extract(epoch FROM "DueAt" - now()) AS s FROM "dbo"."AssistantReminders" WHERE "Id"=$1`, [kiritimati.Id]))[0].s;
      assert.ok(due > 60 && due <= 120, `UTC+14 local "+2 min" is 2 minutes away, got ${due}s`);
      await settle();
    });

    await t.test("missed reminder recovery: after an outage every overdue reminder is delivered in one pass, oldest first", async () => {
      const made = [];
      for (let h = 6; h >= 1; h -= 1) made.push(await remind({ to: users.workerA, title: `Missed ${h}h ${tag}`, due: `now() - interval '${h} hours'` }));
      // One claimed by a worker that died 10 minutes ago.
      const crashed = await remind({ to: users.procB, title: `Crashed ${tag}`, due: `now() - interval '30 minutes'` });
      await claimDirect("dead");
      await sql(`UPDATE "dbo"."AssistantReminders" SET "ClaimedAt"=now()-interval '10 minutes' WHERE "CreatedBy" = ANY($1) AND "Status"='PROCESSING'`, [ids()]);
      const sentOrder = [];
      const result = await runner({ batchSize: 3, log: { log: (m) => sentOrder.push(m), warn() {} } }).pass();
      assert.equal(result.sent, 7);
      for (const r of [...made, crashed]) assert.equal((await reminder(r.Id)).Status, "SENT", r.Title);
      assert.equal((await notesForReminder(crashed.Id)).length, 1);
      const order = made.map((r) => sentOrder.findIndex((m) => m.includes(r.Id)));
      assert.deepEqual(order, [...order].sort((a, b) => a - b), "oldest due first");
    });
  } finally {
    await settle().catch(() => {});
    await sql(`DELETE FROM "dbo"."OutboxEvents" WHERE "AggregateId" IN (SELECT "Id" FROM "dbo"."Notifications" WHERE "UserId" = ANY($1))`, [ids()]);
    await sql(`DELETE FROM "dbo"."AssistantReminders" WHERE "CreatedBy" = ANY($1) OR "RecipientUserId" = ANY($1)`, [ids()]);
    await sql(`DELETE FROM "dbo"."Notifications" WHERE "UserId" = ANY($1)`, [ids()]);
    await sql(`DELETE FROM "dbo"."Users" WHERE "Id" = ANY($1)`, [ids()]);
    await pool.end();
  }
});
