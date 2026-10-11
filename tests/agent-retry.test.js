import test from "node:test";
import assert from "node:assert/strict";
import { validateConfig } from "../src/config.js";
import { COMPLETIONS_SQL, REPORTS_SQL, runOnce, start } from "../src/jobs/agent-retry/index.js";

const quiet = { log: () => {}, warn: () => {} };

test("report re-queue rules: only FAILED, still SUBMITTED, under the attempt cap, after backoff; history note kept", () => {
  assert.match(REPORTS_SQL, /SET "AgentStatus"='PENDING'/);
  assert.match(REPORTS_SQL, /"Status"='SUBMITTED' AND "AgentStatus"='FAILED'/);
  assert.match(REPORTS_SQL, /"AgentAttemptCount" < \$1/);
  assert.match(REPORTS_SQL, /COALESCE\("AgentCompletedAt", "UpdatedAt"\) <= NOW\(\) - make_interval\(mins => \(\$2 \* power\(2, GREATEST\("AgentAttemptCount", 1\) - 1\)\)::int\)/);
  assert.match(REPORTS_SQL, /COALESCE\("AgentLastError" \|\| E'\\n', ''\) \|\|/, "appends, never erases the last error");
});

test("completion re-queue rules: only FAILED and still COMPLETION_SUBMITTED (never after a supervisor decision)", () => {
  assert.match(COMPLETIONS_SQL, /SET "CompletionAgentStatus"='PENDING'/);
  assert.match(COMPLETIONS_SQL, /"Status"='COMPLETION_SUBMITTED' AND "CompletionAgentStatus"='FAILED'/);
  assert.match(COMPLETIONS_SQL, /"CompletionAgentAttemptCount" < \$1/);
  assert.match(COMPLETIONS_SQL, /COALESCE\("CompletionAgentCompletedAt", "UpdatedAt"\)/);
});

test("runOnce passes max attempts and base minutes, and reports both counts", async () => {
  const calls = [];
  const q = async (sql, params) => {
    calls.push(params);
    return { rows: sql.includes('"Reports"') ? [{ no: "RPT-1", attempts: 1 }, { no: "RPT-2", attempts: 2 }] : [{ no: "WO-1", attempts: 1 }] };
  };
  const logged = [];
  const r = await runOnce({ q, log: { log: (m) => logged.push(m) } });
  assert.deepEqual(r, { reports: 2, completions: 1 });
  assert.deepEqual(calls, [[3, 2], [3, 2]], "defaults: 3 attempts, 2 minute base");
  assert.equal(logged.length, 3);
});

test("start keeps running through database errors and stops cleanly", async () => {
  let passes = 0;
  const q = async () => {
    passes += 1;
    throw new Error("connection timeout expired");
  };
  const stop = start({ q, log: quiet });
  await new Promise((r) => setTimeout(r, 30));
  stop();
  assert.ok(passes >= 1, "first pass runs immediately and its error is contained");
});

test("validateConfig: agent-retry alone needs only DATABASE_URL (no Pusher)", () => {
  const base = { jobRealtimeDispatch: false, jobAgentRetry: true, outboxLeaseSeconds: 60, outboxBatchSize: 10, outboxPublishTimeoutMs: 5000 };
  assert.doesNotThrow(() => validateConfig({ ...base, databaseUrl: "postgresql://u@h/db" }));
  assert.throws(() => validateConfig({ ...base, databaseUrl: "" }), /DATABASE_URL/);
});

// Opt-in: runs the real SQL against PostgreSQL inside a transaction that is always rolled back.
// Reuses one report and one work order, staging each case in turn.
const live = process.env.SEEFIX_INTEGRATION_DB === "1";
test("live: re-queues only eligible failures (rolled back)", { skip: !live && "set SEEFIX_INTEGRATION_DB=1 and DATABASE_URL" }, async (t) => {
  const { pool } = await import("../src/database.js");
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const report = (await client.query(`SELECT "Id" FROM "dbo"."Reports" ORDER BY "CreatedAt" LIMIT 1`)).rows[0];
    const order = (await client.query(`SELECT "Id" FROM "dbo"."WorkOrders" ORDER BY "CreatedAt" LIMIT 1`)).rows[0];
    if (!report || !order) return t.skip("needs at least one report and one work order in the database");
    const params = [3, 2]; // max attempts, base minutes (the defaults)

    const reportCase = async (status, attempts, minutesAgo) => {
      await client.query(
        `UPDATE "dbo"."Reports" SET "Status"=$2,"AgentStatus"='FAILED',"AgentAttemptCount"=$3,"AgentCompletedAt"=NOW()-make_interval(mins => $4),"AgentLastError"='boom' WHERE "Id"=$1`,
        [report.Id, status, attempts, minutesAgo]);
      await client.query(REPORTS_SQL, params);
      return (await client.query(`SELECT "AgentStatus","AgentLastError" FROM "dbo"."Reports" WHERE "Id"=$1`, [report.Id])).rows[0];
    };
    const requeued = await reportCase("SUBMITTED", 1, 10);
    assert.equal(requeued.AgentStatus, "PENDING", "1st failure 10 min ago (> 2 min backoff) -> re-queued");
    assert.equal(requeued.AgentLastError, "boom\nAutomatically requeued by seefix-worker after failed attempt 1.");
    assert.equal((await reportCase("SUBMITTED", 2, 3)).AgentStatus, "FAILED", "2nd failure 3 min ago < 4 min backoff");
    assert.equal((await reportCase("SUBMITTED", 2, 5)).AgentStatus, "PENDING", "2nd failure 5 min ago > 4 min backoff");
    assert.equal((await reportCase("SUBMITTED", 1, 1)).AgentStatus, "FAILED", "too recent");
    assert.equal((await reportCase("SUBMITTED", 3, 60)).AgentStatus, "FAILED", "attempt cap reached");
    // Not SUBMITTED any more (already in review). Cancelled reports are additionally locked by a DB trigger.
    assert.equal((await reportCase("PENDING_REVIEW", 1, 60)).AgentStatus, "FAILED", "no longer SUBMITTED");

    const orderCase = async (status) => {
      await client.query(
        `UPDATE "dbo"."WorkOrders" SET "Status"=$2,"CompletionAgentStatus"='FAILED',"CompletionAgentAttemptCount"=1,"CompletionAgentCompletedAt"=NOW()-interval '10 minutes' WHERE "Id"=$1`,
        [order.Id, status]);
      await client.query(COMPLETIONS_SQL, params);
      return (await client.query(`SELECT "CompletionAgentStatus" FROM "dbo"."WorkOrders" WHERE "Id"=$1`, [order.Id])).rows[0].CompletionAgentStatus;
    };
    assert.equal(await orderCase("COMPLETION_SUBMITTED"), "PENDING", "awaiting supervisor -> re-queued");
    assert.equal(await orderCase("COMPLETED"), "FAILED", "supervisor already decided -> left alone");
  } finally {
    await client.query("ROLLBACK");
    client.release();
    await pool.end();
  }
});
