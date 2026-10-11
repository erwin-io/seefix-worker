import test from "node:test";
import assert from "node:assert/strict";
import { backoffMs, claimBatch, countStale, dispatchOnce, expireLapsed, startDispatcher } from "../src/jobs/realtime-dispatch/outbox-dispatcher.js";

const OPTS = { batchSize: 10, leaseSeconds: 30, maxAttempts: 3, baseMs: 1000, maxMs: 8000, maxAgeMinutes: 60 };
const RID = "11111111-1111-4111-8111-111111111111";

/** Fake query that returns `claimed` for the claim and records every outcome UPDATE. */
function harness(claimed) {
  const updates = [];
  const q = async (sql, params) => {
    if (sql.includes("FOR UPDATE SKIP LOCKED")) return { rows: claimed, rowCount: claimed.length };
    if (sql.includes("lease lapsed")) return { rows: [], rowCount: 0 }; // expireLapsed: nothing lapsed
    if (sql.startsWith("UPDATE")) {
      updates.push({ status: sql.includes("'SENT'") ? "SENT" : params[2], params });
      return { rowCount: 1, rows: [] };
    }
    return { rows: [], rowCount: 0 };
  };
  return { q, updates };
}
const row = (over = {}) => ({ Id: "e1", AggregateType: "REPORT", AggregateId: RID, EventName: "report.assessment.completed", Payload: { reportId: RID }, AttemptCount: 1, ...over });

test("backoff doubles per attempt and is capped", () => {
  assert.deepEqual([1, 2, 3, 4, 5].map((n) => backoffMs(n, OPTS)), [1000, 2000, 4000, 8000, 8000]);
});

test("success publishes to the private channel with eventId, then marks SENT for that attempt only", async () => {
  const { q, updates } = harness([row()]);
  const sent = [];
  const r = await dispatchOnce({ q, publish: async (...a) => sent.push(a), ...OPTS });
  assert.deepEqual(r, { claimed: 1, sent: 1, retried: 0, failed: 0, expired: 0 });
  assert.deepEqual(sent[0], [[`private-report-${RID}`], "report.assessment.completed", { reportId: RID, eventId: "e1" }]);
  assert.equal(updates[0].status, "SENT");
  assert.deepEqual(updates[0].params, ["e1", 1], "guarded by Id + AttemptCount");
});

test("Pusher outage keeps the row: back to PENDING with backoff, nothing lost", async () => {
  const { q, updates } = harness([row({ AttemptCount: 2 })]);
  const r = await dispatchOnce({ q, publish: async () => { throw new Error("ECONNRESET"); }, ...OPTS });
  assert.deepEqual(r, { claimed: 1, sent: 0, retried: 1, failed: 0, expired: 0 });
  assert.equal(updates[0].status, "PENDING");
  assert.equal(updates[0].params[3], "ECONNRESET");
  assert.equal(updates[0].params[4], 2, "2nd attempt waits 2s");
});

test("gives up as FAILED after maxAttempts (row kept for inspection)", async () => {
  const { q, updates } = harness([row({ AttemptCount: 3 })]);
  const r = await dispatchOnce({ q, publish: async () => { throw new Error("401"); }, ...OPTS });
  assert.equal(r.failed, 1);
  assert.equal(updates[0].status, "FAILED");
});

test("unroutable rows are FAILED immediately and never published", async () => {
  const { q, updates } = harness([row({ AggregateType: "UNKNOWN", ChannelName: "public-broadcast" })]);
  let published = false;
  const r = await dispatchOnce({ q, publish: async () => { published = true; }, ...OPTS });
  assert.equal(published, false);
  assert.equal(r.failed, 1);
  assert.equal(updates[0].status, "FAILED");
});

test("claim query uses SKIP LOCKED, a lease, PUSHER-only rows and reclaims expired leases", async () => {
  let captured;
  await claimBatch(async (sql, params) => ((captured = { sql, params }), { rows: [] }), OPTS);
  assert.match(captured.sql, /FOR UPDATE SKIP LOCKED/);
  assert.match(captured.sql, /"Transport"='PUSHER'/);
  assert.match(captured.sql, /"Status"='PROCESSING' AND "NextAttemptAt"<=NOW\(\)/);
  assert.match(captured.sql, /"CreatedAt">=NOW\(\)-make_interval\(mins => \$3\)/);
  assert.deepEqual(captured.params, [10, 30, 60]);
});

test("countStale counts only PUSHER rows left PENDING past the max age", async () => {
  let captured;
  const n = await countStale(async (sql, params) => ((captured = { sql, params }), { rows: [{ n: 22 }] }), { maxAgeMinutes: 60 });
  assert.equal(n, 22);
  assert.match(captured.sql, /"Transport"='PUSHER' AND "Status"='PENDING' AND "CreatedAt"<NOW\(\)-make_interval\(mins => \$1\)/);
  assert.deepEqual(captured.params, [60]);
});

test("startDispatcher warns about stale rows, and a failing stale query never stops dispatching", async () => {
  const warnings = [];
  const log = { warn: (m) => warnings.push(m) };
  const run = async (staleQuery) => {
    let passes = 0;
    const q = async (sql) => {
      if (sql.includes("COUNT(*)")) return staleQuery();
      passes += 1;
      return { rows: [] };
    };
    const stop = startDispatcher({ q, publish: async () => {}, intervalMs: 5, log, ...OPTS });
    await new Promise((r) => setTimeout(r, 30));
    stop();
    return passes;
  };
  assert.ok((await run(async () => ({ rows: [{ n: 3 }] }))) >= 2);
  assert.match(warnings[0], /3 PUSHER event\(s\) older than 60 min stay PENDING/);
  warnings.length = 0;
  assert.ok((await run(async () => { throw new Error("db down"); })) >= 2, "claim loop keeps running");
  assert.deepEqual(warnings, [], "stale-count failure is not fatal or noisy");
  assert.equal(await run(async () => ({ rows: [{ n: 0 }] })) >= 2, true);
  assert.deepEqual(warnings, [], "no warning when nothing is stale");
});

test("expireLapsed cancels only expired PUSHER leases on rows past the max age", async () => {
  let captured;
  const n = await expireLapsed(async (sql, params) => ((captured = { sql, params }), { rows: [], rowCount: 2 }), { maxAgeMinutes: 60 });
  assert.equal(n, 2);
  assert.match(captured.sql, /SET "Status"='FAILED'/);
  assert.match(captured.sql, /"LastError"='expired: lease lapsed after OUTBOX_MAX_AGE_MINUTES; not delivered'/);
  assert.doesNotMatch(captured.sql, /CANCELLED/, "older schemas reject CANCELLED");
  assert.match(captured.sql, /"Transport"='PUSHER' AND "Status"='PROCESSING' AND "NextAttemptAt"<=NOW\(\)/);
  assert.match(captured.sql, /"CreatedAt"<NOW\(\)-make_interval\(mins => \$1\)/);
  assert.deepEqual(captured.params, [60]);
});
