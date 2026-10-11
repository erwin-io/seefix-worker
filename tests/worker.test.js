import test from "node:test";
import assert from "node:assert/strict";
import { validateConfig } from "../src/config.js";
import { runOnce } from "../src/jobs/realtime-dispatch/index.js";

const ok = {
  jobRealtimeDispatch: true,
  databaseUrl: "postgresql://u@db.invalid/seefixdb",
  pusherAppId: "1", pusherKey: "k", pusherSecret: "s", pusherCluster: "ap1",
  outboxLeaseSeconds: 60, outboxBatchSize: 10, outboxPublishTimeoutMs: 5000,
};

test("validateConfig accepts a complete realtime-dispatch config", () => {
  assert.doesNotThrow(() => validateConfig(ok));
});

test("validateConfig lists every missing setting at once", () => {
  assert.throws(() => validateConfig({ ...ok, databaseUrl: "", pusherKey: "", pusherSecret: "" }), (e) =>
    /DATABASE_URL/.test(e.message) && /PUSHER_KEY/.test(e.message) && /PUSHER_SECRET/.test(e.message));
});

test("validateConfig refuses to start with no job enabled", () => {
  assert.throws(() => validateConfig({ ...ok, jobRealtimeDispatch: false }), /No job is enabled/);
});

test("validateConfig enforces lease > batch x publish timeout", () => {
  assert.throws(() => validateConfig({ ...ok, outboxLeaseSeconds: 40 }), /OUTBOX_LEASE_SECONDS/);
});

test("runOnce drains full batches until a partial one and sends each claimed row once", async () => {
  const U = "22222222-2222-4222-8222-222222222222";
  const row = (id) => ({ Id: id, RecipientUserId: U, EventName: "notification.created", Payload: {}, AttemptCount: 1 });
  const batches = [Array.from({ length: 10 }, (_, i) => row(`a${i}`)), Array.from({ length: 10 }, (_, i) => row(`b${i}`)), [row("c0")]];
  const q = async (sql) => {
    if (sql.includes("FOR UPDATE SKIP LOCKED")) {
      const rows = batches.shift() ?? [];
      return { rows, rowCount: rows.length };
    }
    return { rows: [], rowCount: sql.includes("lease lapsed") ? 0 : 1 };
  };
  const sent = [];
  const total = await runOnce({ q, send: async (channels, _event, data) => void sent.push([channels[0], data.eventId]) });
  assert.deepEqual({ passes: total.passes, claimed: total.claimed, sent: total.sent }, { passes: 3, claimed: 21, sent: 21 });
  assert.equal(new Set(sent.map(([, id]) => id)).size, 21);
  assert.ok(sent.every(([ch]) => ch === `private-user-${U}`), "routed to the recipient's private channel");
});
