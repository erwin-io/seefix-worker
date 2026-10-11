import "dotenv/config";

function intEnv(name, fallback, min = 0) {
  const raw = process.env[name];
  const value = raw == null || raw === "" ? fallback : Number.parseInt(raw, 10);
  if (!Number.isFinite(value) || value < min) throw new Error(`${name} must be an integer >= ${min}.`);
  return value;
}
const onOff = (name, fallback = "off") => String(process.env[name] || fallback).trim().toLowerCase() === "on";

// Names and defaults match seefix-api, so the same values can be reused.
export const config = Object.freeze({
  databaseUrl: String(process.env.DATABASE_URL || "").trim(),
  databaseSslMode: String(process.env.DATABASE_SSLMODE || "prefer").trim().toLowerCase(),
  databaseSslRootCert: String(process.env.DATABASE_SSLROOTCERT || "").trim(),
  databaseConnectTimeoutMs: intEnv("DATABASE_CONNECT_TIMEOUT_MS", 10000, 1000),
  databasePoolMax: intEnv("DATABASE_POOL_MAX", 2, 1),

  pusherAppId: String(process.env.PUSHER_APP_ID || "").trim(),
  pusherKey: String(process.env.PUSHER_KEY || "").trim(),
  pusherSecret: String(process.env.PUSHER_SECRET || ""),
  pusherCluster: String(process.env.PUSHER_CLUSTER || "").trim(),

  // Jobs: each is opt-in, so one machine can run only the jobs it should.
  jobRealtimeDispatch: onOff("JOB_REALTIME_DISPATCH"),

  outboxPollMs: intEnv("OUTBOX_POLL_MS", 2000, 200),
  outboxBatchSize: intEnv("OUTBOX_BATCH_SIZE", 10, 1),
  outboxMaxAttempts: intEnv("OUTBOX_MAX_ATTEMPTS", 8, 1),
  outboxLeaseSeconds: intEnv("OUTBOX_LEASE_SECONDS", 60, 5),
  outboxBackoffBaseMs: intEnv("OUTBOX_BACKOFF_BASE_MS", 2000, 100),
  outboxBackoffMaxMs: intEnv("OUTBOX_BACKOFF_MAX_MS", 600000, 1000),
  outboxPublishTimeoutMs: intEnv("OUTBOX_PUBLISH_TIMEOUT_MS", 5000, 500),
  outboxMaxAgeMinutes: intEnv("OUTBOX_MAX_AGE_MINUTES", 60, 1),
});

/** Fails fast with every problem at once; only checks what enabled jobs need. */
export function validateConfig(c = config) {
  const errors = [];
  if (!c.jobRealtimeDispatch) errors.push("No job is enabled. Set JOB_REALTIME_DISPATCH=on.");
  if (c.jobRealtimeDispatch) {
    if (!c.databaseUrl) errors.push("DATABASE_URL is required (the same database seefix-api and seefix-agents use).");
    for (const [name, value] of [["PUSHER_APP_ID", c.pusherAppId], ["PUSHER_KEY", c.pusherKey], ["PUSHER_SECRET", c.pusherSecret], ["PUSHER_CLUSTER", c.pusherCluster]]) {
      if (!value) errors.push(`${name} is required for JOB_REALTIME_DISPATCH.`);
    }
    // A batch is published sequentially under one lease; the lease must outlive the worst-case batch.
    if (c.outboxLeaseSeconds * 1000 <= c.outboxBatchSize * c.outboxPublishTimeoutMs) {
      errors.push("OUTBOX_LEASE_SECONDS must exceed OUTBOX_BATCH_SIZE x OUTBOX_PUBLISH_TIMEOUT_MS.");
    }
  }
  if (errors.length) throw new Error(errors.join("\n"));
}
