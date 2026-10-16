/**
 * Job: realtime-dispatch — delivers dbo."OutboxEvents" (Transport='PUSHER') to Pusher private channels.
 * The delivery engine (outbox-dispatcher.js, channels.js) is a verbatim copy of seefix-api's reviewed code;
 * see SOURCE.md and `npm run sync:check`. This file only wires it to the worker's config and DB.
 */
import { config } from "../../config.js";
import { query } from "../../database.js";
import { publish } from "../../pusher.js"; // rejects on HTTP/network failure, so the row is retried
import { dispatchOnce, startDispatcher } from "./outbox-dispatcher.js";

export const name = "realtime-dispatch";
export const isEnabled = () => config.jobRealtimeDispatch;

const options = () => ({
  batchSize: config.outboxBatchSize,
  leaseSeconds: config.outboxLeaseSeconds,
  maxAttempts: config.outboxMaxAttempts,
  baseMs: config.outboxBackoffBaseMs,
  maxMs: config.outboxBackoffMaxMs,
  maxAgeMinutes: config.outboxMaxAgeMinutes,
});

/** Continuous mode: poll every OUTBOX_POLL_MS. Returns stop(). */
export function start({ log = console } = {}) {
  log.log?.(`[${name}] OutboxEvents -> Pusher, polling every ${config.outboxPollMs} ms.`);
  return startDispatcher({ q: query, publish, intervalMs: config.outboxPollMs, log, ...options() });
}

/** One drain (for `--once` / external schedulers): repeat passes while full batches are claimed. */
export async function runOnce({ q = query, send = publish, maxPasses = 10 } = {}) {
  const total = { passes: 0, claimed: 0, sent: 0, retried: 0, failed: 0, expired: 0 };
  while (total.passes < maxPasses) {
    const r = await dispatchOnce({ q, publish: send, ...options() });
    total.passes += 1;
    for (const k of ["claimed", "sent", "retried", "failed", "expired"]) total[k] += r[k] ?? 0;
    if (r.claimed < config.outboxBatchSize) break;
  }
  return total;
}
