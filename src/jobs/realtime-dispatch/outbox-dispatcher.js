/**
 * Durable OutboxEvents -> Pusher dispatcher.
 *
 * - Business transactions only INSERT outbox rows (PENDING). Nothing here runs
 *   inside a business transaction, so a Pusher outage never blocks or rolls back work.
 * - Claim: `FOR UPDATE SKIP LOCKED` + lease. A claimed row is PROCESSING with
 *   NextAttemptAt = lease expiry; if the process dies, the row is reclaimed after
 *   the lease (restart recovery). Concurrent dispatchers never claim the same row.
 * - Outcome writes are guarded by (Id, Status=PROCESSING, AttemptCount) so a stale
 *   worker whose lease expired cannot overwrite a newer attempt.
 * - Rows older than maxAgeMinutes are never claimed (stale realtime is noise; clients refetch);
 *   an expired lease on such a row is closed as FAILED "expired: ..." (expireLapsed) instead of staying PROCESSING.
 * - Failure: PENDING with exponential backoff; FAILED after maxAttempts or when the
 *   row cannot be routed to a private channel. Rows are never deleted.
 * - Delivery is at-least-once: every message carries `eventId` (the outbox Id) for client dedupe.
 */
import { resolveChannels } from "./channels.js";

export const DEFAULT_TABLE = '"dbo"."OutboxEvents"';

export function backoffMs(attempt, { baseMs, maxMs }) {
  return Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt - 1));
}

export async function claimBatch(q, { table = DEFAULT_TABLE, batchSize, leaseSeconds, maxAgeMinutes }) {
  const r = await q(
    `WITH due AS (
       SELECT "Id" FROM ${table}
        WHERE "Transport"='PUSHER'
          AND "CreatedAt">=NOW()-make_interval(mins => $3)
          AND (("Status"='PENDING' AND ("NextAttemptAt" IS NULL OR "NextAttemptAt"<=NOW()))
            OR ("Status"='PROCESSING' AND "NextAttemptAt"<=NOW()))
        ORDER BY "CreatedAt"
        LIMIT $1
        FOR UPDATE SKIP LOCKED)
     UPDATE ${table} o
        SET "Status"='PROCESSING',
            "AttemptCount"=o."AttemptCount"+1,
            "NextAttemptAt"=NOW()+make_interval(secs => $2)
       FROM due WHERE o."Id"=due."Id"
     RETURNING o.*`,
    [batchSize, leaseSeconds, maxAgeMinutes],
  );
  return r.rows;
}

async function markSent(q, table, row) {
  await q(
    `UPDATE ${table} SET "Status"='SENT',"SentAt"=NOW(),"LastError"=NULL,"NextAttemptAt"=NULL
      WHERE "Id"=$1 AND "Status"='PROCESSING' AND "AttemptCount"=$2`,
    [row.Id, row.AttemptCount],
  );
}

async function markFailure(q, table, row, error, opts, { permanent = false } = {}) {
  const giveUp = permanent || row.AttemptCount >= opts.maxAttempts;
  const message = String(error?.message || error).slice(0, 1000);
  await q(
    `UPDATE ${table}
        SET "Status"=$3::varchar,"LastError"=$4,
            "NextAttemptAt"=CASE WHEN $3::varchar='PENDING' THEN NOW()+make_interval(secs => $5::double precision) ELSE NULL END
      WHERE "Id"=$1 AND "Status"='PROCESSING' AND "AttemptCount"=$2`,
    [row.Id, row.AttemptCount, giveUp ? "FAILED" : "PENDING", message, backoffMs(row.AttemptCount, opts) / 1000],
  );
}

/**
 * A lease that lapsed after its row passed maxAgeMinutes (worker died near the cutoff) is never
 * reclaimed for delivery; close it as FAILED with an "expired:" reason so it cannot stay PROCESSING
 * forever. FAILED (not CANCELLED) because older SEEFIX schemas only allow PENDING/PROCESSING/SENT/FAILED. The WHERE
 * clause is re-checked under the row lock, so concurrent workers expire each row once, and a stale
 * worker's late SENT/FAILED write no longer matches Status='PROCESSING'.
 */
export async function expireLapsed(q, { table = DEFAULT_TABLE, maxAgeMinutes }) {
  const r = await q(
    `UPDATE ${table}
        SET "Status"='FAILED',"NextAttemptAt"=NULL,
            "LastError"='expired: lease lapsed after OUTBOX_MAX_AGE_MINUTES; not delivered'
      WHERE "Transport"='PUSHER' AND "Status"='PROCESSING' AND "NextAttemptAt"<=NOW()
        AND "CreatedAt"<NOW()-make_interval(mins => $1)`,
    [maxAgeMinutes],
  );
  return r.rowCount ?? 0;
}

/** One claim/publish pass. Returns counts for logging and tests. */
export async function dispatchOnce({ q, publish, table = DEFAULT_TABLE, ...opts }) {
  const expired = await expireLapsed(q, { table, ...opts });
  const rows = await claimBatch(q, { table, ...opts });
  const result = { claimed: rows.length, sent: 0, retried: 0, failed: 0, expired };
  for (const row of rows) {
    let channels;
    try {
      channels = await resolveChannels(row, q);
    } catch (error) {
      await markFailure(q, table, row, error, opts);
      result.retried += 1;
      continue;
    }
    if (!channels.length) {
      await markFailure(q, table, row, new Error(`No private channel for ${row.AggregateType} ${row.AggregateId}`), opts, { permanent: true });
      result.failed += 1;
      continue;
    }
    try {
      await publish(channels, row.EventName, { ...(row.Payload || {}), eventId: row.Id });
      await markSent(q, table, row);
      result.sent += 1;
    } catch (error) {
      await markFailure(q, table, row, error, opts);
      if (row.AttemptCount >= opts.maxAttempts) result.failed += 1;
      else result.retried += 1;
    }
  }
  return result;
}

/** PUSHER rows left PENDING past maxAgeMinutes (never claimed by design); surfaced for operators. */
export async function countStale(q, { table = DEFAULT_TABLE, maxAgeMinutes }) {
  const r = await q(
    `SELECT COUNT(*)::int n FROM ${table} WHERE "Transport"='PUSHER' AND "Status"='PENDING' AND "CreatedAt"<NOW()-make_interval(mins => $1)`,
    [maxAgeMinutes],
  );
  return r.rows[0].n;
}

/** Poll loop for long-lived processes. Returns stop(). Never throws into the caller. */
export function startDispatcher({ q, publish, intervalMs, log = console, ...opts }) {
  let stopped = false;
  let timer = null;
  countStale(q, opts)
    .then((n) => n && log.warn?.(`[OUTBOX] ${n} PUSHER event(s) older than ${opts.maxAgeMinutes} min stay PENDING and will not be sent (see docs/REALTIME_ROLLOUT.md).`))
    .catch(() => {});
  const tick = async () => {
    if (stopped) return;
    try {
      const r = await dispatchOnce({ q, publish, ...opts });
      // Drain quickly when a full batch was claimed; otherwise wait for the next poll.
      timer = setTimeout(tick, r.claimed >= opts.batchSize ? 0 : intervalMs);
      if (r.failed) log.warn?.(`[OUTBOX] ${r.failed} event(s) moved to FAILED.`);
      if (r.expired) log.warn?.(`[OUTBOX] ${r.expired} lapsed lease(s) past max age moved to FAILED as expired (not delivered).`);
    } catch (error) {
      log.warn?.(`[OUTBOX] dispatch pass failed: ${error?.message || error}`);
      timer = setTimeout(tick, intervalMs);
    }
  };
  timer = setTimeout(tick, 0);
  return () => {
    stopped = true;
    clearTimeout(timer);
  };
}
