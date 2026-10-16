/**
 * Job: reminders — delivers due dbo."AssistantReminders" (seefix-api docs/V2_ASSISTANT.md §5, migration
 * 2026-10-16-v2-workflow-assistant.sql) as in-app notifications.
 *
 * - Claim: dbo."ClaimDueAssistantReminders"(worker, batch, leaseSeconds): SCHEDULED rows with NextRunAt <= now(),
 *   plus PROCESSING rows whose lease expired (a worker died); FOR UPDATE SKIP LOCKED, Attempts+1 (fencing token).
 *   Due reminders of inactive/REPORTER recipients are CANCELLED there instead.
 * - Deliver, per reminder, in ONE transaction: re-check the recipient, createNotification (+ notification.created
 *   OutboxEvents row) for RecipientUserId only, then dbo."FinishAssistantReminder"(id, attempts, true, ...).
 *   If that returns false (cancelled, or re-claimed by another worker) the transaction rolls back: nothing is sent.
 * - Failure: FinishAssistantReminder(..., false, error): back to SCHEDULED after 2^attempts minutes (max 60),
 *   FAILED after 5 attempts. The dedupe key assistant-reminder:<id> makes any replay a no-op.
 * - Time: every comparison is between UTC instants inside PostgreSQL (DueAt/NextRunAt are timestamptz); TimeZone is
 *   for display only. Nothing here depends on the worker's or the database session's time zone.
 * - Missed reminders (worker down) are all due on the next pass and are drained oldest first, in batches.
 * - Before the reminders migration the claim function doesn't exist: the job stays idle and logs that once.
 * Pusher is not needed here: realtime-dispatch delivers the outbox row, and the notification itself is durable.
 */
import os from "node:os";
import { config } from "../../config.js";
import { pool as dbPool } from "../../database.js";
import { repeat } from "../../repeat.js";
import { createNotification as writeNotification, findNotification } from "./notifications.js";

export const name = "reminders";
export const isEnabled = () => config.jobReminders;
export const NOTIFICATION_TYPE = "ASSISTANT_REMINDER";
export const MAX_ATTEMPTS = 5; // FinishAssistantReminder's cap
export const dedupeKey = (id) => `assistant-reminder:${id}`;

const workerId = () => `seefix-worker:${os.hostname()}:${process.pid}`;
// 42883 undefined_function / 42P01 undefined_table: the reminders migration isn't applied yet.
const SCHEMA_MISSING = new Set(["42883", "42P01"]);

async function inTransaction(pool, fn) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query(result.commit === false ? "ROLLBACK" : "COMMIT");
    return result.status;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

const finish = async (db, row, ok, error, notificationId) =>
  (await db.query(`SELECT "dbo"."FinishAssistantReminder"($1,$2,$3,$4,$5) AS ok`, [row.Id, row.Attempts, ok, error, notificationId])).rows[0].ok;

/** Delivers one claimed reminder. Resolves to "sent" | "fenced" | "retry" | "failed". */
export async function deliver(row, { pool = dbPool, createNotification = writeNotification, log = console } = {}) {
  try {
    return await inTransaction(pool, async (c) => {
      // The claim already cancelled inactive/REPORTER recipients; this closes the window until our commit.
      const recipient = await c.query(
        `SELECT 1 FROM "dbo"."Users" WHERE "Id"=$1 AND "IsActive" AND "Role"<>'REPORTER' FOR SHARE`,
        [row.RecipientUserId],
      );
      if (!recipient.rowCount) throw new Error("RECIPIENT_INACTIVE");
      const key = dedupeKey(row.Id);
      const created = await createNotification(c, {
        userId: row.RecipientUserId,
        type: NOTIFICATION_TYPE,
        title: row.Title,
        message: row.Message ?? row.Title,
        entityType: row.EntityType,
        entityId: row.EntityId,
        deduplicationKey: key,
      });
      const notificationId = created?.id ?? (await findNotification(c, row.RecipientUserId, key));
      if (!(await finish(c, row, true, null, notificationId))) return { status: "fenced", commit: false };
      return { status: "sent" };
    });
  } catch (error) {
    const message = String(error?.message || error).slice(0, 2000);
    if (!(await finish(pool, row, false, message, null))) return "fenced";
    const failed = row.Attempts >= MAX_ATTEMPTS;
    log.warn?.(`[${name}] Reminder ${row.Id} attempt ${row.Attempts} failed${failed ? " (giving up: FAILED)" : ", will retry"}: ${message}`);
    return failed ? "failed" : "retry";
  }
}

/**
 * A runner remembers whether the schema exists (to log its absence once). pass() claims and delivers until a batch
 * comes back short (missed-reminder catch-up), at most maxBatches batches per pass.
 */
export function createRunner({
  pool = dbPool,
  log = console,
  batchSize = config.remindersBatchSize,
  leaseSeconds = config.remindersLeaseSeconds,
  worker = workerId(),
  createNotification = writeNotification,
  maxBatches = 10,
} = {}) {
  let missing = false;

  async function claim() {
    try {
      const r = await pool.query(`SELECT * FROM "dbo"."ClaimDueAssistantReminders"($1,$2,$3)`, [worker, batchSize, leaseSeconds]);
      if (missing) log.log?.(`[${name}] dbo."AssistantReminders" found; delivering reminders.`);
      missing = false;
      return r.rows;
    } catch (error) {
      if (!SCHEMA_MISSING.has(error?.code)) throw error;
      if (!missing) log.warn?.(`[${name}] dbo."AssistantReminders" does not exist yet (migration not applied); idle until it does.`);
      missing = true;
      return null;
    }
  }

  async function pass() {
    const total = { available: true, claimed: 0, sent: 0, retry: 0, failed: 0, fenced: 0 };
    for (let batch = 0; batch < maxBatches; batch += 1) {
      const rows = await claim();
      if (!rows) return { ...total, available: false };
      total.claimed += rows.length;
      for (const row of rows) {
        const status = await deliver(row, { pool, createNotification, log });
        total[status] += 1;
        if (status === "sent") log.log?.(`[${name}] Sent reminder ${row.Id} (due ${new Date(row.DueAt).toISOString()}, attempt ${row.Attempts}).`);
      }
      if (rows.length < batchSize) break;
    }
    return total;
  }

  return { pass };
}

/** One pass (for `--once` / external schedulers). */
export async function runOnce(options = {}) {
  return createRunner(options).pass();
}

/** Continuous mode: one pass every REMINDERS_POLL_MS. Returns stop(). Never throws into the caller. */
export function start({ log = console, pool = dbPool } = {}) {
  log.log?.(`[${name}] Delivering due AssistantReminders every ${config.remindersPollMs / 1000} s (batch ${config.remindersBatchSize}, lease ${config.remindersLeaseSeconds} s).`);
  const runner = createRunner({ pool, log });
  return repeat(name, config.remindersPollMs, () => runner.pass(), log);
}
