/**
 * Job: agent-retry — re-queues FAILED Agent work so transient problems (Agent/Ollama down, timeouts, image
 * download errors) recover without a person. The Agent itself claims PENDING rows from the database; this job
 * never calls the Agent.
 *
 * - Reports: AgentStatus FAILED -> PENDING, only while the report is still SUBMITTED (not cancelled/closed).
 * - Work orders: CompletionAgentStatus FAILED -> PENDING, only while still COMPLETION_SUBMITTED (not yet decided).
 * - Backoff: waits AGENT_RETRY_BASE_MINUTES x 2^(attempts-1) after the failure (2, 4, 8 … minutes by default).
 * - Gives up when the attempt count (incremented by the Agent on each claim) reaches AGENT_RETRY_MAX_ATTEMPTS;
 *   the row stays FAILED for a human to review.
 * Mirrors dbo."RequeueStaleAgentReports": claim fields cleared, a note appended to the last error (never erased).
 * Each UPDATE re-checks its WHERE under the row lock, so concurrent workers can't double-requeue a row.
 */
import { config } from "../../config.js";
import { query } from "../../database.js";
import { repeat } from "../../repeat.js";

export const name = "agent-retry";
export const isEnabled = () => config.jobAgentRetry;

const backoff = (attempts) => `make_interval(mins => ($2 * power(2, GREATEST(${attempts}, 1) - 1))::int)`;

export const REPORTS_SQL = `
UPDATE "dbo"."Reports"
   SET "AgentStatus"='PENDING', "AgentClaimedAt"=NULL, "AgentClaimedBy"=NULL, "AgentStartedAt"=NULL,
       "AgentLastError"=COALESCE("AgentLastError" || E'\\n', '') ||
         'Automatically requeued by seefix-worker after failed attempt ' || "AgentAttemptCount" || '.'
 WHERE "Status"='SUBMITTED' AND "AgentStatus"='FAILED'
   AND "AgentAttemptCount" < $1
   AND COALESCE("AgentCompletedAt", "UpdatedAt") <= NOW() - ${backoff('"AgentAttemptCount"')}
RETURNING "ReportNo" AS no, "AgentAttemptCount" AS attempts`;

export const COMPLETIONS_SQL = `
UPDATE "dbo"."WorkOrders"
   SET "CompletionAgentStatus"='PENDING', "CompletionAgentClaimedAt"=NULL, "CompletionAgentClaimedBy"=NULL,
       "CompletionAgentStartedAt"=NULL,
       "CompletionAgentLastError"=COALESCE("CompletionAgentLastError" || E'\\n', '') ||
         'Automatically requeued by seefix-worker after failed attempt ' || "CompletionAgentAttemptCount" || '.'
 WHERE "Status"='COMPLETION_SUBMITTED' AND "CompletionAgentStatus"='FAILED'
   AND "CompletionAgentAttemptCount" < $1
   AND COALESCE("CompletionAgentCompletedAt", "UpdatedAt") <= NOW() - ${backoff('"CompletionAgentAttemptCount"')}
RETURNING "WorkOrderNo" AS no, "CompletionAgentAttemptCount" AS attempts`;

/** One pass over both queues. Returns what was re-queued. */
export async function runOnce({ q = query, log = console } = {}) {
  const params = [config.agentRetryMaxAttempts, config.agentRetryBaseMinutes];
  const reports = (await q(REPORTS_SQL, params)).rows;
  const completions = (await q(COMPLETIONS_SQL, params)).rows;
  for (const r of reports) log.log?.(`[${name}] Re-queued report ${r.no} (after attempt ${r.attempts}).`);
  for (const w of completions) log.log?.(`[${name}] Re-queued completion check ${w.no} (after attempt ${w.attempts}).`);
  return { reports: reports.length, completions: completions.length };
}

/** Continuous mode: one pass every AGENT_RETRY_INTERVAL_MS. Returns stop(). Never throws into the caller. */
export function start({ log = console, q = query } = {}) {
  log.log?.(`[${name}] Checking every ${config.agentRetryIntervalMs / 1000} s; up to ${config.agentRetryMaxAttempts} attempts, backoff from ${config.agentRetryBaseMinutes} min.`);
  return repeat(name, config.agentRetryIntervalMs, () => runOnce({ q, log }), log);
}
