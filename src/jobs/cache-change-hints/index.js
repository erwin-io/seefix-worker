/**
 * Job: cache-change-hints — tells web clients "a domain changed, check /api/sync/versions now"
 * (seefix-api docs/V2_CACHE_SYNC.md §6).
 *
 * - Source: dbo."CacheDomainVersions", bumped at COMMIT by DB triggers for every writer (API, Agent, this worker).
 *   The table is the durable change log; business transactions write no extra OutboxEvents rows.
 * - Each pass reads the versions (one tiny query) and, per role, publishes `data.changed { versions: { domain: n } }`
 *   to private-role-<ROLE> with only the domains that changed since that role's last successful hint, and only
 *   domains in DOMAINS_BY_ROLE for that role. Names and numbers only: never record data.
 * - Dedupe: a role's watermark moves only after Pusher accepted its hint, so an unchanged version is never re-sent.
 * - Pusher outage: the pass stops at the first failure and waits base x 2^(n-1) (capped) before trying again. Changes
 *   made meanwhile coalesce into one hint with the latest numbers. Clients keep polling REST regardless.
 * - Restart: watermarks live in memory, so a (re)started worker publishes the current versions once. The hint is
 *   idempotent (clients compare numbers), as are duplicates from several worker instances.
 * - Before the cache-versions migration (no table) the job stays idle and logs that once.
 */
import { config } from "../../config.js";
import { query } from "../../database.js";
import { publish as pusherPublish } from "../../pusher.js";
import { repeat } from "../../repeat.js";
import { DOMAINS_BY_ROLE, roleChannel } from "./domains.js";

export const name = "cache-change-hints";
export const isEnabled = () => config.jobCacheChangeHints;
export const EVENT = "data.changed";

/** { domain: version } without the '*' counter, or null when the table does not exist yet. Other errors throw. */
export async function readVersions(q) {
  try {
    const r = await q(`SELECT "Domain", "Version" FROM "dbo"."CacheDomainVersions" WHERE "Domain" <> '*'`);
    return Object.fromEntries(r.rows.map((row) => [row.Domain, Number(row.Version)]));
  } catch (error) {
    if (error?.code === "42P01") return null;
    throw error;
  }
}

/**
 * A hinter keeps the per-role watermarks between passes. pass() resolves to a summary; it throws only when the
 * database can't be read (repeat() logs that), never because Pusher is down.
 */
export function createHinter({
  q = query,
  publish = pusherPublish,
  log = console,
  backoffBaseMs = config.cacheHintsBackoffBaseMs,
  backoffMaxMs = config.cacheHintsBackoffMaxMs,
  now = Date.now,
} = {}) {
  const sent = {}; // role -> { domain: version } last accepted by Pusher
  let failures = 0;
  let retryAt = 0;
  let missing = false;

  async function pass() {
    if (now() < retryAt) return { status: "backoff", published: [] };
    const versions = await readVersions(q);
    if (!versions) {
      if (!missing) log.warn?.(`[${name}] dbo."CacheDomainVersions" does not exist yet (migration not applied); idle until it does.`);
      missing = true;
      return { status: "unavailable", published: [] };
    }
    if (missing) log.log?.(`[${name}] dbo."CacheDomainVersions" found; publishing hints.`);
    missing = false;

    const published = [];
    for (const [role, domains] of Object.entries(DOMAINS_BY_ROLE)) {
      const last = sent[role];
      const changed = {};
      for (const domain of domains) {
        const version = versions[domain];
        // Changed, normally "rose": versions are monotonic, but a restored database may move them back.
        if (Number.isSafeInteger(version) && version !== last?.[domain]) changed[domain] = version;
      }
      if (!Object.keys(changed).length) continue;
      try {
        await publish([roleChannel(role)], EVENT, { versions: changed });
      } catch (error) {
        failures += 1;
        retryAt = now() + Math.min(backoffMaxMs, backoffBaseMs * 2 ** (failures - 1));
        if ((failures & (failures - 1)) === 0) {
          log.warn?.(`[${name}] Pusher publish failed (${failures} in a row), retrying in ${retryAt - now()} ms: ${error?.message || error}`);
        }
        return { status: "pusher-error", published };
      }
      sent[role] = { ...last, ...changed };
      published.push({ role, versions: changed });
    }
    if (failures) log.log?.(`[${name}] Pusher reachable again after ${failures} failed attempt(s).`);
    failures = 0;
    retryAt = 0;
    return { status: "ok", published };
  }

  return { pass };
}

/** One pass with fresh watermarks: publishes the current versions for every role (as after a restart). */
export async function runOnce({ q = query, publish = pusherPublish, log = console } = {}) {
  const r = await createHinter({ q, publish, log }).pass();
  return { status: r.status, roles: r.published.map((p) => p.role) };
}

/** Continuous mode: one pass every CACHE_HINTS_POLL_MS. Returns stop(). Never throws into the caller. */
export function start({ log = console, q = query, publish = pusherPublish } = {}) {
  log.log?.(`[${name}] CacheDomainVersions -> ${EVENT} on private-role-<ROLE>, polling every ${config.cacheHintsPollMs} ms.`);
  const hinter = createHinter({ q, publish, log });
  return repeat(name, config.cacheHintsPollMs, () => hinter.pass(), log);
}
