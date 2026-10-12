/**
 * seefix-worker: runs the enabled background jobs.
 *   npm start          continuous (as a service): each enabled job runs until stopped
 *   npm run once       one pass of each enabled job, then exit (for an external scheduler / quick checks)
 */
import { config, validateConfig } from "./config.js";
import { pool } from "./database.js";
import * as agentRetry from "./jobs/agent-retry/index.js";
import * as realtimeDispatch from "./jobs/realtime-dispatch/index.js";
import * as uploadCleanup from "./jobs/upload-cleanup/index.js";

// Add new jobs here; each exports name, isEnabled(), start() -> stop, runOnce() -> summary.
const JOBS = [realtimeDispatch, agentRetry, uploadCleanup];

async function main() {
  validateConfig(config);
  const enabled = JOBS.filter((job) => job.isEnabled());
  console.log(`[seefix-worker] Jobs: ${enabled.map((j) => j.name).join(", ")}`);

  if (process.argv.includes("--once")) {
    for (const job of enabled) console.log(`[${job.name}]`, JSON.stringify(await job.runOnce()));
    await pool.end();
    return;
  }

  const stops = enabled.map((job) => job.start());
  const shutdown = async (signal) => {
    console.log(`[seefix-worker] ${signal}: stopping.`);
    stops.forEach((stop) => stop());
    await pool.end().catch(() => {});
    process.exit(0);
  };
  for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, () => void shutdown(signal));
}

main().catch(async (error) => {
  console.error(`[seefix-worker] ${error.message}`);
  await pool.end().catch(() => {});
  process.exit(1);
});
