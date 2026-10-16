// Fails when the copied delivery engine differs from seefix-api's current code, so a fix made in the API
// (routing rules, retries, leases) is not silently missing here. See SOURCE.md.
// Usage: npm run sync:check [-- <git ref>]   (default: main)
import { readFileSync } from "node:fs";

const ref = process.argv[2] || "main";
const FILES = [
  ["src/realtime/outbox-dispatcher.js", "src/jobs/realtime-dispatch/outbox-dispatcher.js"],
  ["src/realtime/channels.js", "src/jobs/realtime-dispatch/channels.js"],
];
const normalize = (text) => text.replace(/\r\n/g, "\n").trimEnd();

// Not whole files: only this declaration must match (the role -> cache-domain filter of /api/sync/versions).
const BLOCKS = [["src/routes/sync.js", "src/jobs/cache-change-hints/domains.js", /export const DOMAINS_BY_ROLE = \{[^}]*\};/]];

async function upstreamText(upstream) {
  const url = `https://raw.githubusercontent.com/erwin-io/seefix-api/${ref}/${upstream}`;
  const res = await fetch(url);
  if (!res.ok) {
    console.error(`Cannot fetch ${url}: HTTP ${res.status}`);
    process.exit(2);
  }
  return res.text();
}
const localText = (local) => readFileSync(new URL(`../${local}`, import.meta.url), "utf8");

let drift = 0;
for (const [upstream, local] of FILES) {
  const same = normalize(await upstreamText(upstream)) === normalize(localText(local));
  console.log(`${same ? "in sync" : "DIFFERS"}  ${local}  <->  seefix-api@${ref}:${upstream}`);
  if (!same) drift += 1;
}
for (const [upstream, local, pattern] of BLOCKS) {
  const theirs = normalize(await upstreamText(upstream)).match(pattern)?.[0];
  const ours = normalize(localText(local)).match(pattern)?.[0];
  const same = Boolean(theirs && ours && theirs === ours);
  console.log(`${same ? "in sync" : "DIFFERS"}  ${local} DOMAINS_BY_ROLE  <->  seefix-api@${ref}:${upstream}`);
  if (!same) drift += 1;
}
if (drift) {
  console.error(`\n${drift} file(s) differ. Copy the seefix-api versions over and re-run the tests.`);
  process.exit(1);
}
