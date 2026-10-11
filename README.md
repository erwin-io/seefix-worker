# seefix-worker

Background worker for SEEFIX. It runs **jobs** that must keep going outside a request, on any always-on machine
(a server, a VPS, or a Windows PC as a service). It needs no changes to seefix-api or seefix-agents; it talks only to
the shared PostgreSQL database and external services.

| Job | Setting | What it does |
|---|---|---|
| `realtime-dispatch` | `JOB_REALTIME_DISPATCH=on` | delivers `dbo."OutboxEvents"` rows (`Transport='PUSHER'`) to Pusher private channels, so the web admin and apps update live |
| `agent-retry` | `JOB_AGENT_RETRY=on` | puts `FAILED` Agent work (report assessment, completion check) back to `PENDING` after a backoff, up to an attempt cap, so short outages recover by themselves |

## How Agent work flows
seefix-api never calls the Agent to start an assessment. Everything goes through the database queue:
```
API saves report / completion as PENDING  ->  Agent claims PENDING work (every ~3 s) and writes COMPLETED or FAILED
                                              agent-retry (this worker) puts eligible FAILED work back to PENDING
```
- **Eligible:** the report is still `SUBMITTED` (cancelled or already reviewed reports are never touched), or the work
  order is still `COMPLETION_SUBMITTED` (never after the supervisor decided).
- **Backoff:** `AGENT_RETRY_BASE_MINUTES × 2^(attempt-1)` after the failure. The defaults (2, then 4 minutes) give 3 attempts in total.
- **Limit:** after `AGENT_RETRY_MAX_ATTEMPTS` attempts (counted by the Agent on each claim), the work stays `FAILED` for a person.
- **Audit:** each re-queue appends `Automatically requeued by seefix-worker after failed attempt N.` to the item's last error;
  the original error is kept.
- **Safe with several workers:** each update re-checks its conditions under the row lock.

## Requirements
- Node.js 20+.
- The **same database** as seefix-api and seefix-agents (e.g. Neon).
- The **same Pusher app** as seefix-api.

## Setup
```
git clone https://github.com/erwin-io/seefix-worker.git
cd seefix-worker
npm ci
copy .env.example .env      (cp on Linux/macOS)
```
Fill in `.env`: `DATABASE_URL`, `PUSHER_*` (for realtime-dispatch), and turn on the jobs this machine should run.

## Run
```
npm run once     # one pass of each enabled job, then exit (quick check)
npm start        # continuous; stop with Ctrl+C
```
Expected output:
```
[seefix-worker] Jobs: realtime-dispatch
[realtime-dispatch] OutboxEvents -> Pusher, polling every 2000 ms.
```
Missing settings stop it at startup with a list of what's wrong.

## Run as a service
**Windows (NSSM)**, as Administrator:
```powershell
nssm install SeefixWorker (Get-Command node).Source "src\index.js"
nssm set SeefixWorker AppDirectory C:\Development\seefix-worker
nssm set SeefixWorker Start SERVICE_DELAYED_AUTO_START
nssm set SeefixWorker AppExit Default Restart
nssm set SeefixWorker AppRestartDelay 5000
nssm set SeefixWorker AppStdout C:\Development\seefix-worker\logs\worker.log
nssm set SeefixWorker AppStderr C:\Development\seefix-worker\logs\worker.log
nssm set SeefixWorker AppRotateFiles 1
nssm set SeefixWorker AppRotateBytes 10485760
nssm start SeefixWorker
```
Create the `logs` folder first. Restart after editing `.env`: `nssm restart SeefixWorker`.

**Linux (systemd)**, `/etc/systemd/system/seefix-worker.service`:
```
[Unit]
Description=SEEFIX worker
After=network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=/opt/seefix-worker
ExecStart=/usr/bin/node src/index.js
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```
Then: `sudo systemctl enable --now seefix-worker`.

## Using it with seefix-api
When this worker runs `realtime-dispatch`, the API doesn't need its own dispatcher:
- seefix-api: `OUTBOX_DISPATCHER=off`, `REALTIME_CLIENTS_ENABLED=on`.
- It's safe if both run anyway: rows are claimed with `FOR UPDATE SKIP LOCKED`, so an event is never sent twice
  by concurrent dispatchers. Several worker instances are safe for the same reason.

## Monitoring
- **Logs:** each job's start line; realtime-dispatch warns when events move to `FAILED` or are skipped as too old
  (`OUTBOX_MAX_AGE_MINUTES`); agent-retry logs every re-queue (`Re-queued report RPT-… (after attempt 1)`).
- **Database:**
  ```sql
  SELECT "Status", COUNT(*) FROM dbo."OutboxEvents" WHERE "Transport"='PUSHER' GROUP BY 1;
  ```
  A growing `PENDING` count means the worker isn't running or can't reach Pusher.
  ```sql
  SELECT "ReportNo","AgentStatus","AgentAttemptCount","AgentLastError" FROM dbo."Reports"
   WHERE "AgentStatus"='FAILED' AND "Status"='SUBMITTED';   -- failed for good: needs a person
  ```

## Tests
```
npm test                                   # unit tests, no database needed
SEEFIX_INTEGRATION_DB=1 DATABASE_URL=... npm test   # also the live agent-retry test (staged in a rolled-back transaction)
```

## Adding a job
Create `src/jobs/<name>/index.js` exporting `name`, `isEnabled()`, `start()` (returns a stop function) and `runOnce()`.
Add it to `JOBS` in `src/index.js`, give it a `JOB_<NAME>=on` setting in `src/config.js`, and add tests.

## Shared code
The delivery engine is copied verbatim from seefix-api. See [SOURCE.md](SOURCE.md) and `npm run sync:check`.
