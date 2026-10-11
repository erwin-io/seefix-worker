# seefix-worker

Background worker for SEEFIX. It runs **jobs** that must keep going outside a request, on any always-on machine
(a server, a VPS, or a Windows PC as a service). It needs no changes to seefix-api or seefix-agents; it talks only to
the shared PostgreSQL database and external services.

| Job | Setting | What it does |
|---|---|---|
| `realtime-dispatch` | `JOB_REALTIME_DISPATCH=on` | delivers `dbo."OutboxEvents"` rows (`Transport='PUSHER'`) to Pusher private channels, so the web admin and apps update live |

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
Fill in `.env`: `DATABASE_URL`, `PUSHER_*`, and `JOB_REALTIME_DISPATCH=on`.

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
- **Logs:** the job start line, plus a warning when events move to `FAILED` or are skipped as too old (`OUTBOX_MAX_AGE_MINUTES`).
- **Database:**
  ```sql
  SELECT "Status", COUNT(*) FROM dbo."OutboxEvents" WHERE "Transport"='PUSHER' GROUP BY 1;
  ```
  A growing `PENDING` count means the worker isn't running or can't reach Pusher.

## Adding a job
Create `src/jobs/<name>/index.js` exporting `name`, `isEnabled()`, `start()` (returns a stop function) and `runOnce()`.
Add it to `JOBS` in `src/index.js`, give it a `JOB_<NAME>=on` setting in `src/config.js`, and add tests.

## Shared code
The delivery engine is copied verbatim from seefix-api. See [SOURCE.md](SOURCE.md) and `npm run sync:check`.
