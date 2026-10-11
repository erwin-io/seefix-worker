# Code shared with seefix-api

The realtime-dispatch delivery engine is a **verbatim copy** of seefix-api's reviewed code. Do not edit these two files here.
Change them in seefix-api first (with review), then copy them over:

| This repo | Copied from seefix-api | At |
|---|---|---|
| `src/jobs/realtime-dispatch/outbox-dispatcher.js` | `src/realtime/outbox-dispatcher.js` | `main` @ `eb1f3e1` |
| `src/jobs/realtime-dispatch/channels.js` | `src/realtime/channels.js` | `main` @ `eb1f3e1` |
| `tests/outbox-dispatcher.test.js`, `tests/channels.test.js` | `tests/outbox-dispatcher.test.js`, `tests/realtime-channels.test.js` | same (import paths adjusted) |

`channels.js` also contains `canSubscribe()`, which the worker doesn't use; it's kept so the file stays identical.

Check for drift (fails if either file differs from seefix-api `main`):
```
npm run sync:check
```
CI runs this on every push, and on a weekly schedule, so changes made in seefix-api are noticed even when this repo is quiet.
