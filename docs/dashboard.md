# Dashboard

TaskIQ Horizon includes a web dashboard for queue activity, worker status, and job failures.

## Accessing the Dashboard

Register `HorizonProvider` as described in [Installation](installation.md), start `./alata horizon`, and run your HTTP application in another terminal with `./alata serve`. The provider serves the dashboard at `/horizon` by default:

```
http://127.0.0.1:8000/horizon
```

## Security

Horizon does not add an access policy automatically. Protect `/horizon` and every path beneath it, including `/horizon/api/*`, with application authorization middleware or an authenticated reverse proxy. Restrict access to operators who may inspect job payloads and retry or clear jobs. If you configure a different path or proxy prefix, protect those public URLs too.

## Available Metrics

The dashboard natively reveals:

- **Dashboard / Status Overview:** Realtime throughput (jobs/min), current queue capacities, max wait times.
- **Monitoring:** Tag jobs (e.g. `User:123`) inside your tasks manually or dynamically through middleware, and monitor the subset of tasks connected to that tag.
- **Metrics:** Throughput and runtime charts per job type and queue, built from periodic snapshots.
- **Pending/Completed/Failed jobs:** Inspect detailed stack traces, exceptions, and payloads for failed jobs, and easily click to "Retry" them.

## Metrics history

Schedule `horizon:snapshot` every five minutes as described in [Installation](installation.md#run-horizon). The default retention is 24 snapshots per job and queue, covering roughly two hours at that interval. Set `metrics.trim_snapshots.job` and `.queue` to `288` for a full day of history.
