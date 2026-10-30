# Configuration

TaskIQ Horizon's primary configuration object is the `HorizonConfig` class. You instantiate this in your host application and inject it into the `horizon.config` container binding.

## Full Configuration Example

```python
from taskiq_horizon.config import (
    HorizonConfig,
    HorizonMetricsConfig,
    MetricTrimSettings,
    TrimSettings,
)

horizon_config = HorizonConfig(
    # The Redis key prefix to prevent collision with other apps
    prefix="myapp-horizon:",
    # The Redis connection URL
    redis_url="redis://localhost:6379/1",
    # ── Job Trimming Time-to-Live (in minutes) ───────────────────────────
    trim=TrimSettings(
        recent=60,  # Keep recent jobs for 1 hr
        pending=60,  # Keep pending jobs for 1 hr
        completed=60,  # Keep completed jobs for 1 hr
        recent_failed=10080,  # Keep recently failed list for 7 days
        failed=10080,  # Keep failed job details for 7 days
        monitored=10080,  # Keep monitored jobs for 7 days
    ),
    # ── Wait Time Thresholds ─────────────────────────────────────────────
    # Seconds before a LongWaitDetected event is fired for a queue.
    # Notifications/alerts can be hooked into this event.
    waits={"default": 60, "high": 30},
    # ── Dashboard Silencing ──────────────────────────────────────────────
    # Tasks or tags you don't want polluting the dashboard
    # (e.g., repeating heartbeat or ping jobs).
    silenced=["tasks.heartbeat_job"],
    silenced_tags=["System"],
    # ── Memory Limits ────────────────────────────────────────────────────
    # The maximum memory (in MB) a supervisor process can consume before
    # it is gracefully terminated and automatically restarted.
    memory_limit=512,
    # ── Metrics Snapshot Retention (number of snapshots) ─────────────────────────
    # How many snapshots to keep for each dashboard chart.
    metrics=HorizonMetricsConfig(
        trim_snapshots=MetricTrimSettings(
            job=24,  # Keep 24 job snapshots
            queue=24,  # Keep 24 queue snapshots
        )
    ),
)
```

## Scaling Configuration

Configure supervisors in `HorizonConfig.environments`, keyed by the active Alata environment. Each entry contains a mapping of supervisor names to `SupervisorOptions`.

```python
from taskiq_horizon.supervisor import SupervisorOptions

options = SupervisorOptions(
    name="supervisor-1",
    broker_path="bootstrap.broker:broker",
    queue="high,default,low",
    min_processes=1,
    max_processes=10,  # Global cap across all pools in this supervisor
    balance="auto",  # 'auto', 'simple', or 'false'
    auto_scaling_strategy="time",  # 'time' or 'size'
)
```

### Balancing Modes

- `balance="auto"` creates one process pool per queue and allocates workers by queue pressure. By default it uses time-to-clear, and `auto_scaling_strategy="size"` switches the ratio to job counts.
- `balance="simple"` also creates one process pool per queue, but splits workers evenly instead of using runtime pressure.
- `balance="false"` keeps a single multi-queue TaskIQ worker pool. Use this for prioritized Redis or database queues. SQS workers consume one queue each, so use `auto` or `simple` for multiple SQS queues.

### Scaling Notes

- `max_processes` is a supervisor-wide cap, not a per-queue cap.
- `min_processes` is the minimum target per queue while balancing. Set `max_processes` to at least the number of queues multiplied by `min_processes`; inconsistent limits are rejected.
- If you configure more queues than `max_processes`, some queues may temporarily sit at `0` workers until the next rebalance cycle. This is expected and mirrors Laravel Horizon's global-cap behavior.
