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
        recent=60,            # Keep recent jobs for 1 hr
        pending=60,           # Keep pending jobs for 1 hr
        completed=60,         # Keep completed jobs for 1 hr
        recent_failed=10080,  # Keep recently failed list for 7 days
        failed=10080,         # Keep failed job details for 7 days
        monitored=10080,      # Keep monitored jobs for 7 days
    ),

    # ── Wait Time Thresholds ─────────────────────────────────────────────
    # Seconds before a LongWaitDetected event is fired for a queue.
    # Notifications/alerts can be hooked into this event.
    waits={
        "default": 60, 
        "high": 30
    },

    # ── Dashboard Silencing ──────────────────────────────────────────────
    # Tasks or tags you don't want polluting the dashboard 
    # (e.g., repeating heartbeat or ping jobs).
    silenced=["tasks.heartbeat_job"],
    silenced_tags=["System"],
    
    # ── Memory Limits ────────────────────────────────────────────────────
    # The maximum memory (in MB) a supervisor process can consume before 
    # it is gracefully terminated and automatically restarted.
    memory_limit=512,

    # ── Metrics Trimming Time-to-Live (in hours) ─────────────────────────
    # How long to keep historical snapshot data for the dashboard charts.
    metrics=HorizonMetricsConfig(
        trim_snapshots=MetricTrimSettings(
            job=24,    # Keep job metrics for 24 hours
            queue=24,  # Keep queue metrics for 24 hours
        )
    ),
)
```

## Scaling Configuration

Scaling configurations for the supervisors (like Laravel's `supervisor-1` environments) are passed directly to the `SupervisorOptions` when constructing `Supervisor` instances in your application's setup scripts, rather than through global config.

```python
from taskiq_horizon.supervisor import SupervisorOptions

options = SupervisorOptions(
    name="supervisor-1",
    broker="myapp.broker:broker",
    queue="high,default",
    min_processes=1,
    max_processes=10,        # Enable autoscaling by setting a max > min
    balance="auto",          # 'auto', 'simple', or 'false'
)
```
