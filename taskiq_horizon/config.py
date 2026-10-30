from dataclasses import dataclass, field


@dataclass
class TrimSettings:
    recent: int = 60
    pending: int = 60
    completed: int = 60
    recent_failed: int = 10080
    failed: int = 10080
    monitored: int = 10080


@dataclass
class MetricTrimSettings:
    job: int = 24
    queue: int = 24


@dataclass
class HorizonMetricsConfig:
    trim_snapshots: MetricTrimSettings = field(default_factory=MetricTrimSettings)


@dataclass
class HorizonConfig:
    """Configuration for TaskIQ Horizon."""

    # Prefix for all Redis keys
    prefix: str = "taskiq-horizon:"

    # Dashboard display name suffix (shown in title/header).
    name: str = ""

    # Dashboard route path (matches the upstream "path" config key).
    # Examples: "horizon", "ops/horizon"
    path: str = "horizon"

    # Optional external proxy path prefix used by the Vue router.
    # Example: "/admin"
    proxy_path: str = ""

    # URL to the Redis connection used by Horizon
    redis_url: str = "redis://localhost:6379/0"

    # Job trim times in minutes
    trim: TrimSettings = field(default_factory=TrimSettings)

    # Jobs/tags to silence
    silenced: list[str] = field(default_factory=list)
    silenced_tags: list[str] = field(default_factory=list)

    # Metrics
    metrics: HorizonMetricsConfig = field(default_factory=HorizonMetricsConfig)

    # Wait times thresholds in seconds
    waits: dict = field(default_factory=lambda: {"default": 60})

    # Memory limit for the master process in megabytes.
    memory_limit: int = 512

    # Environment-specific supervisor mappings (same shape as upstream).
    # e.g., environments={"production": {"supervisor-1": SupervisorOptions(...)}}
    environments: dict = field(default_factory=dict)
