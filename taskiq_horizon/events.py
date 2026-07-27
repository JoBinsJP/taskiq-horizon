"""
Domain Events — ports the upstream Events/ directory.

Each event is a simple dataclass carrying context about what happened.
Events are fired by the supervisor loop and middleware, and consumed
by listeners registered via event_map.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


# ── Job lifecycle events ─────────────────────────────────────────────────────


@dataclass
class JobPushed:
    """Fired when a job is pushed onto the queue (pre_send)."""

    payload: Any  # TaskPayload
    connection: str = "redis"
    queue: str = "default"


@dataclass
class JobReserved:
    """Fired when a worker picks up a job (pre_execute)."""

    payload: Any
    connection: str = "redis"
    queue: str = "default"


@dataclass
class JobCompleted:
    """Fired when a job finishes successfully (post_execute)."""

    payload: Any
    connection: str = "redis"
    queue: str = "default"
    runtime_ms: float = 0.0


@dataclass
class JobFailed:
    """Fired when a job fails with an exception (on_error)."""

    payload: Any
    exception: Exception | None = None
    connection: str = "redis"
    queue: str = "default"


@dataclass
class JobReleased:
    """Fired when a job is released back to the queue (retry)."""

    payload: Any
    connection: str = "redis"
    queue: str = "default"


# ── Supervisor lifecycle events ──────────────────────────────────────────────


@dataclass
class SupervisorLooped:
    """Fired on every supervisor monitor loop iteration (~1s)."""

    supervisor: Any  # Supervisor instance


@dataclass
class MasterSupervisorLooped:
    """Fired on every master supervisor loop iteration (~1s)."""

    master: Any  # MasterSupervisor instance


# ── Process events ───────────────────────────────────────────────────────────


@dataclass
class WorkerProcessRestarting:
    """Fired when a worker process is being restarted."""

    supervisor: Any
    worker_pid: int = 0


@dataclass
class SupervisorProcessRestarting:
    """Fired when a supervisor process is being restarted."""

    master: Any
    supervisor_name: str = ""


# ── Alert events ─────────────────────────────────────────────────────────────


@dataclass
class LongWaitDetected:
    """Fired when a queue's wait time exceeds the configured threshold."""

    connection: str
    queue: str
    seconds: int = 0


# ── Deployment events ────────────────────────────────────────────────────────


@dataclass
class JobPending:
    """Fired when a job enters the pending state before being picked up."""

    payload: Any
    connection: str = "redis"
    queue: str = "default"


@dataclass
class JobsMigrated:
    """Fired when jobs are migrated between queues (e.g. delayed → ready)."""

    connection: str = "redis"
    queue: str = "default"
    count: int = 0


@dataclass
class MasterSupervisorDeployed:
    """Fired when a MasterSupervisor starts for the first time."""

    master: Any  # MasterSupervisor instance


@dataclass
class MasterSupervisorOutOfMemory:
    """Fired when the master supervisor process exceeds its memory limit."""

    master: Any
    memory_usage_mb: float = 0.0


@dataclass
class MasterSupervisorReviving:
    """Fired when a previously dead master supervisor is being revived."""

    master: Any
    previous_pid: int = 0


@dataclass
class SupervisorOutOfMemory:
    """Fired when a child supervisor exceeds its memory limit."""

    supervisor: Any
    memory_usage_mb: float = 0.0


@dataclass
class UnableToLaunchProcess:
    """Fired when a worker process fails to spawn."""

    supervisor: Any
    exception: Exception | None = None
