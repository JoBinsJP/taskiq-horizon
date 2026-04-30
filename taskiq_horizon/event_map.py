"""
EventMap — declarative event-to-listener mapping.

Mirrors Laravel Horizon's EventMap trait. Call `register_default_listeners()`
to wire all standard Horizon listeners into an EventDispatcher instance.

This is called during the supervisor boot phase, NOT by the host app.
The host app only needs to create the repositories and pass them in.
"""

import logging

from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.event_dispatcher import EventDispatcher
from taskiq_horizon.events import (
    LongWaitDetected,
    MasterSupervisorLooped,
    SupervisorLooped,
)
from taskiq_horizon.listeners.expire_supervisors import ExpireSupervisorsListener
from taskiq_horizon.listeners.monitor_master_memory import MonitorMasterSupervisorMemoryListener
from taskiq_horizon.listeners.monitor_supervisor_memory import MonitorSupervisorMemoryListener
from taskiq_horizon.listeners.monitor_wait_times import MonitorWaitTimesListener
from taskiq_horizon.listeners.send_notification import SendNotificationListener
from taskiq_horizon.listeners.trim_failed_jobs import TrimFailedJobsListener
from taskiq_horizon.listeners.trim_monitored_jobs import TrimMonitoredJobsListener
from taskiq_horizon.listeners.trim_recent_jobs import TrimRecentJobsListener

logger = logging.getLogger("horizon.events")


def register_default_listeners(
    dispatcher: EventDispatcher,
    *,
    job_repo=None,
    master_repo=None,
    supervisor_repo=None,
    wait_time_calculator=None,
    config: HorizonConfig | None = None,
) -> None:
    """
    Register all default Horizon listeners.

    Mirrors the $events array in Laravel's EventMap trait:

        MasterSupervisorLooped => [TrimRecentJobs, TrimFailedJobs,
                                   TrimMonitoredJobs, ExpireSupervisors,
                                   MonitorMasterSupervisorMemory]
        SupervisorLooped       => [MonitorWaitTimes, MonitorSupervisorMemory]
        LongWaitDetected       => [SendNotification]
    """

    # ── MasterSupervisorLooped listeners ─────────────────────────────────
    if job_repo:
        dispatcher.listen(
            MasterSupervisorLooped,
            TrimRecentJobsListener(job_repo).handle,
        )
        dispatcher.listen(
            MasterSupervisorLooped,
            TrimFailedJobsListener(job_repo).handle,
        )
        dispatcher.listen(
            MasterSupervisorLooped,
            TrimMonitoredJobsListener(job_repo).handle,
        )

    if master_repo and supervisor_repo:
        dispatcher.listen(
            MasterSupervisorLooped,
            ExpireSupervisorsListener(master_repo, supervisor_repo).handle,
        )

    # Monitor master supervisor memory usage
    memory_limit = config.memory_limit if config else 512
    dispatcher.listen(
        MasterSupervisorLooped,
        MonitorMasterSupervisorMemoryListener(dispatcher, memory_limit_mb=memory_limit).handle,
    )

    # ── SupervisorLooped listeners ───────────────────────────────────────
    if wait_time_calculator and config:
        dispatcher.listen(
            SupervisorLooped,
            MonitorWaitTimesListener(wait_time_calculator, dispatcher, config).handle,
        )

    # Monitor child supervisor memory usage
    dispatcher.listen(
        SupervisorLooped,
        MonitorSupervisorMemoryListener(dispatcher, memory_limit_mb=memory_limit).handle,
    )

    # ── LongWaitDetected listeners ───────────────────────────────────────
    dispatcher.listen(
        LongWaitDetected,
        SendNotificationListener().handle,
    )

    logger.info("Horizon default event listeners registered.")
