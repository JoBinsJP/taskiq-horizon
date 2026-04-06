r"""
MonitorSupervisorMemory — mirrors Laravel\Horizon\Listeners\MonitorSupervisorMemory.

Checks each child supervisor's memory usage on every supervisor loop.
If the supervisor process exceeds its memory limit, fires SupervisorOutOfMemory
and terminates it for a clean restart by the master.
r"""

import logging

import psutil

from taskiq_horizon.events import SupervisorLooped, SupervisorOutOfMemory

logger = logging.getLogger("horizon.listeners")


class MonitorSupervisorMemoryListener:
    """Terminate a supervisor if it exceeds its memory limit."""

    def __init__(self, dispatcher, memory_limit_mb: int = 256) -> None:
        self.dispatcher = dispatcher
        self.memory_limit_mb = memory_limit_mb

    async def handle(self, event: SupervisorLooped) -> None:
        process = psutil.Process()
        memory_mb = process.memory_info().rss / (1024 * 1024)

        if memory_mb > self.memory_limit_mb:
            logger.warning(
                f"Supervisor '{event.supervisor.name}' using {memory_mb:.1f}MB "
                f"(limit: {self.memory_limit_mb}MB) — terminating"
            )
            await self.dispatcher.fire(
                SupervisorOutOfMemory(
                    supervisor=event.supervisor,
                    memory_usage_mb=memory_mb,
                )
            )
            event.supervisor.terminate(status=2)
