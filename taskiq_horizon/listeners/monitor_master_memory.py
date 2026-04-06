r"""
MonitorMasterSupervisorMemory — mirrors Laravel\Horizon\Listeners\MonitorMasterSupervisorMemory.

Checks the master supervisor's memory usage on every loop iteration.
If the master exceeds its memory limit, fires MasterSupervisorOutOfMemory
to trigger a graceful shutdown and restart.
r"""

import logging

import psutil

from taskiq_horizon.events import MasterSupervisorLooped, MasterSupervisorOutOfMemory

logger = logging.getLogger("horizon.listeners")


class MonitorMasterSupervisorMemoryListener:
    """Kill the master supervisor if it exceeds its memory limit."""

    def __init__(self, dispatcher, memory_limit_mb: int = 512) -> None:
        self.dispatcher = dispatcher
        self.memory_limit_mb = memory_limit_mb

    async def handle(self, event: MasterSupervisorLooped) -> None:
        process = psutil.Process()
        memory_mb = process.memory_info().rss / (1024 * 1024)

        if memory_mb > self.memory_limit_mb:
            logger.warning(
                f"Master supervisor using {memory_mb:.1f}MB "
                f"(limit: {self.memory_limit_mb}MB) — triggering shutdown"
            )
            await self.dispatcher.fire(
                MasterSupervisorOutOfMemory(
                    master=event.master,
                    memory_usage_mb=memory_mb,
                )
            )
            # Signal the master to terminate gracefully
            event.master.should_quit = True
