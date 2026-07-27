r"""
MonitorSupervisorMemory — ports the upstream supervisor-memory monitor listener.

Checks each supervisor's memory usage on every supervisor loop. Supervisors
are asyncio tasks inside the master process (not OS processes), so a
supervisor's memory footprint is measured as the SUM of its pools' worker
process RSS. When the total exceeds the limit, SupervisorOutOfMemory is
fired and the supervisor's worker processes are restarted (the pools
terminate and respawn to their current size) — the supervisor itself keeps
running.
r"""

import logging

import psutil

from taskiq_horizon.events import SupervisorLooped, SupervisorOutOfMemory

logger = logging.getLogger("horizon.listeners")


class MonitorSupervisorMemoryListener:
    """Restart a supervisor's worker processes if their total RSS exceeds the limit."""

    def __init__(self, dispatcher, memory_limit_mb: int = 256) -> None:
        self.dispatcher = dispatcher
        self.memory_limit_mb = memory_limit_mb

    def _supervisor_memory_mb(self, supervisor) -> float:
        """Sum the RSS of every worker process across the supervisor's pools."""
        total = 0.0
        for pool in supervisor.process_pools.values():
            for worker in getattr(pool, "workers", []):
                pid = getattr(worker, "pid", None)
                if not pid:
                    continue
                try:
                    total += psutil.Process(pid).memory_info().rss / (1024 * 1024)
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
        return total

    async def handle(self, event: SupervisorLooped) -> None:
        supervisor = event.supervisor
        memory_mb = self._supervisor_memory_mb(supervisor)

        if memory_mb > self.memory_limit_mb:
            logger.warning(
                f"Supervisor '{supervisor.name}' workers using {memory_mb:.1f}MB "
                f"(limit: {self.memory_limit_mb}MB) — restarting worker processes"
            )
            await self.dispatcher.fire(
                SupervisorOutOfMemory(
                    supervisor=supervisor,
                    memory_usage_mb=memory_mb,
                )
            )
            # Restart the worker processes: each pool terminates its workers
            # and respawns to its current size (zero-downtime drain included).
            for pool in supervisor.process_pools.values():
                pool.restart()
