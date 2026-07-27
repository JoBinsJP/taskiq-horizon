r"""
ExpireSupervisors — ports the upstream supervisor-expiry listener.

Flushes stale master/supervisor heartbeats from Redis on every loop.
Unlike trim listeners, this runs on EVERY loop iteration (no throttle)
because stale heartbeats must be cleaned immediately.
r"""

import logging

from taskiq_horizon.events import MasterSupervisorLooped

logger = logging.getLogger("horizon.listeners")


class ExpireSupervisorsListener:
    """Flush expired supervisor heartbeats from Redis."""

    def __init__(self, master_repo, supervisor_repo) -> None:
        self.master_repo = master_repo
        self.supervisor_repo = supervisor_repo

    async def handle(self, event: MasterSupervisorLooped) -> None:
        await self.master_repo.flush_expired()
        await self.supervisor_repo.flush_expired()
