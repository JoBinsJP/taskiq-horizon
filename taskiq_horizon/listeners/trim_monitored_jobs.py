r"""
TrimMonitoredJobs — ports the upstream monitored-jobs trim listener.

Periodically trims monitored (tagged) job entries from Redis.
r"""

import logging
import time

from taskiq_horizon.contracts.job_repository import JobRepository
from taskiq_horizon.events import MasterSupervisorLooped

logger = logging.getLogger("horizon.listeners")


class TrimMonitoredJobsListener:
    """Trims monitored jobs from Redis on each MasterSupervisorLooped event."""

    def __init__(self, jobs: JobRepository, frequency: int = 60) -> None:
        self.jobs = jobs
        self.frequency = frequency
        self.last_trimmed: float | None = None

    async def handle(self, event: MasterSupervisorLooped) -> None:
        now = time.time()

        if self.last_trimmed is None:
            self.last_trimmed = now - self.frequency - 1

        if (now - self.last_trimmed) < self.frequency:
            return

        await self.jobs.trim_monitored_jobs()
        self.last_trimmed = now
        logger.debug("Trimmed monitored jobs.")
