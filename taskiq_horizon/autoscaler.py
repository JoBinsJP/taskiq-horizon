from __future__ import annotations

import math
from typing import TYPE_CHECKING

from taskiq_horizon.contracts.metrics_repository import MetricsRepository
from taskiq_horizon.wait_time import WaitTimeCalculator

if TYPE_CHECKING:
    from taskiq_horizon.supervisor import Supervisor


class AutoScaler:
    """
    Scale a single supervisor's pool based on its configured queue set.

    Unlike Laravel Horizon, this port currently manages one process pool per
    supervisor, so the autoscaler returns a single target worker count rather
    than per-queue allocations.
    """

    def __init__(self, metrics: MetricsRepository, wait_time: WaitTimeCalculator):
        self.metrics = metrics
        self.wait_time = wait_time

    async def scale(self, supervisor: Supervisor) -> int:
        queues = self._queues_for(supervisor)
        if not queues:
            return supervisor.options.min_processes

        max_processes = supervisor.options.max_processes
        min_processes = supervisor.options.min_processes

        total_size = 0
        total_time_ms = 0.0
        for queue in queues:
            size = max(0, await self.wait_time._get_queue_length(queue))
            runtime = max(0.0, await self.metrics.runtime_for_queue(queue))
            total_size += size
            total_time_ms += size * runtime

        if total_size == 0:
            return min_processes

        if supervisor.options.balance == "simple":
            return min(max_processes, max(min_processes, total_size))

        if supervisor.options.auto_scale_by_num_jobs or total_time_ms == 0:
            target = total_size
        else:
            target = math.ceil(total_time_ms / 1000)

        return min(max_processes, max(min_processes, target))

    def _queues_for(self, supervisor: Supervisor) -> list[str]:
        return [queue.strip() for queue in supervisor.options.queue.split(",") if queue.strip()]
