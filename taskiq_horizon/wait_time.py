"""
Wait Time Calculator — mirrors Laravel Horizon's WaitTimeCalculator.php.

Calculates the estimated time to clear each queue based on:
  - Queue depth (via Redis LLEN or injected fetcher)
  - Average runtime per job (from MetricsRepository)
  - Total process count per queue (from SupervisorRepository)
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from taskiq_horizon.contracts.metrics_repository import MetricsRepository


class WaitTimeCalculator:
    """
    Mirrors Laravel\\Horizon\\WaitTimeCalculator.

    Dependencies:
      - metrics_repo: for runtime_for_queue (average ms per job)
      - queue_length_fetcher: callable(queue_name) -> int (Redis LLEN)
      - supervisor_fetcher: callable() -> list[dict] (supervisor records with processes)
    """

    def __init__(
        self,
        metrics_repo: MetricsRepository,
        queue_length_fetcher: Callable[[str], Awaitable[int]] | None = None,
        supervisor_fetcher: Callable[[], Awaitable[list[dict[str, Any]]]] | None = None,
    ) -> None:
        self.metrics_repo = metrics_repo
        self.queue_length_fetcher = queue_length_fetcher
        self.supervisor_fetcher = supervisor_fetcher

    async def _get_queue_length(self, queue: str) -> int:
        """Get queue depth via the injected fetcher."""
        if self.queue_length_fetcher:
            try:
                return await self.queue_length_fetcher(queue)
            except Exception:
                pass
        return 0

    async def _total_processes_for(self, queue: str) -> int:
        """
        Get the total process count assigned to a queue across all supervisors.

        Mirrors WaitTimeCalculator::totalProcessesFor() in Laravel Horizon.
        """
        if not self.supervisor_fetcher:
            return 0

        try:
            supervisors = await self.supervisor_fetcher()
        except Exception:
            return 0

        total = 0
        for supervisor in supervisors:
            processes = supervisor.get("processes", {})
            total += int(processes.get(queue, 0))
        return total

    async def calculate(self) -> dict[str, float]:
        """
        Calculate wait times for all measured queues in seconds.

        For each queue: wait = (queue_size * avg_runtime_ms) / total_processes / 1000

        Mirrors WaitTimeCalculator::calculate() in Laravel Horizon.
        Returns {queue_name: wait_seconds}, sorted descending.
        """
        queues = await self.metrics_repo.measured_queues()
        wait_times: dict[str, float] = {}

        for q in queues:
            wait_times[q] = await self.calculate_time_to_clear(q)

        # Sort descending (longest wait first) — matches Laravel's ->sort()->reverse()
        return dict(sorted(wait_times.items(), key=lambda x: x[1], reverse=True))

    async def calculate_for(self, queue: str) -> float:
        """
        Calculate wait time for a single queue in seconds.

        Mirrors WaitTimeCalculator::calculateFor() in Laravel Horizon.
        """
        return await self.calculate_time_to_clear(queue)

    async def calculate_time_to_clear(self, queue: str) -> float:
        """
        Calculate time to clear a queue in seconds, distributed over processes.

        Formula: (queue_size * avg_runtime_ms) / total_processes / 1000 ms

        If total_processes is 0, returns raw time without division
        (matches Laravel: $totalProcesses === 0 ? round($timeToClear / 1000)).

        Mirrors WaitTimeCalculator::calculateTimeToClear() in Laravel Horizon.
        """
        # Handle split queues (comma-separated)
        if "," in queue:
            sub_queues = [q.strip() for q in queue.split(",")]
            time_to_clear_ms = 0.0
            for sq in sub_queues:
                time_to_clear_ms += await self._time_to_clear_for(sq)
        else:
            time_to_clear_ms = await self._time_to_clear_for(queue)

        total_processes = await self._total_processes_for(queue)

        if total_processes == 0:
            return round(time_to_clear_ms / 1000, 2)

        return round((time_to_clear_ms / total_processes) / 1000, 2)

    async def _time_to_clear_for(self, queue: str) -> float:
        """
        Get total time to clear a single queue in milliseconds.

        Formula: queue_size * avg_runtime_ms

        Mirrors WaitTimeCalculator::timeToClearFor() in Laravel Horizon.
        """
        size = await self._get_queue_length(queue)
        runtime = await self.metrics_repo.runtime_for_queue(queue)
        return size * runtime
