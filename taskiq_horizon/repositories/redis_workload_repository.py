"""
Redis Workload Repository — mirrors Laravel Horizon's RedisWorkloadRepository.php.

Computes the current workload of each queue by combining:
  - Queue depth (Redis LLEN)
  - Wait time estimates (from WaitTimeCalculator)
  - Process counts (aggregated across all supervisors)
  - Split queue breakdowns (for comma-separated queue definitions)
"""

from __future__ import annotations

from typing import Any

from redis.asyncio import Redis

from taskiq_horizon.contracts.workload_repository import WorkloadRepository
from taskiq_horizon.repositories.redis_master_repository import RedisSupervisorRepository
from taskiq_horizon.wait_time import WaitTimeCalculator


class RedisWorkloadRepository(WorkloadRepository):
    """
    Mirrors Laravel\\Horizon\\Repositories\\RedisWorkloadRepository.

    Injected dependencies match Laravel's constructor:
      - redis (QueueFactory equivalent — used for LLEN queue depth)
      - wait_time (WaitTimeCalculator)
      - supervisor_repo (SupervisorRepository — for process counts)
    """

    def __init__(
        self,
        redis: Redis,
        wait_time: WaitTimeCalculator,
        supervisor_repo: RedisSupervisorRepository,
    ) -> None:
        self.redis = redis
        self.wait_time = wait_time
        self.supervisor_repo = supervisor_repo

    async def get(self) -> list[dict[str, Any]]:
        """
        Get the current workload of each queue.

        Returns a list of dicts matching Laravel's shape::

            {
                "name": str,
                "length": int,
                "wait": float,
                "processes": int,
                "split_queues": None | list[{"name": str, "wait": float, "length": int}]
            }

        Mirrors RedisWorkloadRepository::get() in Laravel Horizon.
        """
        processes = await self._processes()
        wait_times = await self.wait_time.calculate()

        results = []
        for queue, wait_sec in wait_times.items():
            total_processes = processes.get(queue, 0)

            # Handle split queues (comma-separated queue names like "high,default,low")
            if "," in queue:
                sub_queues = [q.strip() for q in queue.split(",")]
                sub_lengths = {}
                for sq in sub_queues:
                    sub_lengths[sq] = await self._queue_length(sq)

                split_queues = []
                cumulative_wait = 0.0
                for sq in sub_queues:
                    cumulative_wait += await self.wait_time.calculate_for(sq)
                    split_queues.append(
                        {
                            "name": sq,
                            "length": sub_lengths[sq],
                            "wait": cumulative_wait,
                        }
                    )

                total_length = sum(sub_lengths.values())
            else:
                total_length = await self._queue_length(queue)
                split_queues = None

            results.append(
                {
                    "name": queue,
                    "length": total_length,
                    "wait": wait_sec,
                    "processes": total_processes,
                    "split_queues": split_queues,
                }
            )

        return results

    async def _processes(self) -> dict[str, int]:
        """
        Aggregate process counts across all supervisors, grouped by queue.

        Mirrors RedisWorkloadRepository::processes() in Laravel Horizon::

            collect($this->supervisors->all())
                ->pluck('processes')
                ->reduce(fn ($final, $queues) => ...)
        """
        supervisors = await self.supervisor_repo.all_supervisors()
        totals: dict[str, int] = {}

        for supervisor in supervisors:
            for queue, count in supervisor.get("processes", {}).items():
                totals[queue] = totals.get(queue, 0) + int(count)

        return totals

    async def _queue_length(self, queue: str) -> int:
        """Get the depth of a Redis list queue via LLEN."""
        try:
            return await self.redis.llen(queue)
        except Exception:
            return 0
