from __future__ import annotations

import math
from typing import TYPE_CHECKING

from taskiq_horizon.contracts.metrics_repository import MetricsRepository
from taskiq_horizon.process_pool import ProcessPool
from taskiq_horizon.wait_time import WaitTimeCalculator

if TYPE_CHECKING:
    from taskiq_horizon.supervisor import Supervisor, SupervisorOptions


class AutoScaler:
    """
    Balance a supervisor's worker pools using Laravel Horizon's queue-based model.

    When balancing is enabled, each queue gets its own process pool and the
    autoscaler adjusts those pools independently while respecting the
    supervisor-level min / max process limits.
    """

    def __init__(self, metrics: MetricsRepository, wait_time: WaitTimeCalculator):
        self.metrics = metrics
        self.wait_time = wait_time

    async def scale(self, supervisor: Supervisor) -> None:
        pools = self._pools_by_queue(supervisor)
        if not pools:
            return

        workers = self._number_of_workers_per_queue(
            supervisor,
            await self._time_to_clear_per_queue(supervisor, pools),
        )

        for queue, worker_target in workers.items():
            self._scale_pool(supervisor, pools[queue], worker_target)

    def _pools_by_queue(self, supervisor: Supervisor) -> dict[str, ProcessPool]:
        return supervisor.process_pools

    async def _time_to_clear_per_queue(
        self,
        supervisor: Supervisor,
        pools: dict[str, ProcessPool],
    ) -> dict[str, dict[str, float]]:
        results: dict[str, dict[str, float]] = {}

        for queue in pools:
            sizes = []
            for sub_queue in [part.strip() for part in queue.split(",") if part.strip()]:
                size = max(0, await self.wait_time._get_queue_length(sub_queue))
                runtime = max(0.0, await self.metrics.runtime_for_queue(sub_queue))
                sizes.append({"size": size, "time": size * runtime})

            results[queue] = {
                "size": sum(item["size"] for item in sizes),
                "time": sum(item["time"] for item in sizes),
            }

        return results

    def _number_of_workers_per_queue(
        self,
        supervisor: Supervisor,
        queues: dict[str, dict[str, float]],
    ) -> dict[str, float]:
        max_processes = supervisor.options.max_processes
        min_processes = supervisor.options.min_processes
        queue_count = max(1, len(supervisor.process_pools))

        time_to_clear_all = sum(queue["time"] for queue in queues.values())
        total_jobs = sum(queue["size"] for queue in queues.values())

        workers: dict[str, float] = {}

        for queue, time_to_clear in queues.items():
            if not supervisor.options.balancing:
                target = min(max_processes, max(min_processes, int(time_to_clear["size"])))
            elif time_to_clear_all > 0 and supervisor.options.auto_scaling:
                if self._auto_scale_by_number_of_jobs(supervisor.options):
                    ratio = time_to_clear["size"] / total_jobs if total_jobs > 0 else 0.0
                else:
                    ratio = time_to_clear["time"] / time_to_clear_all
                target = ratio * max_processes
            elif time_to_clear_all == 0 and supervisor.options.auto_scaling:
                target = max_processes if time_to_clear["size"] else min_processes
            else:
                target = max_processes / queue_count

            workers[queue] = target

        return dict(sorted(workers.items(), key=lambda item: item[1]))

    def _scale_pool(self, supervisor: Supervisor, pool: ProcessPool, workers: float) -> None:
        supervisor.prune_terminating_processes()

        total_process_count = pool.total_process_count
        desired_process_count = math.ceil(workers)
        pool_count = max(1, len(supervisor.process_pools))

        if desired_process_count > total_process_count:
            max_up_shift = min(
                max(0, supervisor.options.max_processes - supervisor.total_process_count),
                supervisor.options.balance_max_shift,
            )

            target = min(
                total_process_count + max_up_shift,
                max(
                    supervisor.options.min_processes,
                    supervisor.options.max_processes - ((pool_count - 1) * supervisor.options.min_processes),
                ),
                desired_process_count,
            )

            if target != total_process_count:
                pool.scale(target)
            return

        if desired_process_count < total_process_count:
            max_down_shift = min(
                max(0, supervisor.total_process_count - supervisor.options.min_processes),
                supervisor.options.balance_max_shift,
            )

            target = max(
                total_process_count - max_down_shift,
                supervisor.options.min_processes,
                desired_process_count,
            )

            if target != total_process_count:
                pool.scale(target)

    def _auto_scale_by_number_of_jobs(self, options: SupervisorOptions) -> bool:
        return options.auto_scale_by_num_jobs or options.auto_scaling_strategy == "size"
