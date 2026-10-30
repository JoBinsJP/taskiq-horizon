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
    Balance a supervisor's worker pools using the upstream queue-based model.

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

        # Release surplus capacity first. Draining processes still count
        # against the global cap until they exit.
        supervisor.prune_terminating_processes()
        ordered = sorted(workers, key=lambda queue: workers[queue] - pools[queue].active_process_count)
        for queue in ordered:
            worker_target = workers[queue]
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
        maximum = supervisor.options.max_processes
        minimum = supervisor.options.min_processes
        if not supervisor.options.balancing:
            return {queue: min(maximum, max(minimum, int(info["size"]))) for queue, info in queues.items()}
        if not supervisor.options.auto_scaling:
            weights = dict.fromkeys(queues, 1.0)
        elif self._auto_scale_by_number_of_jobs(supervisor.options):
            weights = {queue: info["size"] for queue, info in queues.items()}
        else:
            # A new queue has no runtime history yet. Give it a provisional
            # runtime so it can earn capacity, including with min_processes=0.
            known = [info["time"] / info["size"] for info in queues.values() if info["time"] and info["size"]]
            runtime = sum(known) / len(known) if known else 1.0
            weights = {queue: info["time"] or info["size"] * runtime for queue, info in queues.items()}
        if not sum(weights.values()):
            return dict.fromkeys(queues, minimum)

        # Constrained proportional allocation: fix small shares at the
        # per-queue minimum, then redistribute the remaining capacity.
        shares: dict[str, float] = {}
        remaining = dict(weights)
        capacity = maximum
        while remaining:
            total = sum(remaining.values())
            proposed = {
                queue: capacity * weight / total if total else capacity / len(remaining)
                for queue, weight in remaining.items()
            }
            below_minimum = [queue for queue, share in proposed.items() if share < minimum]
            if not below_minimum:
                shares.update(proposed)
                break
            for queue in below_minimum:
                shares[queue] = minimum
                capacity -= minimum
                del remaining[queue]

        # Largest remainders make integer targets add up to exactly the cap;
        # rounding every queue up causes persistent starvation of later pools.
        targets = {queue: math.floor(share) for queue, share in shares.items()}
        for queue in sorted(shares, key=lambda q: shares[q] - targets[q], reverse=True)[
            : maximum - sum(targets.values())
        ]:
            targets[queue] += 1
        return targets

    def _scale_pool(self, supervisor: Supervisor, pool: ProcessPool, workers: float) -> None:
        current = pool.active_process_count
        desired = int(workers)
        shift = supervisor.options.balance_max_shift
        if desired > current:
            available = max(0, supervisor.options.max_processes - supervisor.total_process_count)
            target = current + min(desired - current, shift, available)
        else:
            target = max(desired, current - shift)
        if target != current:
            pool.scale(target)

    def _auto_scale_by_number_of_jobs(self, options: SupervisorOptions) -> bool:
        return options.auto_scale_by_num_jobs or options.auto_scaling_strategy == "size"
