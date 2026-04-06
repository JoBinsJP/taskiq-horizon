import math
from typing import Dict, List
import logging
from taskiq_horizon.contracts.metrics_repository import MetricsRepository
from taskiq_horizon.wait_time import WaitTimeCalculator

logger = logging.getLogger("horizon.autoscaler")

class AutoScaler:
    def __init__(self, metrics: MetricsRepository, wait_time: WaitTimeCalculator):
        self.metrics = metrics
        self.wait_time = wait_time

    async def scale(self, supervisor: 'Supervisor') -> None:
        """
        Scales the worker processes managed by the supervisor.
        """
        queues = supervisor.queues
        if not queues:
            return

        pools = {q: q for q in queues} # Simplification: 1 pool per queue
        time_to_clear = await self._time_to_clear_per_queue(pools)
        workers_needed = self._number_of_workers_per_queue(supervisor, time_to_clear)

        for queue, target_workers in workers_needed.items():
            await self._scale_pool(supervisor, queue, target_workers)

    async def _time_to_clear_per_queue(self, pools: Dict[str, str]) -> Dict[str, Dict[str, float]]:
        results = {}
        for queue_name in pools.keys():
            size = await self.wait_time._get_queue_length(queue_name)
            time_val = size * await self.metrics.runtime_for_queue(queue_name)
            results[queue_name] = {
                'size': max(0, size),
                'time': max(0, time_val)
            }
        return results

    def _number_of_workers_per_queue(self, supervisor: 'Supervisor', queues_time: Dict[str, Dict[str, float]]) -> Dict[str, int]:
        total_time = sum(q['time'] for q in queues_time.values())
        total_size = sum(q['size'] for q in queues_time.values())
        
        max_processes = supervisor.max_processes
        min_processes = 1
        balance_strategy = getattr(supervisor, 'balance_strategy', 'auto')
        
        needed = {}
        
        for queue, metrics in queues_time.items():
            if balance_strategy != 'auto':
                # Simple balancing
                target = min(max_processes, max(min_processes, metrics['size']))
                needed[queue] = target
                continue

            # Auto Scaling
            if total_time > 0 and not getattr(supervisor, 'auto_scale_by_num_jobs', False):
                fraction = metrics['time'] / total_time
                target = fraction * max_processes
                needed[queue] = max(min_processes, target)
            elif total_size > 0 and getattr(supervisor, 'auto_scale_by_num_jobs', False):
                fraction = metrics['size'] / total_size
                target = fraction * max_processes
                needed[queue] = max(min_processes, target)
            elif total_time == 0 and total_size == 0:
                needed[queue] = max_processes / len(queues_time)
            else:
                needed[queue] = max_processes if metrics['size'] > 0 else min_processes

        # Map to integers
        return {q: int(math.ceil(val)) for q, val in needed.items()}

    async def _scale_pool(self, supervisor: 'Supervisor', queue: str, target_workers: int) -> None:
        current_workers = len(supervisor.workers.get(queue, []))
        total_supervisor_workers = sum(len(pool) for pool in supervisor.workers.values())
        
        balance_max_shift = getattr(supervisor, 'balance_max_shift', 1)
        min_processes = getattr(supervisor, 'min_processes', 1)
        
        if target_workers > current_workers:
            # Shift up gradually, but tightly constrain so we NEVER breach the global supervisor max_processes
            # even if float ceiling math produced an overrun target.
            max_up_shift = min(
                max(0, supervisor.max_processes - total_supervisor_workers),
                balance_max_shift
            )
            desired = min(
                current_workers + max_up_shift, target_workers, supervisor.max_processes
            )
            
            if desired > current_workers:
                logger.info(f"AutoScaler scaling UP '{queue}' to {desired} (target {target_workers})")
                await supervisor.scale_queue(queue, desired)
            
        elif target_workers < current_workers:
            # Shift down gradually
            max_down_shift = min(
                max(0, total_supervisor_workers - min_processes),
                balance_max_shift
            )
            desired = max(
                current_workers - max_down_shift, target_workers, min_processes
            )
            
            if desired < current_workers:
                logger.info(f"AutoScaler scaling DOWN '{queue}' to {desired} (target {target_workers})")
                await supervisor.scale_queue(queue, desired)
