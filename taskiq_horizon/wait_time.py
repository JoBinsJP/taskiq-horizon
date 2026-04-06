from typing import Dict, Callable, Awaitable, Optional
from taskiq_horizon.contracts.metrics_repository import MetricsRepository

class WaitTimeCalculator:
    def __init__(self, metrics_repo: MetricsRepository, queue_length_fetcher: Optional[Callable[[str], Awaitable[int]]] = None):
        self.metrics_repo = metrics_repo
        self.queue_length_fetcher = queue_length_fetcher
        
    async def _get_queue_length(self, queue: str) -> int:
        if self.queue_length_fetcher:
            try:
                return await self.queue_length_fetcher(queue)
            except Exception:
                pass
        return 0

    async def calculate(self) -> Dict[str, float]:
        """
        Calculates waiting times for all measured queues.
        In Horizon this queries queue depths and divides by measured throughput.
        For Phase 1, we just provide a basic stub based on metrics.
        """
        queues = await self.metrics_repo.measured_queues()
        wait_times: Dict[str, float] = {}
        
        for q in queues:
            size = await self._get_queue_length(q)
            runtime = await self.metrics_repo.runtime_for_queue(q)
            wait_times[q] = size * runtime
            
        return wait_times

    async def calculate_for(self, queue: str) -> float:
        # Horizon uses time = (queue_size * runtime_for_queue) / totalProcesses
        size = await self._get_queue_length(queue)
        runtime = await self.metrics_repo.runtime_for_queue(queue)
        
        # Assumed 1 worker currently. MasterSupervisor/AutoScaler passes total process count internally.
        # This returns total un-divided ms
        return float(size * runtime)
