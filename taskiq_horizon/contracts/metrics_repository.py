from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

class MetricsRepository(ABC):

    @abstractmethod
    async def measured_jobs(self) -> List[str]:
        pass

    @abstractmethod
    async def measured_queues(self) -> List[str]:
        pass

    @abstractmethod
    async def jobs_processed_per_minute(self) -> int:
        pass

    @abstractmethod
    async def throughput(self) -> int:
        pass

    @abstractmethod
    async def throughput_for_job(self, job: str) -> int:
        pass

    @abstractmethod
    async def throughput_for_queue(self, queue: str) -> int:
        pass

    @abstractmethod
    async def runtime_for_job(self, job: str) -> float:
        pass

    @abstractmethod
    async def runtime_for_queue(self, queue: str) -> float:
        pass

    @abstractmethod
    async def queue_with_maximum_runtime(self) -> str:
        pass

    @abstractmethod
    async def queue_with_maximum_throughput(self) -> str:
        pass

    @abstractmethod
    async def increment_job(self, job: str, runtime: Optional[float]) -> None:
        pass

    @abstractmethod
    async def increment_queue(self, queue: str, runtime: Optional[float]) -> None:
        pass

    @abstractmethod
    async def snapshots_for_job(self, job: str) -> List[Any]:
        pass

    @abstractmethod
    async def snapshots_for_queue(self, queue: str) -> List[Any]:
        pass

    @abstractmethod
    async def snapshot(self) -> None:
        pass

    @abstractmethod
    async def acquire_wait_time_monitor_lock(self) -> bool:
        pass

    @abstractmethod
    async def forget(self, key: str) -> None:
        pass

    @abstractmethod
    async def clear(self) -> None:
        pass
