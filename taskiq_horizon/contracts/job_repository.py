from abc import ABC, abstractmethod
from typing import Any

from taskiq_horizon.payload import TaskPayload


class JobRepository(ABC):
    @abstractmethod
    async def next_job_id(self) -> str:
        pass

    @abstractmethod
    async def total_recent(self) -> int:
        pass

    @abstractmethod
    async def total_failed(self) -> int:
        pass

    @abstractmethod
    async def get_recent(self, after_index: int | None = None) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    async def get_failed(self, after_index: int | None = None) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    async def get_pending(self, after_index: int | None = None) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    async def get_completed(self, after_index: int | None = None) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    async def get_silenced(self, after_index: int | None = None) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    async def count_recent(self) -> int:
        pass

    @abstractmethod
    async def count_failed(self) -> int:
        pass

    @abstractmethod
    async def count_pending(self) -> int:
        pass

    @abstractmethod
    async def count_completed(self) -> int:
        pass

    @abstractmethod
    async def count_silenced(self) -> int:
        pass

    @abstractmethod
    async def count_recently_failed(self) -> int:
        pass

    @abstractmethod
    async def get_jobs(self, ids: list[str], index_from: int = 0) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    async def get_jobs_by_tag(self, tag: str, after_index: int | None = None) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    async def pushed(self, connection: str, queue: str, payload: TaskPayload) -> None:
        pass

    @abstractmethod
    async def reserved(self, connection: str, queue: str, payload: TaskPayload) -> None:
        pass

    @abstractmethod
    async def released(self, connection: str, queue: str, payload: TaskPayload, delay: int = 0) -> None:
        pass

    @abstractmethod
    async def remember(self, connection: str, queue: str, payload: TaskPayload) -> None:
        pass

    @abstractmethod
    async def completed(self, payload: TaskPayload, failed: bool = False, silenced: bool = False) -> None:
        pass

    @abstractmethod
    async def delete_monitored(self, ids: list[str]) -> None:
        pass

    @abstractmethod
    async def trim_recent_jobs(self) -> None:
        pass

    @abstractmethod
    async def trim_failed_jobs(self) -> None:
        pass

    @abstractmethod
    async def trim_monitored_jobs(self) -> None:
        pass

    @abstractmethod
    async def find_failed(self, task_id: str) -> dict[str, Any] | None:
        pass

    @abstractmethod
    async def failed(self, exception: Exception, connection: str, queue: str, payload: TaskPayload) -> None:
        pass

    @abstractmethod
    async def store_retry_reference(self, task_id: str, retry_id: str) -> None:
        pass

    @abstractmethod
    async def delete_failed(self, task_id: str) -> int:
        pass

    @abstractmethod
    async def purge(self, queue: str) -> int:
        pass
