from abc import ABC, abstractmethod
from typing import List

class TagRepository(ABC):

    @abstractmethod
    async def monitored(self, tags: List[str]) -> List[str]:
        pass

    @abstractmethod
    async def monitor(self, tag: str) -> None:
        pass

    @abstractmethod
    async def stop_monitoring(self, tag: str) -> None:
        pass

    @abstractmethod
    async def monitors(self) -> List[str]:
        pass

    @abstractmethod
    async def add(self, task_id: str, tags: List[str]) -> None:
        pass

    @abstractmethod
    async def count(self, tag: str) -> int:
        pass

    @abstractmethod
    async def forget(self, tag: str) -> None:
        pass
