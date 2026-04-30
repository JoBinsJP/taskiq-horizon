from abc import ABC, abstractmethod


class TagRepository(ABC):
    @abstractmethod
    async def monitored(self, tags: list[str]) -> list[str]:
        pass

    @abstractmethod
    async def monitor(self, tag: str) -> None:
        pass

    @abstractmethod
    async def stop_monitoring(self, tag: str) -> None:
        pass

    @abstractmethod
    async def monitors(self) -> list[str]:
        pass

    @abstractmethod
    async def add(self, task_id: str, tags: list[str]) -> None:
        pass

    @abstractmethod
    async def count(self, tag: str) -> int:
        pass

    @abstractmethod
    async def forget(self, tag: str) -> None:
        pass
