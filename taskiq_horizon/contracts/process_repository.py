from abc import ABC, abstractmethod
from typing import Any


class ProcessRepository(ABC):
    @abstractmethod
    async def all(self) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    async def update(self, pid: int, data: dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def forget(self, pid: int) -> None:
        pass
