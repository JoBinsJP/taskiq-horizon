from abc import ABC, abstractmethod
from typing import Any


class SupervisorRepository(ABC):
    @abstractmethod
    async def all(self) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    async def update(self, name: str, data: dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def forget(self, name: str) -> None:
        pass
