from typing import List, Dict, Any
from abc import ABC, abstractmethod

class ProcessRepository(ABC):
    @abstractmethod
    async def all(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def update(self, pid: int, data: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def forget(self, pid: int) -> None:
        pass
