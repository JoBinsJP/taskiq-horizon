from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod

class SupervisorRepository(ABC):
    @abstractmethod
    async def all(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    async def update(self, name: str, data: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def forget(self, name: str) -> None:
        pass
