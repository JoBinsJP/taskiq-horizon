from abc import ABC, abstractmethod
from typing import List, Dict, Any

class WorkloadRepository(ABC):

    @abstractmethod
    async def get(self) -> List[Dict[str, Any]]:
        """
        Get the current workload of each queue.
        Returns a list of dicts with shape:
        {
            "name": str,
            "length": int,
            "wait": float,
            "processes": int,
            "split_queues": Optional[List[Dict]]
        }
        """
        pass
