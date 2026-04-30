"""
Workload Repository Contract — mirrors Laravel Horizon's WorkloadRepository.php.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class WorkloadRepository(ABC):
    @abstractmethod
    async def get(self) -> list[dict[str, Any]]:
        """
        Get the current workload of each queue.

        Returns a list of dicts with shape::

            {
                "name": str,           # queue name
                "length": int,         # jobs waiting in queue (LLEN)
                "wait": float,         # estimated seconds to clear
                "processes": int,      # active worker processes for this queue
                "split_queues": None | list[{
                    "name": str,
                    "wait": float,
                    "length": int,
                }]
            }
        """
        pass
