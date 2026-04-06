r"""
HorizonCommandQueue contract — mirrors Laravel\Horizon\Contracts\HorizonCommandQueue.

Defines the interface for pushing commands to supervisors via Redis.
r"""

from abc import ABC, abstractmethod
from typing import Any


class CommandQueue(ABC):
    """
    A Redis-backed command queue for sending instructions to supervisors.

    Commands are pushed by the dashboard API or CLI, and consumed by
    the supervisor's monitor loop on each iteration.
    """

    @abstractmethod
    async def push(self, name: str, command: str, options: dict[str, Any] | None = None) -> None:
        """
        Push a command onto the queue for the named supervisor.

        Args:
            name: The supervisor name (e.g., "TaskIQ-Master-hostname-1234:default").
            command: The command name (e.g., "pause", "terminate", "scale").
            options: Optional dict of command-specific options.
        """
        ...

    @abstractmethod
    async def pending(self, name: str) -> list[dict[str, Any]]:
        """
        Get and atomically consume all pending commands for the named supervisor.

        Returns a list of dicts, each with 'command' and 'options' keys.
        Once returned, the commands are removed from the queue.
        """
        ...

    @abstractmethod
    async def flush(self, name: str) -> None:
        """
        Flush (delete) all pending commands for the named supervisor.

        Used during startup to clear stale commands from a previous run.
        """
        ...
