r"""
RedisCommandQueue — mirrors Laravel\Horizon\RedisHorizonCommandQueue.

Uses a Redis list per supervisor to store JSON-encoded commands.
The supervisor consumes commands atomically via LRANGE + LTRIM pipeline.
r"""

import json
import logging
from typing import Any

from redis.asyncio import Redis

from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.contracts.command_queue import CommandQueue

logger = logging.getLogger("horizon.commands")


class RedisCommandQueue(CommandQueue):
    """Redis-backed command queue for supervisor remote control."""

    def __init__(self, redis: Redis, config: HorizonConfig) -> None:
        self.redis = redis
        self.prefix = config.prefix

    def _key(self, name: str) -> str:
        return f"{self.prefix}commands:{name}"

    async def push(self, name: str, command: str, options: dict[str, Any] | None = None) -> None:
        """Push a command onto the queue for the named supervisor."""
        payload = json.dumps({
            "command": command,
            "options": options or {},
        })
        await self.redis.rpush(self._key(name), payload)
        logger.info(f"Pushed command '{command}' to supervisor '{name}'")

    async def pending(self, name: str) -> list[dict[str, Any]]:
        """
        Atomically consume all pending commands.

        Uses LRANGE + LTRIM in a pipeline to read and clear in one round-trip.
        This is the same pattern used by Laravel's RedisHorizonCommandQueue.
        """
        key = self._key(name)
        length = await self.redis.llen(key)

        if length < 1:
            return []

        async with self.redis.pipeline() as pipe:
            pipe.lrange(key, 0, length - 1)
            pipe.ltrim(key, length, -1)
            results = await pipe.execute()

        commands = []
        for raw in results[0]:
            try:
                data = json.loads(raw)
                commands.append(data)
            except (json.JSONDecodeError, TypeError):
                logger.warning(f"Skipping malformed command: {raw!r}")

        return commands

    async def flush(self, name: str) -> None:
        """Delete all pending commands for the named supervisor."""
        await self.redis.delete(self._key(name))
        logger.debug(f"Flushed command queue for '{name}'")
