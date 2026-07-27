from collections.abc import Callable
from typing import Any

from redis.asyncio import Redis


class Lock:
    """
    Port of Laravel Horizon's Lock.php for distributed locking via setnx/expire.
    """

    def __init__(self, redis: Redis):
        self.redis = redis

    async def execute_with_lock(self, key: str, callback: Callable, seconds: int = 60) -> Any:
        if await self.get(key, seconds):
            try:
                return await callback()
            finally:
                await self.release(key)
        return None

    async def exists(self, key: str) -> bool:
        return await self.redis.exists(key) == 1

    async def get(self, key: str, seconds: int = 60) -> bool:
        result = await self.redis.set(key, 1, ex=seconds, nx=True)
        return bool(result)

    async def release(self, key: str) -> None:
        await self.redis.delete(key)
