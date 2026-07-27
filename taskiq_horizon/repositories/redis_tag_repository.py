import time

from redis.asyncio import Redis

from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.contracts.tag_repository import TagRepository


class RedisTagRepository(TagRepository):
    def __init__(self, redis: Redis, config: HorizonConfig):
        self.redis = redis
        self.config = config
        self.prefix = config.prefix

    def _key(self, name: str) -> str:
        return f"{self.prefix}{name}"

    async def monitored(self, tags: list[str]) -> list[str]:
        if not tags:
            return []
        monitors = await self.monitors()
        return [tag for tag in tags if tag in monitors]

    async def monitor(self, tag: str) -> None:
        await self.redis.sadd(self._key("monitoring"), tag)

    async def stop_monitoring(self, tag: str) -> None:
        await self.redis.srem(self._key("monitoring"), tag)

    async def monitors(self) -> list[str]:
        members = await self.redis.smembers(self._key("monitoring"))
        return [m.decode("utf-8") for m in members]

    async def add(self, task_id: str, tags: list[str]) -> None:
        # Negated-timestamp scores (same convention as the job zsets) so an
        # ascending ZRANGE returns tagged jobs newest-first. Upstream stores a
        # timestamp score here too (RedisTagRepository::add()).
        score = time.time() * -1
        async with self.redis.pipeline() as pipe:
            for tag in tags:
                pipe.zadd(self._key(f"tags:{tag}"), {task_id: score})
            await pipe.execute()

    async def count(self, tag: str) -> int:
        return await self.redis.zcard(self._key(f"tags:{tag}"))

    async def forget(self, tag: str) -> None:
        await self.redis.delete(self._key(f"tags:{tag}"))
