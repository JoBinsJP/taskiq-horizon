import json
from typing import Any

from redis.asyncio import Redis

from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.contracts.process_repository import ProcessRepository


class RedisProcessRepository(ProcessRepository):
    def __init__(self, redis: Redis, config: HorizonConfig):
        self.redis = redis
        self.config = config
        self.prefix = config.prefix

    def _key(self, name: str) -> str:
        return f"{self.prefix}{name}"

    async def all(self) -> list[dict[str, Any]]:
        # Since we use Taskiq OS worker pools, process states are transient.
        # This acts as a foundation for drawing Process Inspector metrics in the UI.
        processes_json = await self.redis.hgetall(self._key("processes"))
        processes = []
        for v in processes_json.values():
            try:
                processes.append(json.loads(v))
            except json.JSONDecodeError:
                continue
        return processes

    async def update(self, pid: int, data: dict[str, Any]) -> None:
        await self.redis.hset(self._key("processes"), str(pid), json.dumps(data))

    async def forget(self, pid: int) -> None:
        await self.redis.hdel(self._key("processes"), str(pid))
