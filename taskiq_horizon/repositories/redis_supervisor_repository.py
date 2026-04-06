import json
import time
from typing import List, Dict, Any
from redis.asyncio import Redis

from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.contracts.supervisor_repository import SupervisorRepository

class RedisSupervisorRepository(SupervisorRepository):
    def __init__(self, redis: Redis, config: HorizonConfig):
        self.redis = redis
        self.config = config
        self.prefix = config.prefix

    def _key(self, name: str) -> str:
        return f"{self.prefix}{name}"

    async def all(self) -> List[Dict[str, Any]]:
        supervisors_json = await self.redis.hgetall(self._key('supervisors'))
        supervisors = []
        now = time.time()
        for k, v in supervisors_json.items():
            try:
                data = json.loads(v)
                # Auto-forget stale supervisors if they haven't pinged in 60s
                if now - data.get('updated_at', 0) > 60:
                    await self.forget(k.decode('utf-8') if isinstance(k, bytes) else k)
                    continue
                supervisors.append(data)
            except json.JSONDecodeError:
                continue
        return supervisors

    async def update(self, name: str, data: Dict[str, Any]) -> None:
        data['updated_at'] = time.time()
        await self.redis.hset(self._key('supervisors'), name, json.dumps(data))

    async def forget(self, name: str) -> None:
        await self.redis.hdel(self._key('supervisors'), name)
