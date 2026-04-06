import json
import time
from typing import List, Dict, Any, Optional
from redis.asyncio import Redis

class RedisMasterSupervisorRepository:
    def __init__(self, redis: Redis, prefix: str = "taskiq-horizon:"):
        self.redis = redis
        self.prefix = prefix

    def _key(self, key: str) -> str:
        return f"{self.prefix}{key}"

    async def get_master_names(self) -> List[str]:
        threshold = time.time() - 14
        masters = await self.redis.zrevrangebyscore(self._key('masters'), '+inf', threshold)
        return [m.decode('utf-8') for m in masters]

    async def get_masters(self, names: List[str]) -> List[Dict[str, Any]]:
        if not names:
            return []
            
        pipe = self.redis.pipeline()
        for name in names:
            pipe.hmget(self._key(f'master:{name}'), ['name', 'pid', 'status', 'supervisors', 'environment'])
        results = await pipe.execute()
        
        masters = []
        for raw in results:
            if not raw or not raw[0]:
                continue
            masters.append({
                'name': raw[0].decode('utf-8') if isinstance(raw[0], bytes) else raw[0],
                'pid': raw[1].decode('utf-8') if isinstance(raw[1], bytes) else raw[1],
                'status': raw[2].decode('utf-8') if isinstance(raw[2], bytes) else raw[2],
                'supervisors': json.loads(raw[3]) if raw[3] else [],
                'environment': raw[4].decode('utf-8') if raw[4] and isinstance(raw[4], bytes) else raw[4],
            })
        return masters

    async def all_masters(self) -> List[Dict[str, Any]]:
        names = await self.get_master_names()
        return await self.get_masters(names)

    async def update_master(self, name: str, pid: int, status: str, supervisors: List[str], environment: str) -> None:
        pipe = self.redis.pipeline()
        pipe.hset(self._key(f'master:{name}'), mapping={
            'name': name,
            'pid': str(pid),
            'status': status,
            'supervisors': json.dumps(supervisors),
            'environment': environment
        })
        pipe.zadd(self._key('masters'), {name: time.time()})
        pipe.expire(self._key(f'master:{name}'), 15)
        await pipe.execute()

    async def flush_expired(self) -> None:
        """Remove master supervisors that haven't heartbeated within 14 seconds."""
        threshold = time.time() - 14
        await self.redis.zremrangebyscore(self._key('masters'), '-inf', threshold)


class RedisSupervisorRepository:
    def __init__(self, redis: Redis, prefix: str = "taskiq-horizon:"):
        self.redis = redis
        self.prefix = prefix

    def _key(self, key: str) -> str:
        return f"{self.prefix}{key}"

    async def get_supervisor_names(self) -> List[str]:
        threshold = time.time() - 29
        supervisors = await self.redis.zrevrangebyscore(self._key('supervisors'), '+inf', threshold)
        return [s.decode('utf-8') for s in supervisors]

    async def get_supervisors(self, names: List[str]) -> List[Dict[str, Any]]:
        if not names:
            return []
            
        pipe = self.redis.pipeline()
        for name in names:
            pipe.hmget(self._key(f'supervisor:{name}'), ['name', 'master', 'pid', 'status', 'processes', 'options'])
        results = await pipe.execute()
        
        supervisors = []
        for raw in results:
            if not raw or not raw[0]:
                continue
            supervisors.append({
                'name': raw[0].decode('utf-8') if isinstance(raw[0], bytes) else raw[0],
                'master': raw[1].decode('utf-8') if isinstance(raw[1], bytes) else raw[1],
                'pid': raw[2].decode('utf-8') if isinstance(raw[2], bytes) else raw[2],
                'status': raw[3].decode('utf-8') if isinstance(raw[3], bytes) else raw[3],
                'processes': json.loads(raw[4]) if raw[4] else {},
                'options': json.loads(raw[5]) if raw[5] else {},
            })
        return supervisors

    async def all_supervisors(self) -> List[Dict[str, Any]]:
        names = await self.get_supervisor_names()
        return await self.get_supervisors(names)

    async def update_supervisor(self, name: str, master: str, pid: int, status: str, processes: Dict[str, int], options: Dict[str, Any]) -> None:
        pipe = self.redis.pipeline()
        pipe.hset(self._key(f'supervisor:{name}'), mapping={
            'name': name,
            'master': master,
            'pid': str(pid),
            'status': status,
            'processes': json.dumps(processes),
            'options': json.dumps(options)
        })
        pipe.zadd(self._key('supervisors'), {name: time.time()})
        pipe.expire(self._key(f'supervisor:{name}'), 30)
        await pipe.execute()

    async def flush_expired(self) -> None:
        """Remove supervisors that haven't heartbeated within 29 seconds."""
        threshold = time.time() - 29
        await self.redis.zremrangebyscore(self._key('supervisors'), '-inf', threshold)
