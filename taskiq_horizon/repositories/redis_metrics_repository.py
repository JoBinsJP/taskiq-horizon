import time
import math
from typing import List, Optional, Any, Dict
from redis.asyncio import Redis

from taskiq_horizon.contracts.metrics_repository import MetricsRepository
from taskiq_horizon.config import HorizonConfig

class RedisMetricsRepository(MetricsRepository):
    def __init__(self, redis: Redis, config: HorizonConfig):
        self.redis = redis
        self.config = config
        self.prefix = config.prefix

    def _key(self, name: str) -> str:
        return f"{self.prefix}metrics:{name}"

    async def measured_jobs(self) -> List[str]:
        keys = await self.redis.hkeys(self._key('jobs'))
        return [k.decode('utf-8') for k in keys]

    async def measured_queues(self) -> List[str]:
        keys = await self.redis.hkeys(self._key('queues'))
        return [k.decode('utf-8') for k in keys]

    async def jobs_processed_per_minute(self) -> int:
        return int(await self.redis.get(self._key('jobs_processed_per_minute')) or 0)

    async def throughput(self) -> int:
        jobs = await self.measured_jobs()
        total = 0
        for job in jobs:
            total += await self.throughput_for_job(job)
        return total

    async def throughput_for_job(self, job: str) -> int:
        return int(await self.redis.hget(self._key('jobs'), job) or 0)

    async def throughput_for_queue(self, queue: str) -> int:
        return int(await self.redis.hget(self._key('queues'), queue) or 0)

    async def runtime_for_job(self, job: str) -> float:
        return float(await self.redis.hget(self._key('jobs:runtime'), job) or 0.0)

    async def runtime_for_queue(self, queue: str) -> float:
        return float(await self.redis.hget(self._key('queues:runtime'), queue) or 0.0)

    async def queue_with_maximum_runtime(self) -> str:
        queues = await self.measured_queues()
        max_runtime = 0.0
        max_queue = None
        for q in queues:
            rt = await self.runtime_for_queue(q)
            if rt > max_runtime:
                max_runtime = rt
                max_queue = q
        return max_queue or ''

    async def queue_with_maximum_throughput(self) -> str:
        queues = await self.measured_queues()
        max_tp = 0
        max_queue = None
        for q in queues:
            tp = await self.throughput_for_queue(q)
            if tp > max_tp:
                max_tp = tp
                max_queue = q
        return max_queue or ''

    async def increment_job(self, job: str, runtime: Optional[float]) -> None:
        from taskiq_horizon.lua_scripts import LuaScripts
        if runtime is None:
            runtime = 0.0
            
        script = LuaScripts.update_metrics()
        # Horizon uses str_replace(',', '.', runtime) to handle locale issues, standard python float(runtime) handles this.
        # KEYS[1] = prefix:metrics:jobs:MyTask (BUT Horizon puts it under jobs hash differently, wait!)
        # In Horizon: KEYS[1] = 'job:'.$job, KEYS[2] = 'measured_jobs'
        # Wait, my PHP logic check says:
        # $this->connection()->eval(LuaScripts::updateMetrics(), 2, 'job:'.$job, 'measured_jobs', (string)$runtime)
        key1 = self._key(f"job:{job}")
        key2 = self._key("measured_jobs")
        await self.redis.eval(script, 2, key1, key2, str(runtime))

    async def increment_queue(self, queue: str, runtime: Optional[float]) -> None:
        from taskiq_horizon.lua_scripts import LuaScripts
        if runtime is None:
            runtime = 0.0
            
        script = LuaScripts.update_metrics()
        key1 = self._key(f"queue:{queue}")
        key2 = self._key("measured_queues")
        await self.redis.eval(script, 2, key1, key2, str(runtime))

    async def snapshots_for_job(self, job: str) -> List[Any]:
        items = await self.redis.lrange(self._key(f"snapshots:job:{job}"), 0, -1)
        import json
        return [json.loads(item) for item in items]

    async def snapshots_for_queue(self, queue: str) -> List[Any]:
        items = await self.redis.lrange(self._key(f"snapshots:queue:{queue}"), 0, -1)
        import json
        return [json.loads(item) for item in items]

    async def _base_snapshot_data(self, key: str) -> Dict[str, Any]:
        async with self.redis.pipeline() as pipe:
            pipe.hmget(key, ['throughput', 'runtime'])
            pipe.delete(key)
            results = await pipe.execute()
            
        throughput, runtime = results[0]
        return {
            'throughput': int(throughput) if throughput else 0,
            'runtime': float(runtime) if runtime else 0.0
        }

    async def _store_snapshot_for_job(self, job: str) -> None:
        key = self._key(f"job:{job}")
        data = await self._base_snapshot_data(key)
        
        import json
        import time
        now = int(time.time())
        snapshot_key = self._key(f"snapshots:job:{job}")
        
        async with self.redis.pipeline() as pipe:
            pipe.zadd(snapshot_key, {
                json.dumps({
                    'throughput': data['throughput'],
                    'runtime': data['runtime'],
                    'time': now
                }): now
            })
            trim = self.config.metrics.trim_snapshots.job
            pipe.zremrangebyrank(snapshot_key, 0, -(trim + 1))
            await pipe.execute()

    async def _store_snapshot_for_queue(self, queue: str) -> None:
        key = self._key(f"queue:{queue}")
        data = await self._base_snapshot_data(key)
        
        import json
        import time
        now = int(time.time())
        snapshot_key = self._key(f"snapshots:queue:{queue}")
        
        # We don't have wait implementation here, it needs wait_time
        wait = 0 
        
        async with self.redis.pipeline() as pipe:
            pipe.zadd(snapshot_key, {
                json.dumps({
                    'throughput': data['throughput'],
                    'runtime': data['runtime'],
                    'time': now,
                    'wait': wait
                }): now
            })
            trim = self.config.metrics.trim_snapshots.queue
            pipe.zremrangebyrank(snapshot_key, 0, -(trim + 1))
            await pipe.execute()

    async def snapshot(self) -> None:
        jobs = await self.measured_jobs()
        for job in jobs:
            await self._store_snapshot_for_job(job)
            
        queues = await self.measured_queues()
        for queue in queues:
            await self._store_snapshot_for_queue(queue)

    async def acquire_wait_time_monitor_lock(self) -> bool:
        return True

    async def forget(self, key: str) -> None:
        await self.redis.delete(self._key(key))

    async def clear(self) -> None:
        pass
