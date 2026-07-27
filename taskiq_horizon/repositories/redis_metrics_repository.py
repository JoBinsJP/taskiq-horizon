import json
import math
import time
from typing import Any

from redis.asyncio import Redis

from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.contracts.metrics_repository import MetricsRepository
from taskiq_horizon.lock import Lock


class RedisMetricsRepository(MetricsRepository):
    def __init__(self, redis: Redis, config: HorizonConfig):
        self.redis = redis
        self.config = config
        self.prefix = config.prefix
        self.lock = Lock(redis)
        self.wait_time_calculator = None

    def _key(self, name: str) -> str:
        return f"{self.prefix}metrics:{name}"

    def _snapshot_key(self, kind: str, name: str) -> str:
        return self._key(f"snapshot:{kind}:{name}")

    def _monitor_lock_key(self) -> str:
        return f"{self.prefix}monitor:time-to-clear"

    def set_wait_time_calculator(self, calculator: Any) -> None:
        self.wait_time_calculator = calculator

    async def measured_jobs(self) -> list[str]:
        return await self._measured_members("job")

    async def measured_queues(self) -> list[str]:
        return await self._measured_members("queue")

    async def _measured_members(self, kind: str) -> list[str]:
        members = await self.redis.smembers(self._key(f"measured_{kind}s"))
        prefix = self._key(f"{kind}:")
        decoded = []

        for member in members:
            value = member.decode("utf-8") if isinstance(member, bytes) else str(member)
            if value.startswith(prefix):
                value = value[len(prefix) :]
            decoded.append(value)

        return sorted(decoded)

    async def jobs_processed_per_minute(self) -> int:
        throughput = await self.throughput()
        minutes = await self._minutes_since_last_snapshot()
        return math.ceil(throughput / minutes)

    async def _minutes_since_last_snapshot(self) -> int:
        last_snapshot = await self.redis.get(self._key("last_snapshot_at"))
        if not last_snapshot:
            return 1

        if isinstance(last_snapshot, bytes):
            last_snapshot = last_snapshot.decode("utf-8")

        elapsed = max(time.time() - float(last_snapshot), 0.0)
        return max(int(elapsed // 60), 1)

    async def throughput(self) -> int:
        queues = await self.measured_queues()
        total = 0
        for queue in queues:
            total += await self.throughput_for_queue(queue)
        return total

    async def throughput_for_job(self, job: str) -> int:
        return int(await self.redis.hget(self._key(f"job:{job}"), "throughput") or 0)

    async def throughput_for_queue(self, queue: str) -> int:
        return int(await self.redis.hget(self._key(f"queue:{queue}"), "throughput") or 0)

    async def runtime_for_job(self, job: str) -> float:
        return float(await self.redis.hget(self._key(f"job:{job}"), "runtime") or 0.0)

    async def runtime_for_queue(self, queue: str) -> float:
        return float(await self.redis.hget(self._key(f"queue:{queue}"), "runtime") or 0.0)

    async def queue_with_maximum_runtime(self) -> str:
        queues = await self.measured_queues()
        max_runtime = 0.0
        max_queue = ""

        for queue in queues:
            runtime = await self.runtime_for_queue(queue)
            if runtime > max_runtime:
                max_runtime = runtime
                max_queue = queue

        return max_queue

    async def queue_with_maximum_throughput(self) -> str:
        queues = await self.measured_queues()
        max_throughput = 0
        max_queue = ""

        for queue in queues:
            throughput = await self.throughput_for_queue(queue)
            if throughput > max_throughput:
                max_throughput = throughput
                max_queue = queue

        return max_queue

    async def increment_job(self, job: str, runtime: float | None) -> None:
        await self._increment_metric("job", job, runtime)

    async def increment_queue(self, queue: str, runtime: float | None) -> None:
        await self._increment_metric("queue", queue, runtime)

    async def _increment_metric(self, kind: str, name: str, runtime: float | None) -> None:
        from taskiq_horizon.lua_scripts import LuaScripts

        runtime = 0.0 if runtime is None else runtime
        metric_key = self._key(f"{kind}:{name}")
        measured_key = self._key(f"measured_{kind}s")

        try:
            await self.redis.eval(
                LuaScripts.update_metrics(),
                2,
                metric_key,
                measured_key,
                str(runtime),
            )
        except Exception:
            await self._increment_metric_without_lua(metric_key, measured_key, runtime)

    async def _increment_metric_without_lua(self, metric_key: str, measured_key: str, runtime: float) -> None:
        current_throughput = int(await self.redis.hget(metric_key, "throughput") or 0)
        current_runtime = float(await self.redis.hget(metric_key, "runtime") or 0.0)
        next_throughput = current_throughput + 1

        if current_throughput == 0:
            next_runtime = runtime
        else:
            next_runtime = ((current_throughput * current_runtime) + runtime) / next_throughput

        async with self.redis.pipeline() as pipe:
            pipe.sadd(measured_key, metric_key)
            pipe.hset(metric_key, mapping={"throughput": next_throughput, "runtime": next_runtime})
            await pipe.execute()

    async def snapshots_for_job(self, job: str) -> list[Any]:
        return await self._snapshots_for(self._snapshot_key("job", job))

    async def snapshots_for_queue(self, queue: str) -> list[Any]:
        return await self._snapshots_for(self._snapshot_key("queue", queue))

    async def _snapshots_for(self, key: str) -> list[Any]:
        items = await self.redis.zrange(key, 0, -1)
        snapshots = []
        for item in items:
            raw = item.decode("utf-8") if isinstance(item, bytes) else item
            snapshots.append(json.loads(raw))
        return snapshots

    async def _base_snapshot_data(self, key: str) -> dict[str, Any]:
        async with self.redis.pipeline() as pipe:
            pipe.hmget(key, ["throughput", "runtime"])
            pipe.delete(key)
            results = await pipe.execute()

        throughput, runtime = results[0]
        return {
            "throughput": int(throughput) if throughput else 0,
            "runtime": float(runtime) if runtime else 0.0,
        }

    async def _store_snapshot_for_job(self, job: str, now: int) -> None:
        data = await self._base_snapshot_data(self._key(f"job:{job}"))
        snapshot_key = self._snapshot_key("job", job)

        async with self.redis.pipeline() as pipe:
            pipe.zadd(
                snapshot_key,
                {
                    json.dumps(
                        {
                            "throughput": data["throughput"],
                            "runtime": data["runtime"],
                            "time": now,
                        }
                    ): now,
                },
            )
            pipe.zremrangebyrank(
                snapshot_key,
                0,
                -(self.config.metrics.trim_snapshots.job + 1),
            )
            await pipe.execute()

    async def _store_snapshot_for_queue(self, queue: str, now: int) -> None:
        data = await self._base_snapshot_data(self._key(f"queue:{queue}"))
        snapshot_key = self._snapshot_key("queue", queue)
        wait = 0.0

        if self.wait_time_calculator is not None:
            wait = await self.wait_time_calculator.calculate_for(queue)

        async with self.redis.pipeline() as pipe:
            pipe.zadd(
                snapshot_key,
                {
                    json.dumps(
                        {
                            "throughput": data["throughput"],
                            "runtime": data["runtime"],
                            "time": now,
                            "wait": wait,
                        }
                    ): now,
                },
            )
            pipe.zremrangebyrank(
                snapshot_key,
                0,
                -(self.config.metrics.trim_snapshots.queue + 1),
            )
            await pipe.execute()

    async def snapshot(self) -> None:
        now = int(time.time())
        jobs = await self.measured_jobs()
        queues = await self.measured_queues()

        for job in jobs:
            await self._store_snapshot_for_job(job, now)

        for queue in queues:
            await self._store_snapshot_for_queue(queue, now)

        await self.redis.set(self._key("last_snapshot_at"), now)

    async def acquire_wait_time_monitor_lock(self) -> bool:
        return await self.lock.get(self._monitor_lock_key(), seconds=60)

    async def forget(self, key: str) -> None:
        await self.redis.delete(self._key(key))

    async def clear(self) -> None:
        cursor = 0
        keys_to_delete: list[str] = []

        while True:
            cursor, keys = await self.redis.scan(cursor=cursor, match=self._key("*"), count=100)
            keys_to_delete.extend(keys)
            if cursor == 0:
                break

        if keys_to_delete:
            await self.redis.delete(*keys_to_delete)
