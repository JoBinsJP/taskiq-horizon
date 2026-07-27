import contextlib
import json
import time
import traceback
from typing import Any

from redis.asyncio import Redis

from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.contracts.job_repository import JobRepository
from taskiq_horizon.payload import TaskPayload


class RedisJobRepository(JobRepository):
    def __init__(self, redis: Redis, config: HorizonConfig):
        self.redis = redis
        self.config = config
        self.prefix = config.prefix

        self.keys = [
            "id",
            "connection",
            "queue",
            "name",
            "status",
            "payload",
            "exception",
            "context",
            "failed_at",
            "completed_at",
            "retried_by",
            "reserved_at",
            "delay",
            "created_at",
            "updated_at",
        ]

    def _key(self, name: str) -> str:
        return f"{self.prefix}{name}"

    async def next_job_id(self) -> str:
        return str(await self.redis.incr(self._key("job_id")))

    async def total_recent(self) -> int:
        return await self.redis.zcard(self._key("recent_jobs"))

    async def total_failed(self) -> int:
        return await self.redis.zcard(self._key("failed_jobs"))

    async def get_recent(self, after_index: int | None = None) -> list[dict[str, Any]]:
        return await self._get_jobs_by_type("recent_jobs", after_index)

    async def get_failed(self, after_index: int | None = None) -> list[dict[str, Any]]:
        return await self._get_jobs_by_type("failed_jobs", after_index)

    async def get_pending(self, after_index: int | None = None) -> list[dict[str, Any]]:
        return await self._get_jobs_by_type("pending_jobs", after_index)

    async def get_completed(self, after_index: int | None = None) -> list[dict[str, Any]]:
        return await self._get_jobs_by_type("completed_jobs", after_index)

    async def get_silenced(self, after_index: int | None = None) -> list[dict[str, Any]]:
        return await self._get_jobs_by_type("silenced_jobs", after_index)

    async def count_recent(self) -> int:
        return await self._count_jobs_by_type("recent_jobs")

    async def count_failed(self) -> int:
        return await self._count_jobs_by_type("failed_jobs")

    async def count_pending(self) -> int:
        return await self._count_jobs_by_type("pending_jobs")

    async def count_completed(self) -> int:
        return await self._count_jobs_by_type("completed_jobs")

    async def count_silenced(self) -> int:
        return await self._count_jobs_by_type("silenced_jobs")

    async def count_recently_failed(self) -> int:
        return await self._count_jobs_by_type("recent_failed_jobs")

    async def _get_jobs_by_type(self, type_name: str, after_index: int | None) -> list[dict[str, Any]]:
        # Scores are negated timestamps, so an ascending ZRANGE yields
        # newest-first — matching the upstream jobs-by-type query.
        start = after_index + 1 if after_index is not None and after_index >= 0 else 0
        end = start + 49  # Fetch 50 items
        job_ids = await self.redis.zrange(self._key(type_name), start, end)
        return await self.get_jobs([j.decode("utf-8") if isinstance(j, bytes) else str(j) for j in job_ids], start)

    async def get_jobs_by_tag(self, tag: str, after_index: int | None = None) -> list[dict[str, Any]]:
        # Tag pagination uses START-INDEX semantics (upstream RedisTagRepository::paginate()):
        # after_index is the offset to start from, not the index of the last seen job.
        start = after_index if after_index is not None and after_index >= 0 else 0
        end = start + 49
        job_ids = await self.redis.zrange(f"{self.prefix}tags:{tag}", start, end)
        return await self.get_jobs([j.decode("utf-8") if isinstance(j, bytes) else str(j) for j in job_ids], start)

    async def _count_jobs_by_type(self, type_name: str) -> int:
        minutes = self._minutes_for_type(type_name)
        now = time.time()
        score = (now - (minutes * 60)) * -1
        return await self.redis.zcount(self._key(type_name), "-inf", score)

    def _minutes_for_type(self, type_name: str) -> int:
        trim = self.config.trim
        return {
            "failed_jobs": trim.failed,
            "recent_failed_jobs": trim.recent_failed,
            "pending_jobs": trim.pending,
            "completed_jobs": trim.completed,
            "silenced_jobs": trim.completed,
            "recent_jobs": trim.recent,
        }.get(type_name, trim.recent)

    async def get_jobs(self, ids: list[str], index_from: int = 0) -> list[dict[str, Any]]:
        if not ids:
            return []

        pipe = self.redis.pipeline()
        for idx in ids:
            pipe.hmget(self._key(idx), self.keys)
        results = await pipe.execute()

        jobs = []
        for i, raw_job in enumerate(results):
            if raw_job and raw_job[0] is not None:
                job_dict = {}
                for key_idx, key in enumerate(self.keys):
                    val = raw_job[key_idx]
                    job_dict[key] = val.decode("utf-8") if isinstance(val, bytes) else val

                # The dashboard expects payload and retried_by to be parsed JSON
                import json

                if "payload" in job_dict and isinstance(job_dict["payload"], str):
                    with contextlib.suppress(json.JSONDecodeError):
                        job_dict["payload"] = json.loads(job_dict["payload"])

                # Context is stored as JSON; the dashboard renders it as an object.
                if isinstance(job_dict.get("context"), str):
                    with contextlib.suppress(json.JSONDecodeError):
                        job_dict["context"] = json.loads(job_dict["context"])

                if "retried_by" in job_dict and isinstance(job_dict["retried_by"], str):
                    try:
                        # Horizon explicitly checks 'retried_by' and expects a list of objects, natively descended by retried_at
                        retried = json.loads(job_dict["retried_by"])
                        if isinstance(retried, list):
                            retried.sort(key=lambda x: x.get("retried_at", 0), reverse=True)
                        job_dict["retried_by"] = retried
                    except json.JSONDecodeError:
                        job_dict["retried_by"] = []
                elif "retried_by" not in job_dict or job_dict["retried_by"] is None:
                    job_dict["retried_by"] = []

                job_dict["index"] = index_from + i
                jobs.append(job_dict)

        return jobs

    async def pushed(self, connection: str, queue: str, payload: TaskPayload) -> None:
        async with self.redis.pipeline() as pipe:
            self._store_job_reference(pipe, "recent_jobs", payload.id())
            self._store_job_reference(pipe, "pending_jobs", payload.id())

            now_str = str(time.time())
            job_key = self._key(payload.id())
            pipe.hset(
                job_key,
                mapping={
                    "id": payload.id(),
                    "connection": connection,
                    "queue": queue,
                    "name": payload.display_name(),
                    "status": "pending",
                    "payload": payload.value,
                    "created_at": now_str,
                    "updated_at": now_str,
                },
            )
            pipe.expire(job_key, self.config.trim.pending * 60)
            await pipe.execute()

    async def reserved(self, connection: str, queue: str, payload: TaskPayload) -> None:
        now_str = str(time.time())
        await self.redis.hset(
            self._key(payload.id()),
            mapping={
                "status": "reserved",
                "updated_at": now_str,
                "reserved_at": now_str,
            },
        )

    async def released(self, connection: str, queue: str, payload: TaskPayload, delay: int = 0) -> None:
        await self.redis.hset(
            self._key(payload.id()),
            mapping={
                "status": "pending",
                "updated_at": str(time.time()),
                "delay": delay,
            },
        )

    async def remember(self, connection: str, queue: str, payload: TaskPayload) -> None:
        async with self.redis.pipeline() as pipe:
            self._store_job_reference(pipe, "monitored_jobs", payload.id())
            job_key = self._key(payload.id())
            pipe.hset(
                job_key,
                mapping={
                    "id": payload.id(),
                    "connection": connection,
                    "queue": queue,
                    "name": payload.display_name(),
                    "status": "completed",
                    "completed_at": str(time.time()),
                },
            )
            pipe.expire(job_key, self.config.trim.monitored * 60)
            await pipe.execute()

    async def completed(self, payload: TaskPayload, failed: bool = False, silenced: bool = False) -> None:
        if payload.is_retry():
            await self._update_retry_information_on_parent(payload, failed)

        # Parity: Silenced Jobs Logic override via framework config
        if payload.display_name() in self.config.silenced:
            silenced = True

        async with self.redis.pipeline() as pipe:
            if silenced:
                self._store_job_reference(pipe, "silenced_jobs", payload.id())
            else:
                self._store_job_reference(pipe, "completed_jobs", payload.id())

            self._remove_job_reference(pipe, "pending_jobs", payload.id())

            job_key = self._key(payload.id())
            pipe.hset(
                job_key,
                mapping={
                    "status": "completed",
                    "completed_at": str(time.time()),
                },
            )
            pipe.expire(job_key, self.config.trim.completed * 60)
            await pipe.execute()

    async def _update_retry_information_on_parent(self, payload: TaskPayload, failed: bool) -> None:
        parent_id = payload.retry_of()
        if not parent_id:
            return
        parent_key = self._key(parent_id)
        retries_json = await self.redis.hget(parent_key, "retried_by")
        if retries_json:
            retries = json.loads(retries_json)
            for retry in retries:
                if retry.get("id") == payload.id():
                    retry["status"] = "failed" if failed else "completed"
            await self.redis.hset(parent_key, "retried_by", json.dumps(retries))

    async def delete_monitored(self, ids: list[str]) -> None:
        async with self.redis.pipeline() as pipe:
            for job_id in ids:
                pipe.expire(self._key(job_id), 7 * 24 * 60 * 60)
            await pipe.execute()

    async def trim_recent_jobs(self) -> None:
        now = time.time()

        score_recent = (now - (self.config.trim.recent * 60)) * -1
        score_recent_failed = (now - (self.config.trim.recent_failed * 60)) * -1
        score_pending = (now - (self.config.trim.pending * 60)) * -1
        score_completed = (now - (self.config.trim.completed * 60)) * -1

        async with self.redis.pipeline() as pipe:
            pipe.zremrangebyscore(self._key("recent_jobs"), score_recent, "+inf")
            pipe.zremrangebyscore(self._key("recent_failed_jobs"), score_recent_failed, "+inf")
            pipe.zremrangebyscore(self._key("pending_jobs"), score_pending, "+inf")
            pipe.zremrangebyscore(self._key("completed_jobs"), score_completed, "+inf")
            pipe.zremrangebyscore(self._key("silenced_jobs"), score_completed, "+inf")
            await pipe.execute()

    async def trim_failed_jobs(self) -> None:
        score = (time.time() - (self.config.trim.failed * 60)) * -1
        await self.redis.zremrangebyscore(self._key("failed_jobs"), score, "+inf")

    async def trim_monitored_jobs(self) -> None:
        score = (time.time() - (self.config.trim.monitored * 60)) * -1
        await self.redis.zremrangebyscore(self._key("monitored_jobs"), score, "+inf")

    async def find_failed(self, task_id: str) -> dict[str, Any] | None:
        jobs = await self.get_jobs([task_id])
        if jobs and jobs[0].get("status") == "failed":
            return jobs[0]
        return None

    @staticmethod
    def _format_exception(exception: Any) -> str:
        """Render the exception with its full traceback for the dashboard's stacktrace panel."""
        if isinstance(exception, BaseException):
            return "".join(traceback.format_exception(type(exception), exception, exception.__traceback__))
        return str(exception)

    def _build_context(self, exception: Exception, connection: str, queue: str, payload: TaskPayload) -> str:
        """Build the exception context stored alongside a failed job.

        Upstream renders whatever the exception's ``context()`` returns;
        taskiq exceptions rarely carry one, so a diagnostic context is always
        assembled automatically (and merged with the exception's own ``context``
        attribute/method when present, matching the upstream convention).
        """
        context: dict[str, Any] = {
            "exception_type": type(exception).__name__,
            "connection": connection,
            "queue": queue,
            "task_name": payload.display_name(),
            "attempts": payload.decoded.get("attempts", 1),
            "tags": payload.decoded.get("tags", []),
            "labels": {str(k): str(v) for k, v in dict(payload.message.labels).items()},
        }

        custom = getattr(exception, "context", None)
        if callable(custom):
            with contextlib.suppress(Exception):
                custom = custom()
        if isinstance(custom, dict):
            context.update({str(k): v for k, v in custom.items()})

        try:
            return json.dumps(context, default=str)
        except (TypeError, ValueError):
            return json.dumps({"exception_type": type(exception).__name__})

    async def failed(self, exception: Exception, connection: str, queue: str, payload: TaskPayload) -> None:
        if payload.is_retry():
            await self._update_retry_information_on_parent(payload, failed=True)

        async with self.redis.pipeline() as pipe:
            self._store_job_reference(pipe, "failed_jobs", payload.id())
            self._store_job_reference(pipe, "recent_failed_jobs", payload.id())
            self._remove_job_reference(pipe, "pending_jobs", payload.id())
            self._remove_job_reference(pipe, "completed_jobs", payload.id())
            self._remove_job_reference(pipe, "silenced_jobs", payload.id())

            job_key = self._key(payload.id())
            pipe.hset(
                job_key,
                mapping={
                    "id": payload.id(),
                    "connection": connection,
                    "queue": queue,
                    "name": payload.display_name(),
                    "status": "failed",
                    "payload": payload.value,
                    "exception": self._format_exception(exception),
                    "context": self._build_context(exception, connection, queue, payload),
                    "failed_at": str(time.time()),
                },
            )
            pipe.expire(job_key, self.config.trim.failed * 60)
            await pipe.execute()

    def _store_job_reference(self, pipe: Any, key: str, task_id: str) -> None:
        score = time.time() * -1
        pipe.zadd(self._key(key), {task_id: score})

    def _remove_job_reference(self, pipe: Any, key: str, task_id: str) -> None:
        pipe.zrem(self._key(key), task_id)

    async def store_retry_reference(self, task_id: str, retry_id: str) -> None:
        task_key = self._key(task_id)
        retries_json = await self.redis.hget(task_key, "retried_by")
        retries = json.loads(retries_json) if retries_json else []
        retries.append({"id": retry_id, "status": "pending", "retried_at": int(time.time())})
        await self.redis.hset(task_key, "retried_by", json.dumps(retries))

    async def delete_failed(self, task_id: str) -> int:
        async with self.redis.pipeline() as pipe:
            pipe.zrem(self._key("failed_jobs"), task_id)
            pipe.zrem(self._key("recent_failed_jobs"), task_id)
            pipe.delete(self._key(task_id))
            removed, _recent_removed, _deleted = await pipe.execute()
        return removed

    async def purge(self, queue: str) -> int:
        try:
            return await self._purge_with_lua(queue)
        except Exception:
            return await self._purge_with_scan(queue)

    async def _purge_with_lua(self, queue: str) -> int:
        from taskiq_horizon.lua_scripts import LuaScripts

        removed = 0
        cursor: str | int = 0

        while True:
            count, cursor = await self.redis.eval(
                LuaScripts.purge(),
                2,
                self._key("recent_jobs"),
                self._key("pending_jobs"),
                self.prefix,
                queue,
                cursor,
            )
            removed += int(count)
            if isinstance(cursor, bytes):
                cursor = cursor.decode("utf-8")
            if str(cursor) == "0":
                break

        return removed

    async def _purge_with_scan(self, queue: str) -> int:
        removed = 0
        cursor = 0

        while True:
            cursor, pairs = await self.redis.zscan(self._key("recent_jobs"), cursor=cursor)
            job_ids = [member.decode("utf-8") if isinstance(member, bytes) else str(member) for member, _score in pairs]

            if job_ids:
                pipe = self.redis.pipeline()
                for job_id in job_ids:
                    pipe.hmget(self._key(job_id), ["status", "queue"])
                statuses = await pipe.execute()

                pipe = self.redis.pipeline()
                for job_id, job_data in zip(job_ids, statuses, strict=False):
                    status, job_queue = job_data
                    status = status.decode("utf-8") if isinstance(status, bytes) else status
                    job_queue = job_queue.decode("utf-8") if isinstance(job_queue, bytes) else job_queue

                    if status in {"reserved", "pending"} and job_queue == queue:
                        pipe.zrem(self._key("recent_jobs"), job_id)
                        pipe.zrem(self._key("pending_jobs"), job_id)
                        pipe.delete(self._key(job_id))
                        removed += 1

                await pipe.execute()

            if cursor == 0:
                break

        return removed
