import base64
import pickle
import time
from typing import Any
from uuid import uuid4

from taskiq.formatters.proxy_formatter import ProxyFormatter
from taskiq.message import TaskiqMessage

from taskiq_horizon.payload import TaskPayload


class RetryFailedJob:
    """
    Ports the upstream failed-job retry orchestration.

    The serialized retry state is internal package data intended for trusted
    Redis storage only.
    """

    def __init__(self, broker, jobs, tags, config) -> None:
        self.broker = broker
        self.jobs = jobs
        self.tags = tags
        self.config = config

    async def handle(self, job_id: str) -> str | None:
        if not self.broker:
            raise RuntimeError("Broker not configured in app state to support retries")

        job = await self.jobs.find_failed(job_id)
        if not job:
            return None

        message = self._prepare_message(job_id, job)
        retry_payload = TaskPayload(message).prepare(self.config)

        await self.jobs.pushed(
            connection=job.get("connection", "redis"),
            queue=job.get("queue", "default"),
            payload=retry_payload,
        )
        await self.jobs.store_retry_reference(job_id, message.task_id)

        monitored = await self.tags.monitored(retry_payload.tags())
        if monitored:
            await self.tags.add(message.task_id, monitored)

        formatter = getattr(self.broker, "formatter", None) or ProxyFormatter(self.broker)
        await self.broker.kick(formatter.dumps(message))

        return message.task_id

    def _prepare_message(self, job_id: str, job: dict[str, Any]) -> TaskiqMessage:
        payload = job.get("payload") or {}
        state = self._decode_retry_state(payload)
        if not state:
            raise RuntimeError("Retry payload unavailable for this failed job")

        task_name = payload.get("task_name") or job.get("name")
        if not task_name:
            raise RuntimeError("Retry payload is missing the task name")

        labels = dict(state.get("labels") or {})
        labels["retry_of"] = job_id
        labels["queue_name"] = job.get("queue") or labels.get("queue_name", "default")
        labels["horizon_pushed_at"] = str(time.time())

        return TaskiqMessage(
            task_id=str(uuid4()),
            task_name=task_name,
            labels=labels,
            labels_types=state.get("labels_types"),
            args=list(state.get("args") or []),
            kwargs=dict(state.get("kwargs") or {}),
        )

    def _decode_retry_state(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        taskiq_payload = payload.get("taskiq", {})
        encoded = taskiq_payload.get("state")
        if not encoded:
            return None

        try:
            return pickle.loads(base64.b64decode(encoded))
        except Exception:
            return None
