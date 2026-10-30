import base64
import pickle
import time
from io import BytesIO
from typing import Any
from uuid import uuid4

from taskiq.formatters.proxy_formatter import ProxyFormatter
from taskiq.labels import LabelType
from taskiq.message import TaskiqMessage
from taskiq_aio_sqs import SQSBroker

from taskiq_horizon.payload import TaskPayload
from taskiq_horizon.queue_backend import SqsQueueBackend


class _RetryStateUnpickler(pickle.Unpickler):
    """Read existing primitive retry payloads without importing executable globals."""

    def find_class(self, module: str, name: str):
        if (module, name) == ("taskiq.labels", "LabelType"):
            return LabelType
        raise pickle.UnpicklingError("Unsupported object in retry state")


class RetryFailedJob:
    """
    Ports the upstream failed-job retry orchestration.

    Restores the original message and reopens its batch slot before dispatch.
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
        batch_id = message.labels.get("batch_id")
        prepared_batch = False
        if batch_id:
            from alata.queue.batch_repository import BatchRepository

            try:
                prepared_batch = await BatchRepository.prepare_retry(batch_id, job_id, message.task_id)
            except ValueError as exc:
                raise RuntimeError(str(exc)) from exc
        retry_payload = TaskPayload(message).prepare(self.config)

        try:
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
            if isinstance(self.broker, SQSBroker):
                await SqsQueueBackend(self.broker).retry(formatter.dumps(message), message.labels["queue_name"])
            else:
                await self.broker.kick(formatter.dumps(message))
        except BaseException as exc:
            if prepared_batch:
                await BatchRepository.finish_retry(batch_id, message.task_id, rollback=True)
            await self.jobs.failed(exc, job.get("connection", "redis"), job.get("queue", "default"), retry_payload)
            raise

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
        labels["_retries"] = 0
        labels["retry_of"] = job_id
        labels["queue_name"] = job.get("queue") or labels.get("queue_name", "default")
        labels["horizon_pushed_at"] = str(time.time())

        return TaskiqMessage(
            task_id=str(uuid4()),
            task_name=task_name,
            labels=labels,
            labels_types={**(state.get("labels_types") or {}), "_retries": LabelType.INT.value},
            args=list(state.get("args") or []),
            kwargs=dict(state.get("kwargs") or {}),
        )

    def _decode_retry_state(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        taskiq_payload = payload.get("taskiq", {})
        encoded = taskiq_payload.get("state")
        if not encoded:
            return None

        try:
            state = _RetryStateUnpickler(BytesIO(base64.b64decode(encoded))).load()
            return state if isinstance(state, dict) else None
        except Exception:
            return None
