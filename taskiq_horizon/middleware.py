import logging
import time

from alata.queue.retry_middleware import RetryDecision, RetryMiddleware, retry_decision
from taskiq import SimpleRetryMiddleware
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.exceptions import NoResultError
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult

from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.contracts.job_repository import JobRepository
from taskiq_horizon.contracts.metrics_repository import MetricsRepository
from taskiq_horizon.contracts.tag_repository import TagRepository
from taskiq_horizon.payload import TaskPayload
from taskiq_horizon.queue_backend import queue_backend

logger = logging.getLogger("horizon")


class HorizonMiddleware(TaskiqMiddleware):
    def __init__(
        self,
        config: HorizonConfig,
        jobs: JobRepository,
        metrics: MetricsRepository,
        tags: TagRepository,
    ):
        self.config = config
        self.jobs = jobs
        self.metrics = metrics
        self.tags = tags

        # State for timing tasks during worker execution
        self.stopwatches = {}

    def _destination(self, message: TaskiqMessage) -> tuple[str, str]:
        broker = getattr(self, "broker", None)
        if broker is None:
            return "redis", message.labels.get("queue_name") or "default"
        backend = queue_backend(broker)
        return backend.connection, backend.queue_for(message)

    def _will_retry(self, message, exception) -> bool:
        for middleware in getattr(getattr(self, "broker", None), "middlewares", ()):
            if isinstance(middleware, RetryMiddleware):
                return middleware.retry_decision(message, exception) is RetryDecision.RETRY
            if isinstance(middleware, SimpleRetryMiddleware):
                return (
                    retry_decision(
                        message,
                        exception,
                        default_retry_count=middleware.default_retry_count,
                        default_retry_label=middleware.default_retry_label,
                        types_of_exceptions=middleware._get_types_of_exceptions(message),
                    )
                    is RetryDecision.RETRY
                )
        return False

    async def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        """Executed on client before message is sent."""
        # Preserve the original enqueue time so worker-side payload rebuilds
        # do not overwrite it during reserve/fail/complete transitions.
        message.labels.setdefault("horizon_pushed_at", str(time.time()))

        try:
            connection, queue = self._destination(message)
            # Carry the dispatch destination through workers with a different
            # primary queue, and through manual retries from the dashboard.
            message.labels["queue_name"] = queue
            payload = TaskPayload(message).prepare(self.config)
            await self.jobs.pushed(connection=connection, queue=queue, payload=payload)

            # Store monitored tags
            if payload.tags():
                monitored = await self.tags.monitored(payload.tags())
                if monitored:
                    await self.tags.add(payload.id(), monitored)

        except Exception as e:
            logger.error(f"Horizon pre_send error: {e}")

        return message

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        """Executed on worker before task starts."""
        try:
            payload = TaskPayload(message).prepare(self.config)
            connection, queue = self._destination(message)

            await self.jobs.reserved(connection=connection, queue=queue, payload=payload)

            # Start timer
            self.stopwatches[id(message)] = time.monotonic()

        except Exception as e:
            logger.error(f"Horizon pre_execute error: {e}")

        return message

    async def on_error(self, message: TaskiqMessage, result: TaskiqResult, exception: BaseException) -> None:
        """Executed on worker if task fails."""
        try:
            if isinstance(exception, NoResultError):
                await self._completed(message)
                return
            payload = TaskPayload(message).prepare(self.config)
            connection, queue = self._destination(message)

            if self._will_retry(message, exception):
                await self.jobs.released(connection=connection, queue=queue, payload=payload)
                return

            await self.jobs.failed(exception=exception, connection=connection, queue=queue, payload=payload)

            monitored = await self.tags.monitored(payload.tags())
            if monitored:
                await self.tags.add(payload.id(), [f"failed:{tag}" for tag in monitored])

        except Exception as e:
            logger.error(f"Horizon on_error error: {e}")
        finally:
            self.stopwatches.pop(id(message), None)

    async def post_execute(self, message: TaskiqMessage, result: TaskiqResult) -> None:
        """Executed on worker after task finishes (success or handled failure)."""
        try:
            # Only process if we haven't popped it from on_error
            # If on_error triggered, we don't mark as completed.
            # But TaskIQ might call post_execute even after on_error.
            if result.is_err:
                return

            await self._completed(message)

        except Exception as e:
            logger.error(f"Horizon post_execute error: {e}")

    async def _completed(self, message: TaskiqMessage) -> None:
        start_time = self.stopwatches.pop(id(message), None)
        payload = TaskPayload(message).prepare(self.config)
        connection, queue = self._destination(message)
        await self.jobs.completed(payload, failed=False, silenced=payload.is_silenced())
        if await self.tags.monitored(payload.tags()):
            await self.jobs.remember(connection, queue, payload)
        runtime = (time.monotonic() - start_time) * 1000 if start_time is not None else None
        await self.metrics.increment_job(payload.command_name(), runtime)
        await self.metrics.increment_queue(queue, runtime)
