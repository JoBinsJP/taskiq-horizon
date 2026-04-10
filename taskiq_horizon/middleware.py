import logging
from typing import Any, Optional
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult

from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.payload import TaskPayload
import time
from taskiq_horizon.contracts.job_repository import JobRepository
from taskiq_horizon.contracts.metrics_repository import MetricsRepository
from taskiq_horizon.contracts.tag_repository import TagRepository

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

    async def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        """Executed on client before message is sent."""
        # Ensure task has a timestamp and a connection name
        payload = TaskPayload(message).prepare(self.config)
        
        try:
            # We treat the broker/queue name loosely here. TaskIQ has 'queues' but usually
            # it's determined per broker. We'll use 'default' if None.
            queue = message.labels.get("queue_name", "default")
            await self.jobs.pushed(connection="redis", queue=queue, payload=payload)
            
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
            queue = message.labels.get("queue_name", "default")
            
            await self.jobs.reserved(connection="redis", queue=queue, payload=payload)
            
            # Start timer
            self.stopwatches[message.task_id] = time.time()
            
        except Exception as e:
            logger.error(f"Horizon pre_execute error: {e}")
            
        return message

    async def on_error(self, message: TaskiqMessage, result: TaskiqResult, exception: BaseException) -> None:
        """Executed on worker if task fails."""
        try:
            payload = TaskPayload(message).prepare(self.config)
            queue = message.labels.get("queue_name", "default")
            
            await self.jobs.failed(
                exception=exception,
                connection="redis",
                queue=queue,
                payload=payload
            )
            
            # Timer logic (failed jobs also take time)
            self.stopwatches.pop(message.task_id, None)
            # Conceivably update metrics even for failed, Horizon does this
                
        except Exception as e:
            logger.error(f"Horizon on_error error: {e}")

    async def post_execute(self, message: TaskiqMessage, result: TaskiqResult) -> None:
        """Executed on worker after task finishes (success or handled failure)."""
        try:
            # Only process if we haven't popped it from on_error
            # If on_error triggered, we don't mark as completed.
            # But TaskIQ might call post_execute even after on_error.
            if result.is_err:
                return
                
            payload = TaskPayload(message).prepare(self.config)
            queue = message.labels.get("queue_name", "default")
            
            await self.jobs.completed(payload, failed=False, silenced=payload.is_silenced())
            
            start_time = self.stopwatches.pop(message.task_id, None)
            runtime = None
            if start_time:
                runtime = (time.time() - start_time) * 1000 # Horizon typically expects milliseconds
                
            await self.metrics.increment_job(payload.command_name(), runtime)
            await self.metrics.increment_queue(queue, runtime)
            
        except Exception as e:
            logger.error(f"Horizon post_execute error: {e}")
