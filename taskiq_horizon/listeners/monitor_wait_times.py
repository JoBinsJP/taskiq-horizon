r"""
MonitorWaitTimes — mirrors Laravel\Horizon\Listeners\MonitorWaitTimes.

Periodically calculates queue wait times and fires LongWaitDetected
events when thresholds are exceeded. Self-throttled to ~1 minute.
r"""

import time
import logging
from typing import Optional

from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.event_dispatcher import EventDispatcher
from taskiq_horizon.events import LongWaitDetected, SupervisorLooped

logger = logging.getLogger("horizon.listeners")


class MonitorWaitTimesListener:
    """Monitor queue wait times and fire alerts when thresholds are breached."""

    def __init__(
        self,
        wait_time_calculator,
        dispatcher: EventDispatcher,
        config: HorizonConfig,
    ) -> None:
        self.wait_time = wait_time_calculator
        self.dispatcher = dispatcher
        self.config = config
        self.last_monitored: Optional[float] = None
        self.frequency = 60  # seconds

    async def handle(self, event: SupervisorLooped) -> None:
        if not self._due_to_monitor():
            return

        wait_times = await self.wait_time.calculate()

        for queue_key, wait_seconds in wait_times.items():
            threshold = self.config.waits.get(queue_key, 60)

            if threshold == 0:
                continue  # disabled for this queue

            if wait_seconds > threshold:
                # Parse connection:queue format
                parts = queue_key.split(":", 1)
                connection = parts[0] if len(parts) > 1 else "redis"
                queue = parts[1] if len(parts) > 1 else parts[0]

                await self.dispatcher.fire(
                    LongWaitDetected(
                        connection=connection,
                        queue=queue,
                        seconds=int(wait_seconds),
                    )
                )

        self.last_monitored = time.time()

    def _due_to_monitor(self) -> bool:
        if self.last_monitored is None:
            return True
        return (time.time() - self.last_monitored) >= self.frequency
