r"""
SendNotification — mirrors Laravel\Horizon\Listeners\SendNotification.

Handles LongWaitDetected events. Logger-only implementation per user decision.
r"""

import logging

from taskiq_horizon.events import LongWaitDetected

logger = logging.getLogger("horizon.alerts")


class SendNotificationListener:
    """Log a warning when a queue's wait time exceeds its threshold."""

    async def handle(self, event: LongWaitDetected) -> None:
        logger.warning(
            f"Long wait detected on queue '{event.queue}' "
            f"(connection: {event.connection}): "
            f"{event.seconds}s exceeds configured threshold."
        )
