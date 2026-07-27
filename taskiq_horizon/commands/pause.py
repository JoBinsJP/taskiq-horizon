r"""
PauseCommand — ports the upstream pause command.

Pauses a supervisor, preventing it from picking up new jobs.
Workers finish their current jobs but don't accept new ones.
r"""

import logging

logger = logging.getLogger("horizon.commands")


class PauseCommand:
    """Pause the supervisor."""

    def process(self, supervisor, options: dict | None = None) -> None:
        logger.info(f"Pausing supervisor '{supervisor.name}'")
        supervisor.pause()
