r"""
ContinueWorkingCommand — ports the upstream continue-working command.

Resumes a paused supervisor so it accepts new jobs again.
r"""

import logging

logger = logging.getLogger("horizon.commands")


class ContinueWorkingCommand:
    """Resume a paused supervisor."""

    def process(self, supervisor, options: dict | None = None) -> None:
        logger.info(f"Continuing supervisor '{supervisor.name}'")
        supervisor.continue_working()
