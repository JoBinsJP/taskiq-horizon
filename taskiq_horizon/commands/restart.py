r"""
RestartCommand — mirrors Laravel\Horizon\SupervisorCommands\Restart.

Terminates all workers and respawns them. Used for deploying new code.
r"""

import logging

logger = logging.getLogger("horizon.commands")


class RestartCommand:
    """Restart all worker processes."""

    def process(self, supervisor, options: dict | None = None) -> None:
        logger.info(f"Restarting supervisor '{supervisor.name}'")
        supervisor.restart()
