r"""
ScaleCommand — mirrors Laravel\Horizon\SupervisorCommands\Scale.

Sets the target number of worker processes for a supervisor.
r"""

import logging

logger = logging.getLogger("horizon.commands")


class ScaleCommand:
    """Scale the supervisor to a target number of workers."""

    def process(self, supervisor, options: dict | None = None) -> None:
        target = (options or {}).get("scale", 1)
        logger.info(f"Scaling supervisor '{supervisor.name}' to {target} workers")
        supervisor.scale(target)
