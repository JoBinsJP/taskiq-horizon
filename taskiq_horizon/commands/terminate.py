r"""
TerminateCommand — ports the upstream terminate command.

Gracefully terminates the supervisor and all its workers.
r"""

import logging

logger = logging.getLogger("horizon.commands")


class TerminateCommand:
    """Gracefully terminate the supervisor."""

    def process(self, supervisor, options: dict | None = None) -> None:
        status = (options or {}).get("status", 0)
        logger.info(f"Terminating supervisor '{supervisor.name}' with status {status}")
        supervisor.terminate(status)
