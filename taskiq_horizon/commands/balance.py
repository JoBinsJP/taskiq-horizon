r"""
BalanceCommand — mirrors Laravel\Horizon\MasterSupervisorCommands\Balance.

Triggers an immediate autoscaler rebalance across all supervisors
managed by a master. Normally autoscaling runs on a timer, but this
command allows on-demand rebalancing from the dashboard or CLI.
r"""

import logging

logger = logging.getLogger("horizon.commands")


class BalanceCommand:
    """Trigger an immediate autoscaler rebalance."""

    def process(self, supervisor, options: dict | None = None) -> None:
        logger.info(f"Balance command received for supervisor '{supervisor.name}'")
        # Mark the supervisor for immediate rebalance on the next loop
        supervisor._needs_balance = True
