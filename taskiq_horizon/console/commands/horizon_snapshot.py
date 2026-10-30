from taskiq_horizon.console.command import HorizonCommandBase


class HorizonSnapshotCommand(HorizonCommandBase):
    """Store a snapshot of the queue metrics"""

    name = "horizon:snapshot"
    description = "Store a snapshot of the queue metrics"

    def handle(self):

        self.line("<info>Taking Horizon metrics snapshot...</info>")
        container = self.alata_application()

        metrics_repo = container.make("horizon.metrics")
        # Building the wait-time calculator wires it into the metrics repository
        # (see service_provider._make_wait_time), so queue snapshots record real
        # wait times instead of always 0.0.
        container.make("horizon.wait_time")

        async def run_snapshot():
            await metrics_repo.snapshot()

        self.run_async(run_snapshot())
        self.line("<info>Horizon metrics snapshot successfully taken.</info>")
