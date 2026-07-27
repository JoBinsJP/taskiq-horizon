from cleo.commands.command import Command


class HorizonSnapshotCommand(Command):
    """Store a snapshot of the queue metrics"""

    name = "horizon:snapshot"
    description = "Store a snapshot of the queue metrics"

    def handle(self):
        import asyncio

        from alata.application import boot
        from alata.container import container

        self.line("<info>Taking Horizon metrics snapshot...</info>")
        boot()

        metrics_repo = container.make("horizon.metrics")
        # Building the wait-time calculator wires it into the metrics repository
        # (see service_provider._make_wait_time), so queue snapshots record real
        # wait times instead of always 0.0.
        container.make("horizon.wait_time")

        async def run_snapshot():
            await metrics_repo.snapshot()

        asyncio.run(run_snapshot())
        self.line("<info>Horizon metrics snapshot successfully taken.</info>")
