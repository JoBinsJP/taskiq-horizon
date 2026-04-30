from cleo.commands.command import Command


class HorizonSnapshotCommand(Command):
    """Store a snapshot of the queue metrics"""

    name = "horizon:snapshot"
    description = "Store a snapshot of the queue metrics"

    def handle(self):
        import asyncio

        from app.core.application import boot
        from app.core.container import container

        self.line("<info>Taking Horizon metrics snapshot...</info>")
        boot()

        metrics_repo = container.make("horizon.metrics")

        async def run_snapshot():
            await metrics_repo.snapshot()

        asyncio.run(run_snapshot())
        self.line("<info>Horizon metrics snapshot successfully taken.</info>")
