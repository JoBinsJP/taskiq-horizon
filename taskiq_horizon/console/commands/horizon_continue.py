from taskiq_horizon.console.command import HorizonCommandBase


class HorizonContinueCommand(HorizonCommandBase):
    """Instruct the master supervisor to continue processing jobs."""

    name = "horizon:continue"

    def handle(self):
        import logging

        logging.basicConfig(level=logging.ERROR)
        container = self.alata_application()

        async def _run():
            command_queue = container.make("horizon.command_queue")
            master_repo = container.make("horizon.masters")

            masters = await master_repo.all_masters()
            for master in masters:
                await command_queue.push(master["name"], "continue")
            return len(masters)

        count = self.run_async(_run())
        self.line(f"<info>Continue requested for {count} Horizon master(s).</info>")
