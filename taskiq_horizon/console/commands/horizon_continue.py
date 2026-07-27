from cleo.commands.command import Command


class HorizonContinueCommand(Command):
    """Instruct the master supervisor to continue processing jobs."""

    name = "horizon:continue"

    def handle(self):
        import asyncio
        import logging

        try:
            from alata.application import boot
            from alata.container import container
        except ImportError:  # legacy consumers with the embedded app.core framework
            from app.core.application import boot
            from app.core.container import container

        logging.basicConfig(level=logging.ERROR)
        boot()

        async def _run():
            command_queue = container.make("horizon.command_queue")
            master_repo = container.make("horizon.masters")

            masters = await master_repo.all_masters()
            count = 0
            for master in masters:
                for sup_name in master.get("supervisors", []):
                    await command_queue.push(sup_name, "continue")
                    count += 1
            return count

        count = asyncio.run(_run())
        self.line(f"<info>Horizon continued successfully. ({count} supervisors signaled)</info>")
