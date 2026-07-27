from cleo.commands.command import Command


class HorizonPauseCommand(Command):
    """Pause the Horizon master supervisor."""

    name = "horizon:pause"

    def handle(self):
        import asyncio
        import logging

        from alata.application import boot
        from alata.container import container

        logging.basicConfig(level=logging.ERROR)
        boot()

        async def _run():
            command_queue = container.make("horizon.command_queue")
            master_repo = container.make("horizon.masters")

            masters = await master_repo.all_masters()
            count = 0
            for master in masters:
                for sup_name in master.get("supervisors", []):
                    await command_queue.push(sup_name, "pause")
                    count += 1
            return count

        count = asyncio.run(_run())
        self.line(f"<info>Horizon paused successfully. ({count} supervisors signaled)</info>")
