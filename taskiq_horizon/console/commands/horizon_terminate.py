from cleo.commands.command import Command


class HorizonTerminateCommand(Command):
    """Terminate the Horizon master supervisor."""

    name = "horizon:terminate"

    def handle(self):
        import asyncio
        import logging

        from app.core.container import container
        from app.providers.app_provider import boot

        logging.basicConfig(level=logging.ERROR)
        boot()

        async def _run():
            command_queue = container.make("horizon.command_queue")
            master_repo = container.make("horizon.masters")

            masters = await master_repo.all_masters()
            count = 0
            for master in masters:
                for sup_name in master.get("supervisors", []):
                    # Passing status=0 mimics normal termination code
                    await command_queue.push(sup_name, "terminate", {"status": 0})
                    count += 1
            return count

        count = asyncio.run(_run())
        self.line(f"<info>Horizon terminated successfully. ({count} supervisors signaled)</info>")
