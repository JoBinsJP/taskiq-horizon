from cleo.commands.command import Command


class HorizonCommand(Command):
    """Start the TaskIQ Horizon master supervisor"""

    name = "horizon"

    def handle(self):
        import asyncio
        import logging

        from alata.application import boot
        from alata.container import container
        from config.settings import settings

        from taskiq_horizon.event_dispatcher import EventDispatcher
        from taskiq_horizon.master_supervisor import MasterSupervisor

        self.line("<info>Booting Horizon configuration...</info>")
        logging.basicConfig(level=logging.INFO)
        boot()

        master = MasterSupervisor(
            config=container.make("horizon.config"),
            redis=container.make("horizon.redis"),
            dispatcher=EventDispatcher(),
            job_repo=container.make("horizon.jobs"),
            wait_time_calculator=container.make("horizon.wait_time"),
            environment=settings.ENVIRONMENT,
        )

        self.line("<info>Starting Horizon Master Supervisor...</info>")
        try:
            asyncio.run(master.monitor())
        except KeyboardInterrupt:
            self.line("<comment>Horizon stopped gracefully.</comment>")
