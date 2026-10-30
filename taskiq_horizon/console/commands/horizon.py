from taskiq_horizon.console.command import HorizonCommandBase


class HorizonCommand(HorizonCommandBase):
    """Start the TaskIQ Horizon master supervisor"""

    name = "horizon"

    def handle(self):
        import logging

        from taskiq_horizon.event_dispatcher import EventDispatcher
        from taskiq_horizon.master_supervisor import MasterSupervisor

        self.line("<info>Booting Horizon configuration...</info>")
        logging.basicConfig(level=logging.INFO)
        container = self.alata_application()

        backend = container.make("horizon.queue_backend")
        if backend is None or backend.connection in {"sync", "null"}:
            self.line_error("<error>Horizon workers require a Redis, database, or SQS queue.</error>")
            return 1
        config = container.make("horizon.config")
        options = config.environments.get(container.env, {})
        if not options:
            self.line_error(f"<error>No Horizon supervisors configured for environment '{container.env}'.</error>")
            return 1
        for options_for_supervisor in options.values():
            queues = options_for_supervisor.queue_names
            if not queues:
                self.line_error("<error>Configure at least one queue for each Horizon supervisor.</error>")
                return 1
            if backend.connection == "sqs" and not options_for_supervisor.balancing and len(queues) != 1:
                self.line_error(
                    "<error>SQS workers consume one queue each; use auto/simple balancing for multiple queues.</error>"
                )
                return 1

        master = MasterSupervisor(
            config=container.make("horizon.config"),
            redis=container.make("horizon.redis"),
            dispatcher=EventDispatcher(),
            job_repo=container.make("horizon.jobs"),
            wait_time_calculator=container.make("horizon.wait_time"),
            environment=container.env,
        )

        self.line("<info>Starting Horizon Master Supervisor...</info>")
        try:
            self.run_async(master.monitor())
        except KeyboardInterrupt:
            self.line("<comment>Horizon stopped gracefully.</comment>")
