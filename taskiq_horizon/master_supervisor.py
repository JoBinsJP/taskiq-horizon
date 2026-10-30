"""
MasterSupervisor — ports the upstream master supervisor.

Top-level orchestrator that:
1. Manages one or more Supervisor instances
2. Heartbeats to Redis (MasterSupervisorRepository)
3. Fires MasterSupervisorLooped events (triggers trim jobs, expiry, etc.)
4. Handles OS signals (SIGTERM, SIGINT, SIGUSR1, SIGUSR2, SIGCONT)
5. Monitors its own memory usage

Usage:
    master = MasterSupervisor(name, redis, config, dispatcher, supervisors)
    await master.monitor()  # runs until terminated
"""

import asyncio
import logging
import os
import signal
import socket
from dataclasses import replace

from taskiq_horizon.autoscaler import AutoScaler
from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.event_dispatcher import EventDispatcher
from taskiq_horizon.event_map import register_default_listeners
from taskiq_horizon.events import MasterSupervisorLooped
from taskiq_horizon.repositories.redis_command_queue import RedisCommandQueue
from taskiq_horizon.repositories.redis_master_repository import (
    RedisMasterSupervisorRepository,
    RedisSupervisorRepository,
)
from taskiq_horizon.supervisor import Supervisor, SupervisorOptions

logger = logging.getLogger("horizon.master")


class MasterSupervisor:
    """
    Top-level supervisor process — one per machine.

    Follows the upstream master loop structure:
    1. Heartbeat to Redis
    2. Process pending signals
    3. Fire MasterSupervisorLooped event
    4. Sleep 1 second
    """

    def __init__(
        self,
        config: HorizonConfig,
        redis,
        dispatcher: EventDispatcher,
        supervisor_options: list[SupervisorOptions] | None = None,
        *,
        environment: str = "local",
        job_repo=None,
        metrics_repo=None,
        wait_time_calculator=None,
    ) -> None:
        self.config = config
        self.redis = redis
        self.dispatcher = dispatcher

        # Determine names
        self.machine_name = socket.gethostname()
        self.name = f"TaskIQ-Master-{self.machine_name}-{os.getpid()}"
        self.environment = environment

        # Repositories
        prefix = config.prefix
        self.master_repo = RedisMasterSupervisorRepository(redis, prefix)
        self.supervisor_repo = RedisSupervisorRepository(redis, prefix)
        self.command_queue = RedisCommandQueue(redis, config)

        # Build supervisors from options or environments config
        self.supervisors: list[Supervisor] = []

        opts_to_use = supervisor_options
        if opts_to_use is None:
            # Fallback to environments dictionary if provided
            opts_to_use = []
            env_config = config.environments.get(self.environment, {})
            for name, opts in env_config.items():
                # Upstream prefixes the supervisor name with the Master's name
                # to prevent collision when multiple masters are running.
                opts_to_use.append(replace(opts, name=f"{self.name}:{name}"))

        for opts in opts_to_use:
            opts.broker_path = opts.broker_path or ""
            autoscaler = None
            autoscaler_metrics = metrics_repo or getattr(wait_time_calculator, "metrics_repo", None)
            if autoscaler_metrics and wait_time_calculator:
                autoscaler = AutoScaler(autoscaler_metrics, wait_time_calculator)
            sup = Supervisor(
                options=opts,
                command_queue=self.command_queue,
                dispatcher=dispatcher,
                autoscaler=autoscaler,
                environment=self.environment,
            )
            self.supervisors.append(sup)

        # State
        self.running = True
        self.working = True
        # Set by listeners (e.g. MonitorMasterSupervisorMemory) to request a
        # graceful shutdown — checked on every loop iteration.
        self.should_quit = False
        self._pending_signals: list[str] = []
        self._supervisor_tasks: list[asyncio.Task] = []
        self._signal_handlers: dict[int, object] = {}

        # Register default event listeners
        register_default_listeners(
            dispatcher,
            job_repo=job_repo,
            master_repo=self.master_repo,
            supervisor_repo=self.supervisor_repo,
            wait_time_calculator=wait_time_calculator,
            config=config,
        )

    @classmethod
    def basename(cls) -> str:
        """Base prefix used to identify masters (matches the upstream basename convention)."""
        return "TaskIQ-Master-"

    async def monitor(self) -> None:
        """Start the master supervisor monitor loop."""
        logger.info(f"MasterSupervisor '{self.name}' starting...")

        # Register signal handlers
        self._listen_for_signals()

        # Start all child supervisors as concurrent tasks
        for supervisor in self.supervisors:
            task = asyncio.create_task(
                supervisor.monitor(),
                name=f"supervisor:{supervisor.name}",
            )
            self._supervisor_tasks.append(task)

        try:
            while self.running:
                # 1. Process buffered signals
                self._process_pending_signals()

                await self._process_commands()
                if not self.running:
                    break

                # A crashed child must not leave a healthy-looking master.
                for task in self._supervisor_tasks:
                    if task.done():
                        task.result()
                        raise RuntimeError(f"Supervisor stopped unexpectedly: {task.get_name()}")

                # 2. Heartbeat to Redis
                await self._heartbeat()

                # 3. Fire MasterSupervisorLooped event (triggers trims, expiry)
                await self.dispatcher.fire(MasterSupervisorLooped(master=self))

                # 4. Honor graceful-shutdown requests from listeners
                #    (e.g. master memory limit exceeded) — same termination
                #    path as horizon:terminate / SIGTERM.
                if self.should_quit:
                    self._terminate()
                    continue

                # 5. Sleep
                await asyncio.sleep(1)

        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown()

    def _listen_for_signals(self) -> None:
        """
        Register signal handlers — matches the upstream signal-listening behavior.

        Signals are buffered in _pending_signals and processed in the
        next loop iteration (not immediately) to avoid race conditions.
        """
        loop = asyncio.get_event_loop()

        def _buffer_signal(signal_name: str):
            self._pending_signals.append(signal_name)

        for signal_name, command in (
            ("SIGTERM", "terminate"),
            ("SIGINT", "terminate"),
            ("SIGUSR1", "restart"),
            ("SIGUSR2", "pause"),
            ("SIGCONT", "continue"),
        ):
            number = getattr(signal, signal_name, None)
            if number is None:
                continue
            try:
                previous = signal.getsignal(number)
                loop.add_signal_handler(number, _buffer_signal, command)
                self._signal_handlers[number] = previous
            except (NotImplementedError, RuntimeError, OSError, ValueError):
                pass

    async def _process_commands(self) -> None:
        try:
            commands = await self.command_queue.pending(self.name)
        except Exception:
            logger.exception("Failed to read master commands")
            return
        handlers = {
            "pause": self._pause,
            "continue": self._continue,
            "terminate": self._terminate,
            "restart": self._restart,
        }
        for command in commands:
            handler = handlers.get(command.get("command"))
            if handler:
                handler()
                if not self.running:
                    break

    def _process_pending_signals(self) -> None:
        """Process buffered signals — deferred, matching the upstream signal handling."""
        while self._pending_signals:
            sig = self._pending_signals.pop(0)
            if sig == "terminate":
                self._terminate()
            elif sig == "restart":
                self._restart()
            elif sig == "pause":
                self._pause()
            elif sig == "continue":
                self._continue()

    def _terminate(self) -> None:
        logger.info("MasterSupervisor received terminate signal.")
        self.running = False
        self.working = False

    def _restart(self) -> None:
        logger.info("MasterSupervisor received restart signal.")
        self.working = True
        for supervisor in self.supervisors:
            supervisor.restart()

    def _pause(self) -> None:
        logger.info("MasterSupervisor received pause signal.")
        self.working = False
        for supervisor in self.supervisors:
            supervisor.pause()

    def _continue(self) -> None:
        logger.info("MasterSupervisor received continue signal.")
        self.working = True
        for supervisor in self.supervisors:
            supervisor.continue_working()

    async def _heartbeat(self) -> None:
        """Persist state to Redis so the dashboard can see us."""
        try:
            supervisor_names = [s.name for s in self.supervisors]
            status = "running" if self.working else "paused"

            await self.master_repo.update_master(
                name=self.name,
                pid=os.getpid(),
                status=status,
                supervisors=supervisor_names,
                environment=self.environment,
            )

            for supervisor in self.supervisors:
                info = supervisor.to_dict()
                await self.supervisor_repo.update_supervisor(
                    name=supervisor.name,
                    master=self.name,
                    pid=info["pid"],
                    status=info["status"],
                    processes=info["processes"],
                    options=info["options"],
                )
        except Exception as e:
            logger.error(f"Heartbeat failed: {e}")

    async def _shutdown(self) -> None:
        """Gracefully shut down all supervisors and clean up Redis."""
        self.running = False
        self.working = False
        logger.info(f"MasterSupervisor '{self.name}' shutting down...")

        # Terminate all child supervisors
        for supervisor in self.supervisors:
            supervisor.terminate(0)

        # Wait for supervisor tasks to complete
        if self._supervisor_tasks:
            await asyncio.gather(*self._supervisor_tasks, return_exceptions=True)

        # Unregister from Redis
        try:
            pipe = self.redis.pipeline()
            pipe.delete(f"{self.config.prefix}master:{self.name}", f"{self.config.prefix}commands:{self.name}")
            pipe.zrem(f"{self.config.prefix}masters", self.name)
            for sup in self.supervisors:
                pipe.delete(f"{self.config.prefix}supervisor:{sup.name}")
                pipe.zrem(f"{self.config.prefix}supervisors", sup.name)
            await pipe.execute()
        except Exception:
            pass

        loop = asyncio.get_running_loop()
        for number, previous in self._signal_handlers.items():
            loop.remove_signal_handler(number)
            signal.signal(number, previous)
        self._signal_handlers.clear()
        logger.info(f"MasterSupervisor '{self.name}' has stopped.")

    # ── Status ───────────────────────────────────────────────────────────

    @property
    def status(self) -> str:
        if not self.running:
            return "stopped"
        if not self.working:
            return "paused"
        return "running"
