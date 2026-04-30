"""
MasterSupervisor — mirrors Laravel Horizon's MasterSupervisor.php.

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

    Mirrors the loop structure from MasterSupervisor.php:
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
                # Laravel prefixes the supervisor name with the Master's name
                # to prevent collision when multiple masters are running.
                opts.name = f"{self.name}:{name}"
                opts_to_use.append(opts)

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
            )
            self.supervisors.append(sup)

        # State
        self.running = True
        self.working = True
        self._pending_signals: list[str] = []
        self._supervisor_tasks: list[asyncio.Task] = []

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
        """Base prefix used to identify masters (like Laravel's basename)."""
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

                # 2. Heartbeat to Redis
                await self._heartbeat()

                # 3. Fire MasterSupervisorLooped event (triggers trims, expiry)
                await self.dispatcher.fire(MasterSupervisorLooped(master=self))

                # 4. Sleep
                await asyncio.sleep(1)

        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown()

    def _listen_for_signals(self) -> None:
        """
        Register signal handlers — mirrors ListensForSignals trait.

        Signals are buffered in _pending_signals and processed in the
        next loop iteration (not immediately) to avoid race conditions.
        """
        loop = asyncio.get_event_loop()

        def _buffer_signal(signal_name: str):
            self._pending_signals.append(signal_name)

        try:
            loop.add_signal_handler(signal.SIGTERM, lambda: _buffer_signal("terminate"))
            loop.add_signal_handler(signal.SIGINT, lambda: _buffer_signal("terminate"))
        except NotImplementedError:
            pass  # Windows

        try:
            loop.add_signal_handler(signal.SIGUSR1, lambda: _buffer_signal("restart"))
            loop.add_signal_handler(signal.SIGUSR2, lambda: _buffer_signal("pause"))
            loop.add_signal_handler(signal.SIGCONT, lambda: _buffer_signal("continue"))
        except (NotImplementedError, AttributeError, OSError):
            pass  # Windows or restricted environments

    def _process_pending_signals(self) -> None:
        """Process buffered signals — deferred like Laravel's processPendingSignals."""
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

    def _restart(self) -> None:
        logger.info("MasterSupervisor received restart signal.")
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
            pipe.delete(f"{self.config.prefix}master:{self.name}")
            pipe.zrem(f"{self.config.prefix}masters", self.name)
            for sup in self.supervisors:
                pipe.delete(f"{self.config.prefix}supervisor:{sup.name}")
                pipe.zrem(f"{self.config.prefix}supervisors", sup.name)
            await pipe.execute()
        except Exception:
            pass

        logger.info(f"MasterSupervisor '{self.name}' has stopped.")

    # ── Status ───────────────────────────────────────────────────────────

    @property
    def status(self) -> str:
        if not self.running:
            return "stopped"
        if not self.working:
            return "paused"
        return "running"
