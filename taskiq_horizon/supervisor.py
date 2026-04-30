"""
Supervisor — mirrors Laravel Horizon's Supervisor.php.

Manages a ProcessPool, consumes commands from the CommandQueue, runs
the AutoScaler, and fires SupervisorLooped events on each loop iteration.

Lifecycle:
    supervisor = Supervisor(options, command_queue, dispatcher, pool)
    await supervisor.monitor()   # runs the 1-second loop
    supervisor.pause()           # from dashboard command
    supervisor.continue_working()
    supervisor.terminate(0)
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field

from taskiq_horizon.commands import COMMAND_MAP
from taskiq_horizon.contracts.command_queue import CommandQueue
from taskiq_horizon.event_dispatcher import EventDispatcher
from taskiq_horizon.events import SupervisorLooped
from taskiq_horizon.process_pool import ProcessPool

logger = logging.getLogger("horizon.supervisor")


@dataclass
class SupervisorOptions:
    """
    Configuration for a single supervisor — mirrors SupervisorOptions.php.

    Each supervisor manages one queue (or comma-separated set of queues)
    with its own scaling and resource limits.
    """

    name: str = ""
    connection: str = "redis"
    queue: str = "default"
    balance: str = "auto"
    auto_scale_by_num_jobs: bool = False
    auto_scaling_strategy: str = "time"
    max_processes: int = 10
    min_processes: int = 1
    memory: int = 128  # MB
    timeout: int = 60
    sleep: int = 3
    max_tries: int = 0
    max_time: int = 0
    max_jobs: int = 0
    nice: int = 0
    balance_cooldown: int = 3
    balance_max_shift: int = 1
    broker_path: str = ""
    worker_args: list[str] = field(default_factory=list)

    @property
    def balancing(self) -> bool:
        return self.balance in ("simple", "auto")

    @property
    def auto_scaling(self) -> bool:
        return self.balance != "simple"


class Supervisor:
    """
    Manages a ProcessPool and responds to commands.

    Mirrors the monitor loop in Laravel's Supervisor.php:
    1. Process pending commands from the Redis command queue
    2. Run the autoscaler (if enabled)
    3. Monitor worker health and memory
    4. Prune terminating processes
    5. Fire SupervisorLooped event
    6. Sleep 1 second
    """

    def __init__(
        self,
        options: SupervisorOptions,
        command_queue: CommandQueue,
        dispatcher: EventDispatcher,
        autoscaler=None,
    ) -> None:
        self.options = options
        self.name = options.name
        self.command_queue = command_queue
        self.dispatcher = dispatcher
        self.autoscaler = autoscaler

        self.pool = ProcessPool(options.broker_path, options.queue, options.worker_args)
        self.working = True  # False when paused
        self.running = True  # False when terminated

        self._last_autoscale: float = 0.0

    async def monitor(self) -> None:
        """
        Start the supervisor monitor loop.

        This is the main entry point — it runs until terminate() is called.
        """
        logger.info(f"Supervisor '{self.name}' starting monitor loop...")

        # Flush stale commands from a previous run
        await self.command_queue.flush(self.name)

        # Initial scale to min_processes
        self.pool.scale(self.options.min_processes)

        while self.running:
            # 1. Consume pending commands
            await self._process_commands()

            # 2. Monitor worker health + memory
            self.pool.monitor(memory_limit_mb=self.options.memory)

            # 3. Prune terminating workers
            self.pool.prune_terminating(timeout=self.options.timeout)

            # 4. Run autoscaler (if enabled and not paused)
            if self.working and self.autoscaler and self.options.balancing:
                await self._autoscale()

            # 5. Fire SupervisorLooped event
            await self.dispatcher.fire(SupervisorLooped(supervisor=self))

            # 6. Sleep
            await asyncio.sleep(1)

        # Cleanup — terminate all workers gracefully
        logger.info(f"Supervisor '{self.name}' shutting down...")
        self.pool.terminate_all()

        # Wait for drain with timeout
        deadline = time.time() + self.options.timeout
        while self.pool.total_process_count > 0:
            self.pool.prune_terminating(timeout=self.options.timeout)
            if time.time() > deadline:
                logger.warning("Drain timeout exceeded. Force killing remaining workers.")
                self.pool.kill_all()
                break
            await asyncio.sleep(0.5)

        # Flush command queue on exit
        await self.command_queue.flush(self.name)
        logger.info(f"Supervisor '{self.name}' has stopped.")

    async def _process_commands(self) -> None:
        """Consume and execute pending commands from the Redis queue."""
        try:
            commands = await self.command_queue.pending(self.name)
        except Exception as e:
            logger.error(f"Failed to read command queue: {e}")
            return

        for cmd_data in commands:
            cmd_name = cmd_data.get("command", "")
            options = cmd_data.get("options", {})

            handler_cls = COMMAND_MAP.get(cmd_name)
            if handler_cls:
                handler_cls().process(self, options)
            else:
                logger.warning(f"Unknown command: {cmd_name}")

    async def _autoscale(self) -> None:
        """Run the autoscaler with cooldown enforcement."""
        now = time.time()
        if (now - self._last_autoscale) < self.options.balance_cooldown:
            return

        try:
            target = await self.autoscaler.scale(self)
            target = max(self.options.min_processes, min(target, self.options.max_processes))

            # Apply balance_max_shift to limit per-cycle change
            current = self.pool.active_process_count
            diff = target - current
            if abs(diff) > self.options.balance_max_shift:
                diff = self.options.balance_max_shift if diff > 0 else -self.options.balance_max_shift
                target = current + diff

            if target != current:
                self.pool.scale(target)

            self._last_autoscale = now
        except Exception as e:
            logger.error(f"Autoscaler error: {e}")

    # ── Command handlers ─────────────────────────────────────────────────

    def pause(self) -> None:
        """Pause the supervisor — workers finish current jobs but don't pick up new ones."""
        self.working = False
        logger.info(f"Supervisor '{self.name}' paused.")

    def continue_working(self) -> None:
        """Resume the supervisor — workers start picking up new jobs."""
        self.working = True
        logger.info(f"Supervisor '{self.name}' resumed.")

    def restart(self) -> None:
        """Restart all workers — terminate current and respawn."""
        logger.info(f"Supervisor '{self.name}' restarting workers...")
        self.pool.terminate_all()
        self.pool.scale(self.options.min_processes)

    def terminate(self, status: int = 0) -> None:
        """Gracefully terminate the supervisor."""
        self.running = False
        self.working = False
        logger.info(f"Supervisor '{self.name}' termination requested (status={status}).")

    def scale(self, target: int) -> None:
        """Scale to a specific number of workers."""
        target = max(self.options.min_processes, min(target, self.options.max_processes))
        self.pool.scale(target)

    # ── Status ───────────────────────────────────────────────────────────

    @property
    def status(self) -> str:
        if not self.running:
            return "stopped"
        if not self.working:
            return "paused"
        return "running"

    def to_dict(self) -> dict:
        """Serialize for Redis heartbeat and API responses."""
        return {
            "name": self.name,
            "status": self.status,
            "pid": os.getpid(),
            "processes": {self.options.queue: self.pool.active_process_count},
            "options": {
                "queue": self.options.queue,
                "balance": self.options.balance,
                "maxProcesses": self.options.max_processes,
                "minProcesses": self.options.min_processes,
                "memory": self.options.memory,
                "timeout": self.options.timeout,
            },
        }
