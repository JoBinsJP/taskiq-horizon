"""
Supervisor — ports the upstream supervisor loop.

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
from dataclasses import dataclass, field, replace

from taskiq_horizon.commands import COMMAND_MAP
from taskiq_horizon.contracts.command_queue import CommandQueue
from taskiq_horizon.event_dispatcher import EventDispatcher
from taskiq_horizon.events import SupervisorLooped
from taskiq_horizon.process_pool import ProcessPool

logger = logging.getLogger("horizon.supervisor")


@dataclass
class SupervisorOptions:
    """
    Configuration for a single supervisor — matches the upstream supervisor options.

    Each supervisor manages one queue (or comma-separated set of queues)
    with its own scaling and resource limits.
    """

    name: str = ""
    queue: str = "default"
    balance: str = "auto"
    auto_scale_by_num_jobs: bool = False
    auto_scaling_strategy: str = "time"
    max_processes: int = 10
    min_processes: int = 1
    memory: int = 128  # MB
    timeout: int = 60
    balance_cooldown: int = 3
    balance_max_shift: int = 1
    broker_path: str = ""
    worker_args: list[str] = field(default_factory=list)
    worker_queue_env_var: str = "ALATA_WORKER_QUEUE"

    def __post_init__(self) -> None:
        if self.balance is False:
            self.balance = "false"
        if self.balance not in {"auto", "simple", "false"}:
            raise ValueError("balance must be auto, simple, or false")
        if self.auto_scaling_strategy not in {"time", "size"}:
            raise ValueError("auto_scaling_strategy must be time or size")
        if not self.queue_names or len(set(self.queue_names)) != len(self.queue_names):
            raise ValueError("Configure distinct, non-empty queue names")
        for name in ("max_processes", "memory", "timeout", "balance_max_shift"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        for name in ("min_processes", "balance_cooldown"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        pools = len(self.queue_names) if self.balancing else 1
        if pools * self.min_processes > self.max_processes:
            raise ValueError("max_processes must cover min_processes for every queue pool")

    @property
    def balancing(self) -> bool:
        return self.balance in ("simple", "auto")

    @property
    def auto_scaling(self) -> bool:
        return self.balance != "simple"

    @property
    def queue_names(self) -> list[str]:
        return [queue.strip() for queue in self.queue.split(",") if queue.strip()]

    def with_queue(self, queue: str) -> "SupervisorOptions":
        return replace(self, queue=queue)


class Supervisor:
    """
    Manages one or more ProcessPools and responds to commands.

    Follows the upstream monitor loop:
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
        environment: str | None = None,
    ) -> None:
        self.options = options
        self.name = options.name
        self.command_queue = command_queue
        self.dispatcher = dispatcher
        self.autoscaler = autoscaler
        self.environment = environment

        self.process_pools = self._create_process_pools()
        self.pool = next(iter(self.process_pools.values()), None)
        self.working = True  # False when paused
        self.running = True  # False when terminated

        self._last_autoscale: float = 0.0
        self._needs_balance = False

    def _create_process_pools(self) -> dict[str, ProcessPool]:
        if self.options.balancing:
            queues = self.options.queue_names or [self.options.queue]
            return {
                queue: ProcessPool(
                    self.options.broker_path,
                    queue,
                    self.options.worker_args,
                    self.options.worker_queue_env_var,
                    environment=self.environment,
                )
                for queue in queues
            }

        return {
            self.options.queue: ProcessPool(
                self.options.broker_path,
                self.options.queue,
                self.options.worker_args,
                self.options.worker_queue_env_var,
                environment=self.environment,
            )
        }

    async def monitor(self) -> None:
        """
        Start the supervisor monitor loop.

        This is the main entry point — it runs until terminate() is called.
        """
        logger.info(f"Supervisor '{self.name}' starting monitor loop...")

        try:
            # Flush stale commands from a previous run
            await self.command_queue.flush(self.name)

            # Initial scaling mirrors Horizon: supervisors with an autoscaler
            # immediately scale their pools (per-queue when balancing, a single
            # pool sized by queue pressure otherwise), while supervisors without
            # one start scaled to the configured minimum.
            if self.running and self.autoscaler:
                await self._autoscale(force=True)
            elif self.running:
                self.scale(self.options.min_processes * len(self.process_pools))

            while self.running:
                # 1. Consume pending commands
                await self._process_commands()

                if not self.running:
                    break

                # 2. Run autoscaler (if enabled and not paused). Non-balancing
                #    supervisors scale their single pool between min_processes and
                #    max_processes based on queue depth.
                if self.working and self.autoscaler:
                    await self._autoscale()

                # 3. Monitor worker health + memory
                if self.working:
                    for pool in self.process_pools.values():
                        pool.monitor(memory_limit_mb=self.options.memory)

                # 4. Prune terminating workers
                self.prune_terminating_processes()

                # 5. Fire SupervisorLooped event
                await self.dispatcher.fire(SupervisorLooped(supervisor=self))

                # 6. Sleep
                await asyncio.sleep(1)

        finally:
            self.running = False
            self.working = False
            await self._shutdown()

    async def _shutdown(self) -> None:
        # Cleanup — terminate all workers gracefully
        logger.info(f"Supervisor '{self.name}' shutting down...")
        for pool in self.process_pools.values():
            pool.terminate_all()

        # Wait for drain with timeout
        deadline = time.monotonic() + self.options.timeout
        while self.total_process_count > 0:
            self.prune_terminating_processes()
            if time.monotonic() > deadline:
                logger.warning("Drain timeout exceeded. Force killing remaining workers.")
                for pool in self.process_pools.values():
                    pool.kill_all()
                break
            await asyncio.sleep(0.5)

        # Flush command queue on exit
        try:
            await self.command_queue.flush(self.name)
        except Exception:
            logger.exception("Failed to clean up supervisor commands")
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
                try:
                    handler_cls().process(self, options)
                except (TypeError, ValueError):
                    logger.warning("Invalid options for command %s: %r", cmd_name, options)
            else:
                logger.warning(f"Unknown command: {cmd_name}")

    async def _autoscale(self, *, force: bool = False) -> None:
        """Run the autoscaler with cooldown enforcement."""
        now = time.time()
        if not force and not self._needs_balance and (now - self._last_autoscale) < self.options.balance_cooldown:
            return

        try:
            await self.autoscaler.scale(self)
            self._last_autoscale = now
            self._needs_balance = False
        except Exception as e:
            logger.error(f"Autoscaler error: {e}")

    # ── Command handlers ─────────────────────────────────────────────────

    def pause(self) -> None:
        """
        Stop consuming new jobs while allowing in-flight work to drain.
        """
        self.working = False
        for pool in self.process_pools.values():
            pool.pause()
        logger.info(f"Supervisor '{self.name}' paused.")

    def continue_working(self) -> None:
        """Resume consuming jobs with the previous pool sizes."""
        self.working = True
        for pool in self.process_pools.values():
            pool.resume()
        logger.info(f"Supervisor '{self.name}' resumed.")

    def restart(self) -> None:
        """Restart all workers — terminate current and respawn."""
        logger.info(f"Supervisor '{self.name}' restarting workers...")
        self.working = True
        for pool in self.process_pools.values():
            pool.resume()
            pool.restart()

    def terminate(self, status: int = 0) -> None:
        """Gracefully terminate the supervisor."""
        self.running = False
        self.working = False
        logger.info(f"Supervisor '{self.name}' termination requested (status={status}).")

    def scale(self, target: int) -> None:
        """
        Scale to a specific total number of workers.

        When balancing is enabled, workers are distributed as evenly as
        possible across the queue-specific pools.
        """
        target = min(self.options.max_processes, max(0, int(target)))
        pool_count = max(1, len(self.process_pools))

        if pool_count == 1:
            next(iter(self.process_pools.values())).scale(target)
            return

        base, remainder = divmod(target, pool_count)
        balance = {}
        for index, queue in enumerate(self.process_pools):
            balance[queue] = base + (1 if index < remainder else 0)

        self.balance(balance)

    def balance(self, balance: dict[str, int]) -> None:
        """Scale the process pools to the provided per-queue counts."""
        remaining = self.options.max_processes
        for queue, scale in balance.items():
            pool = self.process_pools.get(queue)
            if pool is not None:
                target = min(remaining, max(0, int(scale)))
                pool.scale(target)
                remaining -= target

    def prune_terminating_processes(self) -> None:
        for pool in self.process_pools.values():
            pool.prune_terminating(timeout=self.options.timeout)

    @property
    def total_process_count(self) -> int:
        return sum(pool.total_process_count for pool in self.process_pools.values())

    @property
    def active_process_count(self) -> int:
        return sum(pool.active_process_count for pool in self.process_pools.values())

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
            "processes": {queue: pool.active_process_count for queue, pool in self.process_pools.items()},
            "options": {
                "queue": self.options.queue,
                "balance": self.options.balance,
                "maxProcesses": self.options.max_processes,
                "minProcesses": self.options.min_processes,
                "memory": self.options.memory,
                "timeout": self.options.timeout,
            },
        }
