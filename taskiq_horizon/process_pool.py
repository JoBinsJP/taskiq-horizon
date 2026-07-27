"""
ProcessPool — mirrors Laravel Horizon's ProcessPool.php.

Manages a set of WorkerProcesses for a single queue. Tracks
alive and terminating workers separately to enable graceful
scale-down without killing active jobs.
"""

import logging
import time

from taskiq_horizon.worker_process import WorkerProcess

logger = logging.getLogger("horizon.pool")


class ProcessPool:
    """
    A pool of WorkerProcesses for a single queue.

    Mirrors Laravel's ProcessPool which tracks:
    - Active workers (processing or idle)
    - Terminating workers (sent SIGTERM, waiting to drain)

    This separation is what enables zero-downtime scaling.
    """

    def __init__(
        self,
        broker_path: str,
        queue: str = "default",
        worker_args: list[str] | None = None,
        queue_env_var: str = "QUEUE_NAME",
    ) -> None:
        self.broker_path = broker_path
        self.queue = queue
        self.worker_args = worker_args or []
        self.queue_env_var = queue_env_var
        self.workers: list[WorkerProcess] = []
        self.terminating: list[tuple[WorkerProcess, float]] = []  # (worker, terminated_at)
        self.paused = False  # True while workers are suspended via SIGSTOP

    @property
    def total_process_count(self) -> int:
        """Total workers including those draining."""
        return len(self.workers) + len(self.terminating)

    @property
    def active_process_count(self) -> int:
        """Only actively working (non-terminating) workers."""
        return len(self.workers)

    def scale(self, target: int) -> None:
        """Scale the pool to the target number of active workers."""
        current = len(self.workers)

        if target > current:
            self._scale_up(target - current)
        elif target < current:
            self._scale_down(current - target)

    def _scale_up(self, count: int) -> None:
        """Spawn `count` new workers."""
        for _ in range(count):
            worker = WorkerProcess(self.broker_path, self.queue, self.worker_args, self.queue_env_var)
            worker.start()
            if self.paused:
                # Workers spawned while the pool is paused must not consume jobs.
                worker.pause()
            self.workers.append(worker)
        logger.info(f"Pool [{self.queue}] scaled UP by {count} → {len(self.workers)} active")

    def _scale_down(self, count: int) -> None:
        """
        Move `count` workers from active to terminating.

        Workers are sent SIGTERM and moved to the terminating list.
        They'll be pruned once they actually exit.
        """
        to_terminate = self.workers[-count:]
        self.workers = self.workers[:-count]

        now = time.time()
        for worker in to_terminate:
            worker.terminate()
            self.terminating.append((worker, now))

        logger.info(
            f"Pool [{self.queue}] scaled DOWN by {count} → {len(self.workers)} active, {len(self.terminating)} draining"
        )

    def prune_terminating(self, timeout: int = 60) -> None:
        """
        Remove terminated workers from the terminating list.

        Workers that have exited naturally are pruned. Workers that
        haven't exited within `timeout` seconds are force-killed.
        Mirrors Laravel's PruneTerminatingProcesses listener.
        """
        now = time.time()
        still_terminating = []

        for worker, terminated_at in self.terminating:
            if not worker.is_alive():
                logger.debug(f"Worker PID={worker.pid} drained cleanly.")
                continue

            elapsed = now - terminated_at
            if elapsed > timeout:
                logger.warning(
                    f"Worker PID={worker.pid} exceeded drain timeout ({elapsed:.0f}s > {timeout}s). Force killing."
                )
                worker.kill()
                continue

            still_terminating.append((worker, terminated_at))

        self.terminating = still_terminating

    def pause(self) -> None:
        """
        Pause the pool — suspend every active worker with SIGSTOP.

        Terminating workers are left alone so they can finish draining.
        Mirrors Laravel Horizon's real pause semantics: paused workers stop
        consuming jobs entirely.
        """
        self.paused = True
        for worker in self.workers:
            worker.pause()
        logger.info(f"Pool [{self.queue}] paused ({len(self.workers)} workers stopped)")

    def resume(self) -> None:
        """Resume the pool — wake every active worker with SIGCONT."""
        self.paused = False
        for worker in self.workers:
            worker.resume()
        logger.info(f"Pool [{self.queue}] resumed ({len(self.workers)} workers continued)")

    def monitor(self, memory_limit_mb: int = 128) -> None:
        """
        Check worker health and restart dead workers.

        Also checks memory usage and terminates workers that exceed
        the memory limit (mirrors ProcessInspector).
        """
        alive = []
        for worker in self.workers:
            if not worker.is_alive():
                logger.warning(f"Worker PID={worker.pid} died unexpectedly. Respawning...")
                new_worker = WorkerProcess(self.broker_path, self.queue, self.worker_args, self.queue_env_var)
                new_worker.start()
                if self.paused:
                    new_worker.pause()
                alive.append(new_worker)
                continue

            # Memory check
            mem = worker.memory_usage_mb()
            if mem > 0 and mem > memory_limit_mb:
                logger.warning(
                    f"Worker PID={worker.pid} exceeds memory limit ({mem:.1f}MB > {memory_limit_mb}MB). Terminating..."
                )
                worker.terminate()
                self.terminating.append((worker, time.time()))
                # Respawn immediately
                new_worker = WorkerProcess(self.broker_path, self.queue, self.worker_args, self.queue_env_var)
                new_worker.start()
                if self.paused:
                    new_worker.pause()
                alive.append(new_worker)
                continue

            alive.append(worker)

        self.workers = alive

    def terminate_all(self) -> None:
        """Gracefully terminate all workers (for shutdown)."""
        now = time.time()
        for worker in self.workers:
            worker.terminate()
            self.terminating.append((worker, now))
        self.workers = []

    def restart(self) -> None:
        """Restart the pool while preserving its active worker count."""
        current = self.active_process_count
        self.terminate_all()
        self.scale(current)

    def kill_all(self) -> None:
        """Force kill all workers (for hard shutdown)."""
        for worker in self.workers:
            worker.kill()
        for worker, _ in self.terminating:
            worker.kill()
        self.workers = []
        self.terminating = []
