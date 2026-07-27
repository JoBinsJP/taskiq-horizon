"""
WorkerProcess — ports the upstream worker process wrapper.

A thin wrapper around a subprocess.Popen that represents a single
TaskIQ worker OS process. Separates concern of process management
from pool management.
"""

import logging
import os
import signal
import subprocess
import sys

logger = logging.getLogger("horizon.process")


class WorkerProcess:
    """
    Wraps a single TaskIQ worker subprocess.

    Responsibilities:
    - Start/stop a single ``taskiq worker`` process.
    - Report whether the process is alive.
    - Expose the PID for memory inspection.
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
        self.process: subprocess.Popen | None = None
        self.pid: int | None = None
        self.stopped = False  # True while suspended via SIGSTOP (paused)

    def _build_env(self) -> dict[str, str]:
        """
        Build the worker subprocess environment.

        TODO: if Horizon grows first-class non-Redis broker support, replace
        this single env-var hook with a broker-specific worker targeting
        strategy. SQS, for example, uses different queue selection semantics
        than Redis' QUEUE_NAME-based multi-queue worker boot.
        """
        env = os.environ.copy()
        env[self.queue_env_var] = self.queue
        return env

    def start(self) -> None:
        """Spawn the worker subprocess."""
        cmd = [
            sys.executable,
            "-m",
            "taskiq",
            "worker",
            self.broker_path,
        ]
        cmd.extend(self.worker_args)
        cmd.extend(["--workers", "1"])

        self.process = subprocess.Popen(
            cmd,
            env=self._build_env(),
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        self.pid = self.process.pid
        logger.info(f"Worker process started: PID={self.pid} queue={self.queue}")

    def pause(self) -> None:
        """
        Suspend the worker with SIGSTOP so it stops consuming jobs.

        The process is frozen by the kernel (it cannot ignore SIGSTOP) and
        resumes exactly where it left off when resume() sends SIGCONT.
        """
        if self.is_alive() and self.process:
            try:
                self.process.send_signal(signal.SIGSTOP)
                self.stopped = True
                logger.debug(f"SIGSTOP sent to worker PID={self.pid}")
            except OSError as e:
                logger.warning(f"Failed to pause worker PID={self.pid}: {e}")

    def resume(self) -> None:
        """Resume a paused worker with SIGCONT."""
        if self.is_alive() and self.process:
            try:
                self.process.send_signal(signal.SIGCONT)
                logger.debug(f"SIGCONT sent to worker PID={self.pid}")
            except OSError as e:
                logger.warning(f"Failed to resume worker PID={self.pid}: {e}")
        self.stopped = False

    def terminate(self) -> None:
        """
        Send SIGTERM to the worker. Non-blocking — the worker will finish
        its current job before exiting. Use kill() if it doesn't stop.
        """
        if self.is_alive() and self.process:
            # A SIGSTOPped process cannot handle SIGTERM — wake it first.
            if self.stopped:
                self.resume()
            try:
                self.process.terminate()
                logger.debug(f"SIGTERM sent to worker PID={self.pid}")
            except OSError as e:
                logger.warning(f"Failed to terminate worker PID={self.pid}: {e}")

    def kill(self) -> None:
        """Force-kill the worker immediately."""
        if self.process and self.process.poll() is None:
            try:
                self.process.kill()
                logger.warning(f"SIGKILL sent to worker PID={self.pid}")
            except OSError:
                pass

    def is_alive(self) -> bool:
        """Check if the worker process is still running."""
        return self.process is not None and self.process.poll() is None

    def memory_usage_mb(self) -> float:
        """Get the worker's RSS memory usage in megabytes."""
        if not self.pid:
            return 0.0
        try:
            import psutil

            proc = psutil.Process(self.pid)
            return proc.memory_info().rss / (1024 * 1024)
        except (ImportError, Exception):
            return 0.0
