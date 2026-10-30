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
        queue_env_var: str = "ALATA_WORKER_QUEUE",
        environment: str | None = None,
    ) -> None:
        self.broker_path = broker_path
        self.queue = queue
        self.worker_args = worker_args or []
        self.queue_env_var = queue_env_var
        self.environment = environment
        self.process: subprocess.Popen | None = None
        self.pid: int | None = None
        self.stopped = False  # True while suspended via SIGSTOP (paused)

    def _build_env(self) -> dict[str, str]:
        """
        Build the worker subprocess environment.

        Alata preserves ALATA_WORKER_QUEUE when loading application .env files.
        """
        env = os.environ.copy()
        if self.environment is not None:
            env["APP_ENV"] = self.environment
        env["QUEUE_NAME"] = self.queue
        env["ALATA_WORKER_QUEUE"] = self.queue
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
            start_new_session=True,
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
                os.killpg(self.pid, signal.SIGSTOP)
                self.stopped = True
                logger.debug(f"SIGSTOP sent to worker PID={self.pid}")
            except OSError as e:
                logger.warning(f"Failed to pause worker PID={self.pid}: {e}")

    def resume(self) -> None:
        """Resume a paused worker with SIGCONT."""
        if self.is_alive() and self.process:
            try:
                os.killpg(self.pid, signal.SIGCONT)
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
        """Kill the isolated process group, including orphaned TaskIQ children."""
        if self.pid:
            try:
                os.killpg(self.pid, signal.SIGKILL)
                logger.warning(f"SIGKILL sent to worker group PID={self.pid}")
            except ProcessLookupError:
                pass
            if self.process:
                try:
                    self.process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    logger.warning(f"Worker PID={self.pid} has not exited after SIGKILL")

    def is_alive(self) -> bool:
        """Check if the worker process is still running."""
        return self.process is not None and self.process.poll() is None

    def memory_usage_mb(self) -> float:
        """Measure the TaskIQ process tree, including the job executors."""
        if not self.pid:
            return 0.0
        try:
            import psutil

            proc = psutil.Process(self.pid)
            total = 0
            for member in [proc, *proc.children(recursive=True)]:
                try:
                    total += member.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            return total / (1024 * 1024)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return 0.0
