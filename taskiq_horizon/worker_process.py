"""
WorkerProcess — mirrors Laravel Horizon's WorkerProcess.

A thin wrapper around a subprocess.Popen that represents a single
TaskIQ worker OS process. Separates concern of process management
from pool management.
"""

import logging
import subprocess
import sys
from typing import Optional

logger = logging.getLogger("horizon.process")


class WorkerProcess:
    """
    Wraps a single TaskIQ worker subprocess.

    Responsibilities:
    - Start/stop a single ``taskiq worker`` process.
    - Report whether the process is alive.
    - Expose the PID for memory inspection.
    """

    def __init__(self, broker_path: str, queue: str = "default", worker_args: Optional[list[str]] = None) -> None:
        self.broker_path = broker_path
        self.queue = queue
        self.worker_args = worker_args or []
        self.process: Optional[subprocess.Popen] = None
        self.pid: Optional[int] = None

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
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        self.pid = self.process.pid
        logger.info(f"Worker process started: PID={self.pid} queue={self.queue}")

    def terminate(self) -> None:
        """
        Send SIGTERM to the worker. Non-blocking — the worker will finish
        its current job before exiting. Use kill() if it doesn't stop.
        """
        if self.is_alive() and self.process:
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
