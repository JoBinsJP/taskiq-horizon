import subprocess
import sys
import time
from unittest.mock import MagicMock, patch

import psutil

from taskiq_horizon.worker_process import WorkerProcess


def test_worker_process_start_sets_queue_name_env(monkeypatch):
    monkeypatch.setenv("QUEUE_NAME", "default")
    monkeypatch.setenv("EXISTING_ENV", "keep-me")

    process = MagicMock()
    process.pid = 4321

    worker = WorkerProcess(
        broker_path="app.queue.broker:broker",
        queue="critical,default",
        worker_args=["--fs-discover"],
    )

    with patch("taskiq_horizon.worker_process.subprocess.Popen", return_value=process) as popen:
        worker.start()

    assert popen.call_args.kwargs["start_new_session"] is True
    assert worker.pid == 4321

    kwargs = popen.call_args.kwargs
    env = kwargs["env"]
    cmd = popen.call_args.args[0]

    assert env["QUEUE_NAME"] == "critical,default"
    assert env["EXISTING_ENV"] == "keep-me"
    assert cmd[:5] == [sys.executable, "-m", "taskiq", "worker", "app.queue.broker:broker"]
    assert cmd[-3:] == ["--fs-discover", "--workers", "1"]


def test_worker_process_start_uses_configurable_queue_env_var(monkeypatch):
    process = MagicMock()
    process.pid = 9876

    worker = WorkerProcess(
        broker_path="app.queue.broker:broker",
        queue="critical",
        queue_env_var="SQS_QUEUE_NAME",
    )

    with patch("taskiq_horizon.worker_process.subprocess.Popen", return_value=process) as popen:
        worker.start()

    env = popen.call_args.kwargs["env"]

    assert env["SQS_QUEUE_NAME"] == "critical"


def _dummy_worker() -> WorkerProcess:
    """A WorkerProcess wrapping a real dummy subprocess (sleep loop)."""
    worker = WorkerProcess(broker_path="test:broker")
    worker.process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True)
    worker.pid = worker.process.pid
    return worker


def _wait_for_status(pid: int, statuses: set, timeout: float = 5.0) -> str:
    deadline = time.time() + timeout
    status = ""
    while time.time() < deadline:
        status = psutil.Process(pid).status()
        if status in statuses:
            return status
        time.sleep(0.05)
    return status


def test_pause_and_resume_suspend_real_subprocess():
    """pause() SIGSTOPs the OS process; resume() SIGCONTs it."""
    worker = _dummy_worker()
    try:
        worker.pause()
        assert worker.stopped is True
        assert _wait_for_status(worker.pid, {psutil.STATUS_STOPPED}) == psutil.STATUS_STOPPED
        # Still "alive" from the pool's perspective while suspended
        assert worker.is_alive() is True

        worker.resume()
        assert worker.stopped is False
        status = _wait_for_status(worker.pid, {psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING})
        assert status in {psutil.STATUS_RUNNING, psutil.STATUS_SLEEPING}
    finally:
        worker.kill()
        worker.process.wait(timeout=5)


def test_terminate_wakes_stopped_subprocess_so_it_can_exit():
    """terminate() on a paused worker sends SIGCONT first so SIGTERM is delivered."""
    worker = _dummy_worker()
    try:
        worker.pause()
        assert _wait_for_status(worker.pid, {psutil.STATUS_STOPPED}) == psutil.STATUS_STOPPED

        worker.terminate()
        worker.process.wait(timeout=5)
        assert worker.is_alive() is False
    finally:
        worker.kill()
