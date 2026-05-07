import sys
from unittest.mock import MagicMock, patch

from taskiq_horizon.worker_process import WorkerProcess


def test_worker_process_start_sets_queue_name_env(monkeypatch):
    monkeypatch.setenv("QUEUE_NAME", "default")
    monkeypatch.setenv("EXISTING_ENV", "keep-me")

    process = MagicMock()
    process.pid = 4321

    worker = WorkerProcess(
        broker_path="app.core.queue.broker:broker",
        queue="critical,default",
        worker_args=["--fs-discover"],
    )

    with patch("taskiq_horizon.worker_process.subprocess.Popen", return_value=process) as popen:
        worker.start()

    assert worker.pid == 4321

    kwargs = popen.call_args.kwargs
    env = kwargs["env"]
    cmd = popen.call_args.args[0]

    assert env["QUEUE_NAME"] == "critical,default"
    assert env["EXISTING_ENV"] == "keep-me"
    assert cmd[:5] == [sys.executable, "-m", "taskiq", "worker", "app.core.queue.broker:broker"]
    assert cmd[-3:] == ["--fs-discover", "--workers", "1"]


def test_worker_process_start_uses_configurable_queue_env_var(monkeypatch):
    process = MagicMock()
    process.pid = 9876

    worker = WorkerProcess(
        broker_path="app.core.queue.broker:broker",
        queue="critical",
        queue_env_var="SQS_QUEUE_NAME",
    )

    with patch("taskiq_horizon.worker_process.subprocess.Popen", return_value=process) as popen:
        worker.start()

    env = popen.call_args.kwargs["env"]

    assert env["SQS_QUEUE_NAME"] == "critical"
