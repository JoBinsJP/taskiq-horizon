"""Exercise Horizon against Alata's real kernel, broker and retry lifecycle."""

import asyncio
import importlib
import os
import sys
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest
from alata.application import Application
from alata.container import ServiceContainer
from alata.foundation.console.application import ConsoleApplication
from alata.providers.queue_provider import Provider as QueueProvider
from alata.queue.batch_middleware import BatchMiddleware
from alata.queue.brokers.sync_broker import SyncBroker
from alata.queue.retry_middleware import RetryMiddleware
from alata.support.service_provider import ServiceProvider
from cleo.testers.command_tester import CommandTester
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from taskiq import SimpleRetryMiddleware
from taskiq.exceptions import NoResultError
from taskiq.labels import LabelType
from taskiq.message import TaskiqMessage

from taskiq_horizon.alata_provider import HorizonProvider
from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.event_dispatcher import EventDispatcher
from taskiq_horizon.jobs.retry_failed_job import RetryFailedJob
from taskiq_horizon.master_supervisor import MasterSupervisor
from taskiq_horizon.middleware import HorizonMiddleware
from taskiq_horizon.payload import TaskPayload
from taskiq_horizon.repositories.redis_job_repository import RedisJobRepository
from taskiq_horizon.repositories.redis_metrics_repository import RedisMetricsRepository
from taskiq_horizon.repositories.redis_tag_repository import RedisTagRepository
from taskiq_horizon.supervisor import SupervisorOptions
from taskiq_horizon.worker_process import WorkerProcess


@pytest.fixture
def kernel(tmp_path, monkeypatch):
    """A real custom-config Alata app, without the user's app or .env."""
    previous = ServiceContainer._instance
    package_name = f"audit_config_{uuid4().hex}"
    package = tmp_path / package_name
    package.mkdir()
    files = {
        "__init__.py": "",
        "settings.py": "from types import SimpleNamespace\nsettings = SimpleNamespace(ENVIRONMENT='local')\n",
        "database.py": "CONNECTIONS = {}\nDB_CONFIG = {}\n",
        "cache.py": "CACHE_CONFIG = {'default': 'null'}\n",
        "logging.py": "LOG_CONFIG = {'default': 'null', 'channels': {'null': {'driver': 'null'}}}\n",
        "queue.py": """import os
QUEUE_CONFIG = {
    'default': os.getenv('QUEUE_CONNECTION', 'redis'),
    'queue': os.getenv('QUEUE_NAME', 'default'),
    'max_retries': 3, 'retry_delay': 0,
    'connections': {
        'redis': {'broker_url': 'redis://localhost:6379/1', 'result_url': 'redis://localhost:6379/1'},
        'database': {},
        'sqs': {'queue_name': 'default', 'endpoint_url': None, 'result_bucket': None},
    },
}
""",
    }
    for name, contents in files.items():
        (package / name).write_text(contents)
    (tmp_path / ".env").write_text("APP_ENV=testing\nQUEUE_NAME=default\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    for key in ("APP_ENV", "QUEUE_NAME", "ALATA_WORKER_QUEUE", "QUEUE_CONNECTION"):
        monkeypatch.delenv(key, raising=False)
    # Track dotenv writes for restoration as well as fixture's own env changes.
    monkeypatch.setenv("APP_ENV", "testing")
    monkeypatch.setenv("QUEUE_NAME", "default")

    def build(*providers):
        return Application(base_path=tmp_path, config_module=package_name, providers=[QueueProvider, *providers])

    build.package = package
    yield build
    ServiceContainer.set_instance(previous)
    for name in list(sys.modules):
        if name == package_name or name.startswith(package_name + "."):
            sys.modules.pop(name)


@pytest.mark.parametrize("connection", ["redis", "database", "sqs"])
@pytest.mark.parametrize("queue_env_var", ["ALATA_WORKER_QUEUE", "QUEUE_NAME", "CUSTOM_QUEUE"])
def test_worker_assignment_survives_alata_environment_loading(kernel, monkeypatch, connection, queue_env_var):
    monkeypatch.setenv("QUEUE_CONNECTION", connection)
    env = WorkerProcess("bootstrap.broker:broker", queue="critical", queue_env_var=queue_env_var)._build_env()
    for key in ("QUEUE_NAME", "ALATA_WORKER_QUEUE", queue_env_var):
        monkeypatch.setenv(key, env[key])
    app = kernel()
    from taskiq_horizon.queue_backend import queue_backend

    backend = queue_backend(app.make("queue.broker"))
    message = TaskiqMessage(task_id="test", task_name="test", labels={}, args=[], kwargs={})
    assert backend.queue_for(message) == "critical"
    assert os.environ["QUEUE_NAME"] == "default"  # The .env was actually loaded.


def services(redis, config):
    jobs = RedisJobRepository(redis, config)
    tags = RedisTagRepository(redis, config)
    middleware = HorizonMiddleware(config, jobs, RedisMetricsRepository(redis, config), tags)
    return jobs, tags, middleware


@pytest.mark.asyncio
async def test_success_after_automatic_retry_is_never_a_failed_job(redis, config):
    jobs, _, middleware = services(redis, config)
    broker = SyncBroker().with_middlewares(RetryMiddleware(), BatchMiddleware(), middleware)
    calls = 0

    @broker.task(task_name="audit:flaky", retry_on_error=True, max_retries=3)
    async def flaky():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary")
        return "success"

    try:
        task = await flaky.kiq()
        assert (await task.wait_result()).return_value == "success"
        assert calls == 2
        assert await jobs.total_failed() == 0
        assert await jobs.count_recently_failed() == 0
        assert await jobs.count_completed() == 1
        assert await jobs.count_pending() == 0
        stored = (await jobs.get_jobs([task.task_id]))[0]
        assert stored["payload"]["attempts"] == 2
        assert stored["connection"] == "sync"
        assert middleware.stopwatches == {}
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exception,types,expected_calls,expected_failures",
    [
        (RuntimeError("exhausted"), None, 3, 1),
        (TypeError("excluded"), [ValueError], 1, 1),
        (NoResultError(), None, 1, 0),
    ],
)
async def test_retry_policy_and_control_signals(redis, config, exception, types, expected_calls, expected_failures):
    jobs, _, middleware = services(redis, config)
    retry = RetryMiddleware(types_of_exceptions=types)
    retry._persist_failed_job = AsyncMock()
    broker = SyncBroker().with_middlewares(retry, middleware)
    calls = 0

    @broker.task(task_name="audit:failure", retry_on_error=True, max_retries=3)
    async def failing():
        nonlocal calls
        calls += 1
        raise exception

    try:
        if types:
            with pytest.raises(TypeError):
                await failing.kiq()
        else:
            await failing.kiq()
        assert calls == expected_calls
        assert await jobs.total_failed() == expected_failures
        assert await jobs.count_pending() == 0
        assert middleware.stopwatches == {}
        if expected_calls == 3:
            retry._persist_failed_job.assert_awaited_once()
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_type", [RetryMiddleware, SimpleRetryMiddleware])
@pytest.mark.parametrize("retryable", [True, False])
async def test_per_task_exception_filter_matches_horizon_status(redis, config, retry_type, retryable):
    jobs, _, middleware = services(redis, config)
    broker = SyncBroker().with_middlewares(retry_type(types_of_exceptions=[KeyError]), middleware)
    calls = []

    @broker.task(retry_on_error=True, types_of_exceptions=[ValueError] if retryable else [], max_retries=2)
    async def task():
        calls.append(1)
        if len(calls) == 1:
            raise ValueError("task-specific policy")
        return "done"

    try:
        if retryable:
            dispatched = await task.kiq()
            assert (await dispatched.wait_result()).return_value == "done"
            assert len(calls) == 2
            assert await jobs.count_completed() == 1
            assert await jobs.total_failed() == 0
        else:
            with pytest.raises(ValueError, match="task-specific policy"):
                await task.kiq()
            assert len(calls) == 1
            assert await jobs.total_failed() == 1
        assert await jobs.count_pending() == 0
        assert middleware.stopwatches == {}
    finally:
        await broker.shutdown()


@pytest.mark.asyncio
async def test_dashboard_retry_restores_full_retry_budget(redis, config):
    jobs, tags, middleware = services(redis, config)
    broker = SyncBroker().with_middlewares(RetryMiddleware(), middleware)
    calls = 0

    @broker.task(task_name="audit:manual")
    async def flaky():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary")

    exhausted = TaskiqMessage(
        task_id="parent",
        task_name="audit:manual",
        args=[],
        kwargs={},
        labels={"retry_on_error": "True", "max_retries": "3", "_retries": "2"},
        labels_types={"retry_on_error": LabelType.BOOL, "max_retries": LabelType.INT, "_retries": LabelType.INT},
    )
    await jobs.failed(RuntimeError("old failure"), "sync", "default", TaskPayload(exhausted).prepare(config))
    try:
        retry_id = await RetryFailedJob(broker, jobs, tags, config).handle("parent")
        assert calls == 2
        assert (await jobs.get_jobs([retry_id]))[0]["status"] == "completed"
        assert await jobs.total_failed() == 1  # Only the original permanent failure.
        parent = (await jobs.get_jobs(["parent"]))[0]
        assert parent["retried_by"][0]["status"] == "completed"
    finally:
        await broker.shutdown()


class HttpProvider(ServiceProvider):
    def register(self):
        self.app.use_fastapi(FastAPI())


@pytest.mark.asyncio
async def test_native_provider_dashboard_and_shutdown(kernel, redis, monkeypatch):
    config = HorizonConfig()
    close = AsyncMock(wraps=redis.aclose)
    monkeypatch.setattr(redis, "aclose", close)
    with patch("taskiq_horizon.alata_provider.Redis.from_url", return_value=redis):
        app = kernel((HorizonProvider, config), HttpProvider)
        provider = next(p for p in app.providers if isinstance(p, HorizonProvider))
        provider.boot()  # Idempotent middleware attachment.
        broker = app.make("queue.broker")
        assert sum(isinstance(m, HorizonMiddleware) for m in broker.middlewares) == 1
        assert ConsoleApplication(app).find("horizon").container is app
        async with AsyncClient(transport=ASGITransport(app=app.fastapi), base_url="http://test") as client:
            for path in ("/horizon", "/horizon/api/stats", "/horizon/api/masters"):
                assert (await client.get(path)).status_code == 200
        await app.shutdown_providers()
        close.assert_awaited_once()


def test_native_provider_loads_custom_config_without_creating_http_app(kernel, redis):
    (kernel.package / "horizon.py").write_text(
        "from taskiq_horizon.config import HorizonConfig\nhorizon_config = HorizonConfig()\n"
    )
    importlib.invalidate_caches()
    with patch("taskiq_horizon.alata_provider.Redis.from_url", return_value=redis):
        app = kernel(HorizonProvider)
        assert app.make("horizon.config").path == "horizon"
        assert app._fastapi is None


def test_horizon_command_uses_current_kernel_environment(kernel, redis):
    config = HorizonConfig(environments={"testing": {"test": SupervisorOptions(broker_path="bootstrap.broker:broker")}})
    with patch("taskiq_horizon.alata_provider.Redis.from_url", return_value=redis):
        app = kernel((HorizonProvider, config))
        assert app.make("settings").ENVIRONMENT == "local"
        with patch("taskiq_horizon.master_supervisor.MasterSupervisor") as master:
            master.return_value.monitor = AsyncMock()
            command = ConsoleApplication(app).find("horizon")
            assert CommandTester(command).execute("") == 0
            assert master.call_args.kwargs["environment"] == "testing"
            master.return_value.monitor.assert_awaited_once()


@pytest.mark.parametrize("connection", ["sync", "null"])
def test_horizon_rejects_inline_worker_connections(kernel, redis, monkeypatch, connection):
    monkeypatch.setenv("QUEUE_CONNECTION", connection)
    with patch("taskiq_horizon.alata_provider.Redis.from_url", return_value=redis):
        app = kernel((HorizonProvider, HorizonConfig()))
        tester = CommandTester(ConsoleApplication(app).find("horizon"))
        assert tester.execute("") == 1
        assert "require a Redis, database, or SQS queue" in tester.io.fetch_error()


def test_horizon_rejects_missing_environment(kernel, redis):
    with patch("taskiq_horizon.alata_provider.Redis.from_url", return_value=redis):
        app = kernel((HorizonProvider, HorizonConfig()))
        tester = CommandTester(ConsoleApplication(app).find("horizon"))
        assert tester.execute("") == 1
        assert "environment 'testing'" in tester.io.fetch_error()


def test_master_passes_active_environment_through_to_worker_boot(kernel, redis, monkeypatch):
    monkeypatch.delenv("PYTEST_CURRENT_TEST")
    config = HorizonConfig(environments={"production": {"prod": SupervisorOptions(queue="critical")}})
    master = MasterSupervisor(config, redis, EventDispatcher(), environment="production")
    pool = master.supervisors[0].process_pools["critical"]
    with patch.object(WorkerProcess, "start"):
        pool.scale(1)
    env = pool.workers[0]._build_env()
    assert env["APP_ENV"] == "production"
    for key in ("APP_ENV", "QUEUE_NAME", "ALATA_WORKER_QUEUE"):
        monkeypatch.setenv(key, env[key])
    app = kernel()
    assert app.env == "production"
    assert app.make("queue.broker").queue_name == "critical"


@pytest.mark.skipif(not os.getenv("HORIZON_TEST_REDIS_URL"), reason="Set HORIZON_TEST_REDIS_URL for live Redis")
def test_repeated_snapshot_commands_release_event_loop_connections(kernel):
    config = HorizonConfig(redis_url=os.environ["HORIZON_TEST_REDIS_URL"], prefix=f"horizon-test:{uuid4().hex}:")
    app = kernel((HorizonProvider, config))
    command = ConsoleApplication(app).find("horizon:snapshot")
    try:
        assert CommandTester(command).execute("") == 0
        assert CommandTester(command).execute("") == 0
    finally:

        async def cleanup():
            redis = app.make("horizon.redis")
            keys = [key async for key in redis.scan_iter(match=f"{config.prefix}*")]
            if keys:
                await redis.delete(*keys)
            await app.shutdown_providers()

        asyncio.run(cleanup())


@pytest.mark.parametrize("command_name", ["pause", "continue", "terminate"])
def test_console_controls_target_master(kernel, redis, command_name):
    with patch("taskiq_horizon.alata_provider.Redis.from_url", return_value=redis):
        app = kernel((HorizonProvider, HorizonConfig()))
        masters = AsyncMock()
        masters.all_masters.return_value = [{"name": "master-1", "supervisors": ["child-1"]}]
        queue = AsyncMock()
        app.instance("horizon.masters", masters)
        app.instance("horizon.command_queue", queue)
        tester = CommandTester(ConsoleApplication(app).find(f"horizon:{command_name}"))
        assert tester.execute("") == 0
        queue.push.assert_awaited_once_with("master-1", command_name)
