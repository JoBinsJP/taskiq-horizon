"""Tests for HorizonMiddleware and TaskPayload.

Covers:
- queue_name label is correctly read (not 'queue')
- task_name is present in the stored payload
- Horizon tracks jobs through pushed → reserved → completed/failed lifecycle
- Middleware falls back to 'default' when queue_name label is absent
"""

import json
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from taskiq.message import TaskiqMessage

from taskiq_horizon.config import HorizonConfig, TrimSettings
from taskiq_horizon.middleware import HorizonMiddleware
from taskiq_horizon.payload import TaskPayload
from taskiq_horizon.repositories.redis_job_repository import RedisJobRepository


# ── Helpers ───────────────────────────────────────────────────────────────────


def make_message(
    task_name: str = "memory:cognee_write",
    task_id: str = "test-task-id-001",
    labels: dict | None = None,
    kwargs: dict | None = None,
) -> TaskiqMessage:
    """Build a minimal TaskiqMessage for testing."""
    return TaskiqMessage(
        task_id=task_id,
        task_name=task_name,
        labels=labels or {},
        args=[],
        kwargs=kwargs or {},
    )


@pytest.fixture
def job_repo(redis, config):
    return RedisJobRepository(redis, config)


@pytest.fixture
def middleware(config, job_repo):
    metrics = MagicMock()
    metrics.increment_job = MagicMock(return_value=None)
    metrics.increment_queue = MagicMock(return_value=None)

    tags = MagicMock()
    tags.monitored = MagicMock(return_value=None)

    return HorizonMiddleware(config=config, jobs=job_repo, metrics=metrics, tags=tags)


# ── TaskPayload unit tests ────────────────────────────────────────────────────


class TestTaskPayload:
    """TaskPayload wraps a TaskiqMessage and produces a Horizon-compatible JSON blob."""

    def test_task_name_present_in_decoded(self, config):
        """task_name must be in decoded dict so Horizon can display it."""
        msg = make_message(task_name="memory:cognee_write")
        payload = TaskPayload(msg).prepare(config)

        assert payload.decoded["task_name"] == "memory:cognee_write"

    def test_display_name_matches_task_name(self, config):
        msg = make_message(task_name="memory:extract")
        payload = TaskPayload(msg).prepare(config)

        assert payload.display_name() == "memory:extract"
        assert payload.command_name() == "memory:extract"

    def test_id_matches_task_id(self, config):
        msg = make_message(task_id="abc-123")
        payload = TaskPayload(msg).prepare(config)

        assert payload.id() == "abc-123"

    def test_value_is_valid_json(self, config):
        msg = make_message()
        payload = TaskPayload(msg).prepare(config)

        parsed = json.loads(payload.value)
        assert parsed["task_name"] == "memory:cognee_write"
        assert parsed["uuid"] == "test-task-id-001"

    def test_silenced_flag_false_by_default(self, config):
        msg = make_message()
        payload = TaskPayload(msg).prepare(config)

        assert payload.is_silenced() is False

    def test_silenced_flag_true_when_task_in_silenced_list(self, config):
        config.silenced = ["memory:cognee_write"]
        msg = make_message(task_name="memory:cognee_write")
        payload = TaskPayload(msg).prepare(config)

        assert payload.is_silenced() is True

    def test_is_retry_false_by_default(self, config):
        msg = make_message()
        payload = TaskPayload(msg).prepare(config)

        assert payload.is_retry() is False


# ── HorizonMiddleware queue_name label tests ──────────────────────────────────


class TestMiddlewareQueueLabel:
    """Middleware must read 'queue_name' from labels (TaskIQ convention), not 'queue'."""

    @pytest.mark.asyncio
    async def test_pre_send_reads_queue_name_label(self, middleware, job_repo):
        """Jobs dispatched to 'low' queue must be tracked as 'low', not 'default'."""
        msg = make_message(
            task_id="job-low-001",
            labels={"queue_name": "low", "max_retries": 3},
        )
        await middleware.pre_send(msg)

        jobs = await job_repo.get_pending()
        assert len(jobs) == 1
        assert jobs[0]["queue"] == "low"

    @pytest.mark.asyncio
    async def test_pre_send_reads_high_queue_label(self, middleware, job_repo):
        """Jobs dispatched to 'high' queue must be tracked as 'high'."""
        msg = make_message(
            task_id="job-high-001",
            labels={"queue_name": "high"},
        )
        await middleware.pre_send(msg)

        jobs = await job_repo.get_pending()
        assert jobs[0]["queue"] == "high"

    @pytest.mark.asyncio
    async def test_pre_send_falls_back_to_default_when_no_label(self, middleware, job_repo):
        """When queue_name label is absent, fallback is 'default'."""
        msg = make_message(task_id="job-default-001", labels={})
        await middleware.pre_send(msg)

        jobs = await job_repo.get_pending()
        assert jobs[0]["queue"] == "default"

    @pytest.mark.asyncio
    async def test_legacy_queue_label_NOT_used(self, middleware, job_repo):
        """Old 'queue' label key must NOT be used — TaskIQ uses 'queue_name'."""
        msg = make_message(
            task_id="job-wrong-label-001",
            # Only old 'queue' key set — should fall back to 'default', not read this
            labels={"queue": "high"},
        )
        await middleware.pre_send(msg)

        jobs = await job_repo.get_pending()
        # 'queue' label is ignored — should fall back to 'default'
        assert jobs[0]["queue"] == "default"


# ── HorizonMiddleware lifecycle tests ─────────────────────────────────────────


class TestMiddlewareLifecycle:
    """Jobs flow: pushed → reserved → completed/failed with correct status tracking."""

    @pytest.mark.asyncio
    async def test_full_success_lifecycle(self, middleware, job_repo):
        """Happy path: pre_send → pre_execute → post_execute moves job to completed."""
        msg = make_message(task_id="lifecycle-ok-001", labels={"queue_name": "default"})

        # Dispatch
        await middleware.pre_send(msg)
        assert await job_repo.count_pending() == 1

        # Worker picks up
        await middleware.pre_execute(msg)

        # Worker finishes
        result = MagicMock()
        result.is_err = False
        await middleware.post_execute(msg, result)

        assert await job_repo.count_pending() == 0
        assert await job_repo.count_completed() == 1

    @pytest.mark.asyncio
    async def test_failed_lifecycle(self, middleware, job_repo):
        """Failure path: pre_send → pre_execute → on_error moves job to failed."""
        msg = make_message(task_id="lifecycle-fail-001", labels={"queue_name": "default"})

        await middleware.pre_send(msg)
        await middleware.pre_execute(msg)

        result = MagicMock()
        result.is_err = True
        exc = RuntimeError("Something went wrong")
        await middleware.on_error(msg, result, exc)

        assert await job_repo.count_pending() == 0
        assert await job_repo.count_failed() == 1

    @pytest.mark.asyncio
    async def test_post_execute_skipped_on_error(self, middleware, job_repo):
        """post_execute must be a no-op for errored jobs (on_error already handled it)."""
        msg = make_message(task_id="lifecycle-skip-001", labels={"queue_name": "default"})

        await middleware.pre_send(msg)
        await middleware.pre_execute(msg)

        # Simulate on_error called first
        result_err = MagicMock()
        result_err.is_err = True
        await middleware.on_error(msg, result_err, RuntimeError("boom"))

        # Then post_execute called (TaskIQ may call both)
        result_ok = MagicMock()
        result_ok.is_err = True  # is_err=True → skips completed()
        await middleware.post_execute(msg, result_ok)

        # Still counted as failed, not double-counted
        assert await job_repo.count_failed() == 1
        assert await job_repo.count_completed() == 0

    @pytest.mark.asyncio
    async def test_task_name_stored_in_job_hash(self, middleware, job_repo):
        """The stored job must include the task name for Horizon display."""
        msg = make_message(
            task_name="memory:cognee_write",
            task_id="name-check-001",
            labels={"queue_name": "low"},
        )
        await middleware.pre_send(msg)

        jobs = await job_repo.get_pending()
        assert len(jobs) == 1
        # job_repo already parses payload into a dict
        payload = jobs[0]["payload"]
        assert payload["task_name"] == "memory:cognee_write"

