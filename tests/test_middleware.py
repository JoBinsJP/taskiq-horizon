"""Tests for HorizonMiddleware and TaskPayload.

Covers:
- queue_name label is correctly read (not 'queue')
- task_name is present in the stored payload
- Horizon tracks jobs through pushed → reserved → completed/failed lifecycle
- Middleware falls back to 'default' when queue_name label is absent
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from taskiq.message import TaskiqMessage

from taskiq_horizon.middleware import HorizonMiddleware
from taskiq_horizon.payload import TaskPayload
from taskiq_horizon.repositories.redis_job_repository import RedisJobRepository
from taskiq_horizon.repositories.redis_tag_repository import RedisTagRepository

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
def tag_repo(redis, config):
    return RedisTagRepository(redis, config)


@pytest.fixture
def middleware(config, job_repo, tag_repo):
    metrics = MagicMock()
    metrics.increment_job = AsyncMock(return_value=None)
    metrics.increment_queue = AsyncMock(return_value=None)

    return HorizonMiddleware(config=config, jobs=job_repo, metrics=metrics, tags=tag_repo)


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

    def test_retry_state_is_embedded_for_retries(self, config):
        msg = make_message(labels={"queue_name": "default"}, kwargs={"customer_id": 42})
        payload = TaskPayload(msg).prepare(config)

        parsed = json.loads(payload.value)
        assert parsed["taskiq"]["state"]

    def test_is_retry_false_by_default(self, config):
        msg = make_message()
        payload = TaskPayload(msg).prepare(config)

        assert payload.is_retry() is False

    def test_retry_of_is_read_from_labels(self, config):
        msg = make_message(labels={"retry_of": "parent-job-id"})
        payload = TaskPayload(msg).prepare(config)

        assert payload.is_retry() is True
        assert payload.retry_of() == "parent-job-id"

    def test_tags_comma_separated_string_is_canonical(self, config):
        """tags='a,b' (the canonical format, since taskiq stringifies labels) is split."""
        msg = make_message(labels={"tags": "tenant:42, billing"})
        payload = TaskPayload(msg).prepare(config)

        assert payload.tags() == ["tenant:42", "billing"]

    def test_tags_stringified_list_is_parsed(self, config):
        """taskiq stringifies list labels — "['a', 'b']" must parse back to elements."""
        msg = make_message(labels={"tags": "['tenant:42', 'billing']"})
        payload = TaskPayload(msg).prepare(config)

        assert payload.tags() == ["tenant:42", "billing"]

    def test_tags_real_list_still_works(self, config):
        msg = make_message(labels={"tags": ["tenant:42", "billing"]})
        payload = TaskPayload(msg).prepare(config)

        assert payload.tags() == ["tenant:42", "billing"]

    def test_tags_single_plain_string(self, config):
        msg = make_message(labels={"tags": "tenant:42"})
        payload = TaskPayload(msg).prepare(config)

        assert payload.tags() == ["tenant:42"]

    def test_tags_malformed_bracket_string_falls_back_to_comma_split(self, config):
        """An unparseable bracketed string falls back to comma splitting, not a crash."""
        msg = make_message(labels={"tags": "[not, valid python]"})
        payload = TaskPayload(msg).prepare(config)

        assert payload.tags() == ["[not", "valid python]"]

    def test_tags_absent_yields_empty_list(self, config):
        msg = make_message(labels={})
        payload = TaskPayload(msg).prepare(config)

        assert payload.tags() == []


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

    @pytest.mark.asyncio
    async def test_failed_monitored_tags_are_recorded(self, middleware, job_repo, tag_repo):
        await tag_repo.monitor("tenant:42")
        msg = make_message(
            task_id="tagged-failure-001",
            labels={"queue_name": "default", "tags": ["tenant:42"]},
        )

        await middleware.pre_send(msg)
        await middleware.pre_execute(msg)

        result = MagicMock()
        result.is_err = True
        await middleware.on_error(msg, result, RuntimeError("boom"))

        assert await tag_repo.count("tenant:42") == 1
        assert await tag_repo.count("failed:tenant:42") == 1

    @pytest.mark.asyncio
    async def test_stringified_list_tags_still_match_monitoring(self, middleware, job_repo, tag_repo):
        """Tags mangled into "['...']" by taskiq's label stringification still match monitors."""
        await tag_repo.monitor("tenant:42")
        msg = make_message(
            task_id="stringified-tags-001",
            labels={"queue_name": "default", "tags": "['tenant:42']"},
        )

        await middleware.pre_send(msg)

        assert await tag_repo.count("tenant:42") == 1

    @pytest.mark.asyncio
    async def test_failed_job_records_stacktrace(self, middleware, job_repo):
        """The failure path stores the full traceback for the dashboard stacktrace panel."""
        msg = make_message(task_id="stacktrace-001", labels={"queue_name": "default"})

        await middleware.pre_send(msg)
        await middleware.pre_execute(msg)

        def _explode():
            raise ValueError("kaboom")

        try:
            _explode()
        except ValueError as exc:
            caught = exc

        result = MagicMock()
        result.is_err = True
        await middleware.on_error(msg, result, caught)

        failed_job = await job_repo.find_failed("stacktrace-001")
        assert "Traceback (most recent call last):" in failed_job["exception"]
        assert "_explode" in failed_job["exception"]
        assert "ValueError: kaboom" in failed_job["exception"]

    @pytest.mark.asyncio
    async def test_pushed_timestamp_survives_failure_transition(self, middleware, job_repo):
        msg = make_message(task_id="timestamp-failure-001", labels={"queue_name": "default"})

        await middleware.pre_send(msg)
        pending_job = (await job_repo.get_pending())[0]
        pushed_at = pending_job["payload"]["pushedAt"]

        await middleware.pre_execute(msg)
        result = MagicMock()
        result.is_err = True
        await middleware.on_error(msg, result, RuntimeError("boom"))

        failed_job = await job_repo.find_failed("timestamp-failure-001")
        assert failed_job["payload"]["pushedAt"] == pushed_at
