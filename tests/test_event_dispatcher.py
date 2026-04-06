"""Tests for EventDispatcher — the core pub/sub bus."""

import asyncio
import pytest

from taskiq_horizon.event_dispatcher import EventDispatcher
from taskiq_horizon.events import (
    JobCompleted,
    JobPushed,
    LongWaitDetected,
    MasterSupervisorLooped,
    SupervisorLooped,
)


class TestEventDispatcher:
    """Unit tests for the lightweight event dispatcher."""

    @pytest.mark.asyncio
    async def test_listen_and_fire_sync(self, dispatcher):
        """Sync listeners are called when the matching event fires."""
        called_with = []

        def listener(event):
            called_with.append(event)

        dispatcher.listen(JobPushed, listener)

        event = JobPushed(payload="test", queue="default")
        await dispatcher.fire(event)

        assert len(called_with) == 1
        assert called_with[0] is event

    @pytest.mark.asyncio
    async def test_fire_async_listener(self, dispatcher):
        """Async listeners are properly awaited."""
        called_with = []

        async def listener(event):
            await asyncio.sleep(0.01)
            called_with.append(event)

        dispatcher.listen(JobCompleted, listener)

        event = JobCompleted(payload="test", runtime_ms=42.0)
        await dispatcher.fire(event)

        assert len(called_with) == 1
        assert called_with[0].runtime_ms == 42.0

    @pytest.mark.asyncio
    async def test_multiple_listeners_same_event(self, dispatcher):
        """Multiple listeners for the same event type are all called, in order."""
        order = []

        async def first(event):
            order.append("first")

        async def second(event):
            order.append("second")

        def third(event):
            order.append("third")

        dispatcher.listen(LongWaitDetected, first)
        dispatcher.listen(LongWaitDetected, second)
        dispatcher.listen(LongWaitDetected, third)

        await dispatcher.fire(LongWaitDetected(connection="redis", queue="default", seconds=99))

        assert order == ["first", "second", "third"]

    @pytest.mark.asyncio
    async def test_no_listeners_is_noop(self, dispatcher):
        """Firing an event with no listeners should not raise."""
        await dispatcher.fire(JobPushed(payload="test"))
        # No assertion — just should not raise

    @pytest.mark.asyncio
    async def test_listener_exception_does_not_break_others(self, dispatcher):
        """If one listener raises, others still execute."""
        results = []

        async def bad_listener(event):
            raise RuntimeError("boom")

        async def good_listener(event):
            results.append("ok")

        dispatcher.listen(SupervisorLooped, bad_listener)
        dispatcher.listen(SupervisorLooped, good_listener)

        # Should NOT raise — exception is caught and logged
        await dispatcher.fire(SupervisorLooped(supervisor=None))

        assert results == ["ok"]

    @pytest.mark.asyncio
    async def test_different_event_types_isolated(self, dispatcher):
        """Listeners registered for one type don't fire for another type."""
        pushed_calls = []
        completed_calls = []

        dispatcher.listen(JobPushed, lambda e: pushed_calls.append(e))
        dispatcher.listen(JobCompleted, lambda e: completed_calls.append(e))

        await dispatcher.fire(JobPushed(payload="p1"))

        assert len(pushed_calls) == 1
        assert len(completed_calls) == 0

    def test_has_listeners(self, dispatcher):
        """has_listeners() returns True only when listeners exist."""
        assert dispatcher.has_listeners(JobPushed) is False

        dispatcher.listen(JobPushed, lambda e: None)
        assert dispatcher.has_listeners(JobPushed) is True
        assert dispatcher.has_listeners(JobCompleted) is False

    def test_flush_clears_all(self, dispatcher):
        """flush() removes all registered listeners."""
        dispatcher.listen(JobPushed, lambda e: None)
        dispatcher.listen(JobCompleted, lambda e: None)
        dispatcher.listen(LongWaitDetected, lambda e: None)

        dispatcher.flush()

        assert dispatcher.has_listeners(JobPushed) is False
        assert dispatcher.has_listeners(JobCompleted) is False
        assert dispatcher.has_listeners(LongWaitDetected) is False
