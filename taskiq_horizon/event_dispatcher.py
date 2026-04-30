"""
Lightweight Event Dispatcher — pub/sub bus for Horizon domain events.

This is the Python equivalent of Laravel's Illuminate\\Events\\Dispatcher,
scoped to Horizon's needs. Supports both sync and async listeners.

Usage:
    from taskiq_horizon.event_dispatcher import EventDispatcher

    dispatcher = EventDispatcher()
    dispatcher.listen(JobPushed, my_listener.handle)
    await dispatcher.fire(JobPushed(payload=...))
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Callable
from typing import Any

logger = logging.getLogger("horizon.events")


class EventDispatcher:
    """
    Simple callable registry with fire/listen semantics.

    - listen(event_type, callable) — register a handler for an event class.
    - fire(event) — invoke all handlers registered for the event's type.
    - flush() — remove all registered listeners.

    Handlers may be sync or async callables. Async handlers are awaited.
    """

    def __init__(self) -> None:
        self._listeners: dict[type, list[Callable]] = defaultdict(list)

    def listen(self, event_type: type, listener: Callable) -> None:
        """Register a listener for the given event type."""
        self._listeners[event_type].append(listener)

    async def fire(self, event: Any) -> None:
        """Fire an event, invoking all registered listeners in order."""
        event_type = type(event)
        listeners = self._listeners.get(event_type, [])

        for listener in listeners:
            try:
                result = listener(event)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.exception(f"Listener {listener!r} raised an exception while handling {event_type.__name__}")

    def has_listeners(self, event_type: type) -> bool:
        """Check if any listeners are registered for the given event type."""
        return bool(self._listeners.get(event_type))

    def flush(self) -> None:
        """Remove all registered listeners."""
        self._listeners.clear()
