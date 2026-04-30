"""Shared test fixtures for taskiq-horizon test suite."""

import fakeredis.aioredis
import pytest
import pytest_asyncio

from taskiq_horizon.config import HorizonConfig, TrimSettings
from taskiq_horizon.event_dispatcher import EventDispatcher


@pytest.fixture
def config():
    """Default test config with short trim intervals."""
    return HorizonConfig(
        prefix="test-horizon:",
        redis_url="redis://localhost:6379/0",
        trim=TrimSettings(
            recent=1,
            pending=1,
            completed=1,
            recent_failed=1,
            failed=1,
            monitored=1,
        ),
        waits={"default": 10, "high": 5},
    )


@pytest_asyncio.fixture
async def redis():
    """Fake async Redis client for unit tests."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=False)
    yield client
    await client.flushall()
    await client.aclose()


@pytest.fixture
def dispatcher():
    """Fresh EventDispatcher per test."""
    return EventDispatcher()
