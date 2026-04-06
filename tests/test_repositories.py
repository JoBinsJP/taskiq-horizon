"""Tests for Redis repository flush_expired methods."""

import time

import pytest

from taskiq_horizon.repositories.redis_master_repository import (
    RedisMasterSupervisorRepository,
    RedisSupervisorRepository,
)


class TestRedisMasterSupervisorRepository:
    """Tests for master supervisor heartbeat and expiry."""

    @pytest.fixture
    def repo(self, redis):
        return RedisMasterSupervisorRepository(redis, prefix="test-horizon:")

    @pytest.mark.asyncio
    async def test_update_and_retrieve_master(self, repo):
        """update_master() stores state that all_masters() can retrieve."""
        await repo.update_master(
            name="master-1",
            pid=12345,
            status="running",
            supervisors=["sup-1", "sup-2"],
            environment="local",
        )

        masters = await repo.all_masters()
        assert len(masters) == 1
        assert masters[0]["name"] == "master-1"
        assert masters[0]["pid"] == "12345"
        assert masters[0]["status"] == "running"
        assert masters[0]["supervisors"] == ["sup-1", "sup-2"]

    @pytest.mark.asyncio
    async def test_flush_expired_removes_stale(self, repo, redis):
        """flush_expired() removes masters that haven't heartbeated > 14s."""
        # Register a master with a stale timestamp
        key = "test-horizon:masters"
        stale_time = time.time() - 20  # 20 seconds ago
        await redis.zadd(key, {b"stale-master": stale_time})
        await redis.zadd(key, {b"fresh-master": time.time()})

        await repo.flush_expired()

        names = await repo.get_master_names()
        assert "fresh-master" in names
        assert "stale-master" not in names

    @pytest.mark.asyncio
    async def test_flush_expired_keeps_fresh(self, repo):
        """flush_expired() keeps masters that heartbeated recently."""
        await repo.update_master("fresh", 1, "running", [], "local")

        await repo.flush_expired()

        names = await repo.get_master_names()
        assert "fresh" in names


class TestRedisSupervisorRepository:
    """Tests for supervisor heartbeat and expiry."""

    @pytest.fixture
    def repo(self, redis):
        return RedisSupervisorRepository(redis, prefix="test-horizon:")

    @pytest.mark.asyncio
    async def test_update_and_retrieve_supervisor(self, repo):
        """update_supervisor() stores state that all_supervisors() can retrieve."""
        await repo.update_supervisor(
            name="sup-1",
            master="master-1",
            pid=6789,
            status="running",
            processes={"default": 3},
            options={"queue": "default", "maxProcesses": 10},
        )

        sups = await repo.all_supervisors()
        assert len(sups) == 1
        assert sups[0]["name"] == "sup-1"
        assert sups[0]["master"] == "master-1"
        assert sups[0]["processes"] == {"default": 3}

    @pytest.mark.asyncio
    async def test_flush_expired_removes_stale(self, repo, redis):
        """flush_expired() removes supervisors that haven't heartbeated > 29s."""
        key = "test-horizon:supervisors"
        stale_time = time.time() - 35
        await redis.zadd(key, {b"stale-sup": stale_time})
        await redis.zadd(key, {b"fresh-sup": time.time()})

        await repo.flush_expired()

        names = await repo.get_supervisor_names()
        assert "fresh-sup" in names
        assert "stale-sup" not in names
