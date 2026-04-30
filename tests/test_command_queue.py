"""Tests for RedisCommandQueue — push/pending/flush round-trip."""

import json

import pytest

from taskiq_horizon.repositories.redis_command_queue import RedisCommandQueue


class TestRedisCommandQueue:
    """Unit tests for the Redis-backed command queue."""

    @pytest.fixture
    def queue(self, redis, config):
        return RedisCommandQueue(redis, config)

    @pytest.mark.asyncio
    async def test_push_and_pending_roundtrip(self, queue, redis):
        """push() stores a command; pending() consumes it atomically."""
        await queue.push("supervisor-1", "pause")

        commands = await queue.pending("supervisor-1")

        assert len(commands) == 1
        assert commands[0]["command"] == "pause"
        assert commands[0]["options"] == {}

    @pytest.mark.asyncio
    async def test_push_with_options(self, queue):
        """push() stores command options."""
        await queue.push("sup-1", "scale", {"scale": 5})

        commands = await queue.pending("sup-1")

        assert commands[0]["command"] == "scale"
        assert commands[0]["options"]["scale"] == 5

    @pytest.mark.asyncio
    async def test_pending_consumes_atomically(self, queue, redis):
        """After pending() returns, the commands are gone from Redis."""
        await queue.push("sup-1", "pause")
        await queue.push("sup-1", "continue")

        first_batch = await queue.pending("sup-1")
        assert len(first_batch) == 2

        # Second call should return empty
        second_batch = await queue.pending("sup-1")
        assert len(second_batch) == 0

    @pytest.mark.asyncio
    async def test_pending_preserves_order(self, queue):
        """Commands are returned in FIFO order (RPUSH → LRANGE)."""
        await queue.push("sup-1", "pause")
        await queue.push("sup-1", "scale", {"scale": 3})
        await queue.push("sup-1", "continue")

        commands = await queue.pending("sup-1")

        assert [c["command"] for c in commands] == ["pause", "scale", "continue"]

    @pytest.mark.asyncio
    async def test_pending_empty_queue(self, queue):
        """pending() returns empty list when no commands exist."""
        commands = await queue.pending("nonexistent-supervisor")
        assert commands == []

    @pytest.mark.asyncio
    async def test_flush_clears_all_commands(self, queue, redis):
        """flush() deletes the entire command list for a supervisor."""
        await queue.push("sup-1", "pause")
        await queue.push("sup-1", "terminate")

        await queue.flush("sup-1")

        commands = await queue.pending("sup-1")
        assert commands == []

    @pytest.mark.asyncio
    async def test_isolation_between_supervisors(self, queue):
        """Commands for different supervisors are independent."""
        await queue.push("sup-1", "pause")
        await queue.push("sup-2", "terminate")

        sup1_cmds = await queue.pending("sup-1")
        sup2_cmds = await queue.pending("sup-2")

        assert len(sup1_cmds) == 1
        assert sup1_cmds[0]["command"] == "pause"
        assert len(sup2_cmds) == 1
        assert sup2_cmds[0]["command"] == "terminate"

    @pytest.mark.asyncio
    async def test_malformed_json_skipped(self, queue, redis, config):
        """Malformed entries in the list are silently skipped."""
        key = f"{config.prefix}commands:sup-1"
        await redis.rpush(key, b"not-valid-json")
        await redis.rpush(key, json.dumps({"command": "pause", "options": {}}).encode())

        commands = await queue.pending("sup-1")

        assert len(commands) == 1
        assert commands[0]["command"] == "pause"
