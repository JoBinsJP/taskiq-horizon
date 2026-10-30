"""Remote controls must change the master lifecycle, not only its children."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from httpx import ASGITransport, AsyncClient
from test_api import make_app

from taskiq_horizon.event_dispatcher import EventDispatcher
from taskiq_horizon.master_supervisor import MasterSupervisor
from taskiq_horizon.supervisor import SupervisorOptions


@pytest.mark.asyncio
async def test_dashboard_controls_reach_master_and_update_status(redis, config, monkeypatch):
    master = MasterSupervisor(config, redis, EventDispatcher(), supervisor_options=[])
    monkeypatch.setattr(master, "_listen_for_signals", lambda: None)
    app = make_app(redis, config)
    app.state.horizon_master_repo = master.master_repo
    app.state.horizon_super_repo = master.supervisor_repo
    app.state.horizon_command_queue = master.command_queue
    await master._heartbeat()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        for command, status in [("pause", "paused"), ("continue", "running")]:
            assert (await client.post(f"/api/masters/{command}")).status_code == 200
            await master._process_commands()
            await master._heartbeat()
            assert (await client.get("/api/stats")).json()["status"] == status
        await client.post("/api/masters/terminate")
        await asyncio.wait_for(master.monitor(), timeout=2)
    assert not master.running
    assert await master.master_repo.all_masters() == []


@pytest.mark.asyncio
async def test_crashed_child_causes_master_shutdown(redis, config, monkeypatch):
    master = MasterSupervisor(config, redis, EventDispatcher(), supervisor_options=[])
    monkeypatch.setattr(master, "_listen_for_signals", lambda: None)
    child = Mock(name="broken-supervisor")
    child.name = "broken-supervisor"
    child.monitor = AsyncMock(side_effect=RuntimeError("worker supervisor failed"))
    master.supervisors = [child]
    monkeypatch.setattr(master, "_heartbeat", AsyncMock())
    with pytest.raises(RuntimeError, match="worker supervisor failed"):
        await asyncio.wait_for(master.monitor(), timeout=4)
    child.terminate.assert_called_once_with(0)


def test_master_does_not_mutate_shared_config(config):
    options = SupervisorOptions(name="original", broker_path="test:broker")
    config.environments = {"local": {"default": options}}
    first = MasterSupervisor(config, Mock(), EventDispatcher())
    second = MasterSupervisor(config, Mock(), EventDispatcher())
    assert options.name == "original"
    assert first.supervisors[0].options is not second.supervisors[0].options


@pytest.mark.asyncio
@pytest.mark.parametrize("scale", [-1, True, "3"])
async def test_scale_rejects_invalid_input(redis, config, scale):
    app = make_app(redis, config)
    app.state.horizon_command_queue = AsyncMock()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/supervisors/test/scale", json={"scale": scale})
    assert response.status_code == 422
    app.state.horizon_command_queue.push.assert_not_awaited()
