from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.service_provider import HorizonServiceProvider


def test_horizon_service_provider_mount_dashboard_forwards_broker_and_batch_repo():
    container = MagicMock()
    fastapi_app = MagicMock()
    broker = object()
    batch_repo = object()
    dashboard_dist_dir = "/tmp/horizon-dist"

    provider = HorizonServiceProvider(container)

    with patch("taskiq_horizon.service_provider.mount_dashboard") as mount_dashboard:
        provider.mount_dashboard(
            fastapi_app,
            broker=broker,
            batch_repo=batch_repo,
            dashboard_dist_dir=dashboard_dist_dir,
        )

    mount_dashboard.assert_called_once_with(
        fastapi_app,
        container,
        broker=broker,
        batch_repo=batch_repo,
        dashboard_dist_dir=dashboard_dist_dir,
    )


class DummyContainer:
    def __init__(self, config):
        self._services = {
            "horizon.config": config,
            "horizon.jobs": object(),
            "horizon.metrics": object(),
            "horizon.tags": object(),
            "horizon.wait_time": object(),
            "horizon.workload": object(),
            "horizon.masters": object(),
            "horizon.supervisors": object(),
            "horizon.command_queue": object(),
        }

    def singleton(self, name, factory):
        self._services[name] = factory()

    def make(self, name):
        return self._services[name]


@pytest.mark.asyncio
async def test_mount_dashboard_uses_configured_path_and_dist_assets(tmp_path):
    dist_dir = tmp_path / "dist"
    dist_dir.mkdir()
    (dist_dir / "styles.css").write_text("body{color:black;}")
    (dist_dir / "styles-dark.css").write_text("body{color:white;}")
    (dist_dir / "app.css").write_text(".app{display:block;}")
    (dist_dir / "app.js").write_text("console.log('taskiq-horizon');")

    config = HorizonConfig(name="Ops", path="ops/horizon", proxy_path="/proxy")
    container = DummyContainer(config)
    app = FastAPI()

    from taskiq_horizon.service_provider import mount_dashboard

    mount_dashboard(app, container, dashboard_dist_dir=dist_dir)

    route_paths = {route.path for route in app.routes}
    assert "/ops/horizon/api/stats" in route_paths
    assert "/ops/horizon" in route_paths
    assert "/ops/horizon/{path:path}" in route_paths

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.get("/ops/horizon")

    assert response.status_code == 200
    assert "<strong>Alatazone</strong> - Ops" in response.text
    assert '<style data-scheme="dark">body{color:white;}</style>' in response.text
    assert 'window.Horizon = {"path": "ops/horizon", "proxy_path": "/proxy"};' in response.text
    assert '<script type="module" src="/ops/horizon/assets/app.js"></script>' in response.text


def test_resolve_dashboard_dist_dir_prefers_package_dist(monkeypatch, tmp_path):
    def seed_dist(dist_dir):
        dist_dir.mkdir(parents=True)
        for filename in ("styles.css", "styles-dark.css", "app.css", "app.js"):
            (dist_dir / filename).write_text(filename)

    package_root = tmp_path / "taskiq_horizon"
    package_dist = package_root / "dashboard" / "dist"
    upstream_dist = tmp_path / "dist"
    seed_dist(package_dist)
    seed_dist(upstream_dist)

    monkeypatch.setattr("taskiq_horizon.service_provider.__file__", str(package_root / "service_provider.py"))

    from taskiq_horizon.service_provider import _resolve_dashboard_dist_dir

    assert _resolve_dashboard_dist_dir(None) == package_dist
