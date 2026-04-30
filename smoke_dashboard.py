"""Local smoke-test app for the TaskIQ Horizon dashboard.

Run from the repository root:

    ./.venv/bin/python smoke_dashboard.py

Or with reload:

    ./.venv/bin/uvicorn smoke_dashboard:app --reload
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, Callable

import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI

from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.service_provider import mount_dashboard, register_services


class Container:
    """Small lazy singleton container for local smoke testing."""

    def __init__(self) -> None:
        self._bindings: dict[str, Callable[[], Any]] = {}
        self._instances: dict[str, Any] = {}

    def singleton(self, name: str, factory: Callable[[], Any]) -> None:
        self._bindings[name] = factory

    def make(self, name: str) -> Any:
        if name in self._instances:
            return self._instances[name]
        if name not in self._bindings:
            raise KeyError(f"Service is not registered: {name}")

        instance = self._bindings[name]()
        self._instances[name] = instance
        return instance


def build_config() -> HorizonConfig:
    """Build a minimal config from environment variables."""

    return HorizonConfig(
        redis_url=os.getenv("HORIZON_REDIS_URL", "redis://127.0.0.1:6379/0"),
        path=os.getenv("HORIZON_PATH", "horizon"),
        proxy_path=os.getenv("HORIZON_PROXY_PATH", ""),
        prefix=os.getenv("HORIZON_PREFIX", "horizon:"),
    )


def build_container(config: HorizonConfig) -> Container:
    """Register the minimum services needed to render the dashboard."""

    container = Container()
    container.singleton("horizon.config", lambda: config)
    container.singleton("horizon.redis", lambda: aioredis.from_url(config.redis_url))
    register_services(container)
    return container


def create_app() -> FastAPI:
    """Create a local FastAPI app that serves the dashboard."""

    config = build_config()
    container = build_container(config)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            yield
        finally:
            await app.state.horizon_redis.aclose()

    app = FastAPI(title="Alatazone Smoke Dashboard", lifespan=lifespan)
    app.state.horizon_container = container
    app.state.horizon_redis = container.make("horizon.redis")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {
            "status": "ok",
            "dashboard": f"/{config.path.strip('/')}" if config.path.strip("/") else "/",
        }

    mount_dashboard(app, container)
    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run("smoke_dashboard:app", host="127.0.0.1", port=8000, reload=False)
