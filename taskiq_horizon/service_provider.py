"""
Service Provider — package-side wiring for TaskIQ Horizon.

Mirrors Laravel's HorizonServiceProvider + ServiceBindings trait.
This module lives in the PACKAGE and knows how to bind all services
into any container that supports .singleton() and .make().

The HOST app only needs to:
  1. Create a config + redis connection
  2. Call register_services(container, config, redis)
  3. Call mount_dashboard(app, container) during FastAPI lifespan
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger("horizon.provider")


class Container(Protocol):
    """Minimal container interface — any DI container that has these methods."""

    def singleton(self, name: str, factory) -> None: ...
    def make(self, name: str) -> Any: ...


# ── Registration ─────────────────────────────────────────────────────────────


def register_services(container: Container) -> None:
    """
    Bind all Horizon services as lazy singletons.

    Mirrors ServiceBindings trait in Laravel Horizon.
    Requires 'horizon.config' and 'horizon.redis' to already be registered.
    """

    # Repositories
    container.singleton("horizon.jobs", lambda: _make_job_repo(container))
    container.singleton("horizon.metrics", lambda: _make_metrics_repo(container))
    container.singleton("horizon.tags", lambda: _make_tag_repo(container))
    container.singleton("horizon.workload", lambda: _make_workload_repo(container))
    container.singleton("horizon.masters", lambda: _make_master_repo(container))
    container.singleton("horizon.supervisors", lambda: _make_supervisor_repo(container))
    container.singleton("horizon.wait_time", lambda: _make_wait_time(container))
    container.singleton("horizon.command_queue", lambda: _make_command_queue(container))

    # Middleware
    container.singleton("horizon.middleware", lambda: _make_middleware(container))

    logger.info("Horizon services registered.")


# ── Dashboard Mounting ───────────────────────────────────────────────────────


def mount_dashboard(app, container: Container) -> None:
    """
    Mount the Horizon API routes + Vue SPA onto a FastAPI application.

    All routes are namespaced under /horizon:
      - /horizon/api/*   — JSON API
      - /horizon/assets/* — static JS/CSS
      - /horizon          — SPA catch-all

    Call this during the FastAPI lifespan, after all services are registered.
    Mirrors HorizonServiceProvider::registerRoutes().
    """
    from fastapi import APIRouter

    from taskiq_horizon.api.routes import router as horizon_router

    # Inject repos into app.state (Horizon routes read from here)
    app.state.horizon_job_repo = container.make("horizon.jobs")
    app.state.horizon_metrics_repo = container.make("horizon.metrics")
    app.state.horizon_tag_repo = container.make("horizon.tags")
    app.state.horizon_wait_time = container.make("horizon.wait_time")
    app.state.horizon_workload_repo = container.make("horizon.workload")
    app.state.horizon_master_repo = container.make("horizon.masters")
    app.state.horizon_super_repo = container.make("horizon.supervisors")
    app.state.horizon_command_queue = container.make("horizon.command_queue")

    # Mount API under /horizon — routes.py has prefix="/api", so final
    # paths are /horizon/api/stats, /horizon/api/masters, etc.
    horizon_api = APIRouter(prefix="/horizon")
    horizon_api.include_router(horizon_router)
    app.include_router(horizon_api)

    # Mount SPA static assets + catch-all (AFTER API routes so
    # the catch-all doesn't intercept /horizon/api/* requests)
    _mount_spa(app)

    logger.info("Horizon dashboard mounted at /horizon")


# ── Private Factories (mirrors ServiceBindings trait) ────────────────────────


def _make_job_repo(container: Container):
    from taskiq_horizon.repositories.redis_job_repository import RedisJobRepository

    return RedisJobRepository(container.make("horizon.redis"), container.make("horizon.config"))


def _make_metrics_repo(container: Container):
    from taskiq_horizon.repositories.redis_metrics_repository import RedisMetricsRepository

    return RedisMetricsRepository(container.make("horizon.redis"), container.make("horizon.config"))


def _make_tag_repo(container: Container):
    from taskiq_horizon.repositories.redis_tag_repository import RedisTagRepository

    return RedisTagRepository(container.make("horizon.redis"), container.make("horizon.config"))


def _make_workload_repo(container: Container):
    from taskiq_horizon.repositories.redis_workload_repository import RedisWorkloadRepository

    return RedisWorkloadRepository(
        redis=container.make("horizon.redis"),
        wait_time=container.make("horizon.wait_time"),
        supervisor_repo=container.make("horizon.supervisors"),
    )



def _make_master_repo(container: Container):
    from taskiq_horizon.repositories.redis_master_repository import RedisMasterSupervisorRepository

    return RedisMasterSupervisorRepository(
        container.make("horizon.redis"),
        prefix=container.make("horizon.config").prefix
    )


def _make_supervisor_repo(container: Container):
    from taskiq_horizon.repositories.redis_master_repository import RedisSupervisorRepository

    return RedisSupervisorRepository(
        container.make("horizon.redis"),
        prefix=container.make("horizon.config").prefix
    )


def _make_wait_time(container: Container):
    from taskiq_horizon.wait_time import WaitTimeCalculator

    redis = container.make("horizon.redis")
    metrics = container.make("horizon.metrics")
    supervisor_repo = container.make("horizon.supervisors")

    async def queue_len(q):
        return await redis.llen(q)

    async def supervisor_fetcher():
        return await supervisor_repo.all_supervisors()

    return WaitTimeCalculator(
        metrics,
        queue_length_fetcher=queue_len,
        supervisor_fetcher=supervisor_fetcher,
    )



def _make_command_queue(container: Container):
    from taskiq_horizon.repositories.redis_command_queue import RedisCommandQueue

    return RedisCommandQueue(container.make("horizon.redis"), container.make("horizon.config"))


def _make_middleware(container: Container):
    from taskiq_horizon.middleware import HorizonMiddleware

    return HorizonMiddleware(
        container.make("horizon.config"),
        container.make("horizon.jobs"),
        container.make("horizon.metrics"),
        container.make("horizon.tags"),
    )


# ── SPA Mounting (mirrors HorizonServiceProvider::registerDashboard) ─────────


def _mount_spa(app) -> None:
    """Mount the Horizon Vue SPA static files and the catch-all index route."""
    try:
        import taskiq_horizon

        # The built SPA lives at taskiq_horizon/dashboard/dashboard/
        dashboard_dir = Path(taskiq_horizon.__file__).parent / "dashboard" / "dashboard"
        index_html = dashboard_dir / "index.html"

        if not dashboard_dir.is_dir() or not index_html.exists():
            logger.warning(f"Horizon dashboard assets not found at {dashboard_dir}")
            return

        from fastapi import APIRouter
        from fastapi.responses import HTMLResponse
        from fastapi.staticfiles import StaticFiles

        # Serve JS/CSS/asset files
        app.mount(
            "/horizon/assets",
            StaticFiles(directory=str(dashboard_dir)),
            name="horizon-assets",
        )

        # Read and patch index.html.
        # IMPORTANT: We inject a <script> to set path='horizon' instead of using
        # str.replace("path: ''", ...) because that would ALSO match 'proxy_path: ''',
        # corrupting the config and causing cascading /horizon prefixes.
        spa_content = index_html.read_text()

        # Set Horizon.path so app.js computes basePath = '/horizon'
        # (must run AFTER the inline <script> that creates window.Horizon)
        spa_content = spa_content.replace(
            "</head>",
            '<script>window.Horizon.path = "horizon";</script>\n</head>',
        )

        # Rewrite static asset paths
        spa_content = spa_content.replace('src="/app.js"', 'src="/horizon/assets/app.js"')
        spa_content = spa_content.replace('href="/app.css"', 'href="/horizon/assets/app.css"')
        spa_content = spa_content.replace('href="/styles.css"', 'href="/horizon/assets/styles.css"')
        spa_content = spa_content.replace('href="/styles-dark.css"', 'href="/horizon/assets/styles-dark.css"')

        spa_router = APIRouter()

        @spa_router.get("/horizon", response_class=HTMLResponse, include_in_schema=False)
        @spa_router.get("/horizon/{path:path}", response_class=HTMLResponse, include_in_schema=False)
        async def horizon_spa(path: str = ""):
            return spa_content

        app.include_router(spa_router)

        logger.info(f"Horizon SPA assets mounted from {dashboard_dir}")
    except Exception as e:
        logger.warning(f"Horizon dashboard assets not found: {e}")


def commands() -> list:
    """Dynamically expose the package console commands."""
    from taskiq_horizon.console.commands.horizon import HorizonCommand
    from taskiq_horizon.console.commands.horizon_continue import HorizonContinueCommand
    from taskiq_horizon.console.commands.horizon_pause import HorizonPauseCommand
    from taskiq_horizon.console.commands.horizon_snapshot import HorizonSnapshotCommand
    from taskiq_horizon.console.commands.horizon_terminate import HorizonTerminateCommand

    return [
        HorizonCommand(),
        HorizonSnapshotCommand(),
        HorizonPauseCommand(),
        HorizonContinueCommand(),
        HorizonTerminateCommand(),
    ]


class HorizonServiceProvider:
    """
    OOP Service Provider overlay for TaskIQ Horizon.
    Provides Laravel 1:1 parity with the host framework's ServiceProvider pattern.
    """

    def __init__(self, app: Container):
        self.app = app

    def register(self):
        """Bind all Horizon services as lazy singletons."""
        register_services(self.app)

    def boot(self):
        """Bootstrap Horizon services."""
        pass

    def mount_dashboard(self, fastapi_app):
        """Mount the Horizon API routes + Vue SPA onto a FastAPI application."""
        mount_dashboard(fastapi_app, self.app)

    def commands(self) -> list:
        """Dynamically expose the package console commands."""
        return commands()
