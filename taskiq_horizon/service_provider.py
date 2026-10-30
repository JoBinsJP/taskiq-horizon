"""
Service Provider — package-side wiring for TaskIQ Horizon.

Ports the upstream service-provider wiring and service bindings.
This module lives in the PACKAGE and knows how to bind all services
into any container that supports .singleton() and .make().

The HOST app only needs to:
  1. Create a config + redis connection
  2. Call register_services(container, config, redis)
  3. Call mount_dashboard(app, container) during FastAPI lifespan
"""

from __future__ import annotations

import html
import json
import logging
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger("horizon.provider")

_UPSTREAM_DIST_FILES = (
    "styles.css",
    "styles-dark.css",
    "app.css",
    "app.js",
)


class Container(Protocol):
    """Minimal container interface — any DI container that has these methods."""

    def singleton(self, name: str, factory) -> None: ...
    def make(self, name: str) -> Any: ...


# ── Registration ─────────────────────────────────────────────────────────────


def register_services(container: Container) -> None:
    """
    Bind all Horizon services as lazy singletons.

    Matches the upstream service bindings.
    Requires 'horizon.config' and 'horizon.redis' to already be registered.
    """

    # Repositories
    container.singleton("horizon.queue_backend", lambda: _make_queue_backend(container))
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


def mount_dashboard(
    app,
    container: Container,
    *,
    broker=None,
    batch_repo=None,
    dashboard_dist_dir: str | Path | None = None,
) -> None:
    """
    Mount the Horizon API routes + Vue SPA onto a FastAPI application.

    All routes are namespaced under HorizonConfig.path (default: /horizon):
      - /<path>/api/*    — JSON API
      - /<path>/assets/* — static fallback assets
      - /<path>          — SPA catch-all

    Call this during the FastAPI lifespan, after all services are registered.
    Mirrors HorizonServiceProvider::registerRoutes().
    """
    from fastapi import APIRouter

    from taskiq_horizon.api.routes import router as horizon_router

    config = container.make("horizon.config")
    mount_prefix = _dashboard_mount_prefix(config)

    # Inject repos into app.state (Horizon routes read from here)
    app.state.horizon_config = config
    app.state.horizon_job_repo = container.make("horizon.jobs")
    app.state.horizon_metrics_repo = container.make("horizon.metrics")
    app.state.horizon_tag_repo = container.make("horizon.tags")
    app.state.horizon_wait_time = container.make("horizon.wait_time")
    app.state.horizon_workload_repo = container.make("horizon.workload")
    app.state.horizon_master_repo = container.make("horizon.masters")
    app.state.horizon_super_repo = container.make("horizon.supervisors")
    app.state.horizon_command_queue = container.make("horizon.command_queue")
    if broker is not None:
        app.state.broker = broker
    if batch_repo is not None:
        app.state.horizon_batch_repo = batch_repo

    # Mount API under the configured dashboard path. routes.py has prefix="/api",
    # so final paths are e.g. /horizon/api/stats, /horizon/api/masters, etc.
    horizon_api = APIRouter(prefix=mount_prefix)
    horizon_api.include_router(horizon_router)
    app.include_router(horizon_api)

    # Mount SPA static assets + catch-all (AFTER API routes so
    # the catch-all doesn't intercept /horizon/api/* requests)
    _mount_spa(app, config=config, dashboard_dist_dir=dashboard_dist_dir)

    logger.info("Horizon dashboard mounted at %s", mount_prefix or "/")


# ── Private Factories (matches the upstream service bindings) ────────────────


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
        queue_length_fetcher=container.make("horizon.wait_time")._get_queue_length,
    )


def _make_master_repo(container: Container):
    from taskiq_horizon.repositories.redis_master_repository import RedisMasterSupervisorRepository

    return RedisMasterSupervisorRepository(
        container.make("horizon.redis"), prefix=container.make("horizon.config").prefix
    )


def _make_supervisor_repo(container: Container):
    from taskiq_horizon.repositories.redis_master_repository import RedisSupervisorRepository

    return RedisSupervisorRepository(container.make("horizon.redis"), prefix=container.make("horizon.config").prefix)


def _make_wait_time(container: Container):
    from taskiq_horizon.wait_time import WaitTimeCalculator

    redis = container.make("horizon.redis")
    metrics = container.make("horizon.metrics")
    supervisor_repo = container.make("horizon.supervisors")
    backend = container.make("horizon.queue_backend")

    async def queue_len(q):
        if backend is not None:
            return await backend.size(q)
        return await redis.llen(q)

    async def supervisor_fetcher():
        return await supervisor_repo.all_supervisors()

    calculator = WaitTimeCalculator(
        metrics,
        queue_length_fetcher=queue_len,
        supervisor_fetcher=supervisor_fetcher,
        connection=backend.connection if backend is not None else "redis",
    )
    if hasattr(metrics, "set_wait_time_calculator"):
        metrics.set_wait_time_calculator(calculator)
    return calculator


def _make_queue_backend(container: Container):
    from taskiq_horizon.queue_backend import queue_backend

    try:
        broker = container.make("queue.broker")
    except KeyError:
        # Low-level dashboard-only embedding has no worker broker.
        return None
    return queue_backend(broker)


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


def _mount_spa(app, *, config, dashboard_dist_dir: str | Path | None = None) -> None:
    """Mount the Horizon Vue SPA static files and the catch-all index route."""
    try:
        dist_dir = _resolve_dashboard_dist_dir(dashboard_dist_dir)
        if dist_dir is None:
            logger.warning("Horizon dashboard dist not found. Run 'npm run build' in the dashboard/ directory.")
            return

        from fastapi.staticfiles import StaticFiles

        assets_prefix = _dashboard_assets_prefix(config)
        app.mount(
            assets_prefix,
            StaticFiles(directory=str(dist_dir)),
            name="horizon-assets",
        )

        spa_content = _render_dashboard(config=config, dist_dir=dist_dir, assets_prefix=assets_prefix)
        _mount_spa_routes(app, spa_content, config=config)
        logger.info("Horizon SPA mounted from %s", dist_dir)
    except Exception as e:
        logger.warning("Horizon dashboard assets not found: %s", e)


def _mount_spa_routes(app, spa_content: str, *, config) -> None:
    """Register the SPA index + catch-all routes for the configured dashboard path."""
    from fastapi import APIRouter
    from fastapi.responses import HTMLResponse

    mount_prefix = _dashboard_mount_prefix(config)
    spa_router = APIRouter()

    if mount_prefix:

        @spa_router.get(mount_prefix, response_class=HTMLResponse, include_in_schema=False)
        @spa_router.get(f"{mount_prefix}/{{path:path}}", response_class=HTMLResponse, include_in_schema=False)
        async def horizon_spa(path: str = ""):
            return spa_content

    else:

        @spa_router.get("/", response_class=HTMLResponse, include_in_schema=False)
        @spa_router.get("/{path:path}", response_class=HTMLResponse, include_in_schema=False)
        async def horizon_spa(path: str = ""):
            return spa_content

    app.include_router(spa_router)


def _resolve_dashboard_dist_dir(dashboard_dist_dir: str | Path | None) -> Path | None:
    """Resolve the dashboard dist directory containing the built CSS/JS files."""
    candidates: list[Path] = []
    if dashboard_dist_dir is not None:
        candidates.append(Path(dashboard_dist_dir).expanduser())
    candidates.append(Path(__file__).resolve().parent / "dashboard" / "dist")

    for candidate in candidates:
        if candidate.is_dir() and all((candidate / filename).is_file() for filename in _UPSTREAM_DIST_FILES):
            return candidate
    return None


def _render_dashboard(*, config, dist_dir: Path, assets_prefix: str) -> str:
    """Render the dashboard shell with inline styles and an external JS bundle."""
    template_path = Path(__file__).resolve().parent / "dashboard" / "layout.html"
    template = template_path.read_text()

    styles = (dist_dir / "styles.css").read_text()
    styles_dark = (dist_dir / "styles-dark.css").read_text()
    app_css = (dist_dir / "app.css").read_text()

    title = getattr(config, "name", "") or "TaskIQ Horizon"
    header = f"<strong>{html.escape(title)}</strong>"

    # Self-hosted Figtree (no CDN dependency — consistent fonts offline).
    font_faces = "\n".join(
        f"@font-face{{font-family:Figtree;font-style:normal;font-weight:{weight};font-display:swap;"
        f'src:url("{assets_prefix}/fonts/figtree-latin-{weight}-normal.woff2") format("woff2")}}'
        for weight in (300, 400, 500, 600)
    )

    # Order matters: component CSS (app_css) first so the theme sheets'
    # overrides (e.g. the JSON viewer dark rules) win; the dark sheet ships
    # with a prefers-color-scheme media query so the FIRST paint matches the
    # system theme — the SchemeToggler rewrites `media` at runtime for the
    # explicit dark/light choices.
    inline_css = (
        f"<style>{font_faces}</style>\n"
        f"<style>{app_css}</style>\n"
        f'<style data-scheme="light">{styles}</style>\n'
        f'<style data-scheme="dark" media="(prefers-color-scheme: dark)">{styles_dark}</style>'
    )
    inline_js = (
        "<script>\n"
        f"window.Horizon = {json.dumps(_dashboard_script_variables(config))};\n"
        "</script>"
        f'\n<script type="module" src="{assets_prefix}/app.js"></script>'
    )

    return (
        template.replace("__TITLE__", html.escape(title))
        .replace("__HEADER__", header)
        .replace("__INLINE_CSS__", inline_css)
        .replace("__INLINE_JS__", inline_js)
    )


def _dashboard_script_variables(config) -> dict[str, str]:
    return {
        "path": _normalize_dashboard_path(getattr(config, "path", "horizon")),
        "proxy_path": _normalize_proxy_path(getattr(config, "proxy_path", "")),
    }


def _dashboard_mount_prefix(config) -> str:
    path = _normalize_dashboard_path(getattr(config, "path", "horizon"))
    return f"/{path}" if path else ""


def _dashboard_assets_prefix(config) -> str:
    mount_prefix = _dashboard_mount_prefix(config)
    return f"{mount_prefix}/assets" if mount_prefix else "/assets"


def _normalize_dashboard_path(path: str | None) -> str:
    if path is None:
        return "horizon"
    normalized = str(path).strip()
    if normalized in {"", "/"}:
        return ""
    return normalized.strip("/")


def _normalize_proxy_path(proxy_path: str | None) -> str:
    if proxy_path is None:
        return ""
    normalized = str(proxy_path).strip().strip("/")
    return f"/{normalized}" if normalized else ""


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
    OOP wrapper around register_services() with a register()/boot() lifecycle,
    for host apps that wire services through provider objects.
    """

    def __init__(self, app: Container):
        self.app = app

    def register(self):
        """Bind all Horizon services as lazy singletons."""
        register_services(self.app)

    def boot(self):
        """Bootstrap Horizon services."""
        pass

    def mount_dashboard(
        self, fastapi_app, *, broker=None, batch_repo=None, dashboard_dist_dir: str | Path | None = None
    ):
        """Mount the Horizon API routes + Vue SPA onto a FastAPI application."""
        mount_dashboard(
            fastapi_app,
            self.app,
            broker=broker,
            batch_repo=batch_repo,
            dashboard_dist_dir=dashboard_dist_dir,
        )

    def commands(self) -> list:
        """Dynamically expose the package console commands."""
        return commands()
