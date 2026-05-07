# TaskIQ Horizon

A Python port of Laravel Horizon's monitoring dashboard and process management for the [TaskIQ](https://taskiq-python.github.io/) asynchronous task queue framework.

## Features

- **Real-Time Dashboard**: See pending, completed, silenced, and failed jobs as they happen.
- **Metrics**: Track throughput and runtime metrics for specific queues and jobs over time.
- **Laravel-Style Process Management**: The supervisor supports `auto`, `simple`, and `false` balancing modes, including per-queue autoscaling for Redis deployments.
- **Redis-First Runtime**: The dashboard, metrics, workload calculation, and supervisor control plane are production-ready on Redis today.
- **Broker Agnostic Middleware**: Metrics/job lifecycle tracking can still be attached to any TaskIQ broker, even though Horizon's control plane is Redis-backed.

## Documentation

For full installation instructions, architecture overview, and complete configuration details, please refer to the **MkDocs documentation**:

```bash
uv pip install mkdocs-material
mkdocs serve
```

## Quick Installation

TaskIQ Horizon is built for modern Python environments and recommends `uv` for installation:

```bash
# Using uv (Recommended)
uv pip install taskiq-horizon

# Local development installation:
uv pip install -e path/to/taskiq-horizon
```

*(Standard `pip install taskiq-horizon` works as well)*

To run the Vue SPA dashboard and APIs, mount the dashboard routes using the dedicated Service Provider on your ASGI application. Detailed instructions are available in the MkDocs `docs/installation.md` file.

## Local UI Smoke Test

If you want to verify the dashboard UI from this repository checkout without rebuilding the frontend, use the included smoke app:

```bash
cd taskiq-horizon
./.venv/bin/python smoke_dashboard.py
```

Then open [http://127.0.0.1:8000/horizon](http://127.0.0.1:8000/horizon).
