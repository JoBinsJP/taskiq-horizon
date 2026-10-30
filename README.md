# TaskIQ Horizon

Horizon provides queue monitoring and worker management for Alata, powered by [TaskIQ](https://taskiq-python.github.io/). Monitor jobs, investigate failures, and scale workers across Redis, database, and SQS queues.

## Features

- **Real-Time Dashboard**: See pending, completed, silenced, and failed jobs as they happen.
- **Metrics**: Track throughput and runtime metrics for specific queues and jobs over time.
- **Worker Management**: Distribute workers evenly, prioritize queues, or scale each queue's worker pool with demand.
- **Alata Integration**: One service provider connects your application, console commands, and dashboard.
- **Queue Visibility**: Backlog comes from your job connection; Redis stores Horizon history and metrics.
- **Retry Tracking**: Failed attempts remain pending while automatic retries continue. Dashboard retries receive a fresh retry allowance.

## Quick Installation

Requires Python 3.12 or later. The process manager supports Linux and macOS.

From your Alata application, install Horizon from PyPI:

```bash
uv add taskiq-horizon
```

Compatible Alata and TaskIQ dependencies are installed automatically.

Follow the [Horizon guide](https://jobinsjp.github.io/alata-doc/docs/packages/horizon/) to configure and register `HorizonProvider`, then run `./alata horizon`. Run your HTTP application alongside it to access the dashboard at `/horizon`.

## Documentation

The [Horizon guide](https://jobinsjp.github.io/alata-doc/docs/packages/horizon/) covers installation, configuration, dashboard access, and production deployment.

To preview this repository's MkDocs documentation locally:

```bash
uvx --with mkdocs-material mkdocs serve
```

## Development

From this repository's root:

```bash
uv venv
uv pip install 'alata[sqlite,mysql]' -e '.[dev]'
.venv/bin/python -m pytest -q
```

CI runs the suite on Python 3.12, 3.13, and 3.14 with Redis and SQLite, PostgreSQL, or MySQL. SQS API calls are tested with botocore stubs. To include live services locally, set `HORIZON_TEST_REDIS_URL` and `HORIZON_TEST_DATABASE_URL` to disposable test services.

## Local UI Smoke Test

If you want to verify the dashboard UI from this repository checkout without rebuilding the frontend, use the included smoke app:

```bash
./.venv/bin/python smoke_dashboard.py
```

Then open [http://127.0.0.1:8000/horizon](http://127.0.0.1:8000/horizon).
