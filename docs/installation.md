# Installation

Horizon requires Python 3.12 or later. Its process manager runs on Linux and macOS. Redis stores dashboard history, metrics, and supervisor commands; jobs may use Redis, the database queue, or SQS.

Install Horizon from PyPI into your Alata application:

```bash
uv add taskiq-horizon
```

Compatible Alata and TaskIQ dependencies are installed automatically.

Set `REDIS_URL` in your application's `.env` to a running Redis service. For Redis job queues, also set `QUEUE_CONNECTION=redis`. Database and SQS applications can keep their existing queue connection; Horizon still uses Redis for its dashboard.

## Configure Horizon

Create `config/horizon.py` (use your application's configuration package if it has another name):

```python
from taskiq_horizon.config import HorizonConfig
from taskiq_horizon.supervisor import SupervisorOptions
from config.settings import settings

horizon_config = HorizonConfig(
    prefix="myapp-horizon:",
    redis_url=settings.REDIS_URL,
    environments={
        "local": {
            "supervisor-1": SupervisorOptions(
                broker_path="bootstrap.broker:broker",
                queue="default,high,low",
                worker_args=[
                    "--max-async-tasks",
                    "25",
                ],
                min_processes=1,
                max_processes=3,
            ),
        },
    },
)
```

Add a supervisor layout for every environment where you run Horizon. The active Alata environment selects the layout, including when you use `--env`.

## Register the provider

Add the package provider to `bootstrap/providers.py`, after the framework queue provider:

```python
from taskiq_horizon.alata_provider import HorizonProvider

PROVIDERS = [
    # Your other application providers...
    HorizonProvider,
]
```

The provider reads `horizon_config`, attaches job tracking, registers the console commands, and serves the dashboard at `/horizon`. To supply configuration directly, use `(HorizonProvider, horizon_config)` in the provider list.

## Run Horizon

```bash
./alata horizon
./alata horizon:pause
./alata horizon:continue
./alata horizon:terminate
```

Run your HTTP application separately with `./alata serve` to open the dashboard at `http://127.0.0.1:8000/horizon`. The Horizon command starts workers, not an HTTP server.

Schedule `horizon:snapshot` every five minutes to populate historical charts:

```python
schedule.command("horizon:snapshot").every_five_minutes()
```

## Queue connections

| Job connection | Backlog used for workload and autoscaling |
| --- | --- |
| Redis | Pending jobs on the queue broker's Redis connection. Horizon's history may use a separate Redis instance or database. |
| Database | Runnable jobs in the configured queue table, including expired reservations. PostgreSQL, MySQL, and SQLite are supported. Delayed jobs and active reservations are excluded. |
| SQS | SQS's approximate visible-message count. Use `auto` or `simple` balancing for multiple queues; each worker consumes one queue. |

`sync` and `null` run without an external worker queue. Horizon refuses to start workers for these connections.

## Protect the dashboard

The dashboard and JSON API have no built-in authentication. Protect the entire configured Horizon path with your application's authorization middleware or an authenticating reverse proxy before exposing it to other users.

## Local dashboard preview

From the Horizon checkout:

```bash
python smoke_dashboard.py
```

Open `http://127.0.0.1:8000/horizon`. `HORIZON_REDIS_URL`, `HORIZON_PATH`, `HORIZON_PROXY_PATH`, and `HORIZON_PREFIX` customize this preview.
