# Architecture

TaskIQ Horizon manages TaskIQ workers through a master supervisor, queue supervisors, and worker pools. Alata's `HorizonProvider` connects these services to the application's queue broker, console, and HTTP server.

Redis stores dashboard state, metrics, commands, and supervisor heartbeats. Queue inspection uses a broker-specific adapter: Redis lists, Alata's database queue, or SQS. Workload and autoscaling share the same queue-depth reader.

## The Hierarchy

1. **Master Supervisor**
    - A single, long-running daemon that oversees all configuration and child supervisors.
    - Resolves commands from the dashboard (e.g. `restart`, `pause`) and passes them down.
    - Fires global events (e.g., trimming old jobs, cleaning up stale process state).
    
2. **Supervisors**
    - Directly manage a specific set of queues.
    - Utilize an **Autoscaler** to continuously adjust the number of active worker processes based on the current workload.
    - When balancing is enabled, create one **ProcessPool** per queue.
    - When `balance="false"`, retain a single combined pool for the full comma-separated queue string.
    - Restart individual failed workers.
    
3. **Worker Processes**
    - Subprocesses spawned by the Supervisors.
    - These are the standard TaskIQ worker processes that actually execute the jobs.
    
## Components

| Module | Responsibility |
|---|---|
| `master_supervisor.py` | Coordinates supervisors, commands, signals, and shutdown. |
| `supervisor.py` | Manages queue pools and worker allocation. |
| `process_pool.py` | Starts, monitors, and drains worker processes. |
| `repositories/` | Stores job history, metrics, tags, and supervisor state. |
| `event_map.py` and `listeners/` | Connects lifecycle events to metrics, cleanup, and health checks. |
| `middleware.py` | Tracks queued jobs, execution, retries, and failures. |
| `alata_provider.py` | Registers services, commands, tracking, and dashboard routes in Alata. |

## Dashboard

The FastAPI routes serve the dashboard API and a bundled Vue application. The Vue dashboard is adapted from [Laravel Horizon](https://github.com/laravel/horizon).
