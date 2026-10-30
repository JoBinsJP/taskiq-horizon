# Architecture

TaskIQ Horizon is designed to maintain high functional and structural parity with [Laravel Horizon](https://horizon.laravel.com/). Instead of running isolated TaskIQ worker CLI commands, Horizon introduces a robust, multi-tier process management architecture.

Redis stores dashboard state, metrics, commands, and supervisor heartbeats. Queue inspection uses a broker-specific adapter: Redis lists, Alata's database queue, or SQS. Workload and autoscaling share the same queue-depth reader.

## The Hierarchy

1. **Master Supervisor**
    - A single, long-running daemon that oversees all configuration and child supervisors.
    - Resolves commands from the dashboard (e.g. `restart`, `pause`) and passes them down.
    - Fires global events (e.g., trimming old jobs, cleaning up stale process state).
    
2. **Supervisors**
    - Directly manage a specific set of queues.
    - Utilize an **Autoscaler** to continuously adjust the number of active worker processes based on the current workload.
    - When balancing is enabled, create one **ProcessPool** per queue, matching Laravel Horizon's queue-by-queue balancing model.
    - When `balance="false"`, retain a single combined pool for the full comma-separated queue string.
    - Restart individual failed workers.
    
3. **Worker Processes**
    - Subprocesses spawned by the Supervisors.
    - These are the standard TaskIQ worker processes that actually execute the jobs.
    
## Design Parity with Laravel

TaskIQ Horizon closely follows Laravel Horizon's internal domain model on the Redis runtime path.

If you are familiar with Laravel Horizon's codebase, you will recognize the internal domain layout:

| Laravel (PHP) Concept | TaskIQ Horizon (Python) Concept |
|---|---|
| `MasterSupervisor` | `master_supervisor.py` |
| `Supervisor` | `supervisor.py` |
| `ProcessPool` | `process_pool.py` |
| `Repositories\*` | `repositories/` (e.g., `redis_job_repository.py`) |
| `EventMap` | `event_map.py` |
| `Listeners\*` | `listeners/` |
| `Middlewares` | `middleware.py` |

The primary difference lies in the HTTP routing layer (FastAPI vs Laravel Routing) and the transition from PHP Interfaces/Traits to Python ABCs and Dataclasses. The Vue SPA is adapted from the upstream dashboard.
