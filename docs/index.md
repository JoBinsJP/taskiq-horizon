# TaskIQ Horizon

TaskIQ Horizon provides a beautiful, real-time dashboard and robust configuration system for your [TaskIQ](https://taskiq-python.github.io/) queues. It is heavily inspired by Laravel Horizon and aims to provide near 1:1 parity for Redis-backed deployments.

Horizon allows you to easily monitor key metrics of your queue system such as job throughput, runtimes, exceptions, and job failures. All of your queue worker configuration is stored in a single, simple configuration file, allowing your configuration to stay in source control where your entire team can collaborate on it.

Horizon supports Alata 0.12.0.1 or later in the 0.12 series with TaskIQ 0.13.x and Redis, database, or SQS job queues. Redis stores Horizon history and metrics. When balancing is enabled, Horizon creates one worker pool per queue, matching Laravel Horizon's queue-by-queue balancing model.

## Features

- **Real-time Dashboard:** Monitor your queues, pending jobs, failed jobs, and worker metrics in real-time.
- **Metrics Tracking:** Horizon automatically tracks throughput and runtimes for all of your jobs.
- **Process Management:** Automatically scale your workers based on queue workload with Laravel-style per-queue balancing.
- **Graceful Restarts:** Safely pause, continue, or restart your workers without dropping jobs.
- **Time-to-Live (TTL):** Automatically prune old, completed, or failed jobs to keep your Redis instance lean.

## What's Next?

Check out the [Installation](installation.md) guide to get started.
