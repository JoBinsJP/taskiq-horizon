# TaskIQ Horizon

TaskIQ Horizon provides a beautiful, real-time dashboard and robust configuration system for your [TaskIQ](https://taskiq-python.github.io/) queues. It is heavily inspired by Laravel Horizon and aims to provide near 1:1 parity in features.

Horizon allows you to easily monitor key metrics of your queue system such as job throughput, runtimes, exceptions, and job failures. All of your queue worker configuration is stored in a single, simple configuration file, allowing your configuration to stay in source control where your entire team can collaborate on it.

## Features

- **Real-time Dashboard:** Monitor your queues, pending jobs, failed jobs, and worker metrics in real-time.
- **Metrics Tracking:** Horizon automatically tracks throughput and runtimes for all of your jobs.
- **Process Management:** Automatically scale your workers based on queue workload.
- **Graceful Restarts:** Safely pause, continue, or restart your workers without dropping jobs.
- **Time-to-Live (TTL):** Automatically prune old, completed, or failed jobs to keep your Redis instance lean.

## What's Next?

Check out the [Installation](installation.md) guide to get started.
