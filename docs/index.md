# TaskIQ Horizon

TaskIQ Horizon provides a real-time dashboard and worker management for Alata applications, powered by [TaskIQ](https://taskiq-python.github.io/).

Horizon allows you to easily monitor key metrics of your queue system such as job throughput, runtimes, exceptions, and job failures. All of your queue worker configuration is stored in a single, simple configuration file, allowing your configuration to stay in source control where your entire team can collaborate on it.

Horizon supports Redis, database, and SQS job queues. Redis stores Horizon history and metrics. Install `taskiq-horizon` from PyPI to get compatible Alata and TaskIQ dependencies automatically.

## Features

- **Real-time Dashboard:** Monitor your queues, pending jobs, failed jobs, and worker metrics in real-time.
- **Metrics Tracking:** Horizon automatically tracks throughput and runtimes for all of your jobs.
- **Process Management:** Balance worker pools evenly or scale each queue according to its workload.
- **Graceful Restarts:** Pause new work or restart workers while allowing in-flight jobs to finish within the configured shutdown timeout.
- **Time-to-Live (TTL):** Automatically prune old, completed, or failed jobs to keep your Redis instance lean.

## What's Next?

Check out the [Installation](installation.md) guide to get started.
