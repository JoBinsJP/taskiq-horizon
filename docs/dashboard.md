# Dashboard

TaskIQ Horizon includes a beautiful Vue.js Single Page Application to monitor your queues.

## Accessing the Dashboard

If you have mounted the dashboard to your ASGI application (like FastAPI) using the `mount_dashboard(app)` function, it will by default be accessible at:

```
http://your-app-domain.local/horizon
```

## Security

TaskIQ Horizon does NOT include built-in authorization middleware — the dashboard is a standard set of `/horizon` routes attached to your ASGI application.

It is **highly recommended** that you protect these routes in production environments. 

In a FastAPI application, you can secure the dashboard by applying a `dependencies` array indicating authentication requirements when including the API Router, or by utilizing standard authentication middleware that protects requests targeting paths prefixed with `/horizon`.

## Available Metrics

The dashboard natively reveals:

- **Dashboard / Status Overview:** Realtime throughput (jobs/min), current queue capacities, max wait times.
- **Monitoring:** Tag jobs (e.g. `User:123`) inside your tasks manually or dynamically through middleware, and monitor the subset of tasks connected to that tag.
- **Metrics:** Line charts mapping throughput and latency history per job type or queue over the standard rolling 24-hour cycle.
- **Pending/Completed/Failed jobs:** Inspect detailed stack traces, exceptions, and payloads for failed jobs, and easily click to "Retry" them.
