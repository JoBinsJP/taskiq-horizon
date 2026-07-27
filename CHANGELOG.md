# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.3] - 2026-07-27

### Added

- Failed jobs automatically store an exception context (exception type,
  connection, queue, task name, attempts, tags, labels), so the dashboard's
  "Exception Context" card is populated instead of hidden. Exceptions may
  additionally expose a Laravel-style `context` attribute or method returning
  a dict — it is merged into the stored context.

## [0.1.2] - 2026-07-27

UI-contract fixes bringing the API responses and dashboard in line with
Laravel Horizon 5.x frontend expectations.

### Changed

- Figtree is self-hosted (shipped in `dist/fonts`) instead of loaded from
  the bunny.net CDN — the dashboard renders with correct fonts offline and
  behind firewalls.
- Theme stylesheets load in the correct cascade order (component CSS first,
  then light, then dark), so the dark theme's overrides — e.g. for the JSON
  payload viewer — actually apply in dark mode.
- The dark stylesheet ships with a `prefers-color-scheme` media query, so
  the first paint matches the system theme instead of always flashing dark.

### Fixed

- Job lists (recent, pending, completed, silenced, failed) are returned
  newest-first: reads now use ascending `ZRANGE` over the negated-timestamp
  scores instead of `ZREVRANGE`, matching upstream ordering.
- `starting_at=0` (the SPA's first-load value) no longer skips the newest row
  on the failed/recent/pending/completed/silenced job list endpoints; it is
  treated as "from the beginning" like upstream's `?: -1` semantics.
- Tag pagination (`/api/monitoring/{tag}` and `/api/jobs/failed?tag=`) uses
  upstream's start-index semantics, so the first page includes every job.
- Tag zset members are stored with negated-timestamp scores instead of `0`,
  so tag pages order newest-first instead of by UUID accident.
- Job payloads now include an `attempts` field (derived from taskiq's
  `_retries` retry label, defaulting to 1), so the dashboard's "Attempts"
  field is no longer blank.
- `/api/stats` keys the `wait` entry as `redis:{queue}` so the dashboard's
  "Max Wait Time" caption (which parses Laravel's `connection:queue` format)
  renders again.
- Dashboard: the "Exception Context" card on the failed-job detail screen is
  hidden when no context is stored instead of rendering the literal "null".
- Dashboard: the `delayed()` computed on the recent-job detail screen no
  longer calls the undefined `phpunserialize()`; it uses the patched
  `decodeSerializedCommand` helper (patch also added to the upstream sync
  script).
- Dashboard: the batch preview screen guards `batch.options` so a batch
  record without options no longer white-screens (patch also added to the
  upstream sync script).

## [0.1.1] - 2026-07-27

### Fixed

- Dashboard pause/continue now actually pauses and resumes supervisors via
  the command queue instead of only flipping the displayed status.
- Worker memory limits are enforced (memory-exceeded workers are restarted).
- `balance: false` supervisors no longer autoscale; balancing strategies are
  respected.
- Task tags label format: canonical comma-separated strings are parsed, and
  stringified lists produced by taskiq label coercion are handled.
- Retry bookkeeping: failed/completed retries flip their entry on the parent
  job's `retried_by` list instead of staying `pending`.
- Failed jobs store the full formatted traceback so the dashboard stacktrace
  panel works.
- Metric snapshots record queue wait times.
- Packaging: cleo dependency constraint, Python 3.10 compatibility, and the
  LICENSE file are fixed/included.

## [0.1.0] - 2026-07-26

### Added

- Initial release: Laravel Horizon port for Python TaskIQ — master
  supervisor, supervisors, autoscaler, worker process pools, Redis
  repositories (jobs, tags, metrics, workload, masters/supervisors),
  FastAPI dashboard API, and the vendored Horizon 5.x Vue dashboard.
