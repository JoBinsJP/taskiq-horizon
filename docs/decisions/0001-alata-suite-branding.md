# ADR 0001 — Horizon is part of the Alata suite

**Date:** 2026-07-27 · **Status:** Accepted (rename deferred to next major-ish release)

## Decision

This package is a component of the **Alata framework suite** — the Laravel
Horizon analog for Alata applications — not a general-purpose community
extension for the taskiq ecosystem. TaskIQ remains the queue engine and is
credited as such ("powered by TaskIQ"), but the product identity is Alata.

Precedent: Laravel Horizon supervises Redis queues yet is branded *Laravel*
Horizon; the engine is an implementation detail, not the brand.

## Consequences

- The console commands importing `alata` directly (since v0.1.1) are correct
  and stay — no framework-agnostic shim is needed.
- The package will be renamed **`taskiq-horizon` → `alata-horizon`**
  (module `taskiq_horizon` → `alata_horizon`) at the next natural release
  boundary, NOT mid-stream — the v0.1.1 state was just stabilized across all
  remotes and an immediate rename would churn every consumer reference again.

## Rename checklist (execute as one release, target: v0.2.0)

1. Rename GitHub repo `JoBinsJP/taskiq-horizon` → `JoBinsJP/alata-horizon`
   (GitHub redirects old URLs) and the Bitbucket origin repo/remotes to match.
2. `git mv taskiq_horizon alata_horizon`; update all internal imports,
   `pyproject.toml` (`name`, `[project.urls]`, hatch packages), `mkdocs.yml`,
   README, dashboard build config, and the Redis key prefix default if it
   references the old name.
3. In ai-engine-python: update the dependency name + `[tool.uv.sources]`
   entry, `app/providers/horizon_provider.py` imports, `config/horizon.py`
   references, and re-lock.
4. Keep a final `taskiq-horizon` tag pointing at the last pre-rename commit;
   tag the renamed package `v0.2.0`.
5. README keeps a "Powered by TaskIQ" credit line and a note about the
   former name for searchability.
