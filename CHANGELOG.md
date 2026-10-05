## [Unreleased]

- Cancel queued and active ingestion before releasing ownership or pipeline reservations.
- Preserve per-tenant scheduling across overlapping cycles and distributed pause/resume.
- Consume distributed discovery on every worker and schedule only local assignments.
- Correct capacity-weighted rendezvous hashing and reuse bounded Redis connection pools with reserved renewal capacity.
- Add independent resolution concurrency, reducing database pool requirements for large HTTP pipelines.
- Enforce configured stage deadlines and per-attempt timeouts; verify real concurrent HTTP socket waits.
- Fix worker capacity defaults, explicit program names, and long-fetch lease-renewal coverage.
- Add deterministic latency, fault, payload, and timestamp-window fixtures.
- Add local and multi-process Redis/PostgreSQL benchmark tasks with JSON reports.
- Track completed cycles, lease acquisition/skip counts, and bounded polling-lag percentiles.
- Demonstrate version-aware, idempotent PostgreSQL upserts for stale replay safety.
- Expand Ruby and dependency compatibility CI; verify built gem contents and executable installation.

## [0.1.0] - 2026-09-30

- Initial release
