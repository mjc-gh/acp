# Redis coordinator reference

`Acp::RedisCoordinator` supplies shared progress and ownership for workers running
the same program. Load it explicitly with `require "acp/redis"`, share one
coordinator between the runtime's `progress` and `ownership` adapters, and close
it only after runtime drain.

## Identity, membership, and assignment

Tenant IDs are strings or integers. Their type is encoded into identity, so
integer `12` and string `"12"` do not collide. Keys are versioned and scoped by
application, environment, and program. Keep these namespace components stable
across workers that should share state.

Discovery registers tenant IDs in atomic batches. Membership removals happen only
after a complete discovery generation; a failed or interrupted scan leaves old
membership intact. Disabled tenants stop receiving new cycles but retain their
cursor, so reactivation resumes from committed progress. Pause/resume state is
also persisted. `discovery_interval` controls re-enumeration.

Workers register capacity and heartbeat status. The coordinator assigns tenants
using capacity-weighted rendezvous hashing, then atomically claims each tenant.
Each worker schedules only its local assignment snapshot. Membership, pause, and
worker changes are refreshed at `min(discovery_interval, lease_ttl / 3)`, even
when a different worker owns discovery. Assignment caches are advisory and are
invalidated when membership or worker capacities change; claims recompute
authoritative assignment against the live worker registry.

Membership reads use bounded pipelines and revision-keyed caches. Concurrent
snapshot callers share one membership scan and one assignment refresh per
coordinator. A membership scan validates its revision before publishing; after
three conflicting scans it fails so the next refresh can retry. Failed or
cancelled leaders wake waiters and clear the in-flight operation. Cached records
retain their own revision while assignment computation yields. These caches
never authorize mutations. Direct batched tenant registration updates membership
and its revision atomically.

Claims, worker scans, and discovery scans are bounded. The default maximum live
workers is 256; set `worker_scan_limit` for a larger deployment. Worker health is
refreshed independently. When workers appear or expire, assignments converge as
cycles reach safe boundaries. `worker_statuses` returns live heartbeats;
`worker_assignments(tenant_ids)` reports each live worker's status and assigned
count for the supplied IDs.

## Leases and progress

Claims and renewals use Redis server time. Each tenant lease renews at one-third
of its configured TTL. A worker that cannot confirm renewal stops starting work
for that cycle. Fetch and ingestion validate ownership at their execution
boundaries; progress advancement validates ownership atomically with the write.
Queue entry and exit do not add redundant ownership round trips.

Progress cursors have no lease TTL. Initialization writes a permanent marker so
an unexpectedly missing progress record for a previously known tenant raises
`Acp::MissingProgressError` rather than silently resetting. Advancement checks
the lease token, expected revision, monotonic timestamp, and poll ID in one Lua
script. Repeating a poll ID reconciles a lost successful response; even an
unchanged timestamp advances revision. Progress reads distinguish a genuinely
absent cursor from missing initialized progress atomically.

Redis ownership cannot fence PostgreSQL. A former owner can still commit a
transaction after takeover. Ingestion must be idempotent and protect mutable rows
with source-version checks so delayed or out-of-order replay cannot replace newer
data. If Redis failover restores progress ahead of the database, reset only the
affected program namespace to a cursor no later than restored database effects.
Never delete only progress keys or lease keys as routine recovery. A deliberate
full reset requires stopped workers and replay-safe destination writes.

## Connections, timeouts, and scripts

Each coordinator has two bounded reactor-local connection pools:

| Pool | Default | Use |
| --- | ---: | --- |
| `connection_pool_size` | 16 | Ordinary coordinator commands. |
| `renewal_pool_size` | 8 | Lease/discovery renewals and worker heartbeats. |

Blocked ordinary commands cannot exhaust renewal capacity. Checked-out
connections are exclusive to one fiber; failed or cancelled connections are
discarded. Budget both pool limits per worker, plus other Redis clients. Use one
coordinator per worker/reactor. `coordinator.close` runs after runtime drain;
`acp-worker` closes it automatically.

Lua calls use centralized `EVALSHA`. A `NOSCRIPT` reply falls back once to `EVAL`
on the same checked-out connection. This handles script flush, reconnect, and
replacement-primary cache loss without per-connection script-cache assumptions.
Redis ACLs must allow `EVALSHA`, `EVAL`, and the scripts' underlying commands.
Transport failures and other command errors propagate; the executor does not
blindly replay a command. Only poll-ID idempotency reconciles an ambiguous progress
advancement response. Fault-injection adapters need to intercept `evalsha` by
script SHA and cold-cache `eval` to simulate post-execution failures.

For direct Async calls outside a runtime, `operation_timeout:` (default nil)
bounds each checkout plus command operation. In a runtime, `coordination_timeout`
also bounds each custom progress/ownership adapter method; the shorter deadline
wins when both apply. Non-Async callers should configure socket timeouts through
`redis_options`.

## Deployment and durability

Use one writable Redis primary for this version. Redis Cluster is unsupported:
atomic scripts access multiple keys without a cluster hash-tag layout. Configure
`noeviction`, persistence appropriate to the recovery-point objective, and
backups that include all `acp:v1:*` keys. Asynchronous replication can lose
recently acknowledged writes during primary failover; replay-safe ingestion is
still required. Restore Redis and PostgreSQL from a mutually consistent backup
where possible.

Process-level integration tests run against a disposable Redis database:

```sh
ACP_TEST_REDIS_URL=redis://127.0.0.1:6379/15 bundle exec rake test:redis
```

They exercise process termination and lease expiry, conditional progress
updates, lost advancement replies, and renewal during fetches longer than the
initial lease duration. See the [coordination benchmark report](../benchmark/redis-coordination-evidence.md)
for scan/claim measurements and reproduction commands.
