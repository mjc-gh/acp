# Acp

Acp is a Ruby runtime for asynchronous, multi-tenant polling. Your program defines
which tenants to poll and how to fetch and ingest their data; Acp schedules work,
bounds concurrency, tracks progress, and can coordinate workers through Redis.
HTTP clients, destination schemas, and process supervision remain application
choices. Requiring Acp does not load Rails or open database, Redis, or HTTP
connections.

**Supported:** Ruby >= 3.2; Async >= 2.35, < 2.38. Optional integrations support
Rails 8.0/8.1, `pg` >= 1.5, < 2, PostgreSQL 17, and Redis 7. See the
[support and verification section](#5-verify-and-develop) for the tested matrix.

## 1. Define a program

```ruby
class TenantSync < Acp::Program
  interval 60
  fetch_concurrency 300
  ingest_concurrency 5
  resolve_concurrency 5
  pipeline_capacity 100

  tenants do |emit|
    Tenant.active.in_batches do |batch|
      batch.pluck(:id).each { |id| emit.call(id) }
    end
  end

  initial_cursor do |tenant_id|
    Tenant.find(tenant_id).sync_started_at
  end

  resolve do |tenant_id|
    tenant = Tenant.find(tenant_id)
    {id: tenant.id, credentials: tenant.api_credentials}
  end

  fetch do |tenant, context|
    response = MyAPIClient.new(tenant[:credentials]).fetch(since: context.cursor)
    Acp::Batch.new(data: response.records, next_cursor: response.next_timestamp)
  end

  ingest do |batch, context|
    MyImporter.call(tenant_id: context.tenant_id, records: batch.data)
  end
end
```

Definitions are declarative: callbacks are stored when the class loads. Validate
them before starting work with `TenantSync.configuration`.

| Callback | Contract |
| --- | --- |
| `tenants` | Emit tenant IDs incrementally. Duplicate IDs are ignored. |
| `initial_cursor` | Return the starting cursor only when a tenant has no legitimate prior progress. |
| `resolve` | Resolve per-cycle tenant state once; return detached values so database reads do not leak into network waits. |
| `fetch` | Receive resolved tenant data and a `FetchContext`; return an `Acp::Batch`. |
| `ingest` | Apply the batch using its `IngestContext`. Make writes safe to replay. |

Both contexts identify the tenant, committed cursor, poll ID, and one-based
attempt. The poll ID remains stable across fetch/ingest retries. A new cursor is
committed only after ingestion succeeds.

Discovery registration is chunked (`discovery_batch_size`, default 256; allowed
range 1–1024). The runtime buffers at most one chunk, ignores duplicates, flushes
the last partial chunk, and completes empty scans without registering IDs.
`tenants_page` supports page-at-a-time discovery; see
[Rails integration](#rails-and-postgresql) for its checkout behavior.

## 2. Runtime and delivery guarantees

Run the validated program inside an Async task and supply a progress adapter:

```ruby
progress = MyProgressStore.new
runtime = Acp::Runtime.new(
  configuration: TenantSync.configuration,
  progress: progress
)

Async { runtime.run }
```

The progress adapter implements `read(tenant_id)`,
`initialize_cursor(tenant_id, cursor)`, and
`acknowledge(tenant_id, poll_id, cursor)`. Initialization returns the stored
cursor and is idempotent. Acknowledgement is idempotent by poll ID. It follows a
confirmed consumer commit and retries independently; if acknowledgement fails,
the committed batch is not ingested again in that cycle.

`Acp::Batch` contains consumer-owned `data` and an explicit `next_cursor`. Empty
batches and unchanged cursors are valid. Cursors are UTC timestamps with exactly
microsecond precision; `Acp::Timestamp.dump/load` serializes and restores them.
Backward cursor advancement is rejected. Lookback belongs in fetch inputs, not
in durable cursor regression.

The central safety rule is **at-least-once delivery**. A process can lose its
lease after committing destination data but before progress advances; a later
owner then replays from committed progress. Make ingestion idempotent. For
mutable rows, use unique event keys and compare source versions in the database
upsert; see the [versioned importer example](examples/versioned_importer.rb).
Redis leases cannot fence an old PostgreSQL transaction already in progress.
Acp creates neither destination nor progress tables.

The scheduler uses a fair due-time heap and does not create waiting tasks per
tenant. A successful tenant becomes due at `max(cycle_start + interval,
acknowledgement_time)`; exhausted failures are rescheduled after
`retry_cooldown`. An optional ownership adapter implements `acquire(tenant_id)`
and `release(tenant_id, token)`; `Acp::LocalOwnership` claims every tenant by
default.
Failed acquisition skips that cycle. `pause_tenant`/`resume_tenant` pause polling
without resetting progress.

### Concurrency, retries, and memory

`pipeline_capacity` bounds a cycle from fetch reservation through ingestion and
progress acknowledgement. Effective fetch concurrency is
`min(fetch_concurrency, pipeline_capacity)`. Ingestion uses fixed workers bounded
by ingestion concurrency and pipeline capacity. Resolution and initial-cursor
work use `resolve_concurrency` (default: ingestion concurrency), capped by
pipeline capacity. These are independent limits: high HTTP concurrency does not
require an equally large database pool.

Fetch and ingestion retry policies are configured separately with exception
classes and/or predicates. Attempts are one-based; attempt and elapsed budgets
include the original call; backoff grows exponentially to `max_backoff` with
optional symmetric jitter. Per-attempt `timeout` and total `max_elapsed` are
cooperative deadlines, so callbacks and clients must yield and honor cancellation.
Use finite socket timeouts too. Cancellation and process-level exceptions are not
retryable. After a confirmed commit, acknowledgement retries independently.

Retry admission isolation is opt-in (`retry_capacity`, default 0). With P pipeline
slots and R retry slots, failed cycles can park during backoff and release their
pipeline slot while retaining a retry slot, lease, identity, and batch. They need
a pipeline slot again to resume; aggregate cycle occupancy is bounded by P + R.
`upstream_group` plus `upstream_capacity` optionally limits pipeline occupancy per
upstream group. See the [runtime reference](docs/runtime-reference.md) for retry,
bulkhead, and admission details.

Payload accounting is also opt-in. Set `max_batch_bytes` and
`retained_byte_capacity` to account for accepted batches through acknowledgement;
set `max_response_bytes` to enforce a response limit from inside a streaming
callback. A batch limit requires `Acp::Batch.new(..., size_bytes:)`. Configure
`payload_estimate` when the default estimate is not conservative enough. The
consumer must enforce streaming limits before retaining each chunk and bound
decoding against `context.reserved_bytes`. These reservations bound accounted
payloads, **not total Ruby heap or process RSS**. More in the
[runtime reference](docs/runtime-reference.md).

### Callback and reactor boundaries

Callbacks run as Async fibers on one reactor. Scheduler-compatible socket waits
can overlap; blocking native clients, CPU-heavy callbacks, slow notification
subscribers, or synchronous error handlers can stall other work and lease renewal.
Bound request fanout created inside a callback. Keep database access within
discovery, cursor initialization, resolution, and ingestion; return detached
resolved data before network waits. Close callback-scoped clients before return.
The consumer owns connection pools, client lifecycles, and replay safety.

Cancellation unwinds callbacks before releasing cycle reservations. For graceful
shutdown, call `runtime.request_shutdown(timeout: 30)` to stop admission and drain
to a deadline; stopping the surrounding Async task cancels immediately. See the
[runtime reference](docs/runtime-reference.md) for stage deadlines, exporter
lifecycle, reactor responsiveness, and detailed metric boundaries.

## 3. Integrations

### Rails and PostgreSQL

Rails integration is explicit. Enable fiber-isolated execution before Rails
initializes, then construct the configuration with the transaction-owning model
or pool:

```ruby
# config/application.rb
config.active_support.isolation_level = :fiber
```

```ruby
require "acp/rails"

configuration = Acp::Rails.configuration_for(
  TenantSync,
  transaction_owner: ApplicationRecord
)
runtime = Acp::Runtime.new(configuration: configuration, progress: progress_store)
```

The integration validates Rails 8 and fiber isolation. Discovery,
cursor-initialization, and resolution run in the Rails executor with a selected
pool connection checked out only for the callback. Fetch runs in the executor
without a database connection. Ingestion runs in the executor and one transaction
on the selected pool. Only a confirmed commit permits progress acknowledgement;
rollback, ambient transactions, and commit exceptions are not treated as success.

For page-scoped discovery, use `tenants_page` instead of `tenants`:

```ruby
tenants_page do |after_id, limit|
  ids = Tenant.active.where("id > ?", after_id || 0)
    .order(:id).limit(limit).pluck(:id)
  [ids, ids.length == limit ? ids.last : nil]
end
```

The callback gets an opaque cursor (initially `nil`) and maximum page size; it
returns `[enumerable_ids, next_cursor]`. A `nil` cursor ends discovery after that
page. Use stable keyset order and advancing nonterminal cursors. Each page gets
its own executor/checkout, and Redis registration occurs after checkout. Pages
are sequential. Legacy `tenants` keeps its connection checked out for the whole
enumeration, including Redis registration when a chunk fills.

The selected Rails pool must have at least
`min(pipeline_capacity, resolve_concurrency + ingest_concurrency) + 1`
connections for Acp-owned work, plus application headroom. A single cycle holds
at most one selected-pool checkout at a time. Writes to another pool or database
are outside the atomic batch guarantee. Rails retries selected deadlock,
serialization, lock-timeout, and connection failures up to three times by
default; connection failures during COMMIT can have an unknown outcome, so writes
must be idempotent and version-aware. See the
[pool proof](benchmark/rails-connection-budget-proof.md) and
[pool evidence](benchmark/rails-connection-budget-evidence.md).

### Redis coordination

Load `acp/redis` explicitly. Share one coordinator between runtime progress and
ownership:

```ruby
require "acp/redis"

coordinator = Acp::RedisCoordinator.new(
  application: "billing",
  environment: ENV.fetch("RAILS_ENV"),
  program: TenantSync.name,
  redis_url: ENV.fetch("REDIS_URL"),
  worker_id: ENV.fetch("HOSTNAME"),
  lease_ttl: 60
)
runtime = Acp::Runtime.new(
  configuration: TenantSync.configuration,
  progress: coordinator,
  ownership: coordinator
)
Async { runtime.run }
```

Tenant IDs may be strings or integers; type is part of identity. Redis keys are
versioned and namespaced by application, environment, and program. Workers
register tenant membership, distribute tenants with capacity-weighted rendezvous
hashing, and claim work atomically. Membership, pause, and worker changes
converge on a bounded refresh interval; cached assignments are advisory, while
claims remain authoritative. Discovery removes old membership only after a full
successful scan. Disabled tenants retain progress.

Leases renew at one-third of their TTL using Redis server time. A worker that
cannot confirm renewal stops starting work for that cycle. Cursor advancement
atomically checks lease token, expected revision, monotonic timestamp, and poll
ID. Repeating a poll ID reconciles a lost response. Previously initialized
progress cannot silently reset if its Redis record disappears.

The coordinator owns two bounded reactor-local connection pools (defaults: 16
ordinary, 8 renewal/heartbeat). Close it after the runtime drains. Use one
coordinator per worker/reactor and budget both pools along with other Redis
clients. Use a writable Redis primary; Redis Cluster is unsupported. Configure
non-evicting durable Redis storage and backups for the complete `acp:v1:*`
namespace. Failover can lose recently acknowledged asynchronous replication;
replay-safe ingestion remains necessary. See the
[Redis reference](docs/redis-reference.md) for cache, script, timeout, and
recovery details, and the [coordination benchmark report](benchmark/redis-coordination-evidence.md)
for measurements.

## 4. Run and operate workers

For a Rails application, the packaged `acp-worker` boots Rails, validates the
program and pool budget, configures Redis coordination, then runs one reactor in
the current process:

```sh
bundle exec acp-worker \
  --rails config/environment \
  --program TenantSync \
  --transaction-owner ApplicationRecord \
  --application billing \
  --environment production \
  --lease-ttl 60 \
  --discovery-interval 60 \
  --drain-timeout 30
```

Run multiple processes under your supervisor; the worker does not fork or manage
replicas. Program, namespace, capacity, lease, discovery, and drain options have
environment settings; `REDIS_URL` configures Redis. These include
`ACP_PROGRAM`, `ACP_TRANSACTION_OWNER`,
`ACP_APPLICATION`, `ACP_ENVIRONMENT`, `ACP_WORKER_ID`, `ACP_PIPELINE_CAPACITY`,
`ACP_FETCH_CONCURRENCY`, `ACP_INGEST_CONCURRENCY`, `ACP_RESOLVE_CONCURRENCY`,
`ACP_LEASE_TTL`, `ACP_DISCOVERY_INTERVAL`, and `ACP_DRAIN_TIMEOUT`;
`ACP_NAMESPACE` aliases `ACP_APPLICATION`. By default, worker ID combines
`HOSTNAME` and process ID; set `ACP_WORKER_ID` to a stable unique identity when
the supervisor provides one. `SIGTERM`/`SIGINT` stop new cycles and drain active
work to the deadline, then cancel remaining tasks. Set the supervisor grace
period slightly longer than the drain timeout. A force-killed worker recovers
through lease expiry and replay.
Programs with reusable reactor-local API clients may define `acp_shutdown` to
close them after a graceful exit.

With Rails loaded, lifecycle events are published as `*.acp` notifications.
Events include program, worker, tenant, poll, stage, attempt, duration, and error
class, but omit credentials, exception messages, and batch bodies. Runtime
metrics and health snapshots expose aggregate progress, queue, retry, lease,
discovery, and reactor-pressure data; tenant IDs are not metric labels. Notification
subscribers and `on_error` run synchronously on the reactor. For blocking export,
use the bounded `Acp::BoundedExporter`; it drops new events when its bounded queue
is full. Details are in the [runtime reference](docs/runtime-reference.md).

## 5. Verify and develop

Run the default test and lint suite:

```sh
bundle exec rake
```

Redis integration tests use a disposable Redis database:

```sh
ACP_TEST_REDIS_URL=redis://127.0.0.1:6379/15 bundle exec rake test:redis
```

PostgreSQL and Redis integration tests:

```sh
ACP_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/acp_test \
ACP_TEST_REDIS_URL=redis://127.0.0.1:6379/15 \
  bundle exec rake test:postgres
```

Integration tasks create and drop only their consumer-owned test tables. CI tests
Ruby >= 3.2, Rails 8.0/8.1, Async 2.35–2.37, and `pg` 1.5/1.6; the locked stack
uses PostgreSQL 17 and Redis 7. `bundle exec rake package:verify` builds and
checks the gem locally.

Benchmarks are workload-specific evidence, not production capacity promises:

- [Capacity and memory](benchmark/capacity-memory-evidence.md)
- [Reactor responsiveness](benchmark/reactor-responsiveness-evidence.md)
- [Runtime pressure metrics](benchmark/runtime-pressure-evidence.md)
- [Redis coordination](benchmark/redis-coordination-evidence.md)
- [Retry isolation](benchmark/retry-isolation-evidence.md)

See [`examples/tenant_sync.rb`](examples/tenant_sync.rb) for a load-safe program
definition. For full runtime semantics, budgets, histogram definitions, and
operational boundaries, see the [runtime reference](docs/runtime-reference.md).
