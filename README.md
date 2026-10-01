# Acp

Acp defines the consumer-facing contract for asynchronous tenant polling. A
program supplies discovery, cursor initialization, tenant resolution, fetching,
and ingestion; it does not need to provide an HTTP adapter. Acp does not load
Rails or create database, Redis, or HTTP connections when required.
The gem supports Async `>= 2.35, < 2.38`, whose declared Ruby support includes
the gem's minimum Ruby 3.2.

## Program definition

```ruby
class TenantSync < Acp::Program
  interval 60
  fetch_concurrency 300
  ingest_concurrency 5
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

Definitions are declarative: callbacks are stored, not executed while the class
loads. Call `TenantSync.configuration` before starting workers to validate the
required settings and callback blocks. Discovery receives an `emit` callable and
should emit IDs incrementally. `resolve` runs once per polling cycle; return
detached values where possible so lazy database reads do not happen during HTTP
waits. The fetch callback receives a `FetchContext` containing `tenant_id`, the
committed `cursor`, stable cycle `poll_id`, and one-based `attempt`. Ingestion
receives the same identity and cursor via `IngestContext`.

The runtime calls `initial_cursor` only when initializing a tenant with no
legitimate prior progress. Missing progress for a previously known tenant needs
an explicit recovery policy. Generate one ID per cycle with
`Acp::Context.new_poll_id` and reuse it across fetch/ingest contexts and retries;
increment `attempt` for the stage being retried.

`pipeline_capacity` bounds the complete path from fetch reservation through
ingestion and progress acknowledgement. Thus effective fetch concurrency is
`min(fetch_concurrency, pipeline_capacity)`. The ingestion callback must not
share unsafe mutable state with other cycles. The consumer owns connection-pool
selection, client lifecycles, and making ingestion safe to replay. Rails
integration is explicit and belongs to the application. Keep database work
inside the consumer's discovery, cursor-initialization, resolution, and ingestion
callbacks; return detached resolved values before network waits, and acquire
database connections only for database work.

## Rails 8 and PostgreSQL

The optional Rails integration targets Rails 8.x, Ruby >= 3.2, and `pg` >= 1.5,
< 2. The locked integration stack is Rails 8.1.4 with `pg` 1.6.3. Before Rails
initializes, configure fiber-isolated execution state in
`config/application.rb`:

```ruby
config.active_support.isolation_level = :fiber
```

Then explicitly build the runtime configuration with its transaction-owning
model or pool:

```ruby
require "acp/rails"

configuration = Acp::Rails.configuration_for(
  TenantSync,
  transaction_owner: ApplicationRecord
)
runtime = Acp::Runtime.new(configuration: configuration, progress: progress_store)
```

`configuration_for` validates Rails 8 and fiber isolation; it does not change
execution isolation after the application has started. Discovery,
cursor-initialization, and resolution callbacks run inside the Rails executor
with a connection checked out only for the callback. Fetch runs inside the
executor without a database connection, so resolution must return detached
values. Ingestion runs inside the executor and one transaction on the selected
pool. Only a confirmed commit allows progress acknowledgement. An explicit
`ActiveRecord::Rollback`, an ambient transaction, or a commit exception cannot
be acknowledged as success.

The selected pool must have at least
`pipeline_capacity + min(ingest_concurrency, pipeline_capacity)` connections for
the runtime's simultaneous resolution and ingestion work. Add headroom for web
requests, jobs, and other users of that same pool. Discovery runs before polling
begins. A consumer must write destination rows through the selected pool; writes
to another database and transaction work spawned in consumer-created threads
are outside the atomic batch guarantee. The gem creates neither destination nor
progress tables.

Rails ingestion retries selected deadlock, serialization, lock-timeout, and
connection failures up to three times by default, with the same materialized
batch and poll ID. A connection failure during COMMIT can leave the outcome
unknown, so connection failures are replayed only through this same idempotent
ingestion contract. Keep event writes protected by unique constraints and make
mutable updates version-aware. Exhausted or unclassified failures do not advance
progress; a later cycle can replay the batch. A commit already confirmed before
a progress-store failure is never rerun in that cycle.

The supported stack is verified for ordinary PostgreSQL I/O under Async with
the Rails pool and adapter in `bundle exec rake test:postgres`. The `pg` driver
cooperates with Ruby's fiber scheduler for ordinary socket waits, but CPU-heavy
transformations and blocking native calls inside callbacks can still stall the
reactor. Validate the full Rails/pool/adapter stack when changing versions or
customizing adapters.

Run the PostgreSQL and Redis integration task locally with both services available:

```sh
ACP_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/acp_test \
  ACP_TEST_REDIS_URL=redis://127.0.0.1:6379/15 \
  bundle exec rake test:postgres
```

The integration task creates and drops only its own consumer-owned test tables.
CI runs it against PostgreSQL 17 in a separate job.

## Batches, cursors, and retries

`Acp::Batch` holds consumer-owned `data` and an explicit `next_cursor`; empty
data and an unchanged cursor are valid. Cursors use UTC with exactly
microsecond precision. `Acp::Timestamp.dump` serializes them as ISO 8601 with six
fractional digits, and `load` restores a UTC `Time`. Finer precision is rejected
rather than silently discarded. Backward advancement is rejected by
`Batch#validate_after!`; consumer lookback changes fetch inputs, never durable
cursor monotonicity.

Configure independent retry policies with exception classes and/or predicates:

```ruby
fetch_retry on: [Timeout::Error], max_attempts: 4, max_elapsed: 45,
  backoff: 0.5, max_backoff: 8, jitter: 0.2

ingest_retry on: ->(error) { error.is_a?(DatabaseBusy) }, max_attempts: 3,
  max_elapsed: 30, backoff: 1, max_backoff: 5
```

Attempts are one-based and budgets include the original attempt. Backoff doubles
from `backoff`, is capped by `max_backoff`, and applies symmetric fractional
jitter. Cancellation and process-level exceptions are never retryable. The
program contract describes policies; a runtime is responsible for enforcing the
elapsed deadline and applying the returned delay.

## Local runtime

Run a validated program inside an Async task and provide a progress adapter:

```ruby
progress = MyProgressStore.new
runtime = Acp::Runtime.new(configuration: TenantSync.configuration, progress: progress)

Async { runtime.run }
```

The adapter implements `read(tenant_id)`, `initialize_cursor(tenant_id, cursor)`,
and `acknowledge(tenant_id, poll_id, cursor)`. Initialization must be
idempotent and return the stored cursor. Acknowledgement must be idempotent by
`poll_id`; it is retried independently after ingestion returns, which represents
a confirmed consumer commit. Runtime cancellation propagates through callback,
retry, queue, and worker waits. Stop the surrounding Async task for immediate
cancellation, or call `runtime.request_shutdown(timeout: 30)` to stop scheduling
new cycles and drain active work to a bounded deadline. At the deadline,
outstanding tasks are cancelled. Use `pause_tenant(id)` and `resume_tenant(id)` to
pause polling without resetting its committed cursor.

An optional ownership adapter implements `acquire(tenant_id)` and
`release(tenant_id, token)`. A false/nil acquisition skips that cycle and tries
again after one interval. The default `Acp::LocalOwnership` claims every tenant.
An optional `on_error` callable receives `(tenant_id, error, stage)` for
recoverable tenant and acknowledgement failures. Tests and embedded runtimes can
inject a clock implementing `now`, `sleep(duration)`, and `wait(duration) { ... }`;
the default clock uses monotonic process time and Async-aware waits.

## Redis coordination

Load `acp/redis` explicitly and share one coordinator between runtime progress
and ownership:

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

Tenant IDs must be strings or integers; their type is encoded into the identity,
so integer `12` and string `"12"` never collide. Redis keys are versioned and
scoped by application, environment, and program. The coordinator registers the
discovered tenant set, assigns tenants among live workers using a capacity-weighted
consistent hash, and claims them atomically. Claims, worker scans, and discovery
scans are bounded (the default maximum live workers is 256; configure
`worker_scan_limit` for a larger deployment). Worker health is refreshed
independently; when a worker appears or expires, assignments converge as tenant
cycles reach safe boundaries. Each in-flight lease renews at one-third of its
configured duration using Redis server time. A worker that cannot confirm renewal
stops starting work for that cycle.

Cursor state has no lease TTL. Initialization records a permanent marker so an
unexpectedly missing progress hash raises `Acp::MissingProgressError` rather than
silently resetting a previously known tenant. Advancement checks the lease token,
expected revision, monotonic timestamp, and poll ID in one Lua script. Repeating
the same poll ID resolves a lost response; even an unchanged timestamp increments
the revision. Runtime retains the batch reservation after a confirmed database
commit while retrying Redis. If ownership is lost, the next owner replays from
durable progress. Redis leases cannot fence PostgreSQL: an old process can still
commit a transaction after takeover, so ingestion must be idempotent and safe for
out-of-order replay.

The client opens a dedicated Redis connection per command. This avoids a blocked
command monopolizing the connection used for renewal; use a Redis endpoint and
server connection limit sized for active runtime operations. It uses ordinary
Redis Ruby sockets, which cooperate with Ruby's fiber scheduler. Use a single
writable Redis primary for v1. Redis Cluster is unsupported because the atomic
scripts touch multiple keys without a cluster hash-tag layout.

Configure Redis for durable, non-evicting coordination data: use a `noeviction`
policy, persistence appropriate to the acceptable recovery-point objective, and
backups that include all `acp:v1:*` keys. A primary failover can lose recently
acknowledged writes if replication is asynchronous; after failover, replay-safe
ingestion is still required. Restore Redis and PostgreSQL from a mutually
consistent backup where possible. If progress may be ahead of the restored
database, reset only the affected program namespace to a cursor known to be no
later than restored database effects; never delete only progress keys or lease
keys as a routine recovery action. A deliberate full reset removes the namespace
after workers are stopped and requires replay-safe destination writes.

Run process-level Redis coordination tests against a disposable Redis database:

```sh
ACP_TEST_REDIS_URL=redis://127.0.0.1:6379/15 bundle exec rake test:redis
```

The integration task exercises process termination and lease expiry, conditional
progress updates, response-loss reconciliation, and renewal during a fetch longer
than the lease's initial duration.

The scheduler staggers first due times deterministically per program and tenant,
uses a fair due-time heap, and creates no per-tenant waiting tasks. Pipeline
capacity includes active fetches, completed batches queued for ingestion, and
cycles awaiting progress acknowledgement. Fetch concurrency is separately
bounded by `min(fetch_concurrency, pipeline_capacity)`; ingestion uses a fixed
worker group bounded by both ingest concurrency and pipeline capacity. Successful
cycles become due at `max(cycle_start + interval, acknowledgement_time)`. A
failed cycle is isolated and rescheduled after one interval; stage retries retain
their cursor, poll ID, resolved context, or batch as appropriate.

## Dedicated Rails worker

The gem packages `acp-worker`. It boots the Rails environment, resolves the
program and transaction-owner constants, validates the Rails/PostgreSQL pool
budget, creates Redis coordination, and runs one reactor in the current process.
It does not fork Rails or manage replicas; run multiple processes under systemd,
Kubernetes, Nomad, or another supervisor:

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

Options can also be supplied as `ACP_PROGRAM`, `ACP_TRANSACTION_OWNER`,
`ACP_APPLICATION`, `ACP_ENVIRONMENT`, `ACP_WORKER_ID`, `ACP_PIPELINE_CAPACITY`,
`ACP_FETCH_CONCURRENCY`, `ACP_INGEST_CONCURRENCY`, `ACP_LEASE_TTL`,
`ACP_DISCOVERY_INTERVAL`, and `ACP_DRAIN_TIMEOUT`; `REDIS_URL` configures Redis.
`ACP_NAMESPACE` aliases the application component of the Redis namespace.
By default, the worker ID combines `HOSTNAME` and the process ID; set
`ACP_WORKER_ID` when the supervisor provides a stable unique process identity.
Capacity overrides are validated against the selected Rails pool before polling
starts. Startup/configuration failures exit with status 78; runtime failures exit
nonzero so the supervisor can restart the process.

`SIGTERM` and `SIGINT` start a bounded drain. The worker stops acquiring new
cycles, allows active fetch/ingestion/commit/cursor work to finish until the drain
deadline, then cancels remaining tasks and exits. Set the supervisor's termination
grace period slightly longer than `ACP_DRAIN_TIMEOUT`. A force-killed process is
recovered through lease expiry and replay from committed progress. Use rolling
deployments with enough remaining replicas to serve work during the drain window.

Discovery runs under a renewable Redis coordinator lease. Each emitted tenant is
registered incrementally, while membership removals are applied only after a full
enumeration completes. An interrupted or failed scan leaves prior membership
intact. Disabled tenants stop receiving new cycles; their progress is retained and
reactivation resumes from that cursor. Pausing and resuming is also persisted by
Redis. `discovery_interval` controls re-enumeration and `retry_cooldown` controls
the delay following exhausted failures.

With Rails loaded, lifecycle events are published as `*.acp`
`ActiveSupport::Notifications` events. They include program, worker, tenant, poll,
stage, attempt, duration, and classified error type, but omit credentials,
exception messages, and batch bodies. Use tenant IDs for event-level diagnostics;
aggregate metrics from `runtime.metrics` without tenant labels. The metrics
snapshot includes assigned tenants, active fetches/cycles/ingests, queue wait,
ingestion duration, cursor-update delay, retries, polling lag, discovery health,
and lease renewal misses. `runtime.health` reports running/stopping state and the
same aggregate snapshot; Redis `worker_statuses` reports live worker capacity and
activity.

Size the Rails pool for at least
`pipeline_capacity + min(ingest_concurrency, pipeline_capacity)` plus web/job
headroom; startup enforces that minimum. Redis opens a dedicated connection per
command to keep lease renewal independent of blocked commands. Budget Redis
connections for each worker's active cycles, heartbeat, discovery and progress
operations, plus other application clients. Scale replica count and pipeline
capacity within those limits. Acp leaves process supervision and replica scaling
to the deployment environment.

Programs that reuse reactor-local API clients can define a class method
`acp_shutdown`; the executable calls it inside the Rails executor after a graceful
runtime exit. Close reusable clients there. Clients scoped to one callback should
be closed by that callback's own ensure block.

See [`examples/tenant_sync.rb`](examples/tenant_sync.rb) for a load-safe consumer
definition. Requiring it does not run discovery or open connections.

## Development

Run `bundle exec rake test` and `bundle exec rake rubocop` to verify changes.
