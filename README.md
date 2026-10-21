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

Definitions are declarative: callbacks are stored, not executed while the class
loads. Call `TenantSync.configuration` before starting workers to validate the
required settings and callback blocks. Discovery receives an `emit` callable and
should emit IDs incrementally. `resolve` runs once per polling cycle; return
detached values where possible so lazy database reads do not happen during HTTP
waits. The fetch callback receives a `FetchContext` containing `tenant_id`, the
committed `cursor`, stable cycle `poll_id`, and one-based `attempt`. Ingestion
receives the same identity and cursor via `IngestContext`.

Discovery registrations are buffered in `discovery_batch_size` chunks (default
**256**, configurable from 1 through **1024**). For N distinct emitted IDs and
batch size B, Redis registration requires `ceil(N / B)` atomic operations and
membership revision increments, plus one revision increment on completion. The
last partial chunk is flushed before completion; an empty pass completes without
registration. Duplicates are ignored by the runtime. Discovery retains scheduler
records for all distinct IDs, but the registration buffer holds at most B IDs.

The runtime calls `initial_cursor` only when initializing a tenant with no
legitimate prior progress. Missing progress for a previously known tenant needs
an explicit recovery policy. Generate one ID per cycle with
`Acp::Context.new_poll_id` and reuse it across fetch/ingest contexts and retries;
increment `attempt` for the stage being retried.

By default, `pipeline_capacity` bounds the complete path from fetch reservation through
ingestion and progress acknowledgement. Thus effective fetch concurrency is
`min(fetch_concurrency, pipeline_capacity)`. The ingestion callback must not
share unsafe mutable state with other cycles. The consumer owns connection-pool
selection, client lifecycles, and making ingestion safe to replay. Rails
integration is explicit and belongs to the application. Keep database work
inside the consumer's discovery, cursor-initialization, resolution, and ingestion
callbacks; return detached resolved values before network waits, and acquire
database connections only for database work.

Fetch callbacks run in Async fibers on one reactor. HTTP socket waits overlap
when the client cooperates with Ruby's fiber scheduler; a blocking native client
or CPU-heavy callback can stall requests and lease renewal. `fetch_concurrency`
limits callbacks, so consumer-created request fanout needs its own limit. The
real HTTP test uses Ruby's `Net::HTTP` and verifies overlapping socket waits while
ownership renewals continue. A callback-scoped client should close before returning:

```ruby
require "net/http"
require "json"

fetch do |tenant, context|
  uri = URI.parse(tenant.fetch(:endpoint))
  parameters = URI.decode_www_form(uri.query || "")
  parameters << ["since", Acp::Timestamp.dump(context.cursor)]
  uri.query = URI.encode_www_form(parameters)
  client = Net::HTTP.new(uri.host, uri.port, nil)
  client.use_ssl = uri.scheme == "https"
  client.open_timeout = 5
  client.read_timeout = 20
  client.write_timeout = 20
  response = client.start { |http| http.get(uri.request_uri) }
  raise IOError, "upstream HTTP #{response.code}" unless response.is_a?(Net::HTTPSuccess)

  payload = JSON.parse(response.body)
  Acp::Batch.new(data: payload.fetch("records"), next_cursor: payload.fetch("next_cursor"))
end
```

`resolve_concurrency` separately limits cursor initialization and resolution;
it defaults to `ingest_concurrency` and is capped by pipeline capacity. Large
HTTP concurrency therefore does not require an equally large database pool.

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

### Page-scoped discovery

Legacy `tenants { |emit| ... }` callbacks retain their original checkout scope:
Rails holds the selected connection throughout enumeration, including Redis I/O
when a registration buffer fills. To release it between database pages and
before Redis registration, replace `tenants` with `tenants_page`:

```ruby
discovery_batch_size 256
tenants_page do |after_id, limit|
  ids = Tenant.active.where("id > ?", after_id || 0)
    .order(:id).limit(limit).pluck(:id)
  [ids, ids.length == limit ? ids.last : nil]
end
```

The callback receives an opaque cursor (initially nil) and a maximum page size.
Return `[enumerable_ids, next_cursor]`; a nil next cursor ends discovery **after**
processing that page. Empty final pages are valid. Use stable keyset ordering;
nonterminal cursors must advance. A page exceeding the supplied limit raises an
error and cannot complete the generation. Declaring either discovery callback
replaces the other, including inherited callbacks.

Each page callback runs in its own Rails executor/selected-pool checkout. Rails
also materializes a returned lazy enumerable inside that checkout; only detached
IDs leave it. Redis registration runs after both scopes have exited. Pages run
sequentially, so discovery still uses at most one selected-pool
connection at a time and does not add page-level tasks. Runtime-owned
page storage and the separate registration buffer each hold at most B IDs;
application-owned query results/cursors need their own size bounds. Without the
Rails integration, the runtime materializes the enumerable after callback return,
so custom connection wrappers should materialize IDs before releasing their own
checkout, or acquire their connection inside the enumerable. Work using additional
pools remains application-managed. Applications relying on a connection spanning
the entire enumeration should retain the legacy callback until migrated.

The selected pool must have at least
`min(pipeline_capacity, resolve_concurrency + ingest_concurrency) + 1`
connections for Acp-owned checkouts. Limits 8/8/8 require 9 connections;
100/5/7 require 13; 1/5/7 require 2 (pipeline/resolve/ingest).
Each runnable cycle holds at most one selected-pool checkout for cursor
initialization, resolution, or ingestion. Fetch/network and progress waits hold
none under the detached-data callback contract. Initial-cursor callbacks share
the resolution semaphore. One discovery callback can overlap the cycles;
initial discovery runs before polling but is conservatively included.
Parked retries hold no checkout and must regain a pipeline slot before resuming;
discovery pages remain sequential. Checkout and executor scopes unwind before
reservation release, including errors, cancellation, and lease loss. See the
[checkout proof and revalidation rules](benchmark/rails-connection-budget-proof.md) and
[exact-bound tests and sizing smoke evidence](benchmark/rails-connection-budget-evidence.md).

Add application headroom for web requests, jobs, nested consumer thread/fiber
fanout, and other users of that same pool. Other pools need separate budgets.
Callbacks must return detached data and must not retain connections or spawn
database work outliving their scope. Preexisting application checkouts remain
application-owned and need their own headroom. A consumer must write destination rows
through the selected pool; writes to another database and transaction work spawned
in consumer-created threads
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
fetch_retry on: [Timeout::Error], max_attempts: 4, timeout: 20, max_elapsed: 45,
  backoff: 0.5, max_backoff: 8, jitter: 0.2

ingest_retry on: ->(error) { error.is_a?(DatabaseBusy) }, max_attempts: 3,
  max_elapsed: 30, backoff: 1, max_backoff: 5
```

Attempts are one-based and budgets include the original attempt. Backoff doubles
from `backoff`, is capped by `max_backoff`, and applies symmetric fractional
jitter. Cancellation and process-level exceptions are never retryable. The
runtime enforces the total `max_elapsed` deadline across attempts and backoff,
and optional `timeout` limits each attempt. These are cooperative deadlines:
callbacks must yield for cancellation to take effect. An unspecified deadline or
timeout is unlimited; configure client socket timeouts and stage deadlines for
production requests. An expired stage raises `Acp::StageTimeoutError`, a
`Timeout::Error` subclass. Ingestion's elapsed budget includes queue waits and
unwinds active ingestion before releasing its reservation. After a confirmed
commit, progress acknowledgement is retried independently until success,
ownership loss, or cancellation; exhausting ingestion retries does not refetch
an already committed batch in the same cycle.

### Retry admission isolation and upstream bulkheads

Isolation is opt-in; these settings can be added to a program definition:

```ruby
pipeline_capacity 100
retry_capacity 100
upstream_capacity 25
upstream_group { |tenant_id| ROUTING.fetch(tenant_id) } # opaque, nonsecret group ID
resolve_timeout 10
initial_cursor_timeout 15
coordination_timeout 5
cleanup_timeout 1
acknowledge_retry on: [IOError, Timeout::Error], max_attempts: 4,
  timeout: 5, backoff: 0.5, max_backoff: 5
```

`retry_capacity` defaults to **0**, preserving coupled pipeline admission. With
P pipeline slots and R retry slots, a failed fetch or ingestion parks its cycle
during backoff and releases its pipeline slot. It retains one retry slot through
the rest of that cycle, including resumed work. Resumption requires a pipeline
slot again. Eligible retries and fresh work alternate; due-time/FIFO ordering
within each class prevents either class from starving the other. A full retry
budget ends a newly failing uncommitted cycle and reschedules it after
`retry_cooldown`, incrementing `retry_overflows`.

The aggregate bound is **P + R cycle tasks**, contexts, lease heartbeats, and
retained batches. At most P cycles hold runnable pipeline slots, including
queued ingestion and pending acknowledgements. Enable the separate byte budget
below to bound accounted payload reservations across both P and R. Poll ID,
cursor, resolved value, and the materialized ingestion batch keep
their identity across parking. Leases continue renewing while parked; resumed
attempts validate ownership before invoking callbacks. Ingestion concurrency,
resolution concurrency, and the Rails pool budget above remain unchanged.
Including lease heartbeats, I fixed ingestion workers and their callback tasks,
and background tasks, the runtime-owned task bound is **2(P + R) + 2I + 6**;
the caller's task and consumer-created tasks are excluded. The event queue is
bounded at 2(P + R) + 1 messages.

`upstream_group` is an optional synchronous, nonblocking routing callback given
the tenant ID **before admission**, independent of credential resolution. Return
a nonempty String of at most 256 bytes identifying the upstream/credential group;
use an opaque ID rather than credentials. Only its SHA-256 digest is retained in
reservation bookkeeping, and no group keys are emitted in diagnostics.
`upstream_capacity` defaults to P and limits each group's pipeline occupancy
across every stage. Set it below P to leave capacity for other groups. Saturated
groups wait as scheduler records, without creating tasks. Group bookkeeping has
at most P entries and removes idle entries. Group routing must stay stable during
a cycle; routing changes take effect on the next cycle.

Acknowledgements retain pipeline slots, including retry backoff. A prolonged
coordination outage therefore stops new admission at P committed pending
acknowledgements, with at most R other parked cycles. Confirmed ingestion is
never rerun to retry an acknowledgement. `acknowledge_retry` is independent of
`ingest_retry`; if omitted it inherits that policy for compatibility. After its
retry window is exhausted, acknowledgement keeps trying at `retry_cooldown`
until success, ownership loss, or cancellation. Its `timeout` bounds an attempt;
its `max_elapsed` selects the retry window, not a deadline that abandons a commit.

`resolve_timeout` includes resolution semaphore checkout and callback service.
`initial_cursor_timeout` covers the complete initial cursor operation, including
progress reads, resolution checkout, callback, and initialization writes.
`coordination_timeout` bounds **each adapter method**, including Redis checkout
and command I/O. These three settings default to nil (unlimited). Retry `timeout`
includes stage queue waits; `max_elapsed` additionally includes backoff and retry
readmission. Cleanup uses `cleanup_timeout` (default **1 second**), capped by the
remaining shutdown drain budget. Once the drain deadline expires, only a
nonblocking ownership release can finish; an unsuccessful release expires by TTL.
All budgets are cooperative: callbacks and cleanup must yield and propagate
cancellation. Callback unwinding completes before reservations are released.
Immediate cancellation shares one `cleanup_timeout` deadline across releases.

Metrics expose current and `max_` peaks for `pipeline_reservations`,
`retry_reservations`, `parked_cycles`, `retained_batches`,
`pending_acknowledgements`, and `upstream_groups`. `active_cycles` now includes
parked cycles; `pipeline_utilization` counts pipeline reservations only.
`pressure[:retry_admission_wait]` measures eligible retry queue time.

### Payload and complete-pipeline budgets

Memory limits are opt-in and inherited with the program definition:

```ruby
max_response_bytes 32 * 1024
max_batch_bytes 64 * 1024
retained_byte_capacity 256 * 1024 * 1024
payload_estimate { |_tenant_id| 64 * 1024 } # optional; defaults to max_batch_bytes
```

`retained_byte_capacity` requires `max_batch_bytes`. Limits are positive integer
bytes; `Batch.new(data:, next_cursor:, size_bytes:)` accepts a nonnegative integer
size. A batch limit requires every returned batch to declare `size_bytes`.
Without a batch limit/budget this metadata remains optional for compatibility.

The consumer estimates the retained Ruby object graph, including strings,
arrays/hashes, record metadata, and copies retained by ingestion. The pre-fetch
estimate must additionally cover peak fetch-local materialization, wire buffers,
decoding temporaries, and any callback-local copies. It is a synchronous,
nonblocking, stable per-cycle callback given the tenant ID before resolution;
use a conservative upper bound when the size is unknown. The scheduler reserves
the estimate **before creating the cycle task**. Byte-blocked tenants wait as
scheduler records, ordered by due time/FIFO among eligible groups; smaller work
cannot repeatedly overtake the oldest eligible large request. This deliberately
allows head-of-line blocking. Retries retain their byte reservation while parked,
so readmission needs only a pipeline slot and cannot deadlock behind byte waiters.

Reservations cover resolution, fetch-local work, queued/active ingestion,
ingestion retry backoff, and pending acknowledgement, and are held at their full
estimate until the cycle and all ingestion callback cleanup have unwound. They
are not shrunk to the accepted batch size. Ownership skips, failures, retry
exhaustion, lease loss, and cancellation release the reservation. Callbacks must
propagate cancellation and must not retain batch references in detached tasks,
global caches, or other work that outlives their callback. Mutations/copies must
stay within the estimate.

Wire length is **not decoded Ruby memory**: compression, JSON object overhead,
encoding, SQL serialization, and temporary copies can multiply it. Consumers
must enforce the response limit while streaming, using
`context.validate_response_bytes!(cumulative_bytes)` **before retaining each
chunk**, and bound decoding against `context.reserved_bytes`. This helper raises
`Acp::PayloadLimitError`; configuring `max_response_bytes` does not intercept an
arbitrary HTTP client's reads. For larger results, use bounded streaming/spooling
with callback-owned cleanup, or split the upstream page before materialization.

After fetch returns, Acp rejects missing size metadata, a batch above
`max_batch_bytes`, or a batch above its reservation. It never tries to grow a held
reservation. An estimate above the entire budget fails immediately instead of
waiting. These are recoverable tenant failures rescheduled after `retry_cooldown`,
without ingestion or progress advancement and without retrying the rejected
payload in that cycle. Post-materialization rejection cannot undo the allocation:
**the budget bounds accounted reservations, not total RSS or arbitrary consumer
allocations**. A strict payload-memory bound requires the streaming/decoder
contract above. Ruby heaps, GC retention, task/tenant state, native libraries,
connection buffers, and application memory need separate process headroom.

Metrics expose `reserved_bytes`/`max_reserved_bytes` (including fetch-local
allowance), `retained_bytes`/`max_retained_bytes` (declared sizes of accepted
batches through acknowledgement), `byte_budget_waits`, current/peak
`byte_budget_waiters`, and `pressure.byte_budget_wait`. Unreturned fetch-local
objects and rejected oversized allocations are represented by their reservation,
not measured as accepted retained bytes. `payload_rejections` counts rejected
cycles; `oversized_batches` and `estimate_overflows` distinguish batch-limit and
estimate failures. Stage count peaks include `max_active_resolves`, fetches,
ingests, cycle tasks, and pipeline/retry reservations. `pressure.pipeline_residence`
includes callback and ownership cleanup plus completion-event delivery, up to
final reservation release; it complements the shorter `cycle_duration`.

For sizing, start with **slots = ceil(target cycles/sec per worker × mean stage
residence seconds / target utilization)**. For fetch/ingest/resolve, include the
expected number of attempts per cycle; initial cursor resolution is extra startup
work. For pipeline sizing use the **entire admitted-cycle residence**, including
queued ingestion, retry/backoff, cursor reads, and acknowledgement. With retry
isolation, also size the separately parked R occupancy; the aggregate task bound
remains P + R. Validate tails and renewal slack separately from these mean-value
estimates. An indefinitely unavailable acknowledgement adapter has no finite
sustainable capacity; count and byte admission provide backpressure.

The named local `capacity-local-headroom` workload measured mean fetch 20.19 ms,
ingestion 10.14 ms, acknowledgement 5.15 ms, and complete pipeline 38.71 ms.
At 550 cycles/sec and a **75% target utilization**, the formula suggests 15 fetch,
8 ingestion, and 29 pipeline slots. The tested 16/8/32 configuration delivered
553.1 and 546.9 cycles/sec in equal local windows, with admission-lag p95 ≤1 ms.
Its 32 × 64 KiB reservations need 2 MiB, independently of the larger configured
256 MiB ceiling. This is local smoke evidence, not a service-fleet capacity claim.
The independent sweeps demonstrate useful overlap at P=32 and ingestion
saturation beyond it. See [capacity/memory evidence](benchmark/capacity-memory-evidence.md)
for commands, full service failures, sizing assumptions, and connection budgets.

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

## Reactor responsiveness and telemetry

Runtime discovery, admission scans, state/heap construction, membership and
assignment transformations check elapsed work at invariant-safe boundaries and
cooperate after **5 ms**. A heap insertion/removal and an admission/reservation
mutation finish before yielding. Tenant resume uses indexed O(log n) heap deletion.
An individual callback, allocation, GC pause, native call, or TLS handshake can
exceed the target; it is a cooperative work budget, not a preemptive deadline.
Metrics snapshots of tenant state are advisory and may span yields.

Use scheduler-compatible socket/database clients with exclusive checkout per
fiber, bounded pools, streaming response limits, and finite connection/read/write
timeouts. Cancellation must unwind the request, close/discard an interrupted
connection, and propagate to Acp before its reservation can be released. Finish
the runtime drain before closing persistent clients. The measured reference client
in `benchmark/http_workload.rb` verifies TLS, reuses connections for 30 seconds,
disables automatic request replay, and closes clients on exceptions/cancellation.

`ActiveSupport::Notifications` subscribers and `on_error` run **synchronously on
the calling reactor**. Subscriber errors are swallowed by emission; `on_error`
errors propagate. Their elapsed times appear in
`pressure.notification_service` and `pressure.error_handler_service`, alongside
`reactor_lag`, renewal wakeup/service times and Redis lease-renewal slack. Slow
logging/export in either hook can delay every callback and renewal; separate
Redis connection pools do not prevent CPU/native reactor stalls.

For I/O-heavy telemetry, embedded runtimes can use a bounded exporter directly:

```ruby
exporter = Acp::BoundedExporter.new(capacity: 256, max_event_bytes: 4096) do |json|
  telemetry_client.write(json) # thread-owned client with finite I/O deadlines
end
runtime = Acp::Runtime.new(configuration: TenantSync.configuration,
  progress: progress, exporter: exporter)
begin
  Async { runtime.run }.wait
ensure
  exporter.close(timeout: 0.5)
end
```

One consumer thread owns copied, frozen JSON strings. Storage is at most 256
pending records plus one in flight, each at most 4096 bytes (plus Ruby overhead).
`offer` never waits for queue capacity: **drop the new event** when full, oversized,
or closed. Serialization occurs on the producer, so offer small, flat metadata,
never batches, models, or exception objects. `metrics` exposes accepted, dropped,
exported, error and queued counts; runtime snapshots expose these as `telemetry`.
For a Notifications bridge, the synchronous subscriber should only call `offer`.

The caller owns the exporter's lifecycle. `close(timeout:)` rejects new events,
waits cooperatively up to its timeout, and discards remaining queued events.
It returns false if a consumer call is still executing; that call may finish later.
It is never killed and cannot hold a polling reservation or mutate destination
state. Export errors increment a counter and consumption continues without an
unbounded retry buffer. No Rails executor or database context is inherited by the
thread: the consumer must establish its own executor/connection scope if needed.
Its I/O must release the GVL and have its own timeout. Native calls holding the GVL
and CPU-heavy exporters still require process isolation. Do not call `close` from
the consumer itself. Export drain is an explicit additional shutdown budget.

### CPU work, offload boundary, and measured gates

CRuby's GVL prevents Ruby CPU callbacks from running in parallel inside one worker;
more fibers/threads and more connection slots do not provide CPU parallelism.
Independent worker processes can use independent cores. The measured 50 ms
blocking parse delivered **19.5 → 40.1 cycles/sec** with one → two processes,
using **10.01 → 19.94 CPU seconds** over 10 seconds; reactor lag still failed the
supported responsiveness envelope. Keep per-callback CPU sections short, split
large transforms into bounded chunks that yield, or isolate CPU work in processes.

Acp does not offload polling/ingestion callbacks into detached threads. Measured
normal parsing/transformation fits the process model; offload would add mutation
ownership and shutdown hazards without eliminating CRuby CPU serialization.
Any future blocking-work offload must satisfy this contract before use:

- Fixed worker and pending-queue bounds; reject/backpressure before submission and
  charge queued/running inputs against the originating cycle's count/byte budgets.
- Transfer immutable/copied inputs and thread-owned clients; never share a
  reactor-local context, Active Record connection, transaction, or mutable batch.
  Establish Rails executor and connection/transaction scopes in the executor.
- Return results/errors to the originating task. Disable new destination writes
  after lease loss; destination fencing/idempotence remains mandatory.
- Cancellation stops queue admission and removes work that has not started.
  Running, non-interruptible work must finish/unwind **before releasing its lease
  or reservation**. A timeout is not proof that a thread/external write stopped.
- Shutdown stops submission and joins workers; if a call cannot be bounded or
  interrupted, use a separately supervised process and define its termination and
  destination-fencing policy. Never claim a bounded in-process drain while that
  call can still mutate destination state.

For the measured representative workload (3 s TTL, renewal every 1 s), performance
gates are reactor-lag **p99 ≤25 ms, maximum ≤150 ms**, minimum renewal slack
**≥1.5 s**, no renewal deadline misses, and callback drain within **10.3 s**
(10 s drain plus observation allowance). Unit tests use gates/interleavings;
these wall-clock thresholds belong to performance checks. The idle, saturated
synthetic/real TLS, 256 KiB payload, bounded-telemetry, and 30,000-tenant cases met
these gates. Intentional blocking parse/telemetry are negative controls.
See [reactor evidence and reproduction commands](benchmark/reactor-responsiveness-evidence.md)
for CPU/GC, socket/RSS, admission lag, parsing costs, and process-count results.

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
discovered tenant set, assigns tenants among live workers using capacity-weighted
rendezvous hashing, and claims them atomically. Workers schedule only their
locally assigned tenant snapshot. Every worker consumes membership, pause, and
worker-registry changes at `min(discovery_interval, lease_ttl / 3)` intervals,
even when another worker owns discovery. Membership reads are pipelined in bounded
chunks and cached by revision; assignments are cached until membership or worker
capacities change. Atomic claims remain authoritative during convergence.
Concurrent snapshot callers share one in-flight membership scan and one in-flight
assignment refresh per coordinator. Membership scans validate their revision again
before publishing; after three conflicting scans they fail for the next refresh
to retry. Failed or cancelled leaders wake their waiters and clear the flight.
Cached membership records carry their own revision, including when assignment
computation yields. These caches are advisory and never authorize mutations.
Direct `register_tenants` batches also update records and their revision atomically.
Claims, worker scans, and discovery scans are bounded (the default maximum live
workers is 256; configure
`worker_scan_limit` for a larger deployment). Worker health is refreshed
independently; when a worker appears or expires, assignments converge as tenant
cycles reach safe boundaries. Each in-flight lease renews at one-third of its
configured duration using Redis server time. A worker that cannot confirm renewal
stops starting work for that cycle.

For monitoring, `worker_statuses` returns current live worker heartbeats, and
`worker_assignments(tenant_ids)` reports each live worker's status and the number
of supplied tenant IDs assigned to it by the same capacity-weighted hash.

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

The coordinator reuses exclusively checked-out Redis connections in two bounded
reactor-local pools: ordinary commands (`connection_pool_size`, default 16) and
lease/discovery renewal plus worker heartbeats (`renewal_pool_size`, default 8).
Blocked ordinary commands cannot exhaust renewal capacity. Failed or cancelled
connections are discarded. Call `coordinator.close` after the runtime drains;
`acp-worker` does this automatically. Use one coordinator per worker/reactor and
budget the sum of both pool limits per worker, plus other Redis clients. It uses
ordinary Redis Ruby sockets, which cooperate with Ruby's fiber scheduler. Use a single
writable Redis primary for v1. Redis Cluster is unsupported because the atomic
scripts touch multiple keys without a cluster hash-tag layout.

All Lua operations use centralized `EVALSHA` execution. A `NOSCRIPT` reply falls
back once to `EVAL` on that checked-out connection, loading and executing the
script. No per-connection script-cache assumptions are retained, so script flush,
reconnect, and replacement-primary cache loss recover automatically. Redis ACLs
must permit both `EVALSHA` and `EVAL` as well as the scripts' underlying commands.
Transport failures and other command errors are propagated, never blindly
replayed by this executor. Only the idempotent poll-ID advancement protocol
reconciles an ambiguous advancement reply. Fault-injection wrappers must intercept
`evalsha` by script SHA as well as cold-cache `eval` when injecting post-execution
failures.

Ownership validity checks use server time and token equality. Fetch attempts and
ingestion callbacks validate ownership at their execution boundaries; Redis
advancement validates it atomically with the progress mutation. Queue entry and
exit no longer add redundant ownership round trips. Progress reads atomically
distinguish an absent cursor from missing previously initialized progress.
Claims still compute authoritative weighted rendezvous assignment against the
live worker registry, rejecting stale cached assignments after rebalance. The
measured O(worker count) cost and trigger for a versioned O(1)-claim follow-up are
recorded in [the coordination benchmark report](benchmark/redis-coordination-evidence.md).

For direct Async coordinator calls outside a runtime, `operation_timeout:`
(default nil) bounds each checkout plus command operation. Within the runtime,
`coordination_timeout` also covers custom progress/ownership adapters. When both
are set, the shorter deadline wins. Non-Async callers should configure Redis
socket timeouts through `redis_options`.

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
failed cycle is isolated and rescheduled after `retry_cooldown`; stage retries retain
their cursor, poll ID, resolved context, or batch as appropriate.
With retry isolation enabled, the additional bounded retry reservations described
above hold parked work outside pipeline occupancy.

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
`ACP_FETCH_CONCURRENCY`, `ACP_INGEST_CONCURRENCY`, `ACP_RESOLVE_CONCURRENCY`, `ACP_LEASE_TTL`,
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

Discovery runs under a renewable Redis coordinator lease. Bounded chunks of IDs
are registered atomically under its token, while membership removals are applied only after a full
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
snapshot includes assigned and discovered tenant counts (including tenants
assigned to other workers), active fetches/cycles/ingests, queue wait,
ingestion duration, cursor-update delay, retries, polling lag, discovery health,
lease acquisition/skips, completed cycles, and lease renewal misses. `pressure`
contains fixed-size timing distributions; `redis` contains the ownership
coordinator's metrics when available. All times below are **seconds**, measured
with monotonic clocks. Tenant and poll IDs occur only in diagnostic events.
`runtime.health` reports running/stopping state and the same aggregate snapshot;
Redis `worker_statuses` reports live worker capacity and activity.

### Runtime pressure metrics

| Metric | Measurement boundary |
| --- | --- |
| `overdue_tenants`, `oldest_overdue_age` | Current count and maximum age of due tenants waiting for pipeline admission. Snapshots include unfinished/failing workloads; exclude in-flight, locally paused, and disabled tenants. Redis pause/removal changes appear when assignment refresh consumes them. Both are zero while stopped. Reading health performs no Redis I/O. |
| `pressure.admission_lag` | `max(cycle_start - scheduled_due, 0)`, recorded before acquisition or cursor work for **every admission**, including ownership skips, failures, and cancellation. |
| `polling_lag`, `polling_lag_p50/p95/p99` | Compatibility keys now alias the last admission lag and its lifetime percentile bucket bounds. Previously these described only successful cycle overrun. Use `pressure.cycle_overrun` for the old concept. |
| `pressure.cycle_duration`, `pressure.cycle_overrun` | Admission to acknowledgement/terminal failure/cancellation, before lease cleanup. Overrun is `max(duration - interval, 0)`. Includes ownership, cursor work, stage waits, retries, and backoff. Every terminated cycle contributes, including skipped ownership. Incomplete cycles remain visible through active-cycle gauges and overdue waiting work. |
| `pressure.resolve_wait`, `pressure.resolve_service` | Semaphore wait versus resolver callback execution. Includes both initial-cursor and tenant-resolution callbacks, each separately sampled. |
| `pressure.fetch_wait`, `pressure.fetch_service` | Fetch semaphore wait versus callback execution, separately sampled for each attempt. |
| `pressure.ingest_wait`, `pressure.ingest_service` | Enqueue to worker dequeue versus ingestion callback execution for each attempt. Cancelled/timed-out queued messages record their elapsed wait once, even if never dequeued. Ownership validation between dequeue and callback is outside these measurements. |
| `pressure.acknowledge_wait`, `pressure.acknowledge_service` | There is no acknowledgement semaphore: admission wait is zero per attempt. Service covers each idempotent progress attempt, including its adapter I/O. |
| `pressure.cursor_read_service`, `pressure.cursor_initialize_service` | Progress adapter read and initialization operations; initial-cursor callback timing is included in resolution above. |
| `pressure.cursor_update_delay` | Entire acknowledgement lifecycle after ingestion, including retries/backoff; records unsuccessful/cancelled lifecycles too. |
| `pressure.fetch_backoff`, `pressure.ingest_backoff`, `pressure.acknowledge_backoff` | Actual elapsed retry/cooldown sleep, separately from attempt service and stage wait. A cancelled sleep records its elapsed portion. |
| `pressure.reactor_lag` | One runtime-owned sampler wakes every 100 ms and records `max(wakeup - expected_wakeup, 0)`. Resets its next expectation after waking: one delayed observation per stall, no catch-up tasks. Stops with the runtime. |
| `pressure.renewal_wakeup_lag`, `pressure.renewal_service` | Tenant heartbeat lateness relative to the requested sleep, and full renewal operation duration (including Redis checkout). Uses the coordinator's renewal interval (Redis TTL / 3). |
| `redis.pressure.commands_checkout`, `redis.pressure.renewal_checkout` | Ordinary/renewal lane semaphore checkout waits. Heartbeats and discovery renewals share the renewal lane. Cancellation before checkout records wait only. |
| `redis.pressure.commands_service`, `redis.pressure.renewal_service` | Time holding a checked-out connection, including lazy client creation and all commands/pipelines in that checkout. These are **checkout-block** durations, not individual wire-command latencies; no command or key labels are retained. Failed/cancelled commands contribute and discard their client. |
| `redis.pressure.lease_renewal_slack` | Previous confirmed tenant lease's conservative monotonic deadline minus renewal reply/failure time, **including checkout and command latency**. May be negative. Failed renewal never moves the deadline. A confirmed successful claim/renewal sets the deadline to command-start + TTL, never reply-time + TTL. Redis server timestamps are not compared to local monotonic time. |
| `redis.renewal_successes`, `redis.renewal_failures`, `redis.renewal_deadline_misses` | Confirmed tenant renewal results (rejection/ordinary exception counts as failure; cancellation is recorded in timing outcomes), and attempts finishing at/past the previous conservative deadline. A deadline miss may accompany success; a failed renewal can have positive slack. Runtime's existing top-level `renewal_deadline_misses` retains its historical meaning of failed heartbeat operations. |

`queue_wait`, `ingestion_duration`, and `cursor_update_delay` retain their existing
last-value semantics. The corresponding distributions above are preferable for
contention analysis. Timings can be nested: do not sum cycle, adapter, and Redis
durations as independent work. None of these metrics measures source-data
freshness; that requires source timestamps and a separately defined wall-clock
freshness policy.

Each distribution has exact `count`, `sum`, `last`, `min`, `max`, exclusive
`buckets`, and `outcomes` counters (`success`, `error`, `timeout`, `cancelled`),
plus `p50/p95/p99` bucket upper bounds. Timeout/cancellation records partial
elapsed work in ensure blocks. A deadline that stops an active ingestion child
appears as cancellation in child service and timeout in the parent cycle; a
queued ingestion deadline records a timeout wait. Service success means a call
returned without exception; protocol renewal rejection is distinguished by
renewal result counters. No sample is fabricated for a callback that never starts.

Histogram upper boundaries are
`[0, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 10, 30, 60, 300]`,
followed by an open-ended overflow bucket. Overflow percentiles are `nil` (JSON
`null`), rather than a misleading 300-second bound. Nonpositive slack shares the
zero bucket; its exact signed minimum/last remains available. Empty distributions
have zero count/sum/last/percentiles and `nil` min/max. Each runtime owns 23 fixed
distributions, each coordinator five; observations never allocate tenant labels
or historical sample arrays. Counters/histograms/maxima live for the lifetime of
their runtime/coordinator instance, including restarts, startup, and drain; they
do not reset per polling interval. New instances reset them. Snapshots and their
nested data are frozen copies. Snapshot generation scans current scheduler state
for overdue work; observations use constant storage and one sampler task.

Both benchmark reporters include pressure distributions and histogram boundaries;
the local reporter also samples peak overdue work and reports an independent
100 ms reactor observer. See [Story 01 implementation evidence](benchmark/runtime-pressure-evidence.md)
for the seeded before/after and saturated-fetch measurements.

Size the Rails pool for at least
`min(pipeline_capacity, resolve_concurrency + ingest_concurrency) + 1`
plus web/job headroom; startup enforces that minimum. Redis reserves a separate
connection pool for renewals and heartbeats. Budget Redis connections from both
pool limits plus other application clients. Scale replica count and pipeline
capacity within those limits. Acp leaves process supervision and replica scaling
to the deployment environment.

Programs that reuse reactor-local API clients can define a class method
`acp_shutdown`; the executable calls it inside the Rails executor after a graceful
runtime exit. Close reusable clients there. Clients scoped to one callback should
be closed by that callback's own ensure block.

See [`examples/tenant_sync.rb`](examples/tenant_sync.rb) for a load-safe consumer
definition. Requiring it does not run discovery or open connections.

For mutable destination rows, use a unique event key and compare source versions
inside the database upsert. [`examples/versioned_importer.rb`](examples/versioned_importer.rb)
shows a PostgreSQL importer that deduplicates identical events and ignores older
replays, including a delayed transaction from a former lease owner. The example's
integration test verifies that stale data cannot replace the newer row.

## Reliability, load tests, and support matrix

`test/fixtures/reliability_upstream.rb` is a seeded Async-compatible source with
configurable latency ranges, timeout/disconnect/slow responses, payload sizes,
record counts, and timestamp windows. The default CI suite tests the bounded
runtime and fixture behavior. Redis and PostgreSQL fault tests use disposable
services and are run separately with `test:redis` and `test:postgres`; together
they cover process termination/lease expiry, renewal during slow fetch, Redis
outage and lost advancement replies, transaction rollback/cancellation, deadlock
retry, commit-before-progress replay, and progress only after confirmed commit.
Normal Redis lease exclusivity is asserted separately from replay safety after
takeover: an old owner cannot mutate Redis state, but PostgreSQL may still accept
its transaction, so destination version checks remain required.

The local scheduler/load harness uses in-memory progress and isolates local
capacity and memory behavior:

```sh
ACP_BENCH_TENANTS=30000 ACP_BENCH_DURATION=70 \
  ACP_BENCH_FETCH_CONCURRENCY=100 ACP_BENCH_INGEST_CONCURRENCY=16 \
  ACP_BENCH_PIPELINE_CAPACITY=256 bundle exec rake benchmark:runtime
```

The full service benchmark starts independent worker subprocesses against the
configured PostgreSQL and Redis services, and defaults to 30,000 tenants, worker
counts 1 and 2, and 160 seconds per worker count:

```sh
docker compose up -d --wait
ACP_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/acp_test \
ACP_TEST_REDIS_URL=redis://127.0.0.1:6379/15 \
ACP_BENCH_TENANTS=30000 ACP_BENCH_DURATION=160 ACP_BENCH_WORKERS=1,2 \
bundle exec rake benchmark:services
docker compose down
```

Its candidate defaults use a seeded uniform 1–19 second fetch latency (10 seconds
average), 3,500 fetch slots, 4,500 pipeline slots per worker, and 16 ingestion slots. Each
worker defaults to 16 resolution slots and a configured pool size of 37:
`min(4500, 16 + 16) + 1 = 33` Acp connections plus **4 benchmark headroom**.
`ACP_BENCH_POOL_HEADROOM` controls that explicit extra allowance (default 4);
`ACP_TEST_POOL` overrides the total per-worker size. The benchmark reports the
Acp requirement, actual headroom, fleet pool budgets, and its separate one-connection
driver budget. Worker checkout counts, peaks, timeouts, and acquisition durations
(including creation/verification and waiting) are reported without observer checkouts.
`ACP_BENCH_RESOLVE_CONCURRENCY` controls the resolution budget. Each worker also
reserves up to 256 MiB of accounted bytes,
with a 64 KiB default batch/response limit. The previous 2,500/2,500 configuration
left no nominal fetch headroom at 500 fleet cycles/sec; the larger independently
controlled inputs test overlap and a 75% operating target. **Both configurations
failed the full release gate on the recorded host**: the candidate two-worker
windows delivered 425.9/430.5 cycles/sec and 13.5% RSS growth. These are benchmark
inputs, not recommended production settings. Set the `ACP_BENCH_*` values to match the
target API's latency tails, payload distribution, database pool budget, and Redis
connection limit before treating the result as release evidence.

Both commands accept `ACP_BENCH_*` controls for seed, duration, worker count,
latency, payload size, interval, and each capacity. They print JSON with Ruby and
dependency/server versions, configured and measured latency distribution,
payload bytes, worker capacities, throughput, completion/lag percentiles,
destination rows, active-task/batch maxima, and RSS. The service run also exposes
per-worker ownership-acquisition and completion counts to compare work
distribution and fairness. The local benchmark isolates scheduler overhead;
only the service run includes Redis, PostgreSQL transactions, and multiple worker
processes.

Additional controls are `ACP_BENCH_RESOLVE_DELAY`, `ACP_BENCH_INGEST_DELAY`,
`ACP_BENCH_ACKNOWLEDGE_DELAY` (seconds), `ACP_BENCH_TAIL_PROBABILITY` and
`ACP_BENCH_TAIL_MULTIPLIER` (seeded latency and payload tails),
`ACP_BENCH_MAX_BATCH_BYTES`, `ACP_BENCH_MAX_RESPONSE_BYTES`, and
`ACP_BENCH_RETAINED_BYTE_CAPACITY`. `ACP_BENCH_WARMUP` defaults to one-quarter of
the measurement duration. The remaining time is split into two equal windows;
service RSS growth compares their mean sampled fleet RSS. Workers sample exact
shared boundaries and report actual sample lateness. Startup and drain
completions are excluded from window throughput. Final lifetime metrics include
both and label that scope explicitly. `ACP_BENCH_REPORT` saves JSON;
`ACP_BENCH_SUMMARY=1` prints only its location. Run
`bundle exec ruby benchmark/capacity_sweep.rb` for the independent local sweep.
Full 30,000-tenant/160-second comparisons exit nonzero when no tested fleet passes
all release gates. `ACP_BENCH_REQUIRE_RELEASE=1` explicitly enables this exit gate
for any workload (`0` records failures without enforcing the exit status).

Release gates are workload gates, not universal capacity promises. On the
documented deployment shape, run at least 30,000 tenants with representative
payload and latency distributions for at least two complete intervals and compare
the 1-worker and 2-worker JSON reports. Require at least 500 successful cycles per
second fleet-wide for a 30,000/minute target, no missed commit/progress
invariants, no task or batch maxima above configured pipeline/ingestion bounds,
all healthy tenants progressing, and no sustained RSS growth after warmup (less
than 10% growth between the final two equal-duration windows). Record the exact
CPU/memory limits, OS, Ruby and dependency/server versions, seed, durations,
tenant and payload counts, latency percentiles, capacities, worker count,
throughput, lag percentiles, and memory windows alongside the JSON report. A run
that does not reach these thresholds is a failed gate; do not infer a release
capacity from a shorter or smaller happy-path run. No production capacity result
is claimed by this repository's CI smoke runs.

The declared compatibility range is Ruby >= 3.2, Rails 8.0/8.1, Async 2.35–2.37,
and `pg` 1.5/1.6 (Rails and `pg` are optional integration dependencies). CI runs
the minimum/current Ruby versions and a combination matrix across these Rails,
Async, and `pg` endpoints. The locked integration stack is also tested against
PostgreSQL 17 and Redis 7. Run `bundle exec rake package:verify` to build locally,
inspect the gem file list, and install `acp-worker` into a temporary directory;
this does not publish a release.

## Development

Run `bundle exec rake test`, `bundle exec rake rubocop`, and
`bundle exec rake package:verify` to verify changes. The integration and service
benchmark commands above require disposable PostgreSQL/Redis services.
