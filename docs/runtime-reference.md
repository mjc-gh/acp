# Runtime reference

This reference collects the detailed scheduling, retry, memory, and telemetry
contracts behind the user guide. Benchmark results and reproducible commands are
kept in the linked [evidence reports](#evidence).

## Callback and context contracts

Program definitions store callbacks without executing them at class load.
`Program.configuration` validates required callbacks and settings before a runtime
starts.

- `tenants { |emit| ... }` emits IDs incrementally. Discovery registration is
  buffered in `discovery_batch_size` chunks (default 256, range 1–1024). A full
  scan registers each chunk atomically, flushes its final partial chunk, then
  completes membership. An empty scan completes without registration. Duplicate
  IDs are ignored. Runtime scheduler state retains one record per distinct ID;
  registration buffering retains at most one chunk.
- `tenants_page { |cursor, limit| ... }` returns `[ids, next_cursor]`. The cursor
  is opaque and initially nil. A nil next cursor completes after processing that
  page. Empty final pages are valid; nonterminal cursors must advance, and a page
  cannot exceed `limit`. Declaring either discovery callback replaces the other,
  including inherited callbacks. Pages run sequentially. Rails materializes lazy
  IDs inside each page's executor/checkout; without Rails the runtime materializes
  after callback return, so custom connection scopes must materialize before
  releasing their checkout.
- `initial_cursor` is called only when initializing a tenant without legitimate
  prior progress. Missing progress for a previously known tenant is a recovery
  condition, not a reason to silently restart from the initial cursor.
- `resolve` runs once per polling cycle. Return detached values so lazy database
  work does not occur during network waits.
- Fetch receives a `FetchContext` with tenant ID, committed cursor, stable poll
  ID, attempt number, and (when configured) the cycle's reserved byte allowance.
  Ingest receives the same cycle identity and cursor via `IngestContext`.
- Callbacks run on one Async reactor. Socket I/O overlaps only when the client
  cooperates with Ruby's fiber scheduler. `fetch_concurrency` limits callbacks,
  not consumer-created request fanout; bound fanout separately. Blocking native
  calls and CPU-heavy Ruby work can stall callbacks and lease renewal.

The runtime uses a fair due-time heap and does not create one waiting task per
tenant. Resume is indexed O(log n). A successful cycle is next due at
`max(cycle_start + interval, acknowledgement_time)`; a failed cycle is retried
after `retry_cooldown`. Pipeline capacity includes queued ingestion and pending
progress acknowledgement, not only active fetches.

## Cursor, batch, and progress semantics

`Acp::Batch` has consumer-owned `data`, an explicit `next_cursor`, and optional
`size_bytes`. Empty data and an unchanged cursor are valid. Timestamps are UTC
with exactly microsecond precision: `Acp::Timestamp.dump` emits ISO 8601 with six
fractional digits, and `load` restores UTC `Time`. Finer precision is rejected,
not truncated. `Batch#validate_after!` rejects backward advancement. Lookback
changes fetch inputs, never durable cursor monotonicity.

Progress initialization is idempotent and returns the stored cursor. Redis
progress uses a permanent initialization marker to distinguish a new tenant from
lost state for a known tenant. Acknowledgement is idempotent by poll ID and follows a confirmed
destination commit. A progress-store failure after commit retries acknowledgement;
it does not rerun that batch during the same cycle. If ownership is lost, the next
owner replays from durable progress. The destination must therefore support
idempotent, version-aware replay; a Redis lease cannot fence an already-running
PostgreSQL transaction.

## Stage retries and deadlines

Configure `fetch_retry`, `ingest_retry`, and optionally `acknowledge_retry`
independently. A policy accepts exception classes and/or a predicate, with
`max_attempts`, optional per-attempt `timeout`, optional total `max_elapsed`,
`backoff`, `max_backoff`, and symmetric fractional `jitter`:

```ruby
fetch_retry on: [Timeout::Error], max_attempts: 4, timeout: 20,
  max_elapsed: 45, backoff: 0.5, max_backoff: 8, jitter: 0.2

ingest_retry on: ->(error) { error.is_a?(DatabaseBusy) }, max_attempts: 3,
  max_elapsed: 30, backoff: 1, max_backoff: 5
```

Budgets include the original attempt. Backoff doubles from `backoff`, capped by
`max_backoff`, then applies symmetric fractional jitter. Unspecified timeout and
elapsed budgets are unlimited. Cancellation and process-level exceptions are not
retryable. Stage deadlines are cooperative; callbacks must yield for cancellation
to run and must use finite client socket timeouts. An expired stage raises
`Acp::StageTimeoutError`, a `Timeout::Error` subclass.

Ingestion's elapsed budget includes queue waits and unwinds active callback work
before releasing the reservation. After a confirmed commit, acknowledgement
retries independently until success, ownership loss, or cancellation. By default
it inherits the ingestion policy for compatibility. Its `max_elapsed` is a retry
window, not permission to abandon a committed batch: after that window, retries
continue at `retry_cooldown`. `timeout` still bounds each acknowledgement attempt.

Additional cooperative deadlines are:

| Setting | Scope |
| --- | --- |
| `resolve_timeout` | Resolution semaphore wait plus resolution callback. |
| `initial_cursor_timeout` | Complete initial-cursor operation: progress reads, resolution checkout, callback, and initialization writes. |
| `coordination_timeout` | Each progress/ownership adapter method, including checkout and command I/O. |
| `cleanup_timeout` | Cleanup work (default 1 second), capped by remaining shutdown drain time. |

These default to nil except `cleanup_timeout`. When an adapter also has its own
operation timeout, the shorter deadline wins. Cleanup and callback scopes unwind
before reservations are released, including errors, cancellation, and lease loss.
Once the shutdown drain deadline expires, only nonblocking ownership release can
finish; otherwise the lease expires by TTL. Immediate cancellation shares one
cleanup deadline across releases.

## Retry admission and upstream bulkheads

Retry isolation is opt-in. `retry_capacity` defaults to 0, preserving coupled
pipeline admission. For P pipeline reservations and R retry reservations:

- A failed fetch or ingestion can park during backoff and release its pipeline
  slot. It retains one retry slot through the remainder of the cycle, including
  resumed work; resumption must reacquire a pipeline slot.
- Eligible retries and fresh work alternate. Due-time/FIFO ordering within each
  class prevents either class starving the other. A full retry budget ends a new
  failing uncommitted cycle and reschedules it after `retry_cooldown`, incrementing
  `retry_overflows`.
- Aggregate cycle tasks, contexts, lease heartbeats, and retained batches are
  bounded by P + R. At most P cycles hold runnable pipeline slots. Runtime-owned
  task count is bounded by `2(P + R) + 2I + 6`, where I is ingestion concurrency;
  the caller and consumer-created tasks are excluded. The event queue is bounded
  at `2(P + R) + 1` messages.
- Leases continue renewing while parked. Poll ID, cursor, resolved value, and
  materialized batch retain identity. Ingestion and resolution limits and Rails
  pool requirements do not increase when R increases.

`upstream_group` is a synchronous, nonblocking routing callback given the tenant
ID before admission. It returns a nonempty string of at most 256 bytes; use an
opaque, stable group ID, never credentials. Acp retains only its SHA-256 digest
in reservation bookkeeping and does not emit the group key in diagnostics.
`upstream_capacity` defaults to pipeline capacity and caps each group's pipeline
occupancy across all stages. Saturated groups wait as scheduler records, not
tasks. Group routing must remain stable for a cycle; changed routing applies next
cycle. Idle group entries are removed, with at most P entries retained.

Acknowledgements retain pipeline slots, including during retry backoff. A
coordination outage therefore stops admission after P pending acknowledgements,
with at most R additional parked cycles. Confirmed ingestion is never rerun just
to retry acknowledgement.

## Payload estimates and byte admission

The optional memory settings are:

```ruby
max_response_bytes 32 * 1024
max_batch_bytes 64 * 1024
retained_byte_capacity 256 * 1024 * 1024
payload_estimate { |_tenant_id| 64 * 1024 }
```

All limits are positive integer bytes. `retained_byte_capacity` requires
`max_batch_bytes`. A batch limit requires every returned batch to include a
nonnegative integer `size_bytes`; without a limit, size metadata remains optional.
`payload_estimate` is a synchronous, nonblocking, stable per-cycle callback given
the tenant ID before resolution; by default its estimate is `max_batch_bytes`.

Estimate the retained Ruby object graph, including strings, containers, metadata,
and copies retained for ingestion. Also include peak fetch-local materialization,
wire buffers, decoding temporaries, and callback-local copies. Reserve the
estimate before creating the cycle task. Byte-blocked tenants remain scheduler
records; due-time/FIFO ordering among eligible groups prevents small requests from
repeatedly bypassing an older large request, so head-of-line blocking is
intentional. A retry retains its byte reservation while parked and needs only a
pipeline slot to resume.

Reservations span resolution, fetch-local work, queued/active ingestion, retry
backoff, and pending acknowledgement. They remain at the full estimate until the
cycle and ingestion cleanup unwind; they are not shrunk to the accepted batch
size. Failures, ownership skips, retry exhaustion, lease loss, and cancellation
release the reservation. Do not retain batch references in detached tasks or
global caches, and keep callback copies within the estimate.

Response wire length is not decoded Ruby memory. Compression, JSON object
overhead, encodings, SQL serialization, and copies can multiply it. A configured
`max_response_bytes` does not intercept arbitrary client reads: stream the
response and call `context.validate_response_bytes!(cumulative_bytes)` before
retaining each chunk. Bound decoding against `context.reserved_bytes`; use bounded
streaming/spooling or split upstream pages for larger results.

After fetch returns, Acp rejects missing size metadata, a batch above
`max_batch_bytes`, or a batch above its reservation. Reservations are never grown.
An estimate above the whole budget fails immediately. Such rejection does not
ingest or advance progress and is not retried as a payload within that cycle. A
post-materialization rejection cannot undo the allocation: accounting bounds
reserved estimates, not total RSS, Ruby heap/GC retention, native memory,
connection buffers, or arbitrary consumer allocations. Strict memory control
requires streaming and decoder limits plus process headroom.

Metrics include `reserved_bytes`/`max_reserved_bytes`,
`retained_bytes`/`max_retained_bytes`, byte-budget waits and waiters,
`payload_rejections`, `oversized_batches`, and `estimate_overflows`.
`reserved_bytes` includes fetch-local allowance; accepted retained bytes count
declared batch sizes through acknowledgement. Rejected allocations and
unreturned fetch-local objects are represented by reservation, not accepted
retained bytes.

## Capacity planning

As a first estimate, size a stage with
`ceil(target cycles/sec per worker * mean stage residence / target utilization)`.
Include expected attempts per cycle for fetch, ingest, and resolution; initial
cursor resolution adds startup work. Pipeline sizing includes the full admitted
cycle: cursor reads, callback service, queueing, retries/backoff, and
acknowledgement. With retry isolation, size parked R occupancy separately; total
cycle/task occupancy remains P + R. Validate latency tails and lease-renewal slack
separately. An indefinitely unavailable acknowledgement store has no finite
sustainable throughput; count and byte budgets provide backpressure.

For Rails, the Acp-owned selected-pool minimum is
`min(pipeline_capacity, resolve_concurrency + ingest_concurrency) + 1`; add web,
job, nested fanout, preexisting checkout, and other-consumer headroom. Other pools
need separate budgets. See the [Rails connection-budget proof](../benchmark/rails-connection-budget-proof.md)
and [sizing evidence](../benchmark/rails-connection-budget-evidence.md).

## Telemetry and timing boundaries

`runtime.metrics` is an aggregate snapshot; `runtime.health` reports running or
stopping state with that snapshot. Fixed-size metrics cover discovered/assigned
tenants, active cycles/stages, retries, queue waits, ingestion, cursor delay,
discovery, lease acquisition and renewal, and scheduler pressure. Redis metrics
are exposed under `redis` when available. Tenant and poll IDs belong in diagnostic
events, never aggregate metric labels.

Capacity gauges include current and peak pipeline/retry reservations, parked
cycles, retained batches, pending acknowledgements, and upstream groups.
`active_cycles` includes parked cycles; `pipeline_utilization` counts pipeline
reservations only. `pressure.retry_admission_wait` measures eligible retry queue
time. Byte-budget gauges and rejection counters are listed above. Stage-count
peaks include active resolutions, fetches, ingests, cycle tasks, and reservations.

Pressure distributions record `count`, `sum`, `last`, `min`, `max`, exclusive
histogram buckets, outcome counts (`success`, `error`, `timeout`, `cancelled`),
and p50/p95/p99 bucket upper bounds. Boundaries in seconds are
`[0, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 10, 30, 60, 300]` plus
an open-ended overflow bucket. Overflow percentiles are nil, not 300 seconds.
Nonpositive lease slack shares the zero bucket while exact signed min/last remain
available. Empty distributions have zero count/sum/last/percentiles and nil
min/max. Each runtime owns 23 distributions and each coordinator owns five; they
do not retain historical samples or tenant labels. Metrics live for the instance
lifetime and reset on a new instance. Snapshots are frozen copies; overdue-work
snapshots scan scheduler state.

| Metric family | Boundary |
| --- | --- |
| `admission_lag` | `max(cycle_start - scheduled_due, 0)` for every admission, including ownership skips and failures. |
| `cycle_duration`, `cycle_overrun` | Admission to acknowledgement/terminal outcome before lease cleanup; includes ownership, stage waits, retries, and backoff. Overrun is duration above interval. |
| `resolve_wait/service`, `fetch_wait/service`, `ingest_wait/service` | Stage queue/semaphore wait separated from callback service. Cancelled queued ingestion records wait once even if never dequeued. |
| `acknowledge_service`, `cursor_read_service`, `cursor_initialize_service` | Each progress adapter operation; acknowledgement has no semaphore wait. |
| `cursor_update_delay` | Entire post-ingestion acknowledgement lifecycle, including retry/backoff and unsuccessful outcomes. |
| `fetch_backoff`, `ingest_backoff`, `acknowledge_backoff` | Actual elapsed backoff, separate from stage wait and service. |
| `reactor_lag` | One sampler wakes every 100 ms; records lateness once and does not create catch-up tasks. |
| `renewal_wakeup_lag`, `renewal_service`, `lease_renewal_slack` | Heartbeat lateness, full renewal duration including checkout, and conservative remaining lease time including command latency. |
| Redis `*_checkout`, `*_service` pressure | Lane checkout wait and duration holding the checked-out connection, including lazy client creation and commands. These are not per-wire-command latency. |

Resolution timing samples initial-cursor and normal tenant-resolution callbacks
separately; the initial-cursor callback is included in resolution service.
`renewal_successes`, `renewal_failures`, and `renewal_deadline_misses` distinguish
confirmed outcomes from timing. Runtime's historical top-level
`renewal_deadline_misses` counts failed heartbeat operations. A successful
renewal can still miss its previous conservative deadline; failed renewal never
extends that deadline.

Compatibility keys `polling_lag` and its percentiles alias admission lag; use
`pressure.cycle_overrun` for historical cycle-over-interval meaning. `queue_wait`,
`ingestion_duration`, and `cursor_update_delay` retain last-value semantics;
distributions are better for contention analysis. Timings may be nested and must
not be added as independent work. They do not measure source freshness.

`overdue_tenants` and `oldest_overdue_age` describe due tenants waiting for
pipeline admission, excluding in-flight, locally paused, and disabled tenants.
They are zero when stopped; health reads do not perform Redis I/O. Stage count
peaks include active resolutions, fetches, ingests, cycle tasks, and reservations.
`pressure.pipeline_residence` runs through cleanup and completion-event delivery
to final reservation release; it complements the shorter `cycle_duration`.

Notifications subscribers and `on_error` execute synchronously on the reactor.
Notification subscriber exceptions are swallowed; `on_error` exceptions
propagate. Their service times are measured in
`pressure.notification_service` and `pressure.error_handler_service`. Slow logging
or export can delay all callbacks and renewals.

The `on_error` callback receives `(tenant_id, error, stage)` for recoverable
tenant and acknowledgement failures. Embedded runtimes and tests can inject a
clock implementing `now`, `sleep(duration)`, and `wait(duration) { ... }`; the
default clock uses monotonic process time and Async-aware waits.

### Bounded telemetry export

An embedded runtime may pass an `Acp::BoundedExporter`:

```ruby
exporter = Acp::BoundedExporter.new(capacity: 256, max_event_bytes: 4096) do |json|
  telemetry_client.write(json)
end
runtime = Acp::Runtime.new(
  configuration: TenantSync.configuration,
  progress: progress,
  exporter: exporter
)
```

One consumer thread owns copied, frozen JSON strings. Capacity is bounded to the
pending records plus one in-flight record; each event is byte-limited. `offer`
never waits for capacity and drops new events when full, oversized, or closed.
Serialization occurs on the producer, so submit small flat metadata, not batches,
models, or exception objects. Close the exporter after runtime drain with
`close(timeout:)`; it rejects new events, waits cooperatively, then discards
queued events. It returns false if a consumer call is still running; the call is
not killed and cannot own a polling reservation or mutate destination state.
Exporter metrics report accepted, dropped, exported, errored, and queued events.
Exporter shutdown is an additional drain budget. Its thread inherits no Rails
executor or database context; establish its own scope if needed. I/O must release
the GVL and have finite timeouts. Do not close it from its consumer thread.

## Reactor responsiveness and CPU work

Runtime discovery, admission scans, state construction, and membership/assignment
transformations cooperate after 5 ms at invariant-safe boundaries. A single heap
mutation finishes before yielding. This is not a preemptive deadline: one
callback, allocation, garbage collection pause, native call, or TLS handshake can
exceed it.

CRuby's GVL prevents Ruby CPU callbacks from running in parallel within one
worker. More fibers, threads, or database connections do not add CPU parallelism;
independent processes can use independent cores. Keep CPU sections short, split
large transforms into bounded yielding chunks, or isolate CPU work in processes.
Acp does not offload callbacks to detached threads. Such offload would need fixed
worker/queue bounds, input budgets charged to the originating cycle, immutable
copied inputs, thread-owned clients, returned results/errors, cancellation-aware
admission, destination fencing, and a shutdown join before releasing leases or
reservations. A timeout does not prove non-interruptible work has stopped.

Use scheduler-compatible socket/database clients, exclusive checkout per fiber,
finite connect/read/write timeouts, and cancellation that closes or discards
interrupted connections before propagating. Drain the runtime before closing
persistent clients. The reference HTTP client evidence covers TLS, reuse,
cancellation, and bounded closure.

## Evidence

- [Capacity and memory evidence](../benchmark/capacity-memory-evidence.md)
- [Rails connection-budget proof](../benchmark/rails-connection-budget-proof.md)
- [Rails connection-budget evidence](../benchmark/rails-connection-budget-evidence.md)
- [Runtime pressure evidence](../benchmark/runtime-pressure-evidence.md)
- [Reactor responsiveness evidence](../benchmark/reactor-responsiveness-evidence.md)
- [Retry isolation evidence](../benchmark/retry-isolation-evidence.md)
- [Redis coordination evidence](../benchmark/redis-coordination-evidence.md)
