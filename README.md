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
selection, client lifecycles, transactions, and making ingestion safe to replay.
Rails integration is explicit and belongs to the application.
Keep database work inside the consumer's discovery, cursor-initialization,
resolution, and ingestion callbacks; return detached resolved values before
network waits, and acquire database connections only for database work.

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
retry, queue, and worker waits. Stop the surrounding Async task to shut the
runtime down.

An optional ownership adapter implements `acquire(tenant_id)` and
`release(tenant_id, token)`. A false/nil acquisition skips that cycle and tries
again after one interval. The default `Acp::LocalOwnership` claims every tenant.
An optional `on_error` callable receives `(tenant_id, error, stage)` for
recoverable tenant and acknowledgement failures. Tests and embedded runtimes can
inject a clock implementing `now`, `sleep(duration)`, and `wait(duration) { ... }`;
the default clock uses monotonic process time and Async-aware waits.

The scheduler staggers first due times deterministically per program and tenant,
uses a fair due-time heap, and creates no per-tenant waiting tasks. Pipeline
capacity includes active fetches, completed batches queued for ingestion, and
cycles awaiting progress acknowledgement. Fetch concurrency is separately
bounded by `min(fetch_concurrency, pipeline_capacity)`; ingestion uses a fixed
worker group bounded by both ingest concurrency and pipeline capacity. Successful
cycles become due at `max(cycle_start + interval, acknowledgement_time)`. A
failed cycle is isolated and rescheduled after one interval; stage retries retain
their cursor, poll ID, resolved context, or batch as appropriate.

See [`examples/tenant_sync.rb`](examples/tenant_sync.rb) for a load-safe consumer
definition. Requiring it does not run discovery or open connections.

## Development

Run `bundle exec rake test` and `bundle exec rake rubocop` to verify changes.
