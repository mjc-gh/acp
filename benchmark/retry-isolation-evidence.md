# Retry admission isolation evidence

Run on Linux x86_64, Ruby 4.0.6, Async 2.37.0, real Redis 7, 2026-10-21:

```sh
ACP_TEST_REDIS_URL=redis://127.0.0.1:16379/15 \
  bundle exec ruby benchmark/retry_isolation.rb
```

The script prints complete fixed-bucket distributions as JSON. Seed 20261021,
20 tenants, P=2, I=1, R=0 or 2, 100ms polling interval, seeded 1–4ms fetch
latency, 1ms ingestion, 4096-byte materialized payloads, 400ms retry backoff,
1.2s workload and 50ms graceful drain. All tenants start due together to expose
full saturation. Mixed runs fail the first attempt of two tenants every cycle;
all-failed runs fail every attempt. Retry attempt/elapsed limits are 4/1.5s.
No upstream bulkhead is configured, so this comparison isolates retry admission.

These short runs demonstrate interleavings and bounds, not production throughput.
The host was also running test checks; timing variation between healthy runs is
not evidence of an isolation overhead or speedup. Percentiles are histogram
bucket upper bounds, not interpolated exact percentiles. Empty distributions
in the all-failed runs mean no healthy population, not zero latency.

## Healthy progress

| Workload | R | Healthy completions | Admission p50/p95/p99 (ms) | Completion p50/p95/p99 (ms) | Successful retry attempts |
| --- | ---: | ---: | --- | --- | ---: |
| Healthy | 0 | 240 | 1 / 50 / 100 | 10 / 10 / 25 | 0 |
| Healthy | 2 | 227 | 1 / 100 / 100 | 10 / 25 / 25 | 0 |
| Mixed | 0 | 36 | 500 / 1000 / 1000 | 10 / 25 / 25 | 4 |
| Mixed | 2 | 213 | 1 / 50 / 100 | 10 / 10 / 25 | 5 |
| All failed | 0 | — | — | — | 0 (6 retries attempted) |
| All failed | 2 | — | — | — | 0 (5 retries attempted) |

Maximum healthy admission lag in mixed runs fell from 512ms to 75ms, below the
400ms backoff. Retry progress continued alongside healthy work. The isolated
all-failed workload shed 199 cycles at the finite retry budget; it never created
an unbounded set of sleeping tasks.

## Task, memory, renewal, and drain bounds

| Workload | R | Peak cycles / bound | Sampled runtime tasks / bound | Peak retained payload bytes / bound | Drain (ms) |
| --- | ---: | --- | --- | --- | ---: |
| Healthy | 0 | 2 / 2 | 11 / 12 | 8192 / 8192 | 6.1 |
| Healthy | 2 | 2 / 4 | 11 / 16 | 8192 / 16384 | 3.0 |
| Mixed | 0 | 2 / 2 | 11 / 12 | 8192 / 8192 | 51.2 |
| Mixed | 2 | 4 / 4 | 15 / 16 | 8192 / 16384 | 50.8 |
| All failed | 0 | 2 / 2 | 10 / 12 | 0 / 8192 | 27.9 |
| All failed | 2 | 4 / 4 | 14 / 16 | 0 / 16384 | 51.3 |

Runtime task bounds include cycle/lease tasks, fixed ingestion workers and their
children, and background tasks: 2(P+R)+2I+6. Sampling is every 1ms and can miss
short-lived task peaks; gated tests independently assert reservation peaks.
Payload bytes use exact synthetic payload size times peak retained batches;
this measures retained payloads, not Ruby object overhead or process RSS. The
consumer's maximum batch/context size is part of the general memory bound.

Every run ended with zero cycle tasks, parked cycles, and retained batches.
Peak pipeline occupancy was 2 throughout; retry occupancy was at most 2.
The all-failed runs exercised actual lease renewal: both had two samples,
minimum renewal slack 1.996s (R=0) and 1.998s (R=2), under a 3s lease.
Other cycles ended before the one-second renewal interval, so their renewal
distributions are empty. Drain includes scheduler/cancellation overhead around
the configured cooperative 50ms deadline.

## Shared-service verification

The focused runtime suite, `bundle exec rake`, Redis integration, and combined
PostgreSQL+Redis integration passed. PostgreSQL on the documented host port
rejected the documented credentials, so an isolated Compose project used ports
15432/16379 with the same PostgreSQL 17 and Redis 7 images. The new shared-service
scenarios cover a prolonged acknowledgement outage after real PostgreSQL commits,
lost successful Redis responses without duplicate ingestion, ownership loss while
parked, and cancellation/deadline expiry during real Redis socket I/O.
