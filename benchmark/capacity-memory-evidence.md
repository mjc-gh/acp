# Story 04 — capacity and accounted-memory evidence

## Outcome

The byte-admission implementation and lifecycle checks pass. **The full release
capacity gate fails on this host.** Neither the previous 2,500/2,500 fetch/pipeline
limits nor the candidate 3,500/4,500 limits establish 500 successful fleet cycles
per second. Increasing concurrency alone is insufficient. Story 04 acceptance
criteria 6–7 remain unmet; these reports must not be used as a release capacity
claim.

## Environment and reproducibility

Recorded on 2026-10-21: Ubuntu 24.04.4 LTS, x86_64, 8 available processors,
Ruby 4.0.6, Async 2.37.0, Rails 8.1.4, redis gem 5.4.1, pg 1.6.3,
PostgreSQL 17.11 and Redis 7.4.11. No cgroup CPU/memory limit was exposed.
Seed **20261001** throughout. Full comparisons ran sequentially; local runs are
short, noisy smoke evidence. The disposable service images are `postgres:17`
and `redis:7`, without persistent volumes.

The documented default ports were occupied by other host services. The initial
PostgreSQL integration attempt at 5432 failed authentication. Disposable Compose
services were recreated with a temporary override mapping PostgreSQL to **15432**
and Redis to **16379**. All successful shared-service commands used:

```sh
export ACP_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:15432/acp_test
export ACP_TEST_REDIS_URL=redis://127.0.0.1:16379/15
```

The commands below assume those exports. On an unoccupied host the repository's
normal Compose configuration and documented 5432/6379 URLs work unchanged.

```sh
# Independent stage/pipeline/byte sweeps; each subprocess uses benchmark:runtime.
ACP_BENCH_REPORT=benchmark/capacity-memory-sweep.json \
  bundle exec ruby benchmark/capacity_sweep.rb

# Candidate defaults: F=3500, R(resolve)=16, I=16, P=4500, retry slots=0.
ACP_BENCH_TENANTS=30000 ACP_BENCH_DURATION=160 ACP_BENCH_WORKERS=1,2 \
  ACP_BENCH_REPORT=/tmp/opencode/capacity-services-full.json \
  bundle exec rake benchmark:services

# Previous count limits, with the same payload accounting and measurement model.
ACP_BENCH_TENANTS=30000 ACP_BENCH_DURATION=160 ACP_BENCH_WORKERS=1,2 \
  ACP_BENCH_FETCH_CONCURRENCY=2500 ACP_BENCH_PIPELINE_CAPACITY=2500 \
  ACP_BENCH_REPORT=/tmp/opencode/capacity-services-baseline.json \
  bundle exec rake benchmark:services

# Combined latency/payload tail, slow ingestion, and acknowledgement delay.
ACP_BENCH_TENANTS=30000 ACP_BENCH_DURATION=160 ACP_BENCH_WORKERS=1,2 \
  ACP_BENCH_TAIL_PROBABILITY=0.05 ACP_BENCH_TAIL_MULTIPLIER=5 \
  ACP_BENCH_PAYLOAD_BYTES=4096 ACP_BENCH_INGEST_DELAY=0.02 \
  ACP_BENCH_ACKNOWLEDGE_DELAY=0.05 \
  ACP_BENCH_REPORT=/tmp/opencode/capacity-services-tails.json \
  bundle exec rake benchmark:services
```

All other settings use the defaults recorded in JSON: interval 60 seconds,
uniform fetch latency 1–19 seconds, one record per batch, 1,024-byte nominal
payload (4,096 in the combined tail run), 64 KiB batch/response limits, and a
256 MiB accounted reservation budget per worker. The fixture declares payload
bytes plus **512 bytes per record** as its synthetic object-size allowance;
this is a reproducible accounting model, not an ObjectSpace/RSS measurement.
Latency and payload tail selections are seeded independently. There is no real
HTTP transport in these runs.

Raw reports retain all worker samples and per-tenant progress counts locally.
Checked-in `capacity-services-{baseline,candidate,tails}.json` reports are generated
by `benchmark/compact_capacity_report.rb`: they retain server/environment details,
all gate results, per-worker final metrics, boundary samples, sampled PostgreSQL
busy peaks when available, coverage minima/maxima, and stage means in each window.
They omit individual tenant IDs and intermediate histograms. Example:

```sh
ACP_BENCH_REPORT=benchmark/capacity-services-candidate.json \
  bundle exec ruby benchmark/compact_capacity_report.rb \
  /tmp/opencode/capacity-services-full.json
```

The first candidate run predates the added PostgreSQL-pool sampler and explicit
failure exit status: its gate JSON is false, but that command exited zero. The
subsequent baseline and tail comparisons both exited **1**. Current full-workload
commands enforce failure when no tested fleet passes; the intentionally
underprovisioned one-worker comparison does not prevent a passing larger fleet.

## Full service results

Each measurement is exactly 160 seconds, with a 40-second warmup followed by
**40–100 and 100–160 second** windows. Worker counts share the same measurement
schedule. Drain completions are reported separately and excluded below. RSS
growth compares the two windows' mean sampled fleet RSS, not startup/end RSS.

| Workload/count limits | Workers | Window 1 cycles/sec | Window 2 cycles/sec | RSS growth | Release gate |
| --- | ---: | ---: | ---: | ---: | --- |
| Nominal, F/P=2500/2500 | 1 | 227.60 | 239.05 | 8.48% | FAIL |
| Nominal, F/P=2500/2500 | 2 | 410.32 | 417.75 | 6.57% | FAIL |
| Nominal, F/P=3500/4500 | 1 | 236.10 | 250.67 | 6.79% | FAIL |
| Nominal, F/P=3500/4500 | 2 | 425.93 | 430.45 | 13.47% | FAIL |
| Combined tail/delays, F/P=3500/4500 | 1 | 219.32 | 241.10 | 8.90% | FAIL |
| Combined tail/delays, F/P=3500/4500 | 2 | 389.38 | 393.15 | 12.14% | FAIL |

All runs stayed within count and byte reservation maxima and returned cycle,
pipeline/retry, reserved-byte, and accepted-retained-byte gauges to zero after
shutdown. The nominal fleets reached all 30,000 tenants at least once, but did
not give every tenant two successful measured cycles. No run passed the
throughput gate. Neither candidate two-worker run passed the RSS-growth gate.
There were no reported renewal failures/deadline misses in these full runs.
For example, one candidate two-worker process retained at least **5.44 seconds**
of conservative renewal slack with a 9-second lease; its renewal checkout p95
was 250 ms. Per-worker distributions and slack minima remain in the JSON.

For that worker, window means illustrate the sizing difference: fetch service
10.052/10.061 seconds, ingestion queue 10.8/14.4 ms, ingestion service
20.0/22.8 ms, acknowledgement 9.3/10.9 ms, and **pipeline residence
12.176/11.507 seconds**. Pipeline residence includes release/completion-event
delivery; cycle duration stops before that cleanup. At a target 250 cycles/sec
per worker and 75% utilization, mean-value estimates suggest about 3,354 fetch
slots and **4,059 pipeline slots** using the larger observed means. The candidate
3,500/4,500 inputs test that estimate, but the observed fleet failure shows why
those calculations alone do not establish sustainable capacity. Completed-stage
means also exclude unfinished work at a snapshot; tails require separate checks.

## Independent local sweeps

See `capacity-memory-sweep.json` for every environment variable, all capacities,
the seed, stage means/p95 bounds, byte waits, and task/retention maxima. Each run
uses 1,000 tenants, a 100 ms interval, four seconds measured, uniform 10–30 ms
fetches, a 2 ms resolver, 10 ms ingestion, 5 ms acknowledgement, and 1 KiB payloads.
The base F/resolve/I/P limits are **16/4/4/16**. Only the named control changes;
the overlap comparisons explicitly share I=8.

| Change | Equal-window cycles/sec | Peak queued batches | Peak accepted retained bytes |
| --- | --- | ---: | ---: |
| Base P=16, I=4 | 386.0 / 384.0 | 8 | 19,968 |
| P=32, I=4 | 376.0 / 392.0 | 23 | 43,008 |
| P=128, I=4 | 390.7 / 389.3 | 119 | 190,464 |
| I=8, P=16 | 420.0 / 420.7 | 4 | 19,968 |
| I=8, P=32 | 766.0 / 765.3 | 8 | 32,256 |
| I=8, P=128 | 778.7 / 774.0 | 72 | 130,560 |
| I=1 | 98.0 / 97.3 | 14 | 23,040 |
| 10% latency/payload tail ×10 | 274.7 / 270.0 | 7 | 39,936 |
| Aggregate budget 256 KiB | 103.3 / 104.0 | 2 | 6,144 |

At I=8, increasing P from 16 to 32 creates useful stage overlap without changing
fetch capacity. Increasing P again to 128 adds little throughput but quadruples
reserved capacity (2 MiB to 8 MiB) and accepted retention. At I=4, ingestion is
already saturated near 390 cycles/sec, so pipeline growth mostly creates queues.
Fetch limits 8/32 and resolver limits 1/8 were also swept independently; their
results and stage waits are in JSON. All sweep cases released their reservations.

### Local operating-headroom check

`capacity-local-headroom.json` is a separate 12-second run with the same delays,
F/resolve/I/P=16/4/8/32 and interval **1.818181818 seconds** (550 offered cycles/sec).
It delivered **553.11/546.89 cycles/sec** in two equal 4.5-second post-warmup windows,
with admission-lag p95 ≤1 ms, p99 ≤5 ms, zero retries/byte waits, and all byte gauges
released. Mean fetch/ingest/ack/pipeline residence was
20.19/10.14/5.15/38.71 ms. The 75% operating target leaves about 25% service
headroom relative to the measured ingestion saturation point; choose lower
utilization or more workers when representative tails/retries consume it.

```sh
ACP_BENCH_TENANTS=1000 ACP_BENCH_DURATION=12 ACP_BENCH_INTERVAL=1.818181818 \
  ACP_BENCH_SEED=20261001 ACP_BENCH_LATENCY_MIN=0.01 ACP_BENCH_LATENCY_MAX=0.03 \
  ACP_BENCH_FETCH_CONCURRENCY=16 ACP_BENCH_RESOLVE_CONCURRENCY=4 \
  ACP_BENCH_INGEST_CONCURRENCY=8 ACP_BENCH_PIPELINE_CAPACITY=32 \
  ACP_BENCH_RESOLVE_DELAY=0.002 ACP_BENCH_INGEST_DELAY=0.01 \
  ACP_BENCH_ACKNOWLEDGE_DELAY=0.005 \
  ACP_BENCH_REPORT=benchmark/capacity-local-headroom.json \
  bundle exec rake benchmark:runtime
```

This validates the **local named workload only**. It is not a replacement for
the 30,000-tenant shared-service gate.

## Fleet memory, connections, and retry assumptions

- At 250 target cycles/sec per worker and mean 10-second fetch latency, 2,500
  fetch slots give 100% theoretical utilization. A 75% target requires at least
  3,334 before measured timing/attempt overhead; use the complete residence for P.
- Expected stage attempts multiply service demand. Mean stage residence includes
  retries and backoff for P, plus queued/active ingestion and acknowledgement.
  With isolated retries, budget parked occupancy separately as R; byte allowance
  covers **both P and R**. A coordination outage can retain committed payloads
  indefinitely until cancellation/lease loss: no finite throughput promise applies.
- Two candidate workers have 9,000 pipeline slots, 7,000 fetch slots, 32 resolver
  slots, 32 ingestion slots, and **512 MiB aggregate accounted reservation
  capacity**. Reserving 64 KiB for every P slot simultaneously would require
  281.25 MiB per worker, so the independent 256 MiB byte limit can admit at most
  4,096 such estimates. It intentionally provides backpressure before the count
  ceiling when needed. Reservations include fetch-local allowance and remain
  held through retries/acknowledgement; post-fetch `retained_bytes` is a separate
  declared-size gauge. Neither is an RSS bound.
- Rails pools default to 37 per worker: 16 resolve + 16 ingest + 1 discovery +
  4 spare, **74 fleet connections**, plus web/job/administrative clients. Baseline
  sampled busy peaks were 15 and 12 in the two-worker run; combined-tail peaks
  were 20 and 16. Sampling is not a hard peak detector. The earlier candidate
  report has no PostgreSQL pool samples and records null rather than inventing
  them. The theoretical pool bound remains authoritative.
- Redis reserves 16 ordinary + 8 renewal connections per worker: **48 fleet
  connections**, plus the driver/admin/application clients. Checkout waits,
  renewal service and conservative slack are reported; they are not server-wide
  socket counts. Reserve the renewal lane even when ordinary commands stall.
- A real one-request-per-fetch HTTP client would need up to **3,500 active
  requests per worker / 7,000 fleet-wide**, or fewer physical sockets with
  multiplexing. Budget per-origin limits, file descriptors, TLS/native buffers,
  and upstream quotas separately. These synthetic upstreams use **zero HTTP
  sockets** and do not validate a connection pool of that size.
- Reserve RSS outside accounted bytes for Ruby heap/GC, tenant and task state,
  ORM/native/SQL copies, connection buffers, and the application. The measured
  RSS-growth failures require additional investigation or workload/replica changes;
  do not label the 256 MiB byte budget a 256 MiB process-memory guarantee.

## Verification

Runtime tests gate fetch, active ingestion cleanup, retry, and acknowledgement
callbacks; test estimate overflow, missing size, batches above the individual or
entire aggregate budget, weighted FIFO, cancelled waiters, scheduler-level mixed
sizes, parked-retry readmission under byte pressure, retry exhaustion, and lease
loss. No rejected payload advances progress. Count and bytes stay held during
callback cleanup and return to baseline afterwards. Existing PostgreSQL/Redis
fault suites verify commit/progress invariants, rollback, replay, fencing, and
response-loss reconciliation.

Final checks:

- `bundle exec rake`: **107 tests, 670 assertions**, all passing; RuboCop clean
  across 47 Ruby files.
- `ACP_TEST_REDIS_URL=redis://127.0.0.1:16379/15 bundle exec rake test:redis`:
  **17 tests, 203 assertions**, all passing.
- `ACP_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:15432/acp_test ACP_TEST_REDIS_URL=redis://127.0.0.1:16379/15 bundle exec rake test:postgres`:
  **31 tests, 265 assertions**, all passing (includes the Redis suite).
- A final shared-service byte-pressure smoke comparison exercised the final
  shutdown path with a **256 KiB budget**, 100 tenants, ten seconds per fleet,
  one/two workers, one-second interval, 10–50 ms fetches, 10% tails ×5,
  4 KiB payloads, and 20 ms ingestion/acknowledgement delays. Each worker peaked
  at exactly 262,144 reserved bytes; byte-wait counts were 449 (one worker) and
  572/440 (two workers). Reservation-limit and final-release gates passed without
  deadlock. `capacity-services-byte-smoke.json` retains the report; this is not a
  release workload. Reproduce with the corresponding `ACP_BENCH_TENANTS=100`,
  `ACP_BENCH_DURATION=10`, `ACP_BENCH_INTERVAL=1`, `ACP_BENCH_LATENCY_MIN=0.01`,
  `ACP_BENCH_LATENCY_MAX=0.05`, `ACP_BENCH_TAIL_PROBABILITY=0.1`,
  `ACP_BENCH_TAIL_MULTIPLIER=5`, `ACP_BENCH_PAYLOAD_BYTES=4096`,
  `ACP_BENCH_INGEST_DELAY=0.02`, `ACP_BENCH_ACKNOWLEDGE_DELAY=0.02`, and
  `ACP_BENCH_RETAINED_BYTE_CAPACITY=262144` overrides on `benchmark:services`.

Services are stopped with the shared `docker compose down` cleanup command after
verification.
