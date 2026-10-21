# Redis coordination efficiency — Story 03

Measured locally on 2026-10-21, Ruby 4.0.6, Redis 7 and PostgreSQL 17,
using the locked gem dependencies. These are individual local samples, not
production capacity guarantees. Repeat at deployment latency and offered load.

## Reproduction and measurement boundaries

```sh
docker compose up -d --wait
ACP_TEST_REDIS_URL=redis://127.0.0.1:6379/15 \
  bundle exec ruby benchmark/redis_coordination.rb
ACP_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/acp_test \
  ACP_TEST_REDIS_URL=redis://127.0.0.1:6379/15 \
  bundle exec ruby benchmark/redis_coordination_full.rb
```

Use otherwise idle disposable services. Both scripts emit JSON, use unique
namespaces, and remove only their own keys. `TENANTS` defaults to 30,000 (use
at least 1,000 for the fixed-size claim sample). Each registry has 1, 2, 16, or
64 live worker records, below the configured worker limit of 256.

The microbenchmark measures discovery, the first membership/assignment snapshot,
1,000 **successful claim/release pairs**, and 100 warm successful cycles on their
assigned owner. Registry setup, assignment selection for claim targets, and
cursor initialization are outside claim/cycle measurements. Redis server
`INFO` deltas provide commands, input/output bytes and user+system CPU. Commands
include Lua's internal commands; script requests count only `EVAL`/`EVALSHA`.
Counters include a few fixed measurement/client initialization commands. Scripts
are warm in the tabulated final run. Cold caches add at most one `NOSCRIPT`
fallback per missing script, not one per pooled connection.

Pool measurements separately report checkout count/time and service count/time
per phase. Service includes client encoding, I/O, reply decoding, and cache
recovery. Acquisition p50/p99 measure the acquisition call only, excluding release.
The CPU pair samples include release, making them conservative claim CPU bounds.
The original pre-change baseline was collected before editing the coordination
implementation, including the existing uncommitted retry/deadline changes.
Before/after microbenchmarks used the same local Redis endpoint. Rails/full-runtime
and final integration measurements used isolated ACP services on ports 15432 and
16379 because the default host ports were occupied by another project.

## Discovery: 30,000 IDs, B = 256

| Workers | Before elapsed s | After elapsed s | Before Redis CPU s | After Redis CPU s | Before inbound bytes | After inbound bytes |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 7.829 | 0.608 | 1.894 | 0.169 | 24,889,776 | 3,720,389 |
| 2 | 7.714 | 0.560 | 1.885 | 0.167 | 24,889,776 | 3,720,389 |
| 16 | 7.711 | 0.544 | 1.881 | 0.161 | 25,009,783 | 3,750,750 |
| 64 | 7.690 | 0.557 | 1.879 | 0.170 | 25,009,783 | 3,750,750 |

Across all worker counts:

* Registration requests/revision increments: **30,000 → 118**, exactly
  `ceil(30000 / 256)`. Completion adds one revision increment in both versions.
* Total script requests: **30,004 → 122** (acquire, generation, complete,
  release plus registration). Registration requests fall **99.61%**.
* Redis commands including Lua internals: **210,021 → 60,611**, down **71.1%**.
  Per-ID `ZADD`/`HSET` still occur; token/time/revision work is amortized.
* Inbound bytes fall about **85%**; outbound bytes fall from approximately
  246,400 to 7,600. Elapsed time falls approximately **13–14×** in the final run.
* Final-run discovery checkout wait totals **0.199–0.241 ms/pass**. The 122
  command services total **0.290–0.299 s/pass**, or **2.38–2.45 ms/request**.
  Larger requests cost more individually but replace 256 small requests.
* Before-change overall ordinary-pool checkout totals were **38.7–40.5 ms**
  over the entire microbenchmark, versus **1.30–2.16 ms** afterward. Before-change
  overall command service totals were **7.18–7.63 s**, versus **0.94–1.31 s**.

The runtime registration buffer holds at most B distinct IDs. The paged contract
also holds at most B page IDs, independently of that buffer. Canonicalized
registration arguments are bounded by the same batch size. Scheduler and
membership snapshots intentionally remain O(tenant count); this change does not
claim constant overall tenant memory. ID byte size remains application-owned.

## Claims and successful cycles

| Workers | Before pair CPU ms | After pair CPU ms | Before acquisition p50/p99 ms | After acquisition p50/p99 ms | Before/after pair elapsed s (1,000 pairs) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.136 | 0.108 | 0.275 / 0.707 | 0.231 / 0.459 | 0.493 / 0.421 |
| 2 | 0.128 | 0.118 | 0.252 / 0.762 | 0.243 / 0.508 | 0.458 / 0.437 |
| 16 | 0.191 | 0.174 | 0.328 / 0.935 | 0.305 / 0.917 | 0.563 / 0.518 |
| 64 | 0.376 | 0.364 | 0.537 / 1.320 | 0.506 / 1.243 | 0.827 / 0.782 |

Across repeat after-samples, the 64-worker pair CPU was **0.36–0.38 ms** and
acquisition p99 was **1.2–1.8 ms**. Pair wire requests remain 2,000; inbound bytes
drop from **2.10–2.11 MB to 0.805–0.814 MB**. The warm script executor saves bytes
and parsing, not the worker scan itself. After-run pair service totals were
0.373, 0.387, 0.468, and 0.727 seconds respectively; checkout waits were
0.874, 0.874, 0.860, and 0.924 ms over 2,000 operations.

| Workers | Before/after cycle elapsed ms | Before/after cycle Redis CPU ms | Before/after inbound bytes per cycle | After mean command service ms | After total checkout wait ms (100 cycles) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2.361 / 1.754 | 0.423 / 0.325 | 4,163 / 2,008 | 0.208 | 0.713 |
| 2 | 2.313 / 1.644 | 0.452 / 0.338 | 4,164 / 2,008 | 0.196 | 0.682 |
| 16 | 2.406 / 1.797 | 0.503 / 0.406 | 4,180 / 2,022 | 0.221 | 0.671 |
| 64 | 2.710 / 2.171 | 0.690 / 0.650 | 4,181 / 2,022 | 0.272 | 0.734 |

Ordinary warm-cycle wire operations fall **10 → 6**: claim, atomic progress read,
server-time ownership validation before fetch, validation at ingestion execution,
atomic advancement, and release. Initialization, heartbeats and retries are
additional operations when needed. Outbound cycle bytes fall from approximately
505 to 141. Internal Redis commands rise by four per cycle (29 → 33 at one
worker) because two remaining validity checks now verify server deadlines, and
the progress read is atomic. Script calls rise from three to six per cycle
because validity/progress reads are now scripts; counting script calls alone
would misrepresent the wire-command reduction.

Claim/progress fusion was evaluated and deferred. Progress initialization invokes
an application callback, and ownership and progress adapters may be separate.
Bundling them would introduce another optional acquisition return contract for
one read saved. Instead, progress+missing-marker reads are combined, redundant
ownership checks are removed, and Redis advancement retains its atomic fence.
Legacy progress adapters retain a separate check before acknowledgement.

### O(1) claim redesign trigger

Retain authoritative live-registry rendezvous claims for this story. At 30,000
tenants/minute, 500 successful claim/release pairs per second project to
**18–19% of one Redis CPU core** at 64 workers; measured acquisition p99 remains
below 2 ms. The worker scan is measurable but not yet the limiting service at
that cadence. The follow-up is triggered by any of:

1. Measured/projected claim+release CPU reaches **25% of a primary core**
   (approximately 650–690 successful pairs/s at the measured 64-worker cost).
2. Sustained acquisition **p99 ≥ 2 ms** at deployment offered load.
3. A planned deployment exceeds the measured **64 live workers**; re-profile
   before increasing the configured worker limit.

That follow-up must version both the worker registry and precomputed assignments,
atomically reject claims against stale versions, and publish/rebuild assignment
generations on worker arrival, capacity change and expiry. Assignment generation
must never replace token/deadline checks on existing leases or progress. Existing
claims continue safely to a lease boundary while rebalancing; worker failure
must not leave tenants permanently assigned to an expired worker. These
requirements are a follow-up design boundary, not an implemented O(1) protocol.

## Snapshot work and coalescing

Initial snapshot elapsed times before/after were **0.688/0.674**, **0.765/0.747**,
**2.197/2.060**, and **6.758/6.638 seconds** for 1/2/16/64 workers. Final Redis
CPU was **0.052/0.053/0.056/0.059 seconds**. Redis commands are 30,128–30,191
afterward, one more than before for end-revision validation; inbound/outbound
bytes remain approximately **3.23–3.27 MB / 1.82 MB**. At 64 workers most of the
remaining time is Ruby's O(tenants × workers) rendezvous computation, yielded
every bounded scan chunk, rather than Redis CPU. Both scans and computations
are cached, and simultaneous callers share work instead of multiplying it.

Controlled unit tests gate six simultaneous callers and verify one shared
membership/assignment operation. A revision change during a scan forces a new
scan, continuous churn is capped at three attempts, and neither cancellation
nor failure wedges waiters or publishes a partial/mixed-revision snapshot.

## Rails checkout and full-runtime runs

The full benchmark uses real PostgreSQL `generate_series` page reads through the
selected Rails pool. Actual outer checkout/hold boundaries are instrumented;
nested reuse is not double-counted. The following compares **the new batching
implementation's** legacy enumeration with its opt-in page callback, not the
original pre-batching implementation. Both register the same 30,000 IDs.

| Workers | Legacy pass / connection hold s | Paged pass / total hold s | Paged maximum hold ms | Paged total checkout wait ms |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 0.817 / 0.810 | 0.704 / 0.099 | 1.754 | 1.905 |
| 2 | 0.623 / 0.620 | 0.614 / 0.088 | 2.138 | 1.557 |
| 16 | 0.666 / 0.661 | 0.582 / 0.086 | 1.080 | 1.461 |
| 64 | 0.653 / 0.650 | 0.605 / 0.092 | 1.279 | 1.555 |

Legacy holds one checkout throughout the pass, including registration. Paged
discovery performs 118 checkouts, each released before registration. A gated
PostgreSQL+Redis integration test separately proves the discovery fiber has no
selected-pool checkout while registration is blocked, including lazy enumeration.

Each full-runtime run boots one real Rails/Async runtime with all 30,000 tenant
records, scheduler admission, ingestion queue, Redis progress, discovery and
heartbeats. Other worker records model registry peers, **not separate worker
processes**. Fetch/ingestion bodies are synthetic; one fetch sleeps 3.2 seconds
against a 3-second lease. Shutdown begins after at least 100 completions and
three renewals; remaining active work drains. These are responsiveness runs,
not comparable fixed-work throughput samples.

| Worker records | Elapsed s | Completed cycles | Redis commands / script requests | Redis CPU s | Inbound / outbound bytes | DB hold s | Minimum renewal slack s |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 4.655 | 653 | 115,643 / 4,703 | 0.526 | 8,928,948 / 1,870,110 | 0.119 | 1.982 |
| 2 | 4.520 | 1,186 | 137,080 / 8,433 | 0.704 | 10,204,760 / 1,911,924 | 0.118 | 1.988 |
| 16 | 6.037 | 1,571 | 174,152 / 11,129 | 0.971 | 11,244,842 / 1,945,482 | 0.126 | 1.995 |
| 64 | 10.530 | 502 | 141,769 / 3,644 | 0.556 | 8,680,205 / 1,874,162 | 0.105 | 1.997 |

All four runs confirmed three tenant renewals with **zero deadline misses**.
Ordinary-pool total/max checkout waits were **7.58/0.067**, **11.24/0.028**,
**14.71/0.030**, and **5.21/0.026 ms** respectively. An additional real Redis
test gates a large membership scan while saturating a one-connection command
pool, shares the scan with another caller, and verifies renewal completes via
its independent pool before the scan gate opens.

## Correctness verification

Coverage includes empty/final-partial/duplicate discovery, exact revision counts,
mid-pass failure, cancellation, failed final flush, lease expiry/takeover,
single-flight cancellation/failure/churn, real script flush and new-connection
recovery, stale assignment rejection after rebalance, stale token rejection after
takeover, selected-pool checkout release, and lost advancement reply reconciliation
after confirmed PostgreSQL commit. The fault wrapper intercepts both warm SHA
execution and cold-cache fallback; the executor itself never retries ambiguous
transport errors.

Required checks: focused Redis unit tests, `bundle exec rake` (all unit tests and
RuboCop), Redis integration, and PostgreSQL+Redis integration. Integration uses
isolated disposable services when default host ports are occupied.

Final verification results:

* Focused Redis unit tests: **22 tests, 126 assertions**, passing.
* `bundle exec rake`: **97 tests, 588 assertions**, passing; **43 files** clean
  under RuboCop.
* Redis integration: **17 tests, 203 assertions**, passing.
* PostgreSQL+Redis integration: **31 tests, 265 assertions**, passing, including
  the Redis suite again after the final protocol changes.
* `git diff --check`: clean.
