# Rails shared-cycle connection budget — Story 06

Verified on 2026-10-21 with Ruby 4.0.6, Rails 8.1.4, pg 1.6.3,
PostgreSQL 17, and Redis 7. The selected-pool requirement is
`min(pipeline_capacity, resolve_concurrency + ingest_concurrency) + 1`.
See [the checkout proof](rails-connection-budget-proof.md) for its callback contract
and admission/discovery revalidation rules.

## Exact-bound integration checks

`integration/postgresql_test.rb` uses an independent model/pool with a 200 ms
checkout timeout. Setup returns its unrelated application checkout; the driver
only reads `pool.stat`, and never leases from the observed pool.

- 8/8/8: four resolving cycles, four ingesting cycles, and periodic discovery
  hold **9 simultaneous connections**. All eight cycles subsequently complete.
- 100/5/7: five resolving cycles, seven ingesting cycles, and periodic discovery
  hold **13 simultaneous connections**. All twelve cycles subsequently complete.
- An 8/2/3 program gates eight initial-cursor callbacks: only **two** can enter
  and hold checkouts concurrently; all eight cycles subsequently complete on the
  six-connection bound. Suspended fetch and progress acknowledgement hold **zero**
  selected-pool connections.
- Normal return, callback errors, cancellation, and lease loss return checkouts;
  the real Rails executor clears CurrentAttributes. Both `connection` and
  `lease_connection` are exercised under permanent-checkout prevention.
- A 1/5/7 program with four retry reservations progresses through parked ingestion
  retries on a two-connection pool. Multiple retained cycles still share one
  runnable pipeline reservation and one concurrent ingestion checkout.

All checks finish with zero busy connections and zero checkout waiters. Unit
boundaries also include asymmetric limits, P below/equal/above R + I, and rejection
of each pool one connection below the requirement.

Verification passed: `bundle exec rake` (118 tests, 867 assertions, plus RuboCop),
the shared PostgreSQL/Redis `test:postgres` task (37 tests, 394 assertions), and
`git diff --check`. The final lint pass includes the integration scenarios.

## Two-worker service smoke

Both sequential runs used 32 tenants, two independent worker processes, a
10-second measurement, 100 ms polling, 8/8/8 pipeline/resolve/ingest limits,
eight fetch slots, seeded 10–30 ms fetch latency, 2 ms resolution and ingestion
delays, and the harness's default payload/byte budgets. Each run includes actual
Redis coordination and PostgreSQL destination transactions.

| Measurement | Prior sizing | Shared-cycle sizing |
| --- | ---: | ---: |
| Configured pool per worker | 21 (17 + 4) | 13 (9 + 4) |
| Configured worker-fleet connection budget | 42 | 26 |
| Separate benchmark-driver connection | 1 | 1 |
| Observed open connections, worker fleet at end | 16 | 16 |
| Peak busy connections per worker | 8 / 8 | 8 / 8 |
| Checkout timeouts, worker fleet | 0 | 0 |
| Checkout acquisitions, worker lifetime | 7,568 | 7,422 |
| Total acquisition duration, worker lifetime | 206.48 ms | 213.38 ms |
| Longest individual acquisition | 5.43 ms | 5.32 ms |
| Measurement throughput, fleet cycles/sec | 309.9 | 304.1 |
| Busy connections after drain, worker fleet | 0 | 0 |

Acquisition duration includes connection creation/verification and checkout
waiting, measured by wrapping the actual pool `checkout` method. Observer
snapshots do not acquire connections. Acquisition metrics include startup and
drain; throughput excludes them.

The configured worker-fleet allowance fell by **38.1%** with zero checkout
timeouts in either run. Active Record opens connections lazily, so the physical
connection count did not change on this small workload. The 1.9% throughput
difference is a short-run observation, not a demonstrated regression or capacity
claim; the shared-cycle run also overlapped the local unit/lint checks. This is
a sizing/progress smoke, not the full 30,000-tenant release benchmark.

Reproduce against disposable services (the recorded run used alternate ports
55432/56379 because the default local ports were occupied):

```sh
export ACP_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55432/acp_test
export ACP_TEST_REDIS_URL=redis://127.0.0.1:56379/15
export ACP_BENCH_TENANTS=32 ACP_BENCH_DURATION=10 ACP_BENCH_WORKERS=2
export ACP_BENCH_INTERVAL=0.1 ACP_BENCH_PIPELINE_CAPACITY=8
export ACP_BENCH_RESOLVE_CONCURRENCY=8 ACP_BENCH_INGEST_CONCURRENCY=8
export ACP_BENCH_FETCH_CONCURRENCY=8
export ACP_BENCH_LATENCY_MIN=0.01 ACP_BENCH_LATENCY_MAX=0.03
export ACP_BENCH_RESOLVE_DELAY=0.002 ACP_BENCH_INGEST_DELAY=0.002

ACP_TEST_POOL=21 ACP_BENCH_REPORT=/tmp/opencode/acp-budget-prior.json \
  ACP_BENCH_SUMMARY=1 bundle exec rake benchmark:services
ACP_BENCH_POOL_HEADROOM=4 ACP_BENCH_REPORT=/tmp/opencode/acp-budget-shared.json \
  ACP_BENCH_SUMMARY=1 bundle exec rake benchmark:services

bundle exec ruby -Itest test/test_rails.rb
bundle exec rake
bundle exec rake test:postgres
```

Unset any inherited `ACP_TEST_POOL` override for the shared-cycle run; its total
is derived from the formula plus `ACP_BENCH_POOL_HEADROOM`. Production application
headroom must be sized for its actual other pool users, independently of the
benchmark's four-connection allowance.
