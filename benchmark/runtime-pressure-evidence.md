# Story 01 — implementation evidence

Metric names, boundaries, units, inclusion rules, and lifetime/reset semantics
are defined in [README: Runtime pressure metrics](../README.md#runtime-pressure-metrics).
The implementation changes instrumentation, not polling cadence or admission policy.

## Seeded local benchmark

Captured on 2026-10-21. [Selected JSON report fields](runtime-pressure-evidence.json)
preserve environment, workload, exact timing aggregates, and RSS. Unused
backoff/renewal distributions had zero observations in both instrumented runs.

Before changing runtime instrumentation, the local benchmark received an
independent 100 ms reactor observer. That same observer ran afterwards. The
instrumented reporter additionally samples overdue peaks and includes runtime
distributions. Runs used the same seed/workload and were executed sequentially.

```sh
ACP_BENCH_TENANTS=1000 ACP_BENCH_DURATION=5 ACP_BENCH_INTERVAL=1 \
  bundle exec rake benchmark:runtime

# After instrumentation, repeat the command above, then inject fetch contention:
ACP_BENCH_TENANTS=1000 ACP_BENCH_DURATION=5 ACP_BENCH_INTERVAL=1 \
  ACP_BENCH_FETCH_CONCURRENCY=1 bundle exec rake benchmark:runtime
```

| Measurement | Before | After | Saturated fetch |
| --- | ---: | ---: | ---: |
| Completed cycles | 4,987 | 4,990 | 1,099 |
| Elapsed seconds including drain | 5.010 | 5.010 | 6.475 |
| Cycles/second | 995.35 | 996.02 | 169.73 |
| Peak RSS MiB | 30.50 | 30.60 | 36.00 |
| Observer mean reactor lag, ms | 0.193 | 0.146 | 0.237 |
| Observer max reactor lag, ms | 1.620 | 1.171 | 4.309 |
| Admission lag max, ms | unavailable | 4.621 | 3,895.183 |
| Fetch wait p95 upper bound, seconds | unavailable | 0.001 | 2 |
| Fetch service p95 upper bound, seconds | unavailable | 0.01 | 0.01 |
| Ingest wait p95 upper bound, seconds | unavailable | 0.001 | 0.001 |
| Runtime reactor lag p95 upper bound, seconds | unavailable | 0.001 | 0.001 |
| Sampled peak overdue tenants | unavailable | 3 | 997 |

The nominal run changed throughput by +0.067%, peak RSS by +0.10 MiB, and
observer mean lag by -0.046 ms. These short, scheduling-limited runs establish a
smoke baseline, not a statistical claim of zero CPU overhead or a capacity result.
The saturated run identifies fetch **queueing**, not slower fetch service,
ingestion, or reactor stalls: fetch service p95 stays at 10 ms while fetch wait
p95 reaches 2 seconds. Overdue peaks include graceful drain, when admission has
stopped but enabled scheduled work remains due. Final stopped gauges are zero.

## Controlled timing and bounded storage

- Three simultaneously due tenants and one pipeline slot start at exactly
  0/30/60 ms. Two succeed, one fails; admission sum is 90 ms, p50 bound 50 ms,
  p95 bound 100 ms, and every cycle has zero overrun. Live waiting counts go
  2/1/0 as each tenant is admitted.
- A gated indefinite fetch exposes two overdue tenants aged four seconds before
  any completion. Pause, removal, resume, and cancellation update the snapshot.
- Gated resolution/fetch tests independently fix two seconds of semaphore wait
  and callback services of two/three seconds. A timed-out fetch waiter records
  no service and releases no unowned permit.
- Gated ingestion observes waits of 0/5/7 seconds; the last wait is cancelled.
  Callback services of five/two seconds include cancellation of the second.
- Initial cursor adapter work (1 + 2 seconds), two acknowledgement attempts
  (3 + 3 seconds), and four seconds of backoff produce a 13-second cycle with
  exactly three seconds of overrun at a ten-second interval.
- Controlled monotonic jumps distinguish 250 ms reactor/renewal wakeup lateness
  from callback/command duration. A coordinator clock starting at 1000 verifies
  that no runtime/server clock epoch is involved in slack arithmetic.
- Ten thousand observations retain exactly 17 histogram buckets and four fixed
  outcome counters per metric. Overflow is reported as an unbounded percentile
  (`null`), with exact maximum retained. Snapshots remain frozen and independent
  of later observations. Twenty runtime/five coordinator distributions and one
  reactor sampler are fixed for the lifetime of each instance, including across
  the nominal benchmark's five polling intervals.

## Checks

Passed:

```sh
bundle exec ruby -Itest test/test_runtime.rb
# 31 runs, 158 assertions
bundle exec ruby -Itest test/test_redis.rb
# 11 runs, 55 assertions
bundle exec rake
# 73 runs, 320 assertions; RuboCop clean
ACP_TEST_REDIS_URL=redis://127.0.0.1:6380/15 bundle exec rake test:redis
# 10 runs, 153 assertions
```

Default Compose ports were already occupied by another project's services.
Redis integration used a disposable `redis:7` container on localhost:6380. The
real renewal scenario holds the renewal checkout for 2.4 seconds of a three-second
lease, verifies successful renewal with less than 0.7 seconds of old-lease slack,
then waits for expiry and verifies failure plus negative slack/deadline miss.
The existing long-fetch scenario verifies repeated successful renewal and wakeup
samples beyond the original lease lifetime.
