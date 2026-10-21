# Reactor responsiveness evidence (Story 05)

Measured 2026-10-21 on Linux x86_64, CRuby 4.0.6, Async 2.37.0,
Rails 8.1.4, redis gem 5.4.1, pg 1.6.3, Redis 7.4.11, PostgreSQL 17.11.
Host exposes 8 logical CPUs; cgroup CPU/memory ceilings are absent. Both local
service containers have CPUQuota=0, NanoCpus=0 and Memory=0 (unlimited).
Workers and the independently spawned upstream have no additional CPU quota.
Worker soft/hard NOFILE limits: 1,048,576 / 1,048,576. This is local measured
evidence; it does not establish 500 cycles/sec production service capacity.

## Workload and interpretation

Each row measures 10 seconds after coordinated startup, with 2.5 seconds warmup
for the service harness's separate window reports. CPU/allocation/GC deltas are
process-wide over the entire measurement interval; GC and allocation counters
cannot be attributed to individual fibers. Reactor/renewal histograms and HTTP
stage sums cover worker lifetime including startup/drain. Quantiles are fixed
histogram upper bounds, not exact percentiles. RSS is a sampled process maximum;
socket counts are maximum sampled `/proc` descriptors including Redis/PostgreSQL.
Drain is elapsed after the intended measurement endpoint, including sampling
overrun and HTTP-client closure. No offloaded destination mutation was introduced.

Default capacities per worker: fetch 32, pipeline 48, resolve/ingest 4, retained
budget 256 MiB, response/batch allowance 1 MiB. 100 tenants with 10 ms interval
saturate fetch; idle uses 3600 s interval and membership uses 30,000 tenants with
3600 s interval. Membership still admits the subset naturally due during the run.
Synthetic and real upstream latency are both 1.1 s, payload 1 KiB, fixed seed
20261001. Large payload uses 256 KiB, with JSON parse and uppercase transformation.
Blocking parse injects 50 ms of non-yielding work and sets socket latency to zero.
Blocking telemetry burns 20 ms in every synchronous notification subscriber;
bounded telemetry instead sleeps 20 ms on a single export thread with queue 16.
There are no tails/retries injected in the measured ordinary cases.

TLS is verified against an ephemeral self-signed CA. Net::HTTP clients are
exclusive per fiber, persistent, bounded by fetch concurrency, with 30 s
open/read/write and keep-alive timeouts and automatic replay disabled. Interrupted
clients are closed. PostgreSQL connection budget is 13 per worker; Redis is 24.
Lease TTL is 3 s, renewal interval 1 s. Full client cancellation and reservation
cleanup are covered in HTTP/runtime tests, including cancellation during TLS.

| Workload | Processes | Cycles/s | CPU s | GC s | Lag p99/max ms | Slack min s | Renewals/misses | Drain max s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| idle | 1 | 0.0 | 0.058 | 0.002 | 1.0/1.6 | — | 0/0 | 0.003 |
| idle | 2 | 0.0 | 0.146 | 0.003 | 1.0/1.4 | — | 0/0 | 0.004 |
| synthetic | 1 | 29.2 | 1.412 | 0.087 | 5.0/13.8 | 1.983 | 490/0 | 1.234 |
| synthetic | 2 | 58.0 | 2.647 | 0.138 | 10.0/11.4 | 1.980 | 977/0 | 1.773 |
| membership | 1 | 8.2 | 1.385 | 0.111 | 25.0/28.5 | 1.984 | 88/0 | 0.972 |
| membership | 2 | 8.1 | 2.320 | 0.220 | 10.0/45.6 | 1.981 | 102/0 | 1.069 |
| http_tls | 1 | 28.8 | 1.422 | 0.103 | 5.0/13.1 | 1.967 | 474/0 | 1.465 |
| http_tls | 2 | 57.7 | 2.618 | 0.099 | 25.0/116.0 | 1.977 | 816/0 | 1.731 |
| large_payload | 1 | 28.8 | 3.409 | 0.334 | 25.0/34.0 | 1.889 | 451/0 | 1.687 |
| large_payload | 2 | 56.7 | 6.654 | 0.511 | 25.0/41.8 | 1.817 | 769/0 | 1.718 |
| blocking_parse | 1 | 19.5 | 10.014 | 0.064 | 1000/507.8 | 1.781 | 63/0 | 0.199 |
| blocking_parse | 2 | 40.1 | 19.938 | 0.133 | 500.0/457.7 | 1.792 | 122/0 | 0.191 |
| blocking_telemetry | 1 | 7.4 | 9.841 | 0.038 | 250.0/137.9 | 1.595 | 306/0 | 1.612 |
| blocking_telemetry | 2 | 17.0 | 19.895 | 0.083 | 250.0/128.5 | 1.669 | 425/0 | 1.400 |
| bounded_telemetry | 1 | 29.1 | 1.563 | 0.093 | 10.0/11.5 | 1.980 | 352/0 | 1.251 |
| bounded_telemetry | 2 | 57.9 | 3.082 | 0.130 | 10.0/12.2 | 1.974 | 870/0 | 1.996 |

| Workload | Processes | Allocations | Peak RSS MiB | Sampled sockets | Export drops | Parse/transform total s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| idle | 1 / 2 | 11,867 / 20,053 | 85.00 / 170.30 | 2 / 4 | 0 | — |
| synthetic | 1 / 2 | 426,459 / 837,439 | 98.81 / 197.18 | 17 / 36 | 0 | — |
| membership | 1 / 2 | 1,820,327 / 3,280,102 | 120.88 / 252.25 | 13 / 32 | 0 | — |
| http_tls | 1 / 2 | 387,100 / 762,238 | 101.55 / 203.47 | 55 / 102 | 0 | .002/.010; .005/.023 |
| large_payload | 1 / 2 | 401,595 / 747,122 | 226.44 / 508.23 | 53 / 110 | 0 | .029/1.121; .073/2.532 |
| blocking_parse | 1 / 2 | 244,987 / 474,414 | 99.77 / 199.64 | 38 / 73 | 0 | 10.154/.008; 21.760/.018 |
| blocking_telemetry | 1 / 2 | 145,164 / 275,609 | 97.29 / 194.48 | 23 / 46 | 0 | — |
| bounded_telemetry | 1 / 2 | 432,508 / 864,820 | 98.79 / 198.69 | 19 / 40 | 1623 / 3611 | — |

All rows returned cycle, queue, fetch, ingest and byte reservations to zero. Drops
are expected under the tested drop-new policy. Telemetry counters are the final
runtime snapshot before the caller's separate exporter drain. Separate socket,
parse, transform, GC, allocation, notification and admission measurements are
retained in [baseline JSON](reactor-baselines.json) and
[socket-scale JSON](reactor-socket-scale.json).
The socket timer includes TLS, response read and buffering; it is not pure kernel
wait time. Parse/transform timers isolate the subsequent synchronous work.

The independently provisioned upstream completed all 1,072 normal TLS and 1,044
large-payload requests across one/two-process runs, reached 64 concurrent waits,
used 1.616 / 2.128 CPU seconds and had maximum timer lag 2.03 / 5.09 ms.
Blocking-parse upstream used 1.075 CPU seconds with max lag 1.12 ms and at most
2 concurrent requests. This isolates worker CPU saturation from fixture overload.
The TLS rows reused persistent connections for 304 requests in the single worker;
the two workers reused 368 and 304 requests respectively (32 connections each).

## Socket-scale / large membership check

Repeat TLS with 30,000 tenants, fetch 256, pipeline 320, resolve/ingest 16 per
worker, same 10-second measurement window and 1.1-second upstream latency. PostgreSQL budget
37 and HTTP limit 256 per worker; Redis remains 24. Byte allowance limits the
simultaneous 1 MiB reservations to 256 independently of pipeline slots.

| Processes | Cycles/s | CPU s | GC s | Lag p99/max ms | Slack min s | Renewals/misses | Drain max s | RSS MiB | Sockets |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 142.9 | 8.438 | .536 | 25/59.4 | 1.843 | 1685/0 | 1.128 | 167.37 | 298 |
| 2 | 312.9 | 17.395 | 1.129 | 25/120.1 | 1.797 | 3470/0 | 1.263 | 332.30 | 594 |

Fixture: 5,155 completed requests, max 497 concurrent requests, 3.622 CPU seconds,
max timer lag 2.52 ms. All reservations released. This larger run becomes
worker-CPU/coordination limited before its nominal 256/1.1 fetch-slot bound; simply
raising fetch concurrency cannot be assumed to improve throughput. Full-load
controls remain configurable; this 10-second run does not claim sustained release
gates for the earlier 160-second capacity story.

## Thresholds and boundary decision

The supported measured envelope uses p99 reactor lag ≤25 ms, maximum ≤150 ms
(TTL/20), renewal slack ≥1.5 s (TTL/2), zero misses, drain ≤10.3 s. These thresholds
allow observed ordinary TLS/GC startup outliers while keeping ample renewal slack.
All ordinary rows and the socket-scale check pass; deliberately blocking
parse/telemetry fail the lag gate even with isolated Redis pools and zero misses.
Doubling processes doubles the CPU-limited parse throughput, demonstrating the
reason to retain process isolation rather than add a thread callback executor.
See README for the required ownership/cancellation contract of any future offload.

## Reproduction

Use disposable PostgreSQL/Redis databases; services and this harness must not run
concurrently with integration tests that own the same consumer table/index names.
When the default ports are occupied, a Compose override can bind 15432/16379.

```sh
ACP_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:15432/acp_test \
ACP_TEST_REDIS_URL=redis://127.0.0.1:16379/15 \
ACP_BENCH_REPORT=/tmp/opencode/acp-reactor-baselines.json ACP_BENCH_SUMMARY=1 \
bundle exec ruby benchmark/reactor_responsiveness.rb

bundle exec ruby benchmark/compact_reactor_report.rb /tmp/opencode/acp-reactor-baselines.json

ACP_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:15432/acp_test \
ACP_TEST_REDIS_URL=redis://127.0.0.1:16379/15 \
ACP_REACTOR_CASES=http_tls ACP_BENCH_TENANTS=30000 \
ACP_BENCH_FETCH_CONCURRENCY=256 ACP_BENCH_PIPELINE_CAPACITY=320 \
ACP_BENCH_RESOLVE_CONCURRENCY=16 ACP_BENCH_INGEST_CONCURRENCY=16 \
ACP_BENCH_REPORT=/tmp/opencode/acp-reactor-socket-scale.json ACP_BENCH_SUMMARY=1 \
bundle exec ruby benchmark/reactor_responsiveness.rb
```

`ACP_REACTOR_CASES` selects comma-separated scenarios; `ACP_BENCH_WORKERS` defaults
to `1,2`. Increase `ACP_BENCH_DURATION`, tenant count and independent stage/byte
budgets for longer/full-scale checks. The JSON includes measured responsiveness
gates for each process count. Provision upstreams independently for deployment
tests: run `bundle exec ruby benchmark/http_upstream.rb` in its own process or
container (`ACP_HTTP_BIND`, `ACP_HTTP_PORT`, `ACP_HTTP_TLS`, `ACP_HTTP_LATENCY`,
`ACP_HTTP_RECORDS`, `ACP_HTTP_PAYLOAD_BYTES`, `ACP_HTTP_FAIL_EVERY`). It prints its
URL and PEM CA at startup; `/metrics` reports its CPU/GC, concurrency and lag.
Pass `ACP_BENCH_HTTP_URL` and `ACP_BENCH_HTTP_CA_FILE` to
`integration/benchmark_services.rb` to use an externally provisioned fixture.
Sample its `/metrics` independently before/after each worker-count run; compare
fixture CPU/lag with workers to detect fixture saturation. Connection failures are
deterministically injected with `ACP_HTTP_FAIL_EVERY`; lifecycle tests check failed
connections are discarded rather than replayed automatically.

Verification: focused HTTP/runtime tests, `bundle exec rake`, Redis integration,
and PostgreSQL+Redis integration run against the isolated services.
