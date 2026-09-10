# Store latency baseline: postgres (full tier)

- commit: `e3dd018807cf8d62fa833dd229d009974aaff678`
- build: release profile, rustc 1.97.1 (8bab26f4f 2026-07-14)
- host: 4 cpus, Intel(R) Xeon(R) Processor, 16398384 KiB RAM, linux 6.12.94+
- config sha256: `32f3fc272ebaf25904f8c33b4239d3d2ad726159a822bb0ffd096d821b967a5c`; binary sha256: `58e067cf036aa91c7e0c47edeb15d56e70784bea754bf4ac05e737413e85e160`
- repetitions per scenario: 3

> Latency, throughput, and CPU are informational: they measure the host as much as the gateway. Compare artifacts only when environment.hardware, environment.toolchain.cargo_profile, and environment.config.sha256 agree, and read docs/operations/store-latency-baseline.md before comparing two commits.

## Request path (median repetition)

| Scenario | Concurrency | Requests | Accepted req/s | p50 | p95 | p99 | TTFT p95 | Overhead p50 / p95 vs control | CPU cores | Peak RSS | Shed | Errors | Usage missing |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `steady-buffered` | 32 | 3000 | 816 | 28.1 ms | 112.1 ms | 182.5 ms | — | 27.7 ms / 111.5 ms | 0.43 | 26 MiB | 0 | 0 | 0 |
| `steady-streamed` | 32 | 600 | 121 | 257.4 ms | 272.4 ms | 286.7 ms | 55.3 ms | -0.607 ms / 10.5 ms | 0.21 | 26 MiB | 0 | 0 | 0 |
| `burst` | 128 | 1024 | 341 | 91.2 ms | 150.5 ms | 168.5 ms | — | 90.0 ms / 148.7 ms | 0.18 | 35 MiB | 0 | 0 | 0 |
| `summaries` | 32 | 2000 | 861 | 25.7 ms | 105.9 ms | 167.0 ms | — | — | 0.61 | 26 MiB | 0 | 0 | 0 |
| `slow-store` | 32 | 2000 | 783 | 30.5 ms | 110.9 ms | 164.8 ms | — | — | 0.29 | 28 MiB | 0 | 0 | 0 |
| `large-payload` | 16 | 600 | 702 | 17.1 ms | 53.8 ms | 78.9 ms | — | 15.3 ms / 48.2 ms | 0.99 | 62 MiB | 0 | 0 | 0 |

## Management summaries (median repetition)

| Scenario | Readers | Summary req/s | Summary p50 | Summary p95 | Summary errors | Usage rows at end | Store bytes at end |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `summaries` | 4 | 1227.2 | 3.2 ms | 4.5 ms | 0 | 3252 | 1130496 |
| `slow-store` | 4 | 36.0 | 110.9 ms | 141.2 ms | 0 | 202141 | 34496512 |

## Store phases (cumulative over the scenario's process, warmup included)

| Scenario | Operation | Calls | Acquire wait mean | Acquire wait p95 ≤ | Acquire wait max | Query mean | Query p95 ≤ | Query max | Outcomes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `steady-buffered` | `budget_charge` | 9300 | 0.300 ms | 1.0 ms | 6.4 ms | 36.9 ms | 250.0 ms | 360.1 ms | ok=9300 |
| `steady-buffered` | `budget_write` | 2 | 0.002 ms | 0.050 ms | 0.002 ms | 2.1 ms | 2.5 ms | 2.2 ms | ok=2 |
| `steady-buffered` | `namespace_read` | 2 | 0.003 ms | 0.050 ms | 0.003 ms | 0.171 ms | 0.250 ms | 0.189 ms | ok=2 |
| `steady-buffered` | `namespace_resolve` | 9300 | 0.596 ms | 2.5 ms | 41.8 ms | 0.588 ms | 1.0 ms | 31.5 ms | ok=9300 |
| `steady-buffered` | `namespace_write` | 2 | 0.003 ms | 0.050 ms | 0.003 ms | 2.3 ms | 5.0 ms | 2.5 ms | ok=2 |
| `steady-buffered` | `provider_models` | 4 | 0.003 ms | 0.050 ms | 0.004 ms | 0.716 ms | 2.5 ms | 1.3 ms | ok=4 |
| `steady-buffered` | `usage_append` | 7214 | 0.318 ms | 1.0 ms | 3.5 ms | 1.3 ms | 5.0 ms | 9.2 ms | ok=7214 |
| `steady-streamed` | `budget_charge` | 1864 | 0.378 ms | 0.100 ms | 18.4 ms | 4.4 ms | 25.0 ms | 43.9 ms | ok=1864 |
| `steady-streamed` | `budget_write` | 2 | 0.001 ms | 0.050 ms | 0.001 ms | 2.1 ms | 2.5 ms | 2.2 ms | ok=2 |
| `steady-streamed` | `namespace_read` | 2 | 0.003 ms | 0.050 ms | 0.003 ms | 0.193 ms | 0.250 ms | 0.205 ms | ok=2 |
| `steady-streamed` | `namespace_resolve` | 1864 | 2.2 ms | 25.0 ms | 40.3 ms | 1.6 ms | 10.0 ms | 29.0 ms | ok=1864 |
| `steady-streamed` | `namespace_write` | 2 | 0.003 ms | 0.050 ms | 0.005 ms | 2.1 ms | 2.5 ms | 2.4 ms | ok=2 |
| `steady-streamed` | `provider_models` | 4 | 0.002 ms | 0.050 ms | 0.005 ms | 1.1 ms | 2.5 ms | 2.2 ms | ok=4 |
| `steady-streamed` | `usage_append` | 1864 | 0.006 ms | 0.050 ms | 7.8 ms | 1.5 ms | 5.0 ms | 9.0 ms | ok=1864 |
| `burst` | `budget_charge` | 3200 | 40.2 ms | 100.0 ms | 106.4 ms | 25.2 ms | 100.0 ms | 144.2 ms | ok=3200 |
| `burst` | `budget_write` | 2 | 0.001 ms | 0.050 ms | 0.001 ms | 2.1 ms | 2.5 ms | 2.3 ms | ok=2 |
| `burst` | `namespace_read` | 2 | 0.003 ms | 0.050 ms | 0.003 ms | 0.194 ms | 0.250 ms | 0.197 ms | ok=2 |
| `burst` | `namespace_resolve` | 3200 | 19.6 ms | 50.0 ms | 47.9 ms | 4.9 ms | 10.0 ms | 21.7 ms | ok=3200 |
| `burst` | `namespace_write` | 2 | 0.004 ms | 0.050 ms | 0.007 ms | 2.4 ms | 5.0 ms | 2.5 ms | ok=2 |
| `burst` | `provider_models` | 4 | 0.002 ms | 0.050 ms | 0.004 ms | 0.821 ms | 2.5 ms | 1.9 ms | ok=4 |
| `burst` | `usage_append` | 3200 | 0.660 ms | 0.050 ms | 101.0 ms | 0.860 ms | 2.5 ms | 4.1 ms | ok=3200 |
| `summaries` | `budget_charge` | 6200 | 0.884 ms | 2.5 ms | 12.2 ms | 33.7 ms | 250.0 ms | 295.7 ms | ok=6200 |
| `summaries` | `budget_write` | 2 | 0.002 ms | 0.050 ms | 0.002 ms | 2.3 ms | 2.5 ms | 2.4 ms | ok=2 |
| `summaries` | `namespace_read` | 8710 | 0.984 ms | 2.5 ms | 7.9 ms | 0.254 ms | 1.0 ms | 7.8 ms | ok=8710 |
| `summaries` | `namespace_resolve` | 6200 | 1.2 ms | 2.5 ms | 47.9 ms | 0.865 ms | 2.5 ms | 37.3 ms | ok=6200 |
| `summaries` | `namespace_write` | 2 | 0.004 ms | 0.050 ms | 0.007 ms | 1.6 ms | 2.5 ms | 1.8 ms | ok=2 |
| `summaries` | `provider_models` | 4 | 0.002 ms | 0.050 ms | 0.005 ms | 0.860 ms | 2.5 ms | 1.6 ms | ok=4 |
| `summaries` | `usage_append` | 3252 | 0.836 ms | 2.5 ms | 4.3 ms | 1.5 ms | 5.0 ms | 15.7 ms | ok=3252 |
| `summaries` | `usage_summary` | 8708 | 1.0 ms | 2.5 ms | 8.1 ms | 0.797 ms | 2.5 ms | 10.9 ms | ok=8708 |
| `slow-store` | `budget_charge` | 6200 | 2.2 ms | 10.0 ms | 26.5 ms | 31.8 ms | 100.0 ms | 271.0 ms | ok=6200 |
| `slow-store` | `budget_write` | 3 | 0.001 ms | 0.050 ms | 0.002 ms | 1.9 ms | 2.5 ms | 1.9 ms | ok=3 |
| `slow-store` | `namespace_read` | 280 | 1.9 ms | 5.0 ms | 10.2 ms | 0.516 ms | 2.5 ms | 4.6 ms | ok=280 |
| `slow-store` | `namespace_resolve` | 6200 | 2.8 ms | 10.0 ms | 48.5 ms | 2.0 ms | 10.0 ms | 36.2 ms | ok=6200 |
| `slow-store` | `namespace_write` | 2 | 0.004 ms | 0.050 ms | 0.007 ms | 0.956 ms | 2.5 ms | 1.6 ms | error=1 ok=1 |
| `slow-store` | `provider_models` | 4 | 0.002 ms | 0.050 ms | 0.005 ms | 0.727 ms | 2.5 ms | 1.6 ms | ok=4 |
| `slow-store` | `usage_append` | 2141 | 1.7 ms | 10.0 ms | 20.4 ms | 2.2 ms | 10.0 ms | 22.1 ms | ok=2141 |
| `slow-store` | `usage_summary` | 278 | 2.0 ms | 5.0 ms | 6.6 ms | 106.7 ms | 250.0 ms | 167.7 ms | ok=278 |
| `large-payload` | `budget_charge` | 1864 | 0.027 ms | 0.050 ms | 6.8 ms | 17.7 ms | 50.0 ms | 117.7 ms | ok=1864 |
| `large-payload` | `budget_write` | 2 | 0.002 ms | 0.050 ms | 0.003 ms | 2.2 ms | 5.0 ms | 2.6 ms | ok=2 |
| `large-payload` | `namespace_read` | 2 | 0.003 ms | 0.050 ms | 0.003 ms | 0.218 ms | 0.250 ms | 0.234 ms | ok=2 |
| `large-payload` | `namespace_resolve` | 1864 | 0.214 ms | 0.050 ms | 12.9 ms | 1.0 ms | 2.5 ms | 13.3 ms | ok=1864 |
| `large-payload` | `namespace_write` | 2 | 0.004 ms | 0.050 ms | 0.007 ms | 1.3 ms | 2.5 ms | 1.5 ms | ok=2 |
| `large-payload` | `provider_models` | 4 | 0.002 ms | 0.050 ms | 0.005 ms | 0.680 ms | 2.5 ms | 1.5 ms | ok=4 |
| `large-payload` | `usage_append` | 1759 | 0.001 ms | 0.050 ms | 0.020 ms | 1.6 ms | 5.0 ms | 8.8 ms | ok=1759 |

## Background index queue and connections

| Scenario | Connections opened | Index enqueues | Index queue max depth | Index wait mean | Index wait max |
| --- | --- | --- | --- | --- | --- |
| `steady-buffered` | 111 | 7214 | 256 | 388.0 ms | 486.6 ms |
| `steady-streamed` | 488 | 1864 | 18 | 2.4 ms | 30.0 ms |
| `burst` | 619 | 3200 | 202 | 106.8 ms | 258.2 ms |
| `summaries` | 118 | 3252 | 256 | 552.7 ms | 755.6 ms |
| `slow-store` | 184 | 2141 | 256 | 901.7 ms | 1426.1 ms |
| `large-payload` | 53 | 1759 | 256 | 280.4 ms | 447.3 ms |
