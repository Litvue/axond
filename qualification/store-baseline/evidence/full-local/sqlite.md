# Store latency baseline: sqlite (full tier)

- commit: `e3dd018807cf8d62fa833dd229d009974aaff678`
- build: release profile, rustc 1.97.1 (8bab26f4f 2026-07-14)
- host: 4 cpus, Intel(R) Xeon(R) Processor, 16398384 KiB RAM, linux 6.12.94+
- config sha256: `3914e0851559037b684725d6ac4f53fb8dbe20e5eb8ae3aa41f86881d992bb39`; binary sha256: `58e067cf036aa91c7e0c47edeb15d56e70784bea754bf4ac05e737413e85e160`
- repetitions per scenario: 3

> Latency, throughput, and CPU are informational: they measure the host as much as the gateway. Compare artifacts only when environment.hardware, environment.toolchain.cargo_profile, and environment.config.sha256 agree, and read docs/operations/store-latency-baseline.md before comparing two commits.

## Request path (median repetition)

| Scenario | Concurrency | Requests | Accepted req/s | p50 | p95 | p99 | TTFT p95 | Overhead p50 / p95 vs control | CPU cores | Peak RSS | Shed | Errors | Usage missing |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `steady-buffered` | 32 | 3000 | 581 | 53.2 ms | 89.8 ms | 105.6 ms | — | 52.9 ms / 89.2 ms | 0.30 | 28 MiB | 0 | 0 | 0 |
| `steady-streamed` | 32 | 600 | 116 | 264.7 ms | 314.5 ms | 317.3 ms | 99.4 ms | 6.7 ms / 51.4 ms | 0.20 | 26 MiB | 0 | 0 | 0 |
| `burst` | 128 | 1024 | 282 | 96.5 ms | 192.9 ms | 262.2 ms | — | 95.2 ms / 191.1 ms | 0.13 | 36 MiB | 0 | 0 | 0 |
| `summaries` | 32 | 2000 | 554 | 53.7 ms | 99.7 ms | 121.7 ms | — | — | 0.31 | 27 MiB | 0 | 0 | 0 |
| `slow-store` | 32 | 2000 | 124 | 250.9 ms | 390.8 ms | 472.8 ms | — | — | 0.79 | 29 MiB | 0 | 0 | 0 |
| `large-payload` | 16 | 600 | 525 | 28.9 ms | 50.3 ms | 60.5 ms | — | 27.2 ms / 45.1 ms | 0.77 | 64 MiB | 0 | 0 | 0 |

## Management summaries (median repetition)

| Scenario | Readers | Summary req/s | Summary p50 | Summary p95 | Summary errors | Usage rows at end | Store bytes at end |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `summaries` | 4 | 75.6 | 50.2 ms | 101.3 ms | 0 | 6200 | 5361216 |
| `slow-store` | 4 | 18.0 | 229.2 ms | 342.3 ms | 0 | 206200 | 29576864 |

## Store phases (cumulative over the scenario's process, warmup included)

| Scenario | Operation | Calls | Acquire wait mean | Acquire wait p95 ≤ | Acquire wait max | Query mean | Query p95 ≤ | Query max | Outcomes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `steady-buffered` | `budget_charge` | 9300 | 28.2 ms | 100.0 ms | 168.4 ms | 0.978 ms | 5.0 ms | 26.1 ms | ok=9300 |
| `steady-buffered` | `budget_write` | 2 | 0.014 ms | 0.050 ms | 0.020 ms | 2.1 ms | 2.5 ms | 2.2 ms | ok=2 |
| `steady-buffered` | `namespace_read` | 2 | 0.012 ms | 0.050 ms | 0.012 ms | 0.074 ms | 0.100 ms | 0.079 ms | ok=2 |
| `steady-buffered` | `namespace_resolve` | 9300 | 29.0 ms | 100.0 ms | 136.7 ms | 0.110 ms | 0.250 ms | 26.4 ms | ok=9300 |
| `steady-buffered` | `namespace_write` | 2 | 0.130 ms | 0.250 ms | 0.246 ms | 1.4 ms | 2.5 ms | 2.0 ms | ok=2 |
| `steady-buffered` | `provider_models` | 4 | 0.015 ms | 0.050 ms | 0.018 ms | 0.545 ms | 2.5 ms | 1.2 ms | ok=4 |
| `steady-buffered` | `usage_append` | 9300 | 0.958 ms | 10.0 ms | 50.5 ms | 0.808 ms | 2.5 ms | 6.8 ms | ok=9300 |
| `steady-streamed` | `budget_charge` | 1864 | 9.8 ms | 50.0 ms | 77.8 ms | 1.5 ms | 5.0 ms | 7.1 ms | ok=1864 |
| `steady-streamed` | `budget_write` | 2 | 0.015 ms | 0.050 ms | 0.015 ms | 3.4 ms | 5.0 ms | 3.5 ms | ok=2 |
| `steady-streamed` | `namespace_read` | 2 | 0.019 ms | 0.050 ms | 0.022 ms | 0.076 ms | 0.100 ms | 0.082 ms | ok=2 |
| `steady-streamed` | `namespace_resolve` | 1864 | 12.8 ms | 50.0 ms | 79.3 ms | 0.232 ms | 0.250 ms | 27.7 ms | ok=1864 |
| `steady-streamed` | `namespace_write` | 2 | 0.058 ms | 0.250 ms | 0.101 ms | 1.9 ms | 5.0 ms | 2.6 ms | ok=2 |
| `steady-streamed` | `provider_models` | 4 | 0.076 ms | 0.250 ms | 0.156 ms | 0.857 ms | 2.5 ms | 1.6 ms | ok=4 |
| `steady-streamed` | `usage_append` | 1864 | 1.0 ms | 10.0 ms | 59.4 ms | 1.1 ms | 5.0 ms | 5.8 ms | ok=1864 |
| `burst` | `budget_charge` | 3200 | 84.0 ms | 250.0 ms | 368.6 ms | 1.1 ms | 5.0 ms | 7.1 ms | ok=3200 |
| `burst` | `budget_write` | 2 | 0.084 ms | 0.250 ms | 0.153 ms | 3.1 ms | 5.0 ms | 3.3 ms | ok=2 |
| `burst` | `namespace_read` | 2 | 0.010 ms | 0.050 ms | 0.013 ms | 0.061 ms | 0.100 ms | 0.063 ms | ok=2 |
| `burst` | `namespace_resolve` | 3200 | 20.0 ms | 50.0 ms | 253.3 ms | 0.133 ms | 0.500 ms | 1.6 ms | ok=3200 |
| `burst` | `namespace_write` | 2 | 0.179 ms | 0.250 ms | 0.221 ms | 2.9 ms | 5.0 ms | 4.2 ms | ok=2 |
| `burst` | `provider_models` | 4 | 0.068 ms | 0.250 ms | 0.213 ms | 1.9 ms | 5.0 ms | 3.8 ms | ok=4 |
| `burst` | `usage_append` | 3200 | 1.0 ms | 0.050 ms | 217.0 ms | 0.857 ms | 5.0 ms | 6.4 ms | ok=3200 |
| `summaries` | `budget_charge` | 6200 | 24.4 ms | 100.0 ms | 114.8 ms | 0.867 ms | 2.5 ms | 5.5 ms | ok=6200 |
| `summaries` | `budget_write` | 2 | 0.089 ms | 0.250 ms | 0.161 ms | 2.9 ms | 5.0 ms | 3.5 ms | ok=2 |
| `summaries` | `namespace_read` | 832 | 24.7 ms | 100.0 ms | 104.7 ms | 0.060 ms | 0.250 ms | 0.578 ms | ok=832 |
| `summaries` | `namespace_resolve` | 6200 | 28.2 ms | 100.0 ms | 118.3 ms | 0.081 ms | 0.250 ms | 0.912 ms | ok=6200 |
| `summaries` | `namespace_write` | 2 | 0.130 ms | 0.250 ms | 0.245 ms | 2.1 ms | 2.5 ms | 2.1 ms | ok=2 |
| `summaries` | `provider_models` | 4 | 0.015 ms | 0.050 ms | 0.020 ms | 1.1 ms | 5.0 ms | 2.5 ms | ok=4 |
| `summaries` | `usage_append` | 6200 | 0.928 ms | 10.0 ms | 46.8 ms | 0.702 ms | 2.5 ms | 5.6 ms | ok=6200 |
| `summaries` | `usage_summary` | 830 | 23.6 ms | 100.0 ms | 86.1 ms | 0.572 ms | 1.0 ms | 1.3 ms | ok=830 |
| `slow-store` | `budget_charge` | 6200 | 116.1 ms | 250.0 ms | 671.9 ms | 1.5 ms | 5.0 ms | 7.6 ms | ok=6200 |
| `slow-store` | `budget_write` | 3 | 0.089 ms | 0.250 ms | 0.129 ms | 0.574 ms | 2.5 ms | 1.3 ms | ok=3 |
| `slow-store` | `namespace_read` | 854 | 97.3 ms | 250.0 ms | 284.6 ms | 0.086 ms | 0.250 ms | 0.383 ms | ok=854 |
| `slow-store` | `namespace_resolve` | 6200 | 132.1 ms | 1000.0 ms | 516.7 ms | 0.144 ms | 0.250 ms | 25.8 ms | ok=6200 |
| `slow-store` | `namespace_write` | 2 | 0.186 ms | 0.250 ms | 0.217 ms | 3.4 ms | 10.0 ms | 6.5 ms | error=1 ok=1 |
| `slow-store` | `provider_models` | 4 | 0.012 ms | 0.050 ms | 0.015 ms | 0.970 ms | 2.5 ms | 2.3 ms | ok=4 |
| `slow-store` | `usage_append` | 6200 | 6.3 ms | 100.0 ms | 314.2 ms | 1.1 ms | 5.0 ms | 6.8 ms | ok=6200 |
| `slow-store` | `usage_summary` | 852 | 92.1 ms | 250.0 ms | 302.8 ms | 38.7 ms | 50.0 ms | 48.6 ms | ok=852 |
| `large-payload` | `budget_charge` | 1864 | 12.7 ms | 50.0 ms | 52.7 ms | 1.0 ms | 5.0 ms | 5.3 ms | ok=1864 |
| `large-payload` | `budget_write` | 2 | 0.135 ms | 0.500 ms | 0.260 ms | 1.9 ms | 2.5 ms | 2.0 ms | ok=2 |
| `large-payload` | `namespace_read` | 2 | 0.016 ms | 0.050 ms | 0.019 ms | 0.047 ms | 0.100 ms | 0.061 ms | ok=2 |
| `large-payload` | `namespace_resolve` | 1864 | 13.7 ms | 50.0 ms | 51.5 ms | 0.167 ms | 0.500 ms | 1.8 ms | ok=1864 |
| `large-payload` | `namespace_write` | 2 | 0.058 ms | 0.250 ms | 0.110 ms | 0.657 ms | 1.0 ms | 0.698 ms | ok=2 |
| `large-payload` | `provider_models` | 4 | 0.013 ms | 0.050 ms | 0.015 ms | 0.440 ms | 1.0 ms | 0.814 ms | ok=4 |
| `large-payload` | `usage_append` | 1864 | 0.975 ms | 10.0 ms | 21.6 ms | 0.800 ms | 2.5 ms | 4.9 ms | ok=1864 |

## Background index queue and connections

| Scenario | Connections opened | Index enqueues | Index queue max depth | Index wait mean | Index wait max |
| --- | --- | --- | --- | --- | --- |
| `steady-buffered` | 0 | 9300 | 62 | 14.7 ms | 84.8 ms |
| `steady-streamed` | 0 | 1864 | 31 | 7.3 ms | 58.9 ms |
| `burst` | 0 | 3200 | 119 | 65.4 ms | 215.1 ms |
| `summaries` | 0 | 6200 | 40 | 13.8 ms | 48.0 ms |
| `slow-store` | 0 | 6200 | 60 | 71.6 ms | 367.0 ms |
| `large-payload` | 0 | 1864 | 23 | 8.3 ms | 31.0 ms |
