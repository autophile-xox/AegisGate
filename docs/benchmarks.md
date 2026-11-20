# Stage 1 benchmark methodology

The load generator schedules requests at a fixed offered rate. It measures latency from the intended arrival time and reports generator launch lag and dropped arrivals. Only valid HTTP 200 responses enter successful-request percentiles and successful throughput. Requests that time out, fail validation, or receive an HTTP error have their own counts.

The tests use an identical response body and connection-slot count for each paired comparison. The proxy sweep and admission comparisons warm each trial and rotate execution order. The reports keep each trial and request sample. The endurance run additionally samples resident memory, CPU, connections, and queue depths over time.

## Build the Release binaries

~~~bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel 4
(cd upstream-simulator && go build -trimpath -o ../build/upstream-simulator .)
~~~

## Execute the scenarios

Run from the repository root with Python 3.10+, Redis 7, and Linux taskset. Choose available CPU IDs and fresh output directories. The harnesses launch their own loopback origins, gateway processes, and Redis where needed.

~~~bash
taskset -c 8,10 python3 benchmarks/m4_stage1.py \
  --gateway build/aegisgate --simulator build/upstream-simulator \
  --output .local/benchmark-results/m4-stage1 --rates 750,4000,16000 \
  --rate 4000 --seconds 2 --repetitions 3 --endurance-seconds 60

taskset -c 8,10 python3 benchmarks/m2_resilience.py \
  --gateway build/aegisgate --simulator build/upstream-simulator \
  --output .local/benchmark-results/m4-balancing --rate 750 --seconds 3

taskset -c 8,10 python3 benchmarks/m3_rate_limit.py \
  --gateway build/aegisgate --simulator build/upstream-simulator \
  --output .local/benchmark-results/m4-rate-limit --rate 750 --seconds 3
~~~

| Scenario | Experiment |
| --- | --- |
| Proxy overhead | Direct Go origin and the gateway forwarding to that origin at the same offered rates |
| Load balancing | Round-robin and Peak-EWMA with a controlled delay on one of two origins |
| Circuit breaking | Same offered workload with the breaker enabled and disabled during ordinal-based HTTP 503 bursts |
| Rate limiting | Disabled admission and Redis-enabled leasing below the configured quota |
| Redis efficiency | Batch-one authorization and batch-64 leases with actual Redis command counts |
| Shared quota | Two gateway processes attempting to consume one fixed Redis budget |
| Endurance | Prescribed concurrent traffic with Redis admission and live resource sampling |

## Profile socket activity

~~~bash
taskset -c 8,10 python3 benchmarks/m4_stage1.py \
  --gateway build/aegisgate --simulator build/upstream-simulator \
  --output .local/benchmark-results/m4-profile --scenario profile --rate 150 --seconds 2
~~~

The profile uses strace on Linux to record socket and epoll calls. Tracing perturbs timing, so compare syscall counts under instrumentation and use separate Release trial runs for performance. Keep raw evidence, host topology, compiler, build configuration, release commit, and the workload alongside any numeric comparison.

The supplied CPU assignments refer to logical CPUs. WSL and container hosts may schedule them differently from physical performance and efficiency cores. Single-host loopback measurements include the actual load generator and origin. Late generation or an overloaded origin limits any conclusion about gateway saturation. Treat the highest observed successful rate as workload-specific evidence.
