# AegisGate project progress

## Current milestone

M4 completes Stage 1. A fresh Release run independently verified the published M3 baseline at `7abae1d` before M4 changes. The final implementation passes combined request-path tests and verification of the live deployment.

| M4 work | Status |
| --- | --- |
| M1–M3 regression and combined request-path test | Verified on the baseline and on the final implementation |
| Prometheus endpoint | Live scrape, measured counter assertions, and promtool validation passed |
| Compose, Prometheus, and Grafana | Two gateways and all 14 panels verified with live traffic, quota rejection, and backend recovery |
| CI and sanitizers | Four C++ build variants, simulator checks, and the live deployment passed on GitHub |
| Benchmark and profile | All seven scenario categories executed. Socket trace repeated after the pool optimization |
| Final audit and documentation | Source review, overload regression, private evidence check, and reproduction instructions complete |

## Completed functionality

- Fixed worker pool with one Linux epoll reactor and one `SO_REUSEPORT` listener per worker. Accept, connect, read and write use nonblocking sockets with bounded work per loop.
- Host and path routing, fixed-length and chunked body streaming, persistent connections and ordered pipelining. llhttp validates HTTP message boundaries, and forwarding removes hop-by-hop fields.
- Per-worker backend pools with connection and idle caps, stale socket detection, and a bounded FIFO. Upstream failures are never replayed.
- Client idle, absolute header, upstream connect, and upstream exchange deadlines. Signal-driven shutdown drains admitted exchanges within a configured grace period.
- Named backend groups with selectable round-robin or Peak-EWMA routing, active request counts, latency expiry, and exploration of idle endpoints.
- Per-backend circuit breakers with a rolling outcome window, cooldown, bounded half-open trials, recovery hysteresis, and logged state transitions.
- Nonblocking active HTTP checks with consecutive failure and recovery thresholds. Routing requires both health and circuit eligibility.
- A Go upstream simulator with ordinal-based latency spikes and 503 bursts, seeded TCP resets, independent health status, and restartable data listeners controlled through a loopback HTTP API.
- Atomic Redis Lua reservations for global, tenant, and route quotas, with process-wide token leases shared by workers.
- Environment-backed API-key identity and a single local decision across applicable quotas. Rate-limit errors distinguish quota exhaustion, pending refills, and Redis failures.
- Dedicated Redis synchronization, bounded refill queues, low-watermark prefetch, lease expiry, deadline enforcement, and fail-closed, bounded fallback, and explicit fail-open policies.
- Separate Prometheus listener with worker-local metrics, Redis outcomes, fixed-bucket latency histograms, and process resource statistics. Compose provisions two gateways, two configurable origins, Redis, Prometheus, and Grafana.

## Architecture decisions

- Worker ownership keeps socket state and pooling off shared hot-path locks. Epoll registrations use monotonic tokens so a reused file descriptor cannot inherit an earlier event.
- Bounded userspace forwarding buffers and level-triggered interests propagate backpressure. A worker-local ordered deadline index keeps one timer entry per client.
- Numeric endpoints avoid resolver work in reactors. Configuration is parsed once into a shared, read-only snapshot.
- Malformed upstream exchanges return an error before downstream bytes are sent and close the client connection after streaming begins. The proxy handles plaintext HTTP/1.1 and processes one exchange per downstream connection at a time. [Protocol scope and build instructions](README.md)
- Group admission prefers immediate pool capacity. Scores combine latency to valid upstream response headers with in-flight request counts. Completed 5xx responses and transport failures penalize selection and feed the passive breaker.
- Health probes use separate TCP sockets and judge the HTTP response head. Green active health permits application recovery trials. Closing a half-open circuit requires successful application requests. This separates infrastructure reachability from application failures.
- Workers take ordered try-locks on up to three local pools and spend tokens from every applicable quota together. A separate thread owns Redis I/O, and the reactor uses bounded timer waits for missing local tokens.
- Expired and ambiguous reservations remain charged to Redis and receive no local credit. One outstanding refill per pool, fresh connections after timeouts, and generation checks on publication prevent double credit. [M3 architecture and limits](docs/rate-limiting.md)

## Test status

Verified with GCC 13.3, CMake 3.28, Ninja, Python 3.12, Redis 7.0.15, and Go 1.22.2 on Ubuntu 24.04 under WSL2. llhttp 9.4.4, hiredis 1.3.0, and GoogleTest 1.16.0 are pinned by archive checksums.

~~~bash
cmake -S . -B ~/aegisgate-build/debug -G Ninja -DCMAKE_BUILD_TYPE=Debug
cmake --build ~/aegisgate-build/debug --parallel 4
ctest --test-dir ~/aegisgate-build/debug --output-on-failure

cmake -S . -B ~/aegisgate-build/release -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build ~/aegisgate-build/release --parallel 4
ctest --test-dir ~/aegisgate-build/release --output-on-failure

cmake -S . -B ~/aegisgate-build/asan -G Ninja -DCMAKE_BUILD_TYPE=Debug -DAEGISGATE_SANITIZERS=ON
cmake --build ~/aegisgate-build/asan --parallel 4
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
  ctest --test-dir ~/aegisgate-build/asan --output-on-failure

cmake -S . -B ~/aegisgate-build/tsan -G Ninja -DCMAKE_BUILD_TYPE=Debug -DAEGISGATE_TSAN=ON
cmake --build ~/aegisgate-build/tsan --parallel 3
TSAN_OPTIONS=halt_on_error=1 setarch x86_64 -R \
  ctest --test-dir ~/aegisgate-build/tsan --output-on-failure

(cd upstream-simulator && go test -race -count=1 ./... && go vet ./...)
~~~

Debug, Release, ASan/UBSan, and TSan each passed all 31 CTest entries. This covers 27 GoogleTest cases, 36 HTTP and resilience integration cases, 14 rate-limiter integration cases, three real Redis Lua cases, and a targeted short-I/O repeat. No sanitizer findings were reported. GCC TSan initially rejected the address-space layout during startup on WSL2. The full suite ran successfully with the process-local `setarch -R` override above. The three Go cases passed under the race detector, and `go vet` passed. CI also compiled the Python sources and built the C++ targets with warnings as errors. [Verified workflow for e13f35a](https://github.com/autophile-xox/AegisGate/actions/runs/44677362848)

## Benchmark and final audit

The Release runs covered direct and proxied traffic at three offered rates, Peak-EWMA versus round-robin with a slow origin, breakers with and without isolation during HTTP 503 bursts, disabled and enabled rate limits, Redis leases versus per-request authorization, concurrent shared quota, and a one-minute run with resource sampling. The high-rate runs include generator drops. The shared fixed-budget test exhausted the single Redis ledger without observed over-admission. Repeatable raw evidence and derived claims are in the ignored `resume_metrics.local.md` and `.local/benchmark-results/` paths. [Benchmark reproduction](docs/benchmarks.md)

The final review checked epoll registration lifetimes, idle pool reuse, parser framing and limits, shutdown deadlines, worker-local health and breaker state, Redis refill generations and debits, API-key handling, and bounded metric labels. The socket trace motivated removal of a duplicate check when returning sockets to the idle pool. Idle epoll monitoring and checkout validation remain. The overload regression verifies stable queue depth and recovery to the initial process descriptor count. The trace confirmed fewer socket probes. The untraced results did not isolate a CPU or latency improvement from that change. [Architecture and failure semantics](docs/architecture.md)

## Known issues and limitations

The verified configurations have no known critical defects or failing tests. Redis coordination assumes a single trusted primary with a durable ledger. Leases can cause conservative rejection and additional burst carryover. Redis write loss during failover can affect quotas. The benchmark used one WSL2 host, so cross-host throughput, a generator-independent capacity ceiling, and long-duration resource behavior remain unverified. Stage 1 supports plaintext HTTP/1.1 without tunnels or protocol upgrades.

## Git status

The last verified runtime and benchmark commit is `e13f35aa198d6a4f5e1949600577d15b1d5ce416` on GitHub `main`. Its history includes live metrics `3224f5d`, deployment and CI `07b60f77`, and pool optimization `7d00288`. Use `git log -1 --oneline` to identify the current HEAD. [Public GitHub history](https://github.com/autophile-xox/AegisGate/commits/main/)

## Pending milestones

- Stage 2. TLS, HTTP/2, dynamic configuration, authenticated Redis transport, and cross-host and failover testing.

## Exact next steps

For a Stage 1 rerun, use the build matrix above, the Compose commands in the README, and the workloads in the benchmark guide. Stage 2 remains future work.
