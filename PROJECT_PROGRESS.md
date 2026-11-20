# AegisGate project progress

## Current milestone

M4 is in progress on the published M3 baseline. A fresh Release run verified the M1–M3 suites. The integrated HTTP, quota, resilience, and metrics test passes.

| M4 work | Status |
| --- | --- |
| Baseline regression and combined request-path test | Release passed |
| Prometheus endpoint | Implemented and verified at the HTTP and counter level |
| Compose, Prometheus scrape, and Grafana | In deployment verification |
| CI, sanitizers, benchmarks, final audit and documentation | Pending |

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

Debug, Release, ASan/UBSan, and TSan each passed all 31 CTest entries. This covers 27 GoogleTest cases, 35 M1/M2 TCP cases, 13 rate-limiter integration cases, three real Redis Lua cases, and a targeted short-I/O repeat. No sanitizer findings were reported. The default GCC TSan process initially failed during runtime startup with an unexpected memory mapping. The full suite ran successfully with the process-local `setarch -R` override above. The three Go cases passed under the race detector, and `go vet` passed.

The M3 benchmark completed three fixed-rate trials each for disabled admission, per-request Redis authorization, and leased tokens. It also verified an exact shared fixed quota across two processes and measured behavior during a Redis pause and outage. Private M1–M3 results are in the ignored `resume_metrics.local.md` and `.local/benchmark-results/` paths. [M3 benchmark reproduction](docs/rate-limiting.md#verification-and-measurement)

## Known issues and limitations

No known critical defects or failing tests under the verified configurations. Redis coordination assumes one healthy primary with a durable ledger. Leases can cause conservative rejection and additional burst carryover, and Redis write loss during failover can affect quotas. The benchmark used a single WSL2 host and one offered rate. Exported metrics, CI, and deployment packaging remain M4 work.

## Git status

M3 history on `main` includes quota configuration and Lua `888699a`, the gateway limiter and integration tests `11bd24c`, and the benchmark harness `60554f6`. Use `git log -1 --oneline` to identify the current HEAD. [Public GitHub history](https://github.com/autophile-xox/AegisGate/commits/main/)

## Pending milestones

- M4. Monitoring, deployment, full benchmarking and repository finalization.

## Exact next steps

Begin M4 in a separate session. Re-run the Release, ASan/UBSan, TSan, and Go suites. Add Prometheus export for the HTTP, resilience, and rate-limiter paths. Wire Grafana, Compose, and CI. Extend the benchmark workload across offered loads and fault scenarios while retaining raw evidence privately.
