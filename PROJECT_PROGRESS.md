# AegisGate project progress

## Current milestone

M2 resilience is implemented and verified on top of the published M1 networking and HTTP forwarding path.

## Completed functionality

- Fixed worker pool with one Linux epoll reactor and one `SO_REUSEPORT` listener per worker. Accept, connect, read and write use nonblocking sockets with bounded work per loop.
- Host and path routing, fixed-length and chunked body streaming, persistent connections and ordered pipelining. llhttp validates HTTP message boundaries, and forwarding removes hop-by-hop fields.
- Per-worker backend pools with connection and idle caps, stale socket detection, and a bounded FIFO. Upstream failures are never replayed.
- Client idle, absolute header, upstream connect, and upstream exchange deadlines. Signal-driven shutdown drains admitted exchanges within a configured grace period.
- Named backend groups with selectable round-robin or Peak-EWMA routing, active request counts, latency expiry, and exploration of idle endpoints.
- Per-backend circuit breakers with a rolling outcome window, cooldown, bounded half-open trials, recovery hysteresis, and logged state transitions.
- Nonblocking active HTTP checks with consecutive failure and recovery thresholds. Routing requires both health and circuit eligibility.
- A Go upstream simulator with ordinal-based latency spikes and 503 bursts, seeded TCP resets, independent health status, and restartable data listeners controlled through a loopback HTTP API.

## Architecture decisions

- Worker ownership keeps socket state and pooling off shared hot-path locks. Epoll registrations use monotonic tokens so a reused file descriptor cannot inherit an earlier event.
- Bounded userspace forwarding buffers and level-triggered interests propagate backpressure. A worker-local ordered deadline index keeps one timer entry per client.
- Numeric endpoints avoid resolver work in reactors. Configuration is parsed once into a shared, read-only snapshot.
- Malformed upstream exchanges return an error before downstream bytes are sent and close the client connection after streaming begins. The proxy handles plaintext HTTP/1.1 and processes one exchange per downstream connection at a time. [Protocol scope and build instructions](README.md)
- Group admission prefers immediate pool capacity. Scores combine latency to valid upstream response headers with in-flight request counts. Completed 5xx responses and transport failures penalize selection and feed the passive breaker.
- Health probes use separate TCP sockets and judge the HTTP response head. Green active health permits application recovery trials. Closing a half-open circuit requires successful application requests. This separates infrastructure reachability from application failures.

## Test status

Verified with GCC 13.3, CMake 3.28, Ninja, Python 3.12, and Go 1.22.2 on Ubuntu 24.04 under WSL2. llhttp 9.4.4 and GoogleTest 1.16.0 are pinned by archive checksums.

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

(cd upstream-simulator && go test -race -count=3 ./... && go vet ./...)
~~~

All three builds passed 25 GoogleTest cases, 35 Python TCP integration cases, and a targeted short-I/O repeat. ASan, LeakSanitizer and UBSan produced no findings. The suite covers M1 networking and M2 routing, fast 503s, breaker thresholds and stale completions, transport errors, active health isolation, and recovery. Three Go test cases passed with the race detector across three repetitions, and `go vet` passed.

M2's fixed-offered-load comparison completed three slowdown trials per policy with matching workloads. A separate run measured 503 detection, isolation after listeners stopped, and recovery after restart. Private measurements and prior M1 results are in the ignored `resume_metrics.local.md` and `.local/benchmark-results/` paths. [M2 benchmark reproduction](README.md#reproduce-the-m2-comparison)

## Known issues and limitations

No known critical defects or failing tests. Circuit, latency, and health state are worker-local. Transition logs are available on stderr. TSan, exported metrics, and CI are scheduled for M4. The M2 benchmark ran on WSL2 loopback with a single gateway worker, two local origins, and one offered rate.

## Git status

M2 history on `main` includes routing `6cf3439`, circuit breakers `4175780`, active health `4dadce8`, the Go simulator `2d40b64`, and the benchmark harness `43507ad`. Use `git log -1 --oneline` to identify the current HEAD. [Public GitHub history](https://github.com/autophile-xox/AegisGate/commits/main/)

## Pending milestones

- M3. Redis quotas and local token leases.
- M4. Monitoring, deployment, full benchmarking and repository finalization.

## Exact next steps

Begin M3 in a separate session. Re-run the Release and Go suites. Define trusted tenant mapping and quota keys, then implement the atomic Redis Lua reservation script. Add a bounded asynchronous refill path and local token leases. Verify shared quotas across gateway processes and test the configured Redis outage behavior.
