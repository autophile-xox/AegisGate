# AegisGate project progress

## Current milestone

M1 networking and HTTP forwarding are implemented and verified. Work is scoped to M1.

## Completed functionality

- Fixed worker pool with one Linux epoll reactor and one `SO_REUSEPORT` listener per worker. Accept, connect, read and write use nonblocking sockets with bounded work per loop.
- Host and path routing, fixed-length and chunked body streaming, persistent connections and ordered pipelining. llhttp validates HTTP message boundaries, and forwarding removes hop-by-hop fields.
- Per-worker backend pools with connection and idle caps, stale socket detection, and a bounded FIFO. Upstream failures are never replayed.
- Client idle, absolute header, upstream connect, and upstream exchange deadlines. Signal-driven shutdown drains admitted exchanges within a configured grace period.

## Architecture decisions

- Worker ownership keeps socket state and pooling off shared hot-path locks. Epoll registrations use monotonic tokens so a reused file descriptor cannot inherit an earlier event.
- Bounded userspace forwarding buffers and level-triggered interests propagate backpressure. A worker-local ordered deadline index keeps one timer entry per client.
- Numeric endpoints avoid resolver work in reactors. Configuration is parsed once into a shared, read-only snapshot.
- Malformed upstream exchanges return an error before downstream bytes are sent and close the client connection after streaming begins. The proxy handles plaintext HTTP/1.1 and processes one exchange per downstream connection at a time. [Protocol scope and build instructions](README.md)

## Test status

Verified with GCC 13.3, CMake 3.28, Ninja, and Python 3.12 on Ubuntu 24.04 under WSL2. llhttp 9.4.4 and GoogleTest 1.16.0 are pinned by archive checksums.

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
~~~

All three builds passed 13 GoogleTest cases, 25 Python TCP integration cases, and a targeted repeat with short reads, short writes, `EINTR`, and `EAGAIN` forced by test-only syscall wrappers. ASan, LeakSanitizer and UBSan produced no findings. The socket suite includes client resets, a full backend listen queue, and a 24 MiB transfer through a 4 KiB forwarding buffer.

The paired Release loopback benchmark completed three measured trials per path and a separate 30-second gateway run. Private notes and raw results are in the ignored `resume_metrics.local.md` and `.local/benchmark-results/` paths. [Benchmark reproduction](README.md#verification-and-benchmarks)

## Known issues and limitations

No known critical defects or failing M1 tests. TSan and CI are scheduled for M4. The local connect deadline test uses Linux listen backlog behavior. Runtime support requires Linux epoll, including when developing on Windows.

## Git status

Local branch `main` contains the verified gateway commit `a71c815`, the socket integration commit `34ddd32`, and the short-I/O test commit `1f6152c`. Use `git log -1 --oneline` to identify the current HEAD. No GitHub remote is configured, and no commits have been pushed.

## Pending milestones

- M2. Peak-EWMA, circuit breakers, health checks and fault simulator.
- M3. Redis quotas and local token leases.
- M4. Monitoring, deployment, full benchmarking and repository finalization.

## Exact next steps

Begin M2 in a separate session. Re-run the M1 Release suite. Extend route configuration to name backend groups while preserving single-backend routes. Add round-robin and worker-local Peak-EWMA selection using upstream exchange outcomes. Implement circuit state transitions and active/passive health checks, then add the Go fault simulator for deterministic slowdown and failure tests. Compare round-robin with Peak-EWMA under the same verified slowdown workload.
