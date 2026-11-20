# AegisGate

AegisGate is a Linux C++20 reverse proxy for routing HTTP/1.1 requests by host and path. It streams HTTP bodies through bounded buffers, steers traffic around slower or unhealthy backends, and shares rate limits across replicas through Redis.

~~~mermaid
flowchart LR
    C[HTTP/1.1 clients] --> L[Worker listeners with SO_REUSEPORT]
    L --> E[One epoll reactor per worker]
    E --> P[llhttp parsing and route selection]
    P --> R[Local quota admission]
    D[Redis Lua reservations] --> R
    R --> B[Eligible backend group]
    H[Active HTTP probes] --> B
    B --> Q[Worker-local backend pools]
    Q --> A[Backend A]
    Q --> O[Backend B]
    Q --> S[Latency and circuit state]
    S --> B
~~~

## Build and run

Use Linux or WSL2 with a C++20 compiler, CMake 3.25 or newer, and Python 3.10 or newer for integration tests. CMake downloads checksum-pinned llhttp 9.4.4, hiredis 1.3.0, and GoogleTest 1.16.0. [llhttp](https://github.com/nodejs/llhttp#readme) · [hiredis](https://github.com/redis/hiredis/tree/v1.3.0) · [GoogleTest](https://github.com/google/googletest/releases/tag/v1.16.0)

~~~bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure
~~~

Start a demo backend and the gateway in separate Linux terminals from the repository root.

~~~bash
python3 -m http.server --bind 127.0.0.1 9000
~~~

~~~bash
./build/aegisgate config/gateway.conf
~~~

~~~bash
curl -i http://127.0.0.1:8080/README.md
~~~

The demo Python server defaults to HTTP/1.0. The integration suite uses persistent HTTP/1.1 origins to verify pooling.

## Configuration

The startup file uses whitespace-delimited directives and comments beginning with `#`. Listening and backend endpoints take numeric IPv4 or IPv6 addresses. Here is a two-backend routing example.

~~~text
listen 127.0.0.1 8080
workers 2
backend api 127.0.0.1 9000 64 32 128
backend admin 127.0.0.1 9001 16 8 32
route admin.example.test /api admin
route * /api api
~~~

The three backend pool limits are the maximum total pooled connections, idle pooled connections, and queued requests **per worker**. Each client socket belongs to one worker for its lifetime. Exact host routes take precedence over wildcard routes. Within a host, the longest matching path prefix wins on a segment boundary. Matching ignores a host port and a path query string, and uses the encoded path as received. A route can name a single backend or a group. The gateway forwards the original target and Host field.

[Single-backend configuration](config/gateway.conf) · [Resilient backend group configuration](config/resilient.conf)

## Request handling

- A worker validates request headers before routing. It forwards fixed-length and chunked bodies incrementally. Chunk extensions and trailers are consumed locally. Forwarded chunk sizes can differ while body bytes are preserved.
- Each client connection has one active exchange. Pipelined requests are processed in wire order after the current response drains from the gateway buffer.
- The gateway removes hop-by-hop headers and reserializes framing. Backend sockets return to the pool only after a complete response and a fully forwarded request. Idle sockets are monitored and checked before reuse. Upstream failures end the exchange without replay.
- A header deadline starts when parsing the first request byte. Client idle timers cover read and write inactivity when the gateway can make progress. The upstream response deadline starts at route admission and covers queueing, connection establishment, upload and response completion. A separate connect deadline bounds the TCP handshake.
- A full backend queue returns `503`. A refused connection or invalid upstream response returns `502` before downstream bytes are sent. Upstream deadlines return `504` before the response starts, and a broken or timed-out streamed response closes the downstream socket after any bytes already sent.
- `SIGINT` and `SIGTERM` stop listeners and drain admitted exchanges until the shutdown deadline. A client can half-close its write side and still read completed responses.

## Backend selection and resilience

Declare a group with `group NAME round_robin BACKEND...` or `group NAME peak_ewma BACKEND...`, then use the group name as a route target. Both policies prefer a backend with immediate pool capacity, then use a bounded queue when available. A route with no eligible backend returns `503`.

**Peak-EWMA.** The worker samples two distinct eligible backends and picks the lower score, estimated latency multiplied by one plus the in-flight request count. The estimate rises immediately on a slower successful sample and falls with exponential smoothing. Samples measure time from backend assignment to validated response headers, including connect and upload, and enter the estimate after the response finishes. The score ages toward a configurable initial latency. Every sixteenth two-candidate choice can explore an idle endpoint with a missing or stale estimate. Failed attempts receive a latency penalty. [Finagle client load-balancing guide](https://twitter.github.io/finagle/guide/Clients.html)

**Circuit breaker.** Each worker keeps a rolling outcome window per backend. A closed circuit opens when its sample count and failure percentage meet configured thresholds. After the cooldown, a limited number of application requests can enter half-open probation. A failed trial reopens the circuit, and enough successful trials close it. Upstream 5xx responses, transport and protocol errors, and upstream deadlines count as failures. Client cancellations, queue expiry, and client-side stalls do not count as backend failures. A request already queued for an endpoint that loses eligibility receives `503` without being sent upstream.

**Health checks.** A worker can periodically issue a nonblocking HTTP GET to each backend on a separate socket. A valid 2xx response head counts as success. Consecutive-result thresholds govern isolation and recovery. Backends enter service optimistically at startup, and requests require both health and circuit eligibility. A successful active check restores an endpoint's health eligibility. The application circuit still requires successful application trials. Transition logs include the worker, backend, old and new state, reason, and monotonic timestamp.

The single-backend configuration keeps both the breaker and active checks disabled by default. The resilient example enables them. Each worker learns independently and can issue one probe per backend at a time.

## Protocol scope

Clients use plaintext HTTP/1.1 with origin-form targets. HTTP/1.0 backend responses are supported, with each connection retired after the exchange. Close-delimited responses are re-encoded as chunked downstream. TLS, protocol upgrades, CONNECT tunnels, informational responses, and Expect negotiation are outside M1. [HTTP/1.1 message framing](https://www.rfc-editor.org/rfc/rfc9112.html)

## Distributed rate limiting

Redis can authorize global, per-tenant, and per-route quotas. Each process leases batches from Redis via Lua and shares its local ledger across workers. A dedicated thread performs all Redis operations. Workers check every applicable scope before spending tokens, and use short reactor timer retries when refills are pending. Trusted tenant API keys come from environment variables.

Exhausted quotas return `429`. Redis failures default to fail-closed with `503` after cached authorizations are spent or expire. The rate-limiter configuration also supports bounded fallback and explicit fail-open. [Configuration, failure policy, and consistency details](docs/rate-limiting.md)

For a demo, start the origin on port 9000 and a local Redis process in separate terminals, then launch the gateway with a generated demo API key.

~~~bash
redis-server --bind 127.0.0.1 --port 6381 --save "" --appendonly no
~~~

~~~bash
export AEGISGATE_DEMO_TENANT_KEY="$(openssl rand -hex 24)"
./build/aegisgate config/rate-limited.conf &
curl --retry 3 --retry-connrefused -i -H "X-API-Key: $AEGISGATE_DEMO_TENANT_KEY" http://127.0.0.1:8080/api
~~~

[Rate-limited example configuration](config/rate-limited.conf)

## Verification and benchmarks

CTest runs GoogleTest cases, a real-socket Python integration suite, and a separate binary-echo test that forces short reads, short writes, `EINTR`, and `EAGAIN`. The sanitizer build runs the same tests.

~~~bash
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DAEGISGATE_SANITIZERS=ON
cmake --build build-asan --parallel
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1 \
  ctest --test-dir build-asan --output-on-failure
~~~

The M1 benchmark uses `wrk` on Linux loopback with a fixed-response Python origin and persistent connections. Adjust the `taskset` CPU IDs for your host. The command writes raw output and a JSON report under the ignored benchmark directory.

~~~bash
python3 benchmarks/m1_smoke.py --gateway build/aegisgate \
  --output .local/benchmark-results/m1-loopback \
  --origin-cpu 2 --gateway-cpus 4,6 --load-cpus 8,10 --soak 30
~~~

## Go upstream simulator

Build with Go 1.22 or later. Run the two origins and the gateway in separate terminals.

~~~bash
(cd upstream-simulator && go build -trimpath -o ../build/upstream-simulator .)
(cd upstream-simulator && go test -race ./... && go vet ./...)

./build/upstream-simulator -name fast -listen 127.0.0.1:9000 -control 127.0.0.1:9100 -delay-ms 1
./build/upstream-simulator -name slow -listen 127.0.0.1:9001 -control 127.0.0.1:9101 -delay-ms 1
./build/aegisgate config/resilient.conf
curl -i http://127.0.0.1:8080/data
~~~

The controller accepts a full JSON plan on a separate loopback listener. Its POST resets the traffic sequence and keeps cumulative counters. Delays and bursts are selected by request ordinal. TCP resets follow a reproducible seed and ordinal. Concurrent arrival order can change which client request receives a specific ordinal.

| Plan fields | Effect |
| --- | --- |
| `delay_ms` | Base application response delay |
| `spike_ms`, `spike_every`, `spike_length` | Add delay to the first requests in each configured period |
| `burst_503_every`, `burst_503_length` | Return 503 during the first requests in each configured period |
| `seed`, `drop_permille` | Select abrupt TCP resets reproducibly |
| `health_status` | Set the independent health endpoint to 204 or 503 |
| `unavailable` | Stop the data listener and close its connections until a new plan reopens it |

~~~bash
curl -fsS -H 'Content-Type: application/json' \
  -d '{"seed":42,"delay_ms":1,"spike_ms":80,"spike_every":20,"spike_length":3}' \
  http://127.0.0.1:9101/faults
curl -fsS http://127.0.0.1:9101/stats
curl -fsS -H 'Content-Type: application/json' -d '{"delay_ms":1}' http://127.0.0.1:9101/faults
~~~

### Reproduce the M2 comparison

The benchmark alternates three fixed-rate trials per policy, with the same healthy warmup and the same delayed origin. It also runs an application 503 phase, stops both listeners, and measures recovery after they reopen. Latency percentiles use successful responses, and HTTP and transport failures are counted separately. Adjust the Linux CPU IDs to match your host and choose a fresh output directory for each run.

~~~bash
taskset -c 8,10 python3 benchmarks/m2_resilience.py \
  --gateway build/aegisgate --simulator build/upstream-simulator \
  --output .local/benchmark-results/m2-resilience \
  --fast-cpu 2 --gateway-cpu 4 --slow-cpu 6
~~~

Redis and the simulator are included in the reproducible [M3 benchmark](docs/rate-limiting.md#verification-and-measurement). Exported metrics, deployment, CI, and expanded benchmarking follow in M4. [Project progress](PROJECT_PROGRESS.md)
