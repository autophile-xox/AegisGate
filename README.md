# AegisGate

AegisGate is a Linux C++20 reverse proxy for routing HTTP/1.1 requests by host and path. It reuses backend connections and streams message bodies through bounded buffers. Milestone 1 delivers the networking and HTTP forwarding path.

~~~mermaid
flowchart LR
    C[HTTP/1.1 clients] --> L[Worker listeners with SO_REUSEPORT]
    L --> E[One epoll reactor per worker]
    E --> P[llhttp parsing and route selection]
    P --> Q[Worker-local backend pools]
    Q --> A[Backend A]
    Q --> B[Backend B]
~~~

## Build and run

Use Linux or WSL2 with a C++20 compiler, CMake 3.25 or newer, and Python 3.10 or newer for integration tests. CMake downloads checksum-pinned llhttp 9.4.4 and GoogleTest 1.16.0. [llhttp documentation](https://github.com/nodejs/llhttp#readme), [GoogleTest release](https://github.com/google/googletest/releases/tag/v1.16.0)

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

The three backend limits are the maximum total connections, idle connections, and queued requests **per worker**. Each client socket belongs to one worker for its lifetime. Exact host routes take precedence over wildcard routes. Within a host, the longest matching path prefix wins on a segment boundary. Matching ignores a host port and a path query string, and uses the encoded path as received. A matching request reaches one configured backend with its original target and Host field.

[Example with all size and timeout settings](config/gateway.conf)

## Request handling

- A worker validates request headers before routing. It forwards fixed-length and chunked bodies incrementally. Chunk extensions and trailers are consumed locally. Forwarded chunk sizes can differ while body bytes are preserved.
- Each client connection has one active exchange. Pipelined requests are processed in wire order after the current response drains from the gateway buffer.
- The gateway removes hop-by-hop headers and reserializes framing. Backend sockets return to the pool only after a complete response and a fully forwarded request. Idle sockets are monitored and checked before reuse. Upstream failures end the exchange without replay.
- A header deadline starts when parsing the first request byte. Client idle timers cover read and write inactivity when the gateway can make progress. The upstream response deadline starts at route admission and covers queueing, connection establishment, upload and response completion. A separate connect deadline bounds the TCP handshake.
- A full backend queue returns `503`. A refused connection or invalid upstream response returns `502` before downstream bytes are sent. Upstream deadlines return `504` before the response starts, and a broken or timed-out streamed response closes the downstream socket after any bytes already sent.
- `SIGINT` and `SIGTERM` stop listeners and drain admitted exchanges until the shutdown deadline. A client can half-close its write side and still read completed responses.

## Protocol scope

Clients use plaintext HTTP/1.1 with origin-form targets. HTTP/1.0 backend responses are supported, with each connection retired after the exchange. Close-delimited responses are re-encoded as chunked downstream. TLS, protocol upgrades, CONNECT tunnels, informational responses, and Expect negotiation are outside M1. [HTTP/1.1 message framing](https://www.rfc-editor.org/rfc/rfc9112.html)

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

Peak-EWMA, circuit breakers, and health checks are planned for M2. Redis rate limits follow in M3, and observability, deployment, and open-loop benchmarking follow in M4. [Project progress](PROJECT_PROGRESS.md)
