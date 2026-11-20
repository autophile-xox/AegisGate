# Stage 1 architecture

AegisGate is a Linux HTTP/1.1 reverse proxy for services that need host and path routing, isolation from unhealthy origins, and a shared admission budget across gateway processes.

~~~mermaid
stateDiagram-v2
    [*] --> ReadingHeaders
    ReadingHeaders --> Admission: valid route and headers
    Admission --> WaitingForLease: quota pending
    WaitingForLease --> Admission: reactor timer
    Admission --> UpstreamQueue: eligible backend has no free socket
    UpstreamQueue --> Connecting: pool dispatch
    Admission --> Connecting: pool capacity
    Connecting --> Forwarding: connection ready or reusable socket
    Forwarding --> SendingResponse: response available
    SendingResponse --> ReadingHeaders: complete keep-alive exchange
    ReadingHeaders --> Closing: parse error or deadline
    Admission --> Closing: rejection
    WaitingForLease --> Closing: admission deadline
    Forwarding --> Closing: transport or framing failure
    SendingResponse --> Closing: close or shutdown
    Closing --> [*]
~~~

## Thread and connection ownership

Each worker owns its TCP listener, epoll descriptor, client state, upstream sockets, timers, and backend pools. SO_REUSEPORT spreads accepts across listeners. Epoll uses increasing tokens so events for an old socket cannot be applied to a newly reused file descriptor. Forwarding buffers are bounded, and epoll interest changes propagate backpressure when a downstream client or upstream origin stalls.

HTTP parsing is incremental. A worker forwards one exchange per client connection and processes pipelined requests in order. After a full response has been handed to the downstream socket, the worker either keeps the connection alive or sends a FIN and performs a bounded drain. Read, write, header, connect, exchange, and drain deadlines have distinct roles. A partial response that fails cannot be replaced with a second HTTP response.

Backend sockets return to the idle pool only after the request and response are complete and the response permits reuse. Idle sockets remain monitored for EOF or unexpected bytes. Checkout validates the socket before assigning another request. An upstream failure ends the current exchange without automatic replay.

## Routing and resilience

The immutable configuration maps exact or wildcard host and longest segment prefix to a backend group. Round-robin and Peak-EWMA both prefer eligible endpoints with immediate pool capacity. Peak-EWMA evaluates two candidates using a latency estimate weighted by active requests. The estimator immediately absorbs a higher latency sample, decays older estimates, and explores idle backends with missing or stale samples.

Worker-local circuits progress from closed to open after enough failures in their rolling windows. An open circuit cools down before allowing bounded half-open trials. Closing requires enough successful application trials. Active HTTP health probes use separate connections and independent thresholds. Healthy probe results restore health eligibility while application trials determine breaker recovery. Backend 5xxs and transport failures do not become fast success samples.

## Redis and isolation

Admission precedes pool assignment. Trusted API keys come from environment variables and are removed from forwarded headers. Route, tenant, and global scopes can apply to one request. Workers acquire all applicable pool locks with try-locks and consume tokens together. A bounded work queue and a dedicated thread keep Redis I/O out of epoll workers. [Token lifecycle and failure policies](rate-limiting.md)

The public gateway and process metrics use separate listeners. A collector reads worker-owned counters and histograms as atomic snapshots and obtains process statistics outside the hot path. Routes, backends, worker indices, and protocol outcome enums are bounded labels. Runtime request paths, request IDs, and API keys never become labels. Counters are process-local and snapshots can reflect adjacent points in time under concurrent traffic.

## Verification and security boundaries

The test suite covers the parser, real socket forwarding, pooling and lifecycle, fault injection, Redis Lua atomicity, multiple replicas, and a combined authenticated request path. Compose verification sends authenticated traffic to both gateways and exercises quota exhaustion, origin isolation, circuit recovery, live scrape, and Grafana queries. [Benchmark methodology and reproduction](benchmarks.md)

Stage 1 proxies plaintext HTTP/1.1 and assumes operator-controlled numeric origins and a trusted Redis primary. The demo binds public host ports to loopback and keeps Redis and the metrics endpoints on its internal network. The simulator controller is loopback-only in each origin. TLS termination, dynamic routing configuration, Redis authentication or TLS, and Redis failover verification belong to future work.
