# Distributed rate limiting

An epoll worker validates a request and matches its route before admission. If Redis rate limiting is configured, the worker resolves the request's applicable layers, spends one token from each, then selects an upstream. A route can target either one backend or an M2 group.

## Configuration and identity

| Directive | Meaning |
| --- | --- |
| `redis IP PORT` | Numeric address of a Redis 7 primary |
| `rate_namespace NAME` | Prefix shared by all gateway processes enforcing a quota |
| `quota global CAPACITY RATE BATCH` | Quota shared by all matched routes |
| `quota tenant TENANT CAPACITY RATE BATCH` | Quota for a mapped tenant |
| `quota route HOST PREFIX CAPACITY RATE BATCH` | Quota for the matching configured route |
| `tenant_key TENANT ENV_VARIABLE` | Read a tenant's API key from the process environment at startup |
| `rate_timeout_ms MS` | Deadline from queue submission through the Redis reply |
| `rate_wait_ms MS` | Maximum time a request waits on reactor timers for local tokens |
| `rate_lease_ms MS` | Monotonic lease lifetime measured from refill submission |
| `rate_queue LENGTH` | Maximum queued refills per process, plus one executing operation |
| `rate_failure closed\|fallback\|open` | Policy for confirmed infrastructure failure |
| `rate_fallback CAPACITY RATE` | Local emergency bucket per quota per gateway when fallback is enabled |
| `rate_diagnostics on` | Emit aggregate timing and token counters at shutdown |

Capacities and batches are whole tokens, and the rate is tokens per second. A zero rate creates a fixed budget. Redis rate limiting is opt-in. With tenant quotas configured, each matched request needs one valid `X-API-Key`. Keys are compared against trusted configuration and stripped before forwarding. Duplicate, missing, or unknown keys receive `401`. Client-supplied tenant IDs have no effect on tenant selection.

Replica configurations must agree on the namespace, scope identifiers, capacities, and rates. Batch size is local and can differ. The gateway derives Redis keys for the global scope, tenant name, and configured host/path pair. Redis errors on a capacity or rate mismatch without modifying the stored bucket. Changes to the gateway configuration take effect on process restart. [Example configuration](../config/rate-limited.conf)

## Reservation and local spend

The Lua script uses the Redis server clock, refills an integer millitoken balance up to capacity, and deducts the granted whole tokens in a single atomic operation. It persists each bucket as a Redis hash. Every returned grant has already been deducted before a worker can spend it. [Redis Lua execution](https://redis.io/docs/latest/interact/programmability/eval-intro/) · [Reservation script](../redis/reserve.lua)

Workers share pools inside one process. Admission uses ordered try-locks on at most three pools. It spends all applicable tokens together after checking lease deadlines. When an applicable token is missing, the worker can retry through its existing timers, while unrelated sockets remain serviceable. Admission waiters count against the existing client limits.

A low-water mark of one fifth of the batch triggers a refill when traffic consumes tokens. Each pool holds up to one batch's worth of tokens across two independently expiring grants. The worker can queue at most one refill for a pool. A dedicated thread owns the Redis socket and uses hiredis for RESP encoding and parsing. Bounded queue submission uses an eventfd, and network polling enforces an absolute deadline. Batch size one skips proactive refill and gives a per-request Redis authorization baseline.

When several quotas apply, Redis reserves each separately. A rejected request may trigger reservations that remain unused. The local decision spends either every applicable token or none. Tokens are also consumed at admission if a later backend fails. The gateway discards expired grants and leaves unused grants debited in Redis. An ambiguous write can debit Redis without adding local tokens. It is never replayed as the same reservation. Late replies on a timed-out socket are discarded, and a generation check prevents publishing a refill to a retired pool.

## Failures and recovery

| Condition | Result |
| --- | --- |
| A required quota has no local tokens and Redis reports exhaustion | `429 quota_exhausted` |
| Tokens are unavailable while a refill is queued or pending past the request wait | `503 refill_pending` |
| Redis timeout, disconnect, or invalid reply with no usable lease | Fail-closed returns `503` with the corresponding reason |
| Confirmed failure with bounded fallback selected | Spend emergency tokens in the missing scopes, or return `503 fallback_exhausted` |
| Confirmed failure with explicit fail-open selected | Admit the missing scopes under the configured policy |

The reason appears in `X-AegisGate-Rate-Limit` on gateway rejections. Previously authorized leases remain spendable until expiry during a Redis failure. Fallback and fail-open cannot override a known exhausted scope. A pool with a Redis failure retries on demand after a short backoff and returns to normal admission when it receives a valid reply. Static exhausted quotas are rechecked periodically on demand so Redis or key changes can be observed.

## Consistency limits

The configured capacity and rate govern grants at one healthy Redis primary. Local admission can lag those grants, so briefly buffered leases may add to a later burst. Per-replica batch caps and expiry bound the carryover. Uneven traffic can leave grants stranded in idle replicas until they expire. A larger batch reduces Redis operations but can increase hoarding, discarded tokens, and cross-replica unfairness.

Keeping quota keys private to the gateway, with durable storage and a suitable no-eviction policy, preserves the central ledger. Redis data loss, key deletion or eviction, or a failover that loses writes can permit additional grants. Choose capacity and lease settings for that failure model. The M3 client uses plaintext TCP to a trusted Redis endpoint. Redis AUTH, TLS, cluster routing, and hot configuration updates are outside this implementation.

## Verification and measurement

When both Redis binaries are installed, CTest adds tests for the Lua script and real gateway admission. The tests start isolated Redis processes and use temporary fault fixtures for timeout, malformed responses, and shutdown. Run the suite from the project root.

~~~bash
ctest --test-dir build --output-on-failure
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DAEGISGATE_TSAN=ON
cmake --build build-tsan --parallel
ctest --test-dir build-tsan --output-on-failure
~~~

On the WSL2 verification host, GCC's TSan required a process-local ASLR override. The full suite passed with `setarch "$(uname -m)" -R ctest --test-dir build-tsan --output-on-failure`.

The fixed-rate benchmark compares disabled admission, batch-one Redis authorization, and leased tokens at the same offered rate. It records only HTTP 200s as successes and counts Redis `EVAL` commands, local admission check time, reservation turnaround, lease use, CPU and RSS. It also checks a shared fixed budget across two processes, pauses Redis, and compares fail-closed and fallback while Redis is down.

~~~bash
taskset -c 8,10 python3 benchmarks/m3_rate_limit.py \
  --gateway build/aegisgate --simulator build/upstream-simulator \
  --output .local/benchmark-results/m3-rate-limit \
  --gateway-cpus 4,6 --origin-cpu 2 --redis-cpu 0
~~~

Choose an unused output directory and CPU IDs available on your Linux host. The benchmark launches its own loopback Redis and simulator. The optional shutdown counters include warmup checks and refill operations, while throughput and Redis command deltas come from the measured interval.
