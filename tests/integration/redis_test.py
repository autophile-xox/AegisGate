"""Tests against an isolated Redis process, using empty temporary storage."""

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import secrets
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import unittest

import gateway_test as gateway


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "redis/reserve.lua"
HOST = "127.0.0.1"


def free_port():
    with socket.socket() as listener:
        listener.bind((HOST, 0))
        return listener.getsockname()[1]


class RedisServer:
    def __init__(self, port=None):
        self.port = port or free_port()
        self.directory = tempfile.TemporaryDirectory(prefix="aegisgate-redis-")
        self.process = None

    def start(self):
        with (Path(self.directory.name) / "redis.log").open("ab") as log:
            self.process = subprocess.Popen(
                ["redis-server", "--bind", HOST, "--port", str(self.port),
                 "--dir", self.directory.name, "--save", "", "--appendonly", "no"],
                stdout=log, stderr=log,
            )
        gateway.eventually(lambda: self.cli("PING", check=False) == "PONG")
        return self

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            self.process.wait(3)

    def close(self):
        self.stop()
        self.directory.cleanup()

    def cli(self, *args, check=True):
        proc = subprocess.run(["redis-cli", "-h", HOST, "-p", str(self.port), "--raw",
                               *map(str, args)], capture_output=True, text=True, timeout=3)
        if check:
            proc.check_returncode()
        return proc.stdout.strip()

    def reserve(self, key, capacity, rate, wanted):
        return tuple(map(int, self.cli("--eval", SCRIPT, key, ",",
                                        capacity, rate, wanted).splitlines()))


class LuaTests(unittest.TestCase):
    def setUp(self):
        self.redis = RedisServer()
        self.addCleanup(self.redis.close)
        self.redis.start()

    def test_concurrent_reservations_cannot_overspend_capacity(self):
        with ThreadPoolExecutor(max_workers=20) as callers:
            results = list(callers.map(lambda _: self.redis.reserve("fixture", 103, 0, 7), range(40)))
        self.assertEqual(sum(grant for grant, _ in results), 103)
        self.assertIn((0, -1), results)
        self.assertEqual(self.redis.reserve("fixture", 103, 0, 7), (0, -1))

    def test_refill_clamps_capacity_and_time_regression(self):
        key = "fixture"
        self.assertEqual(self.redis.reserve(key, 4, 10, 4)[0], 4)
        base = int(self.redis.cli("HGET", key, "time"))
        self.redis.cli("HSET", key, "time", base + 30000, "tokens", 0)
        self.assertEqual(self.redis.reserve(key, 4, 10, 4), (0, 100))
        self.redis.cli("HSET", key, "time", base - 30000)
        self.assertEqual(self.redis.reserve(key, 4, 10, 4)[0], 4)

    def test_invalid_or_inconsistent_configuration_mints_no_tokens(self):
        self.assertEqual(self.redis.reserve("fixture", 20, 0, 4)[0], 4)
        self.assertIn("configuration mismatch",
                      self.redis.cli("--eval", SCRIPT, "fixture", ",", 21, 0, 4))
        self.assertIn("invalid reservation arguments",
                      self.redis.cli("--eval", SCRIPT, "fixture", ",", 20, 0, 30))
        self.assertEqual(self.redis.cli("HGET", "fixture", "tokens"), "16000")


class FakeRedis(socketserver.ThreadingTCPServer):
    daemon_threads = True

    def __init__(self, reply):
        super().__init__((HOST, 0), FakeRedisHandler)
        self.reply = reply
        self.requests = []
        self.lock = threading.Lock()
        self.release = threading.Event()
        self.thread = threading.Thread(target=self.serve_forever, args=(0.02,), daemon=True)
        self.thread.start()

    @property
    def port(self):
        return self.server_address[1]

    def close(self):
        self.release.set()
        self.shutdown()
        self.server_close()
        self.thread.join()


class FakeRedisHandler(socketserver.StreamRequestHandler):
    def handle(self):
        try:
            while line := self.rfile.readline():
                args = []
                for _ in range(int(line[1:])):
                    size = int(self.rfile.readline()[1:])
                    args.append(self.rfile.read(size + 2)[:-2])
                with self.server.lock:
                    index = len(self.server.requests)
                    wanted = int(args[-1])
                    self.server.requests.append((args[3], wanted))
                data = self.server.reply(self.server, index, wanted)
                if data is not None:
                    self.wfile.write(data)
        except (OSError, ValueError):
            pass


def grant(count, retry=0):
    return f"*2\r\n:{count}\r\n:{retry}\r\n".encode()


class RateTests(unittest.TestCase):
    def setUp(self):
        self.backend = gateway.Backend("quota")
        self.addCleanup(self.backend.close)

    def redis(self):
        server = RedisServer()
        self.addCleanup(server.close)
        return server.start()

    def fake(self, reply):
        server = FakeRedis(reply)
        self.addCleanup(server.close)
        return server

    def start(self, redis_port, quotas, *, extra="", env=None, workers=3, routes=None):
        port = free_port()
        directory = tempfile.TemporaryDirectory(prefix="aegisgate-rate-")
        self.addCleanup(directory.cleanup)
        config = (
            f"listen {HOST} {port}\nworkers {workers}\n"
            f"backend origin {HOST} {self.backend.server_port} 64 32 64\n"
            + (routes or "route * / origin\nroute * /limited origin\n"
                         "route * /another origin\nroute * /third origin\n")
            +
            f"redis {HOST} {redis_port}\nrate_diagnostics on\n"
            + ("" if "rate_lease_ms " in extra else "rate_lease_ms 5000\n")
            + extra + quotas
        )
        process = gateway.ServerProcess(Path(directory.name), config, env)
        self.addCleanup(process.close)
        gateway.eventually(process.ready)
        return port, process

    def get(self, port, path="/", headers=b""):
        with gateway.Wire(port) as connection:
            connection.send(gateway.request(path, headers=headers))
            return connection.response()

    def reason(self, port, path="/"):
        status, headers, _ = self.get(port, path)
        return status, headers.get(b"x-aegisgate-rate-limit")

    def test_single_instance_exact_capacity_and_exhaustion(self):
        redis = self.redis()
        port, _ = self.start(redis.port, "quota global 19 0 5\n")
        self.assertEqual([self.get(port)[0] for _ in range(19)], [200] * 19)
        self.assertEqual(self.reason(port), (429, b"quota_exhausted"))
        self.assertEqual(redis.cli("HGET", "aegisgate:global", "tokens"), "0")

    def test_two_processes_enforce_one_quota_under_concurrency(self):
        redis = self.redis()
        config = "quota global 64 0 8\n"
        a, _ = self.start(redis.port, config)
        b, _ = self.start(redis.port, config)
        with ThreadPoolExecutor(max_workers=28) as clients:
            results = list(clients.map(lambda n: self.get(a if n % 2 else b)[0], range(192)))
        self.assertEqual(results.count(200), 64)
        self.assertEqual(results.count(429), 128)
        self.assertEqual(redis.cli("HGET", "aegisgate:global", "tokens"), "0")

    def test_independent_pools_and_coherent_multi_scope_admission(self):
        redis = self.redis()
        a, b = (secrets.token_urlsafe() for _ in range(2))
        env = dict(os.environ, AEGISGATE_TEST_TENANT_A=a, AEGISGATE_TEST_TENANT_B=b)
        port, process = self.start(redis.port, (
            "quota global 20 0 20\nquota tenant a 4 0 2\nquota tenant b 6 0 3\n"
            "tenant_key a AEGISGATE_TEST_TENANT_A\ntenant_key b AEGISGATE_TEST_TENANT_B\n"
            "quota route * /limited 2 0 2\n"), env=env)
        header_a = f"X-Api-Key: {a}\r\n".encode()
        header_b = f"X-Api-Key: {b}\r\n".encode()
        self.assertEqual(self.get(port, headers=b"X-Tenant: a\r\n")[0], 401)
        self.assertEqual(self.get(port, headers=header_a + header_b)[0], 401)
        self.assertEqual(self.get(port, "/limited", header_a)[0], 200)
        self.assertEqual(self.get(port, "/limited", header_a)[0], 200)
        self.assertEqual(self.get(port, "/limited", header_a)[0], 429)
        self.assertEqual(self.get(port, "/limited", header_b)[0], 429)
        self.assertEqual(self.get(port, "/identity", header_a)[2], b"no key")
        with ThreadPoolExecutor(max_workers=10) as clients:
            results = list(clients.map(
                lambda i: self.get(port, headers=header_a if i < 4 else header_b)[0], range(16)))
        self.assertEqual(results[:4].count(200), 1)
        self.assertEqual(results[4:].count(200), 6)
        self.assertEqual(results.count(429), 9)
        process.close()
        self.assertIn("spent=10", process.log.read_text())

    def test_prefetch_threshold_and_one_outstanding_refill(self):
        def delayed(server, index, wanted):
            if index > 0:
                server.release.wait(1)
            return grant(wanted)

        redis = self.fake(delayed)
        port, _ = self.start(redis.port, "quota global 100 10 10\n",
                             extra="rate_timeout_ms 1000\nrate_wait_ms 35\n")
        for _ in range(7):
            self.assertEqual(self.get(port)[0], 200)
        self.assertEqual([row[1] for row in redis.requests], [10])
        self.assertEqual(self.get(port)[0], 200)
        gateway.eventually(lambda: len(redis.requests) == 2)
        self.assertEqual(redis.requests[-1][1], 8)
        with ThreadPoolExecutor(max_workers=10) as clients:
            results = list(clients.map(lambda _: self.reason(port), range(10)))
        self.assertEqual(sum(status == 200 for status, _ in results), 2)
        self.assertEqual(results.count((503, b"refill_pending")), 8)
        self.assertEqual(len(redis.requests), 2)
        redis.release.set()
        self.assertEqual(self.get(port)[0], 200)

    def test_timeout_does_not_block_worker_and_late_grant_is_discarded(self):
        def delayed(server, index, wanted):
            if index == 0:
                server.release.wait(1)
                return grant(wanted)
            return grant(0, -1)

        redis = self.fake(delayed)
        port, _ = self.start(redis.port, "quota route * /limited 10 0 4\n", workers=1,
                             extra="rate_timeout_ms 160\nrate_wait_ms 300\n")
        with ThreadPoolExecutor(max_workers=1) as client:
            limited = client.submit(self.reason, port, "/limited")
            gateway.eventually(lambda: len(redis.requests) == 1)
            self.assertEqual(self.get(port, "/unmetered")[0], 200)
            self.assertFalse(limited.done())
            self.assertEqual(limited.result(), (503, b"redis_timeout"))
        redis.release.set()
        gateway.eventually(lambda: self.reason(port, "/limited") == (429, b"quota_exhausted"))

    def test_malformed_grant_is_rejected(self):
        for reply in (b"+OK\r\n", grant(100), grant(0), b"!invalid\r\n"):
            with self.subTest(reply=reply):
                redis = self.fake(lambda *_: reply)
                port, _ = self.start(redis.port, "quota global 12 0 4\n")
                self.assertEqual(self.reason(port), (503, b"redis_invalid_response"))

    def test_disconnect_and_recovery(self):
        redis = self.redis()
        port, _ = self.start(redis.port, "quota global 100 10 2\n")
        self.assertEqual(self.get(port)[0], 200)
        redis.stop()
        gateway.eventually(lambda: self.reason(port) == (503, b"redis_disconnected"))
        redis.start()
        gateway.eventually(lambda: self.get(port)[0] == 200)

    def test_bounded_fallback_and_explicit_fail_open(self):
        with socket.socket() as unavailable:
            unavailable.bind((HOST, 0))
            redis_port = unavailable.getsockname()[1]
            port, _ = self.start(redis_port, "quota global 20 0 4\n",
                                 extra="rate_failure fallback\nrate_fallback 3 0\n")
            self.assertEqual([self.get(port)[0] for _ in range(3)], [200] * 3)
            self.assertEqual(self.reason(port), (503, b"fallback_exhausted"))
            port, _ = self.start(redis_port, "quota global 20 0 4\n", extra="rate_failure open\n")
            self.assertEqual([self.get(port)[0] for _ in range(6)], [200] * 6)

    def test_lease_expires_and_unused_tokens_are_not_returned(self):
        redis = self.redis()
        port, process = self.start(redis.port, "quota global 4 0 4\n",
                                   extra="rate_lease_ms 100\n")
        self.assertEqual(self.get(port)[0], 200)
        time.sleep(0.14)
        self.assertEqual(self.reason(port), (429, b"quota_exhausted"))
        process.close()
        self.assertIn("granted=4 spent=1 expired=3", process.log.read_text())

    def test_full_refill_queue_preserves_backpressure_and_recovers(self):
        def delayed(server, index, wanted):
            if index == 0:
                server.release.wait(1)
            return grant(wanted)

        redis = self.fake(delayed)
        quotas = "".join(f"quota route * /{path} 10 0 4\n"
                         for path in ("limited", "another", "third"))
        port, _ = self.start(redis.port, quotas,
                             extra="rate_queue 1\nrate_timeout_ms 1000\nrate_wait_ms 35\n")
        self.assertEqual(self.reason(port, "/limited"), (503, b"refill_pending"))
        gateway.eventually(lambda: len(redis.requests) == 1)
        self.assertEqual(self.reason(port, "/another"), (503, b"refill_pending"))
        self.assertEqual(self.reason(port, "/third"), (503, b"refill_pending"))
        self.assertEqual(len(redis.requests), 1)
        redis.release.set()
        self.assertEqual(self.get(port, "/limited")[0], 200)
        self.assertEqual(self.get(port, "/another")[0], 200)
        self.assertEqual(self.get(port, "/third")[0], 200)
        self.assertEqual(len(redis.requests), 3)

    def test_fail_open_still_honors_known_quota_exhaustion(self):
        redis = self.redis()
        port, _ = self.start(redis.port, "quota global 1 0 1\n", extra="rate_failure open\n")
        self.assertEqual(self.get(port)[0], 200)
        self.assertEqual(self.reason(port), (429, b"quota_exhausted"))

    def test_complete_post_and_pipelined_request_cross_admission_wait(self):
        redis = self.redis()
        port, _ = self.start(redis.port, "quota global 2 0 1\n")
        payload = b"streamed through quota wait"
        with gateway.Wire(port) as wire:
            wire.send(gateway.request("/echo", "POST",
                                       headers=f"Content-Length: {len(payload)}\r\n".encode())
                      + payload + gateway.request("/next"))
            self.assertEqual(wire.response()[2], payload)
            self.assertEqual(wire.response()[2], b"quota:/next")
        self.assertEqual(self.reason(port), (429, b"quota_exhausted"))

    def test_shutdown_cancels_outstanding_redis_command(self):
        def delayed(server, _, wanted):
            server.release.wait(6)
            return grant(wanted)

        redis = self.fake(delayed)
        port, process = self.start(redis.port, "quota global 20 0 4\n",
                                   extra="rate_wait_ms 0\nrate_timeout_ms 5000\n")
        self.assertEqual(self.reason(port), (503, b"refill_pending"))
        gateway.eventually(lambda: len(redis.requests) == 1)
        started = time.monotonic()
        process.close()
        self.assertLess(time.monotonic() - started, 1.2)

    def test_combined_http_resilience_quota_and_metrics(self):
        redis = self.redis()
        bad = gateway.Backend("bad")
        self.addCleanup(bad.close)
        bad.status = 503
        identity = secrets.token_urlsafe()
        headers = f"X-API-Key: {identity}\r\n".encode()
        monitor = free_port()
        port, _ = self.start(
            redis.port,
            "quota global 20 0 4\nquota tenant team 20 0 4\n"
            "tenant_key team AEGISGATE_TEST_STAGE_ONE_KEY\nquota route * /limited 2 0 2\n",
            workers=1, env=dict(os.environ, AEGISGATE_TEST_STAGE_ONE_KEY=identity),
            routes="group all peak_ewma bad origin\nroute * / all\nroute * /limited all\nroute * /bad bad\n",
            extra=f"backend bad {HOST} {bad.server_port} 4 2 2\nmetrics_listen {HOST} {monitor}\n"
                  "circuit_breaker on\ncircuit_min_samples 1\ncircuit_failure_percent 100\n"
                  "circuit_cooldown_ms 750\ncircuit_recovery_successes 1\nhealth_interval_ms 25\n",
        )
        self.assertEqual(self.get(monitor, "/healthz")[0], 200)
        self.assertEqual(self.get(port)[0], 401)
        self.assertEqual(self.get(port, "/bad", headers)[0], 503)
        with gateway.Wire(port) as connection:
            connection.send(gateway.request("/echo", "POST",
                            headers=headers + b"Transfer-Encoding: chunked\r\n")
                            + b"4\r\ndata\r\n0\r\n\r\n")
            self.assertEqual(connection.response()[2], b"data")
            for path in ("/limited?opaque_request_label", "/limited"):
                connection.send(gateway.request(path, headers=headers))
                self.assertEqual(connection.response()[0], 200)
            connection.send(gateway.request("/limited", headers=headers))
            self.assertEqual(connection.response()[0], 429)
        self.assertEqual(bad.requests, ["/bad"])
        bad.status = 200
        time.sleep(0.8)
        self.assertEqual(self.get(port, "/bad", headers)[2], b"bad:/bad")
        text = self.get(monitor, "/metrics")[2].decode()
        for sample in (
            "aegisgate_requests_total 7\n",
            "aegisgate_requests_successful_total 4\n",
            "aegisgate_requests_rejected_total 2\n",
            'aegisgate_http_responses_total{status="429"} 1\n',
            "aegisgate_request_duration_seconds_count 7\n",
            'aegisgate_circuit_transitions_total{backend="bad",state="open"} 1\n',
            'aegisgate_circuit_transitions_total{backend="bad",state="closed"} 1\n',
            'aegisgate_backend_failures_total{backend="bad",reason="upstream_5xx"} 1\n',
            'aegisgate_rate_rejections_total{reason="quota_exhausted"} 1\n',
            'aegisgate_tokens_consumed_total{source="lease"} 12\n',
            "aegisgate_redis_connected 1\n",
        ):
            self.assertIn(sample, text)
        self.assertNotIn("opaque_request_label", text)
        self.assertNotIn(identity, text)


if __name__ == "__main__":
    if "--gateway" in sys.argv:
        position = sys.argv.index("--gateway")
        gateway.BINARY = Path(sys.argv.pop(position + 1)).resolve()
        sys.argv.pop(position)
    unittest.main()
