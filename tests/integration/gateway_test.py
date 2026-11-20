"""Exercise real TCP connections against locally controlled HTTP backends."""

from concurrent.futures import ThreadPoolExecutor
import errno
import hashlib
import http.server
import os
from pathlib import Path
import select
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest


HOST = "127.0.0.1"
BLOCK = bytes(range(256)) * 64
LARGE_BLOCKS = 1536


def eventually(check, timeout=3):
    deadline = time.monotonic() + timeout
    while not check():
        if time.monotonic() >= deadline:
            raise AssertionError("condition did not become true before deadline")
        time.sleep(0.01)


def request(path="/", method="GET", host="example.test", headers=b""):
    return f"{method} {path} HTTP/1.1\r\nHost: {host}\r\n".encode() + headers + b"\r\n"


class Wire:
    def __init__(self, port, receive_buffer=None):
        self.socket = socket.socket()
        self.socket.settimeout(5)
        if receive_buffer is not None:
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
        self.socket.connect((HOST, port))
        self.stream = self.socket.makefile("rb")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.stream.close()
        self.socket.close()

    def send(self, data):
        self.socket.sendall(data)

    def head(self):
        line = self.stream.readline()
        if not line.startswith(b"HTTP/1.1 "):
            raise AssertionError(f"invalid or missing response line: {line!r}")
        status = int(line.split()[1])
        headers = {}
        for line in iter(self.stream.readline, b"\r\n"):
            if not line:
                raise EOFError("incomplete response headers")
            key, value = line.rstrip(b"\r\n").split(b":", 1)
            headers[key.lower()] = value.strip()
        return status, headers

    def response(self, bodyless=False):
        status, headers = self.head()
        body = b""
        if not bodyless and status not in (204, 304):
            if headers.get(b"transfer-encoding") == b"chunked":
                body = read_chunked(self.stream)
            else:
                size = int(headers[b"content-length"])
                body = self.stream.read(size)
                if len(body) != size:
                    raise EOFError("incomplete fixed-length body")
        return status, headers, body


def read_chunked(stream):
    parts = []
    while size := int(stream.readline(), 16):
        parts.append(stream.read(size))
        if stream.read(2) != b"\r\n":
            raise EOFError("incomplete chunk")
    if stream.readline() != b"\r\n":
        raise AssertionError("unexpected trailers")
    return b"".join(parts)


class Backend(http.server.ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128

    def __init__(self, name, delay=0):
        super().__init__((HOST, 0), Handler)
        self.label = name.encode()
        self.delay = delay
        self.lock = threading.Lock()
        self.accepted = 0
        self.requests = []
        self.entered = threading.Event()
        self.proceed = threading.Event()
        self.finished = threading.Event()
        self.thread = threading.Thread(target=self.serve_forever, args=(0.02,), daemon=True)
        self.thread.start()

    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(5)
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        with self.lock:
            self.accepted += 1
        return connection, address

    def close(self):
        self.proceed.set()
        self.shutdown()
        self.server_close()
        self.thread.join()

    def handle_error(self, request_socket, address):
        error = sys.exc_info()[1]
        if not isinstance(error, (BrokenPipeError, ConnectionResetError, TimeoutError)):
            super().handle_error(request_socket, address)


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def fixed(self, payload, status=200):
        self.send_response(status)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    def record(self):
        with self.server.lock:
            self.server.requests.append(self.path)

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        self.record()
        if self.server.delay:
            time.sleep(self.server.delay)
        if self.path == "/no-content":
            self.send_response(204)
            self.end_headers()
        elif self.path == "/hold":
            self.server.entered.set()
            self.server.proceed.wait(5)
            self.fixed(b"released")
        elif self.path == "/stale":
            self.fixed(b"will close")
            self.close_connection = True
            self.server.finished.set()
        elif self.path == "/eof":
            self.wfile.write(b"HTTP/1.0 200 OK\r\n\r\nclose-delimited")
            self.close_connection = True
        elif self.path in ("/stream", "/truncate"):
            self.wfile.write(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nab")
            self.server.entered.set()
            self.server.proceed.wait(5)
            if self.path == "/stream":
                self.wfile.write(b"cd")
            else:
                self.close_connection = True
        elif self.path == "/large":
            self.send_response(200)
            self.send_header("Content-Length", str(len(BLOCK) * LARGE_BLOCKS))
            self.end_headers()
            self.server.entered.set()
            for _ in range(LARGE_BLOCKS):
                self.wfile.write(BLOCK)
            self.server.finished.set()
        elif self.path == "/headers":
            value = self.headers.get("X-Client-Hop", "filtered").encode()
            self.wfile.write(b"HTTP/1.1 200 OK\r\nConnection: X-Hop, close\r\nX-Hop: gone\r\n"
                             b"Content-Length: 0\r\nX-Client-Hop: " + value + b"\r\n\r\n")
            self.close_connection = True
        elif self.path == "/large-headers":
            self.wfile.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nX-Big: " +
                             b"x" * 1500 + b"\r\n\r\n")
            self.close_connection = True
        else:
            self.fixed(self.server.label + b":" + self.path.encode())

    def do_POST(self):
        self.record()
        if self.path == "/disconnect":
            self.close_connection = True
            return
        if self.path == "/early":
            self.fixed(b"upload not accepted", 413)
            self.server.entered.set()
            self.server.proceed.wait(5)
            self.close_connection = True
            return
        if self.headers.get("Transfer-Encoding") == "chunked":
            payload = read_chunked(self.rfile)
            self.send_response(200)
            self.send_header("Transfer-Encoding", "chunked")
            self.send_header("Trailer", "X-Trailer")
            self.end_headers()
            self.wfile.write(f"{len(payload):x};tag=fixture\r\n".encode() + payload +
                             b"\r\n0\r\nX-Trailer: ignored\r\n\r\n")
        else:
            size = int(self.headers.get("Content-Length", "0"))
            payload = self.rfile.read(size)
            if len(payload) == size:
                self.fixed(payload)


class ServerProcess:
    def __init__(self, directory, config):
        self.log = directory / "gateway.log"
        config_path = directory / "gateway.conf"
        config_path.write_text(config, encoding="ascii")
        with self.log.open("w") as output:
            self.process = subprocess.Popen([BINARY, config_path], stdout=output, stderr=output)

    def ready(self):
        log = self.log.read_text()
        if self.process.poll() is not None:
            raise AssertionError(f"gateway exited during startup: {log}")
        return "AegisGate ready" in log

    def stop(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)

    def close(self):
        self.stop()
        try:
            status = self.process.wait(6)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
            raise
        if status != 0:
            raise AssertionError(f"gateway returned {status}: {self.log.read_text()}")
        if os.getenv("AEGISGATE_EXPECT_SHORT_IO") and "short I/O exercised" not in self.log.read_text():
            raise AssertionError("test did not observe short reads, short writes and transient I/O")

    def fd_count(self):
        return len(list(Path(f"/proc/{self.process.pid}/fd").iterdir()))


class GatewayTests(unittest.TestCase):
    def start(self, *, options=None, backends=None, routes=None, pool="4 2 4"):
        self.one = Backend("one")
        self.addCleanup(self.one.close)
        settings = {
            "workers": 1, "header_bytes": 1024, "body_bytes": 8 * 1024 * 1024,
            "stream_buffer_bytes": 4096, "client_idle_ms": 2500, "header_timeout_ms": 1000,
            "response_timeout_ms": 3500, "connect_timeout_ms": 500, "shutdown_timeout_ms": 700,
        }
        settings.update(options or {})
        with socket.socket() as reservation:
            reservation.bind((HOST, 0))
            self.port = reservation.getsockname()[1]
        config = f"listen {HOST} {self.port}\nbackend one {HOST} {self.one.server_port} {pool}\n"
        config += "".join(f"{key} {value}\n" for key, value in settings.items())
        for name, port in (backends or {}).items():
            config += f"backend {name} {HOST} {port} 4 2 4\n"
        config += routes or "route * / one\n"
        directory = tempfile.TemporaryDirectory(prefix="aegisgate-integration-")
        self.addCleanup(directory.cleanup)
        self.gateway = ServerProcess(Path(directory.name), config)
        self.addCleanup(self.gateway.close)
        eventually(self.gateway.ready)
        self.baseline = self.gateway.fd_count()

    def wire(self, receive_buffer=None):
        return Wire(self.port, receive_buffer)

    def test_named_group_round_robin_routes_in_order(self):
        two = Backend("two")
        self.addCleanup(two.close)
        self.start(backends={"two": two.server_port},
                   routes="group api round_robin one two\nroute * / api\n")
        with self.wire() as wire:
            for label in (b"one", b"two") * 4:
                wire.send(request("/balance"))
                self.assertEqual(wire.response()[2], label + b":/balance")

    def test_peak_ewma_steers_away_from_measured_slow_backend(self):
        two = Backend("two", delay=0.06)
        self.addCleanup(two.close)
        self.start(backends={"two": two.server_port},
                   routes="group api peak_ewma one two\nroute * / api\n"
                          "route * /fast one\nroute * /slow two\n")
        with self.wire() as wire:
            for path, label in (("/fast", b"one"), ("/slow", b"two")):
                wire.send(request(path))
                self.assertEqual(wire.response()[2], label + b":" + path.encode())
            for _ in range(12):
                wire.send(request("/balance"))
                self.assertEqual(wire.response()[2], b"one:/balance")

    def test_routes_by_host_then_path_and_returns_404(self):
        two = Backend("two")
        self.addCleanup(two.close)
        self.start(backends={"two": two.server_port},
                   routes="route * /api one\nroute app.test /api two\nroute app.test /api/deep one\n")
        with self.wire() as wire:
            for path, host, body in [
                ("/api/item?x=1", "APP.Test:8080", b"two:/api/item?x=1"),
                ("/api/deep/next", "app.test", b"one:/api/deep/next"),
                ("/api", "other.test", b"one:/api"),
            ]:
                wire.send(request(path, host=host))
                status, _, payload = wire.response()
                self.assertEqual((status, payload), (200, body))
            wire.send(request("/apiary"))
            self.assertEqual(wire.response()[0], 404)

    def test_keep_alive_and_upstream_reuse_including_head(self):
        self.start()
        with self.wire() as wire:
            wire.send(request("/a", "HEAD"))
            status, headers, _ = wire.response(bodyless=True)
            self.assertEqual((status, headers[b"content-length"]), (200, b"6"))
            wire.send(request("/no-content"))
            self.assertEqual(wire.response()[0], 204)
            wire.send(request("/b", headers=b"Connection: close\r\n"))
            self.assertEqual(wire.response()[2], b"one:/b")
            self.assertEqual(wire.stream.read(1), b"")
        with self.wire() as wire:
            wire.send(request("/new-client"))
            self.assertEqual(wire.response()[2], b"one:/new-client")
        self.assertEqual(self.one.accepted, 1)

    def test_pipeline_order_survives_client_half_close(self):
        self.start()
        with self.wire() as wire:
            data = request("/ordered-1") + request("/ordered-2")
            wire.send(data[:-3])
            wire.send(data[-3:])
            wire.socket.shutdown(socket.SHUT_WR)
            self.assertEqual(wire.response()[2], b"one:/ordered-1")
            self.assertEqual(wire.response()[2], b"one:/ordered-2")
            self.assertEqual(wire.stream.read(1), b"")

    def test_split_headers_and_binary_echo_cross_small_buffers(self):
        self.start()
        payload = BLOCK * 32 + b"\x00\xffsuffix"
        head = request("/echo", "POST", headers=f"Content-Length: {len(payload)}\r\n".encode())
        with self.wire() as wire:
            for offset in range(0, len(head), 9):
                wire.send(head[offset:offset + 9])
                time.sleep(0.003)
            for offset in range(0, len(payload), 2307):
                wire.send(payload[offset:offset + 2307])
            self.assertEqual(wire.response()[2], payload)
            wire.send(request("/after"))
            self.assertEqual(wire.response()[2], b"one:/after")
        self.assertEqual(self.one.accepted, 1)

    def test_chunked_extensions_trailers_and_reuse(self):
        self.start()
        with self.wire() as wire:
            wire.send(request("/chunk", "POST",
                              headers=b"Transfer-Encoding: Chunked\r\nTrailer: X-Proof\r\n"))
            for fragment in [b"2;tag=yes\r", b"\nab\r\n3\r\nx", b"\x00z\r\n0\r\n",
                             b"X-Proof: local\r\n\r\n"]:
                wire.send(fragment)
                time.sleep(0.005)
            status, headers, body = wire.response()
            self.assertEqual((status, body), (200, b"abx\x00z"))
            self.assertNotIn(b"trailer", headers)
            wire.send(request("/after"))
            self.assertEqual(wire.response()[2], b"one:/after")
        self.assertEqual(self.one.accepted, 1)

    def test_rejects_malformed_unsupported_and_oversize_requests(self):
        self.start(options={"body_bytes": 64})
        for status, data in [
            (400, b"GET / HTTP/1.1\r\n\r\n"),
            (400, request(headers=b"Host: duplicate.test\r\n")),
            (400, request(method="POST", headers=b"Content-Length: 1\r\nContent-Length: 2\r\n")),
            (417, request(method="POST", headers=b"Expect: 100-continue\r\n")),
            (431, request(headers=b"X-Big: " + b"z" * 1200 + b"\r\n")),
            (413, request(method="POST", headers=b"Content-Length: 65\r\n")),
        ]:
            with self.subTest(status=status), self.wire() as wire:
                wire.send(data)
                self.assertEqual(wire.response()[0], status)
                self.assertEqual(wire.stream.read(1), b"")
        self.assertEqual(self.one.accepted, 0)
        with self.wire() as wire:
            wire.send(request(method="POST", headers=b"Transfer-Encoding: chunked\r\n") + b"41\r\n")
            self.assertEqual(wire.response()[0], 413)

    def test_filters_hop_headers_and_rejects_large_backend_headers(self):
        self.start()
        with self.wire() as wire:
            wire.send(request("/headers", headers=b"Connection: X-Client-Hop\r\nX-Client-Hop: gone\r\n"))
            status, headers, _ = wire.response()
            self.assertEqual(status, 200)
            self.assertNotIn(b"x-hop", headers)
            self.assertEqual(headers[b"x-client-hop"], b"filtered")
            wire.send(request("/large-headers"))
            self.assertEqual(wire.response()[0], 502)

    def test_forwards_response_before_backend_finishes(self):
        self.start()
        with self.wire() as wire:
            wire.send(request("/stream"))
            self.assertEqual(wire.head()[0], 200)
            self.assertEqual(wire.stream.read(2), b"ab")
            self.assertFalse(self.one.proceed.is_set())
            self.one.proceed.set()
            self.assertEqual(wire.stream.read(2), b"cd")

    def test_slow_reader_backpressure_preserves_large_response(self):
        self.start(options={"response_timeout_ms": 10000, "body_bytes": len(BLOCK) * LARGE_BLOCKS})
        with self.wire(receive_buffer=32768) as wire:
            wire.send(request("/large"))
            self.assertTrue(self.one.entered.wait(2))
            self.assertFalse(self.one.finished.wait(0.12))
            status, _, body = wire.response()
            self.assertEqual(status, 200)
            self.assertEqual(len(body), len(BLOCK) * LARGE_BLOCKS)
            self.assertEqual(hashlib.sha256(body).digest(),
                             hashlib.sha256(BLOCK * LARGE_BLOCKS).digest())
            self.assertTrue(self.one.finished.wait(1))

    def test_stale_pool_connections_retire_and_eof_body_is_reframed(self):
        self.start()
        with self.wire() as wire:
            wire.send(request("/stale"))
            self.assertEqual(wire.response()[2], b"will close")
            self.assertTrue(self.one.finished.wait(1))
            eventually(lambda: self.gateway.fd_count() == self.baseline + 1)
            wire.send(request("/eof"))
            self.assertEqual(wire.response()[2], b"close-delimited")
            wire.send(request("/after"))
            self.assertEqual(wire.response()[2], b"one:/after")
        self.assertEqual(self.one.accepted, 3)

    def test_backend_disconnect_does_not_retry_post(self):
        self.start()
        with self.wire() as wire:
            wire.send(request("/disconnect", "POST", headers=b"Content-Length: 0\r\n"))
            self.assertEqual(wire.response()[0], 502)
        self.assertEqual(self.one.requests, ["/disconnect"])
        with self.wire() as wire:
            wire.send(request("/recover"))
            self.assertEqual(wire.response()[2], b"one:/recover")
        self.assertEqual(self.one.accepted, 2)

    def test_backend_connection_refusal_returns_502(self):
        with socket.socket() as unavailable:
            unavailable.bind((HOST, 0))
            self.start(backends={"unavailable": unavailable.getsockname()[1]},
                       routes="route * / unavailable\n")
            with self.wire() as wire:
                wire.send(request())
                self.assertEqual(wire.response()[0], 502)
                self.assertEqual(wire.stream.read(1), b"")

    def test_truncated_committed_response_closes_connection(self):
        self.start()
        with self.wire() as wire:
            wire.send(request("/truncate"))
            self.assertEqual(wire.head()[0], 200)
            self.assertEqual(wire.stream.read(2), b"ab")
            self.one.proceed.set()
            self.assertEqual(wire.stream.read(5), b"")

    def test_backend_early_response_stops_upload(self):
        self.start()
        with self.wire() as wire:
            wire.send(request("/early", "POST", headers=b"Content-Length: 100000\r\n"))
            status, headers, payload = wire.response()
            self.assertEqual((status, headers[b"connection"], payload),
                             (413, b"close", b"upload not accepted"))
            self.assertEqual(wire.stream.read(1), b"")

    def test_pool_limits_and_pending_fifo(self):
        self.start(pool="1 1 2")
        with self.wire() as active, self.wire() as first, self.wire() as second, self.wire() as excess:
            active.send(request("/hold"))
            self.assertTrue(self.one.entered.wait(2))
            for wire, path in [(first, "/first"), (second, "/second")]:
                wire.send(request(path))
                self.assertFalse(select.select([wire.socket], [], [], 0.08)[0])
            excess.send(request("/excess"))
            self.assertEqual(excess.response()[0], 503)
            self.one.proceed.set()
            self.assertEqual(active.response()[2], b"released")
            self.assertEqual(first.response()[2], b"one:/first")
            self.assertEqual(second.response()[2], b"one:/second")
        self.assertEqual(self.one.accepted, 1)
        self.assertEqual(self.one.requests, ["/hold", "/first", "/second"])

    def test_cancelled_pending_releases_queue_and_sockets(self):
        self.start(pool="1 0 1")
        with self.wire() as active:
            active.send(request("/hold"))
            self.assertTrue(self.one.entered.wait(2))
            for _ in range(8):
                with self.wire() as queued:
                    queued.send(request("/cancelled"))
                    self.assertFalse(select.select([queued.socket], [], [], 0.02)[0])
                    queued.socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                                             struct.pack("ii", 1, 0))
                eventually(lambda: self.gateway.fd_count() == self.baseline + 2)
            with self.wire() as survivor:
                survivor.send(request("/survivor"))
                self.assertFalse(select.select([survivor.socket], [], [], 0.08)[0])
                self.one.proceed.set()
                self.assertEqual(active.response()[2], b"released")
                self.assertEqual(survivor.response()[2], b"one:/survivor")
        eventually(lambda: self.gateway.fd_count() == self.baseline)
        self.assertEqual(self.one.requests, ["/hold", "/survivor"])

    def test_incomplete_upload_and_client_reset_release_active_pool(self):
        self.start(pool="1 0 1")
        with self.wire() as wire:
            wire.send(request("/short", "POST", headers=b"Content-Length: 12\r\n") + b"ab")
            wire.socket.shutdown(socket.SHUT_WR)
            self.assertEqual(wire.response()[0], 400)
        eventually(lambda: self.gateway.fd_count() == self.baseline)
        with self.wire() as wire:
            wire.send(request("/hold"))
            self.assertTrue(self.one.entered.wait(2))
            wire.socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        eventually(lambda: self.gateway.fd_count() == self.baseline)
        with self.wire() as wire:
            wire.send(request("/recover"))
            self.assertEqual(wire.response()[2], b"one:/recover")

    def test_idle_and_absolute_header_deadlines(self):
        self.start(options={"client_idle_ms": 550, "header_timeout_ms": 220})
        with self.wire() as idle:
            self.assertEqual(idle.stream.read(1), b"")
        with self.wire() as wire:
            wire.send(b"GET / HTTP/1.1\r\n")
            for fragment in [b"H", b"o", b"s"]:
                time.sleep(0.05)
                wire.send(fragment)
            self.assertEqual(wire.response()[0], 408)
        self.assertEqual(self.one.accepted, 0)

    def test_response_deadline_recovers_pool(self):
        self.start(options={"response_timeout_ms": 250}, pool="1 0 1")
        with self.wire() as wire:
            wire.send(request("/hold"))
            self.assertTrue(self.one.entered.wait(2))
            self.assertEqual(wire.response()[0], 504)
        with self.wire() as wire:
            wire.send(request("/ok"))
            self.assertEqual(wire.response()[2], b"one:/ok")

    def test_idle_request_body_times_out_and_releases_upstream(self):
        self.start(options={"client_idle_ms": 220}, pool="1 0 0")
        with self.wire() as wire:
            wire.send(request("/slow", "POST", headers=b"Content-Length: 10\r\n") + b"a")
            self.assertEqual(wire.response()[0], 408)
        eventually(lambda: self.gateway.fd_count() == self.baseline)
        with self.wire() as wire:
            wire.send(request("/ok"))
            self.assertEqual(wire.response()[2], b"one:/ok")

    def test_connect_deadline_on_full_local_listen_queue(self):
        with socket.socket() as listener:
            listener.bind((HOST, 0))
            listener.listen(1)
            address = listener.getsockname()
            # Hold the listen queue full without an external IP or firewall rule.
            for _ in range(2):
                connection = socket.create_connection(address, timeout=1)
                self.addCleanup(connection.close)
            with socket.socket() as probe:
                probe.setblocking(False)
                self.assertEqual(probe.connect_ex(address), errno.EINPROGRESS)
                self.assertFalse(select.select([], [probe], [], 0.06)[1])
            self.start(options={"connect_timeout_ms": 180, "response_timeout_ms": 1500},
                       backends={"blocked": address[1]}, routes="route * / blocked\n")
            with self.wire() as wire:
                started = time.monotonic()
                wire.send(request())
                self.assertEqual(wire.response()[0], 504)
                self.assertGreaterEqual(time.monotonic() - started, 0.14)
                self.assertLess(time.monotonic() - started, 1.2)

    def test_graceful_shutdown_drains_admitted_exchange(self):
        self.start()
        with self.wire() as wire, self.wire() as idle:
            wire.send(request("/hold"))
            self.assertTrue(self.one.entered.wait(2))
            self.gateway.stop()
            time.sleep(0.05)
            self.one.proceed.set()
            self.assertEqual(wire.response()[2], b"released")
            self.assertEqual(wire.stream.read(1), b"")
            self.assertEqual(idle.stream.read(1), b"")
        self.assertEqual(self.gateway.process.wait(2), 0)

    def test_shutdown_grace_period_is_bounded(self):
        self.start(options={"shutdown_timeout_ms": 180})
        with self.wire() as wire:
            wire.send(request("/hold"))
            self.assertTrue(self.one.entered.wait(2))
            self.gateway.stop()
            self.assertEqual(self.gateway.process.wait(1.5), 0)
            self.assertEqual(wire.stream.read(1), b"")

    def test_client_limit_releases_capacity(self):
        self.start(options={"clients_per_worker": 1})
        with self.wire() as first:
            eventually(lambda: self.gateway.fd_count() == self.baseline + 1)
            with self.wire() as excess:
                self.assertEqual(excess.stream.read(1), b"")
            first.send(request("/keep"))
            self.assertEqual(first.response()[2], b"one:/keep")
        eventually(lambda: self.gateway.fd_count() == self.baseline + 1)  # One idle upstream.
        with self.wire() as new:
            new.send(request("/capacity"))
            self.assertEqual(new.response()[2], b"one:/capacity")

    def test_concurrent_clients_across_reuseport_workers(self):
        self.start(options={"workers": 3}, pool="16 8 16")
        connected = threading.Barrier(24, timeout=2)

        def visit(index):
            with self.wire() as wire:
                connected.wait()
                for iteration in range(5):
                    path = f"/concurrent/{index}/{iteration}"
                    wire.send(request(path))
                    status, _, body = wire.response()
                    self.assertEqual((status, body), (200, f"one:{path}".encode()))

        with ThreadPoolExecutor(max_workers=24) as executor:
            list(executor.map(visit, range(24)))
        self.assertEqual(len(self.one.requests), 120)
        self.assertLess(self.one.accepted, 120)


if __name__ == "__main__":
    BINARY = Path(sys.argv.pop(1)).resolve()
    unittest.main()
