"""Tests against an isolated Redis process, using empty temporary storage."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import socket
import subprocess
import tempfile
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


if __name__ == "__main__":
    unittest.main()
