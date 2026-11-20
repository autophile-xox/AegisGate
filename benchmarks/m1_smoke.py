"""Paired M1 loopback measurements with a fixed HTTP/1.1 origin and wrk."""

import argparse
import asyncio
from contextlib import contextmanager
import json
import os
from pathlib import Path
import platform
import socket
from statistics import median
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parent.parent
HOST = "127.0.0.1"
PAYLOAD = b"aegisgate" * 16
RESPONSE = f"HTTP/1.1 200 OK\r\nContent-Length: {len(PAYLOAD)}\r\n\r\n".encode() + PAYLOAD
FIELDS = (
    "duration_us", "requests", "status_errors", "connect_errors", "read_errors",
    "write_errors", "timeouts", "p50_us", "p95_us", "p99_us", "p99_9_us",
)


async def origin(port):
    async def respond(reader, writer):
        try:
            while True:
                await reader.readuntil(b"\r\n\r\n")
                writer.write(RESPONSE)
                await writer.drain()
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, ConnectionError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(respond, HOST, port, limit=4096)
    async with server:
        await server.serve_forever()


@contextmanager
def running(command, port, log):
    with log.open("w") as output:
        process = subprocess.Popen(command, stdout=output, stderr=output)
        try:
            deadline = time.monotonic() + 5
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"server exited. See {log}")
                try:
                    with socket.create_connection((HOST, port), timeout=0.1):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f"server failed to start. See {log}")
                    time.sleep(0.025)
            yield process
            if process.poll() is not None:
                raise RuntimeError(f"server exited during the benchmark. See {log}")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=7)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise


def sample(process):
    proc = Path(f"/proc/{process.pid}")
    stat = (proc / "stat").read_text().rsplit(") ", 1)[1].split()
    hwm = next(line for line in (proc / "status").read_text().splitlines()
               if line.startswith("VmHWM:"))
    return {
        "cpu_seconds": (int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK"),
        "rss_high_water_kib": int(hwm.split()[1]),
        "open_fds_at_sample": len(list((proc / "fd").iterdir())),
    }


def measure(args, port, duration, outfile, processes):
    command = [
        "taskset", "-c", args.load_cpus, "wrk", "-t", str(args.threads),
        "-c", str(args.connections), "-d", f"{duration}s", "--timeout", "2s",
        "-s", str(ROOT / "benchmarks/m1_smoke.lua"), f"http://{HOST}:{port}/benchmark",
    ]
    before = {name: sample(process) for name, process in processes.items()}
    started = time.monotonic()
    completed = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True, timeout=duration + 10)
    wall = time.monotonic() - started
    after = {name: sample(process) for name, process in processes.items()}
    outfile.write_text(completed.stdout)
    completed.check_returncode()
    values = next(line.split()[1] for line in completed.stdout.splitlines()
                  if line.startswith("AEGISGATE_RESULT "))
    result = dict(zip(FIELDS, map(float, values.split(",")), strict=True))
    # The fixed origin always returns 200. Gateway-generated errors are 4xx/5xx.
    result["successful_requests"] = result["requests"] - result["status_errors"]
    result["successful_rps"] = result["successful_requests"] * 1e6 / result["duration_us"]
    result["command"] = command
    result["resources"] = after
    for name, resource in after.items():
        cpu_time = resource["cpu_seconds"] - before[name]["cpu_seconds"]
        resource["cpu_one_core_percent"] = cpu_time * 100 / wall
    return result


def command_output(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def run(args):
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    with socket.socket() as origin_slot, socket.socket() as gateway_slot:
        for slot in (origin_slot, gateway_slot):
            slot.bind((HOST, 0))
        origin_port, gateway_port = (slot.getsockname()[1] for slot in (origin_slot, gateway_slot))
    config = out / "gateway.conf"
    config.write_text(
        f"listen {HOST} {gateway_port}\nworkers 2\nclients_per_worker 4096\n"
        f"backend origin {HOST} {origin_port} 512 512 512\nroute * / origin\n"
    )
    origin_command = ["taskset", "-c", args.origin_cpu, sys.executable, __file__, "--origin", str(origin_port)]
    gateway_command = ["taskset", "-c", args.gateway_cpus, str(args.gateway.resolve()), str(config)]
    report = {
        "commit": command_output("git", "rev-parse", "HEAD"),
        "working_tree": command_output("git", "status", "--porcelain"),
        "kernel": platform.release(),
        "os": platform.freedesktop_os_release()["PRETTY_NAME"],
        "cpu": command_output("lscpu"),
        "memory": Path("/proc/meminfo").read_text().splitlines()[0],
        "payload_bytes": len(PAYLOAD),
        "threads": args.threads,
        "connections": args.connections,
        "warmup_seconds": args.warmup,
        "repetitions": args.repetitions,
        "seconds_per_run": args.seconds,
        "origin_command": origin_command,
        "gateway_command": gateway_command,
        "direct": [],
        "gateway": [],
    }
    with running(origin_command, origin_port, out / "origin.log") as backend:
        with running(gateway_command, gateway_port, out / "gateway.log") as gateway:
            processes = {"origin": backend, "gateway": gateway}
            targets = {"direct": origin_port, "gateway": gateway_port}
            for name, port in targets.items():
                measure(args, port, args.warmup, out / f"{name}-warmup.txt", processes)
            for index in range(args.repetitions):
                order = list(targets) if index % 2 == 0 else list(reversed(targets))
                for name in order:
                    result = measure(args, targets[name], args.seconds,
                                     out / f"{name}-{index + 1:02}.txt", processes)
                    report[name].append(result)
                    print(f"{name} run {index + 1}: {result['successful_rps']:,.0f} successful RPS",
                          flush=True)
            if args.soak:
                report["soak"] = measure(args, gateway_port, args.soak,
                                         out / "gateway-soak.txt", processes)
    (out / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    for name in ("direct", "gateway"):
        rate = median(result["successful_rps"] for result in report[name])
        print(f"{name} median: {rate:,.0f} successful RPS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--origin", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--gateway", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--origin-cpu", default="2")
    parser.add_argument("--gateway-cpus", default="4,6")
    parser.add_argument("--load-cpus", default="8,10")
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--connections", type=int, default=64)
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--seconds", type=int, default=6)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--soak", type=int, default=0, help="Optional gateway-only run in seconds")
    args = parser.parse_args()
    if args.origin is not None:
        asyncio.run(origin(args.origin))
    else:
        if args.gateway is None or args.output is None:
            parser.error("--gateway and --output are required")
        if (min(args.seconds, args.warmup, args.repetitions, args.threads) < 1 or
                args.connections < args.threads or args.soak < 0):
            parser.error("durations, repetitions and threads must be positive, and connections must be at least threads")
        run(args)
