"""Compare routing at fixed offered load and exercise recovery on local Go origins."""

import argparse
import asyncio
from collections import Counter
from contextlib import ExitStack
import http.client
import json
import math
import os
from pathlib import Path
import platform
import re
import socket
import subprocess
import time

from m1_smoke import HOST, ROOT, running


PAYLOAD = b"aegisgate" * 16
HEALTHY = {"delay_ms": 1}
POLICIES = ("round_robin", "peak_ewma")


def now_ms():
    return time.monotonic_ns() // 1_000_000


def free_ports(count):
    with ExitStack() as stack:
        listeners = [stack.enter_context(socket.socket()) for _ in range(count)]
        for listener in listeners:
            listener.bind((HOST, 0))
        return [listener.getsockname()[1] for listener in listeners]


def control(port, path, plan=None):
    connection = http.client.HTTPConnection(HOST, port, timeout=2)
    try:
        if plan is None:
            connection.request("GET", path)
        else:
            connection.request("POST", path, json.dumps(plan), {"Content-Type": "application/json"})
        response = connection.getresponse()
        body = response.read()
        if response.status != 200:
            raise RuntimeError(f"simulator control returned {response.status}: {body!r}")
        return json.loads(body)
    finally:
        connection.close()


def set_fault(port, plan):
    started = now_ms()
    applied = control(port, "/faults", plan)
    return {"request_ms": started, "ack_ms": now_ms(), "plan": applied}


class Connection:
    def __init__(self, port):
        self.port = port
        self.reader = self.writer = None

    def close(self):
        if self.writer is not None:
            self.writer.close()
        self.reader = self.writer = None

    async def get(self, path):
        if self.writer is None:
            self.reader, self.writer = await asyncio.open_connection(HOST, self.port)
        self.writer.write(f"GET {path} HTTP/1.1\r\nHost: benchmark.test\r\n\r\n".encode())
        await self.writer.drain()
        start_line, *lines = (await self.reader.readuntil(b"\r\n\r\n"))[:-4].split(b"\r\n")
        status = int(start_line.split()[1])
        headers = {key.lower(): value.strip()
                   for key, value in (line.split(b":", 1) for line in lines)}
        body = await self.reader.readexactly(int(headers[b"content-length"]))
        backend = headers.get(b"x-backend-id", b"").decode()
        if headers.get(b"connection", b"").lower() == b"close":
            self.close()
        if status == 200 and (body != PAYLOAD or backend not in {"fast", "slow"}):
            raise ValueError("invalid successful response")
        return status, backend, headers.get(b"x-aegisgate-rate-limit", b"").decode()


async def fixed_rate(port, rate, seconds, concurrency):
    slots = [Connection(port) for _ in range(concurrency)]
    outstanding = set()
    samples = []
    start = time.monotonic()

    async def request(slot, due, launched):
        status, backend, detail = 0, "", ""
        try:
            status, backend, detail = await asyncio.wait_for(slot.get("/data"), 2)
        except (OSError, EOFError, ValueError, KeyError, IndexError, asyncio.TimeoutError,
                asyncio.LimitOverrunError) as error:
            detail = type(error).__name__
            slot.close()
        finally:
            slots.append(slot)
        samples.append({"status": status, "backend": backend, "detail": detail,
                        "latency_ms": (time.monotonic() - due) * 1000,
                        "launch_lag_ms": max(0, launched - due) * 1000})

    dropped = 0
    for index in range(round(rate * seconds)):
        due = start + index / rate
        wait = due - time.monotonic()
        if wait > 0:
            await asyncio.sleep(wait)
        if not slots:
            dropped += 1
            continue
        task = asyncio.create_task(request(slots.pop(), due, time.monotonic()))
        outstanding.add(task)
        task.add_done_callback(outstanding.discard)
    if outstanding:
        await asyncio.gather(*outstanding)
    for slot in slots:
        slot.close()
    return samples, dropped, time.monotonic() - start


def percentile(values, rank):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * rank) - 1)] if ordered else None


def phase(args, output, name, port, controllers, seconds):
    before = {name: control(control_port, "/stats") for name, control_port in controllers.items()}
    samples, dropped, elapsed = asyncio.run(fixed_rate(port, args.rate, seconds, args.connections))
    after = {name: control(control_port, "/stats") for name, control_port in controllers.items()}
    success = [item for item in samples if item["status"] == 200]
    latency = [item["latency_ms"] for item in success]
    (output / f"{name}.json").write_text(json.dumps(samples) + "\n")
    return {
        "offered": round(args.rate * seconds),
        "offered_rps": args.rate,
        "duration_s": seconds,
        "elapsed_through_drain_s": elapsed,
        "generator_dropped": dropped,
        "generator_launch_p99_ms": percentile([item["launch_lag_ms"] for item in samples], .99),
        "successful": len(success),
        "successful_rps": len(success) / seconds,
        "status_counts": dict(Counter(item["status"] for item in samples)),
        "gateway_503": sum(item["status"] == 503 and not item["backend"] for item in samples),
        "successful_latency_ms": {name: percentile(latency, rank) for name, rank in
                                  (("p50", .50), ("p95", .95), ("p99", .99), ("p99_9", .999))},
        "successful_by_backend": dict(Counter(item["backend"] for item in success)),
        "upstream_deltas": {
            name: {key: value - before[name][key] for key, value in counts.items()}
            for name, counts in after.items()
        },
    }


def gateway_config(port, fast, slow, policy, resilience):
    return (
        f"listen {HOST} {port}\nworkers 1\n"
        f"backend fast {HOST} {fast} 256 128 256\nbackend slow {HOST} {slow} 256 128 256\n"
        f"group origins {policy} fast slow\nroute * / origins\n"
        "ewma_decay_ms 300\newma_stale_ms 1500\newma_initial_ms 4\n"
        "connect_timeout_ms 250\nresponse_timeout_ms 1250\n"
        + ("circuit_breaker on\ncircuit_window 6\ncircuit_min_samples 3\n"
           "circuit_failure_percent 50\ncircuit_cooldown_ms 400\n"
           "circuit_max_probes 1\ncircuit_recovery_successes 2\n"
           "health_interval_ms 80\nhealth_timeout_ms 150\nhealth_failures 2\nhealth_successes 2\n"
           if resilience else "circuit_breaker off\nhealth_interval_ms 0\n")
    )


def transition(log, backend, field, states, since):
    pattern = re.compile(rf"backend={backend} {field}={states} reason=\S+ monotonic_ms=(\d+)")
    return next((int(match[1]) for match in pattern.finditer(log.read_text())
                 if int(match[1]) >= since), None)


def outage(args, output, gateway_port, log, controllers):
    fast, slow = controllers["fast"], controllers["slow"]
    result = {"healthy": phase(args, output, "outage-healthy", gateway_port, controllers, 1.0)}
    rejected = set_fault(slow, {"burst_503_every": 1, "burst_503_length": 1})
    result["brownout"] = phase(args, output, "outage-brownout", gateway_port, controllers, 1.5)
    circuit_open = transition(log, "slow", "circuit", "closed->open", rejected["request_ms"])

    # Measure a physical outage after the application-error phase while retaining the control plane.
    down = {"fast": set_fault(fast, {"unavailable": True}),
            "slow": set_fault(slow, {"unavailable": True})}
    give_up = time.monotonic() + 3
    detected = {}
    while len(detected) != len(controllers):
        for label, event in down.items():
            tick = transition(log, label, "health", "healthy->unhealthy", event["request_ms"])
            if tick is not None:
                detected[label] = tick - event["request_ms"]
        if time.monotonic() >= give_up:
            raise RuntimeError(f"active health did not isolate both listeners. See {log}")
        time.sleep(0.01)
    result["all_down"] = phase(args, output, "outage-all-down", gateway_port, controllers, 0.5)

    up = {"fast": set_fault(fast, HEALTHY), "slow": set_fault(slow, HEALTHY)}
    result["recovery"] = phase(args, output, "outage-recovery", gateway_port, controllers, 1.5)
    recovered = transition(log, "slow", "circuit", "half_open->closed", up["slow"]["request_ms"])
    result["events"] = {"brownout": rejected, "listener_down": down, "listener_up": up,
                        "circuit_detection_ms": circuit_open - rejected["request_ms"] if circuit_open else None,
                        "active_detection_ms": detected,
                        "slow_circuit_recovery_ms": recovered - up["slow"]["request_ms"] if recovered else None}
    return result


def run(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    fast, slow, fast_control, slow_control, gateway = free_ports(5)
    controllers = {"fast": fast_control, "slow": slow_control}
    report = {
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "working_tree": subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT,
                                                text=True).strip(),
        "kernel": platform.release(),
        "os": platform.freedesktop_os_release()["PRETTY_NAME"],
        "cpu_topology": subprocess.check_output(["lscpu"], text=True),
        "memory": Path("/proc/meminfo").read_text().splitlines()[0],
        "rate": args.rate, "connections": args.connections, "slow_ms": args.slow_ms,
        "gateway_cpu": args.gateway_cpu, "fast_cpu": args.fast_cpu, "slow_cpu": args.slow_cpu,
        "load_cpus": sorted(os.sched_getaffinity(0)), "trials": [],
    }
    with ExitStack() as stack:
        for name, port, cpu in (("fast", fast, args.fast_cpu), ("slow", slow, args.slow_cpu)):
            command = ["taskset", "-c", cpu, str(args.simulator.resolve()), "-name", name,
                       "-listen", f"{HOST}:{port}", "-control", f"{HOST}:{controllers[name]}",
                       "-delay-ms", str(HEALTHY["delay_ms"])]
            stack.enter_context(running(command, controllers[name], output / f"origin-{name}.log"))
        for repetition in range(args.repetitions):
            policies = POLICIES if repetition % 2 == 0 else reversed(POLICIES)
            for policy in policies:
                for controller in controllers.values():
                    set_fault(controller, HEALTHY)
                name = f"{repetition + 1:02}-{policy}"
                config = output / f"{name}.conf"
                config.write_text(gateway_config(gateway, fast, slow, policy, False))
                command = ["taskset", "-c", args.gateway_cpu, str(args.gateway.resolve()), str(config)]
                with running(command, gateway, output / f"{name}.log"):
                    warmup = phase(args, output, name + "-healthy", gateway, controllers, 1.0)
                    fault = set_fault(slow_control, {"delay_ms": args.slow_ms})
                    measured = phase(args, output, name + "-slowdown", gateway, controllers, args.seconds)
                report["trials"].append({"policy": policy, "healthy": warmup, "slowdown": measured,
                                         "fault": fault})
                p99 = measured["successful_latency_ms"]["p99"]
                latency = f"{p99:.2f} ms" if p99 is not None else "unavailable"
                print(f"{name}: {measured['successful']}/{measured['offered']} successful, "
                      f"p99 {latency}", flush=True)
        for controller in controllers.values():
            set_fault(controller, HEALTHY)
        config = output / "outage.conf"
        log = output / "outage.log"
        config.write_text(gateway_config(gateway, fast, slow, "round_robin", True))
        command = ["taskset", "-c", args.gateway_cpu, str(args.gateway.resolve()), str(config)]
        with running(command, gateway, log):
            report["outage"] = outage(args, output, gateway, log, controllers)
    (output / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    answers = report["outage"]
    print("outage transitions:", answers["events"], flush=True)
    if (answers["events"]["circuit_detection_ms"] is None or
            answers["events"]["slow_circuit_recovery_ms"] is None or
            answers["all_down"]["gateway_503"] != answers["all_down"]["offered"]):
        raise RuntimeError(f"outage observations did not meet expected outcomes. See {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway", required=True, type=Path)
    parser.add_argument("--simulator", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--rate", type=int, default=500)
    parser.add_argument("--connections", type=int, default=128)
    parser.add_argument("--seconds", type=float, default=4)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--slow-ms", type=int, default=40)
    parser.add_argument("--fast-cpu", default="2")
    parser.add_argument("--gateway-cpu", default="4")
    parser.add_argument("--slow-cpu", default="6")
    args = parser.parse_args()
    if min(args.rate, args.connections, args.repetitions) < 1 or args.seconds < 1 or not 2 <= args.slow_ms <= 1000:
        parser.error("positive rate, connections and repetitions, seconds >= 1, and slow-ms in 2..1000 required")
    run(args)
