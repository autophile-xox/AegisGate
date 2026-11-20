#!/usr/bin/env python3
"""Constant-rate forwarding, breaker, endurance, and syscall experiments."""

import argparse
import asyncio
from contextlib import ExitStack
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import time
from urllib.request import urlopen

from m1_smoke import sample
from m2_resilience import (HEALTHY, HOST, ROOT, fixed_rate, free_ports, gateway_config, now_ms,
                           phase, running, set_fault, transition)
from m3_rate_limit import config as rate_config, save_trial


def launch_gateway(args, out, name, text, port, *, trace=False):
    location = out / f"{name}.conf"
    location.write_text(text)
    command = ["taskset", "-c", args.gateway_cpus, str(args.gateway.resolve()), str(location)]
    if trace:
        command = ["strace", "-D", "-f", "-s", "0", "-e",
                   "trace=epoll_ctl,epoll_wait,recvfrom,sendto,connect,futex,brk,mmap",
                   "-o", str(out / "syscalls.txt"), *command]
    return running(command, port, out / f"{name}.log")


def forward_config(port, origin, metrics):
    return (f"listen {HOST} {port}\nmetrics_listen {HOST} {metrics}\nworkers 2\n"
            f"backend fast {HOST} {origin} 256 128 256\nroute * / fast\n")


def fixed_phase(args, out, name, port, rate, seconds, process):
    before = sample(process)
    start = time.monotonic()
    data = asyncio.run(fixed_rate(port, rate, seconds, args.connections))
    elapsed = time.monotonic() - start
    after = sample(process)
    after["cpu_one_core_percent"] = (after["cpu_seconds"] - before["cpu_seconds"]) * 100 / elapsed
    row = save_trial(out, name, data, seconds)
    row["process"] = after
    row["rate"] = rate
    return row


def proxy(args, out, ports, origin):
    port = ports["gateway"]
    pairs = []
    with launch_gateway(args, out, "proxy", forward_config(port, ports["fast"], ports["metrics"]), port) as app:
        targets = [("direct", ports["fast"], origin), ("gateway", port, app)]
        for rate in args.rates:
            for rep in range(args.repetitions):
                for name, target, process in targets if rep % 2 == 0 else reversed(targets):
                    asyncio.run(fixed_rate(target, rate, 1, args.connections))
                    row = fixed_phase(args, out, f"proxy-{rate}-{rep + 1}-{name}", target,
                                      rate, args.seconds, process)
                    pairs.append({**row, "target": name, "repetition": rep + 1})
                    print(f"proxy {name} offered={rate} successful={row['successful_rps']:.0f}",
                          flush=True)
    return pairs


def breaker(args, out, ports):
    trials = []
    gateway = ports["gateway"]
    controls = {name: ports[f"{name}_control"] for name in ("fast", "slow")}
    for rep in range(args.repetitions):
        order = (False, True) if rep % 2 == 0 else (True, False)
        for enabled in order:
            for control in controls.values():
                set_fault(control, HEALTHY)
            name = f"breaker-{rep + 1}-{'on' if enabled else 'off'}"
            text = gateway_config(gateway, ports["fast"], ports["slow"], "round_robin", True)
            if not enabled:
                text = text.replace("circuit_breaker on", "circuit_breaker off")
            with launch_gateway(args, out, name, text, gateway):
                phase(args, out, name + "-warmup", gateway, controls, 1)
                fault = set_fault(controls["slow"], {"burst_503_every": 20, "burst_503_length": 12})
                burst = phase(args, out, name + "-burst", gateway, controls, args.seconds)
                restore = set_fault(controls["slow"], HEALTHY)
                recovery = phase(args, out, name + "-recovery", gateway, controls, 1.5)
            log = out / f"{name}.log"
            opened = transition(log, "slow", "circuit", "closed->open", fault["request_ms"])
            closed = transition(log, "slow", "circuit", "half_open->closed", restore["request_ms"])
            if enabled and (opened is None or closed is None):
                raise RuntimeError(f"expected an open and recovery transition in {log}")
            trials.append({"enabled": enabled, "burst": burst, "recovery": recovery,
                           "detection_ms": None if opened is None else opened - fault["request_ms"],
                           "recovery_ms": None if closed is None else closed - restore["request_ms"]})
            print(f"{name} completed {burst['successful']}/{burst['offered']}", flush=True)
    return trials


def live_sample(process, metrics):
    info = sample(process)
    proc = Path(f"/proc/{process.pid}/status").read_text()
    info["rss_kib"] = int(re.search(r"^VmRSS:\s+(\d+)", proc, re.MULTILINE)[1])
    with urlopen(f"http://{HOST}:{metrics}/metrics", timeout=2) as response:
        text = response.read().decode()
    for metric in ("aegisgate_http_connections", "aegisgate_pending_requests",
                   "aegisgate_redis_queue_depth", "aegisgate_backend_active_requests"):
        info[metric] = sum(int(value) for value in re.findall(
            rf"^{metric}(?:\{{[^\n]+\}})? (\d+)$", text, re.MULTILINE))
    return info


async def monitor_load(port, rate, seconds, connections, process, metrics):
    load = asyncio.create_task(fixed_rate(port, rate, seconds, connections))
    points = []
    start = time.monotonic()
    while not load.done():
        point = await asyncio.to_thread(live_sample, process, metrics)
        points.append({**point, "elapsed_s": time.monotonic() - start})
        await asyncio.wait({load}, timeout=1)
    return await load, points


def endurance(args, out, ports):
    redis = ports["redis"]
    gateway = ports["gateway"]
    metrics = ports["metrics"]
    command = ["taskset", "-c", args.redis_cpu, "redis-server", "--bind", HOST, "--port", str(redis),
               "--dir", str(out), "--save", "", "--appendonly", "no"]
    with running(command, redis, out / "redis.log"):
        text = rate_config(gateway, ports["fast"], redis, "leased", "endurance")
        text += f"metrics_listen {HOST} {metrics}\n"
        with launch_gateway(args, out, "endurance", text, gateway) as app:
            asyncio.run(fixed_rate(gateway, args.rate, 1, args.connections))
            data, points = asyncio.run(monitor_load(gateway, args.rate, args.endurance_seconds,
                                                    args.connections, app, metrics))
            time.sleep(0.25)
            settled = live_sample(app, metrics)
    (out / "endurance-resources.json").write_text(json.dumps(points, indent=2) + "\n")
    row = save_trial(out, "endurance", data, args.endurance_seconds)
    return {**row, "settled": settled}


def profile(args, out, ports):
    gateway = ports["gateway"]
    text = forward_config(gateway, ports["fast"], ports["metrics"])
    with launch_gateway(args, out, "profile", text, gateway, trace=True) as app:
        row = fixed_phase(args, out, "profile", gateway, args.rate, args.seconds, app)
    syscalls = (out / "syscalls.txt").read_text()
    row["peek_calls"] = syscalls.count("MSG_PEEK")
    print(f"Observed {row['peek_calls']} MSG_PEEK calls for {row['successful']} successes",
          flush=True)
    return row


def run(args):
    out = args.output.resolve()
    out.mkdir(parents=True)
    keys = ("fast", "slow", "fast_control", "slow_control", "gateway", "metrics", "redis")
    ports = dict(zip(keys, free_ports(len(keys))))
    report = {
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "working_tree": subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True),
        "kernel": platform.release(), "os": platform.freedesktop_os_release()["PRETTY_NAME"],
        "cpu": subprocess.check_output(["lscpu"], text=True),
        "memory": Path("/proc/meminfo").read_text().splitlines()[0],
        "compiler": subprocess.check_output(["g++", "--version"], text=True).splitlines()[0],
        "gateway_cpus": args.gateway_cpus, "fast_cpu": args.fast_cpu, "slow_cpu": args.slow_cpu,
        "redis_cpu": args.redis_cpu, "load_cpus": sorted(os.sched_getaffinity(0)),
        "rate": args.rate, "rates": args.rates, "seconds": args.seconds, "reps": args.repetitions,
        "connections": args.connections, "endurance_seconds": args.endurance_seconds,
    }
    with ExitStack() as stack:
        origins = {}
        for name, cpu in (("fast", args.fast_cpu), ("slow", args.slow_cpu)):
            command = ["taskset", "-c", cpu, str(args.simulator.resolve()), "-name", name,
                       "-listen", f"{HOST}:{ports[name]}", "-control", f"{HOST}:{ports[name + '_control']}"]
            origins[name] = stack.enter_context(running(command, ports[name], out / f"{name}.log"))
        if args.scenario in {"all", "proxy"}:
            report["proxy"] = proxy(args, out, ports, origins["fast"])
        if args.scenario in {"all", "breaker"}:
            report["breaker"] = breaker(args, out, ports)
        if args.scenario in {"all", "endurance"}:
            report["endurance"] = endurance(args, out, ports)
        if args.scenario == "profile":
            report["profile"] = profile(args, out, ports)
    (out / "results.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway", type=Path, required=True)
    parser.add_argument("--simulator", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--scenario", choices=("all", "proxy", "breaker", "endurance", "profile"),
                        default="all")
    parser.add_argument("--rates", type=lambda value: [int(rate) for rate in value.split(",")],
                        default=[750, 4000, 16000])
    parser.add_argument("--rate", type=int, default=750)
    parser.add_argument("--seconds", type=float, default=3)
    parser.add_argument("--endurance-seconds", type=float, default=60)
    parser.add_argument("--connections", type=int, default=128)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--gateway-cpus", default="4,6")
    parser.add_argument("--fast-cpu", default="2")
    parser.add_argument("--slow-cpu", default="1")
    parser.add_argument("--redis-cpu", default="0")
    args = parser.parse_args()
    if min(*args.rates, args.rate, args.seconds, args.endurance_seconds,
           args.connections, args.repetitions) <= 0:
        parser.error("rates, durations, connections and repetitions must be positive")
    run(args)
