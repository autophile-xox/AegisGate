"""Fixed-rate comparison of Redis authorization, leases, and disabled admission."""

import argparse
import asyncio
from collections import Counter
from contextlib import ExitStack
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import time

from m1_smoke import sample
from m2_resilience import HOST, ROOT, fixed_rate, free_ports, percentile, running


MODES = ("disabled", "per_request", "leased")


def config(port, origin, redis, mode, instance, *, capacity=100000, rate=10000,
           batch=None, fallback=False):
    text = (f"listen {HOST} {port}\nworkers 2\n"
            f"backend origin {HOST} {origin} 256 128 256\nroute * / origin\n")
    if mode == "disabled":
        return text
    if batch is None:
        batch = 64 if mode == "leased" else 1
    text += (f"redis {HOST} {redis}\nrate_namespace {instance}\n"
             f"quota global {capacity} {rate} {batch}\n"
             "rate_lease_ms 4000\nrate_timeout_ms 80\nrate_wait_ms 40\nrate_diagnostics on\n")
    if fallback:
        text += "rate_failure fallback\nrate_fallback 32 0\n"
    return text


def stats_file(log):
    totals = {}
    for line in log.read_text().splitlines():
        if line.startswith(("rate_summary ", "rate_pool ")):
            for name, value in re.findall(r"([a-z_]+)=(\d+)", line):
                totals[name] = totals.get(name, 0) + int(value)
    if totals.get("checks"):
        totals["mean_admission_ns"] = totals["admission_ns"] / totals["checks"]
    if totals.get("commands"):
        totals["mean_reservation_ns"] = totals["reservation_ns"] / totals["commands"]
    if totals.get("granted"):
        totals["lease_utilization"] = totals["spent"] / totals["granted"]
    return totals


def summarize(data, seconds):
    rows, dropped, elapsed = data
    successful = [row["latency_ms"] for row in rows if row["status"] == 200]
    return {
        "offered": len(rows) + dropped,
        "generator_dropped": dropped,
        "elapsed_s": elapsed,
        "statuses": dict(Counter(row["status"] for row in rows)),
        "rejection_reasons": dict(Counter(row["detail"] for row in rows if row["detail"])),
        "successful": len(successful),
        "successful_rps": len(successful) / seconds,
        "successful_latency_ms": {name: percentile(successful, rank)
                                  for name, rank in (("p50", .50), ("p95", .95), ("p99", .99),
                                                     ("p99_9", .999))},
        "launch_p99_ms": percentile([row["launch_lag_ms"] for row in rows], .99),
    }


def save_trial(out, name, data, seconds):
    (out / f"{name}-requests.json").write_text(json.dumps(data[0]) + "\n")
    return summarize(data, seconds)


def launch_gateway(args, out, name, text, port):
    path = out / f"{name}.conf"
    path.write_text(text)
    command = ["taskset", "-c", args.gateway_cpus, str(args.gateway.resolve()), str(path)]
    return running(command, port, out / f"{name}.log")


def redis_cli(port, *command):
    return subprocess.check_output(["redis-cli", "-h", HOST, "-p", str(port), "--raw", *command],
                                   timeout=3, text=True)


def eval_count(port):
    value = re.search(r"cmdstat_eval:calls=(\d+)", redis_cli(port, "INFO", "commandstats"))
    return int(value[1]) if value else 0


def samples(processes):
    return {name: sample(process) for name, process in processes.items()}


def compare(args, out, processes, name, mode, ports):
    redis_port, origin_port, port = ports
    text = config(port, origin_port, redis_port, mode, name)
    with launch_gateway(args, out, name, text, port) as gateway:
        processes = {**processes, "gateway": gateway}
        warmup = asyncio.run(fixed_rate(port, args.rate, 1, args.connections))
        save_trial(out, name + "-warmup", warmup, 1)
        before_eval = eval_count(redis_port)
        before = samples(processes)
        start = time.monotonic()
        data = asyncio.run(fixed_rate(port, args.rate, args.seconds, args.connections))
        wall = time.monotonic() - start
        after = samples(processes)
        result = save_trial(out, name, data, args.seconds)
        result["redis_evals"] = eval_count(redis_port) - before_eval
        for name_, resource in after.items():
            resource["cpu_one_core_percent"] = (
                100 * (resource["cpu_seconds"] - before[name_]["cpu_seconds"]) / wall)
        result["process_resources"] = after
    result["diagnostics"] = stats_file(out / f"{name}.log")
    result["mode"] = mode
    result["name"] = name
    print(f"{name} completed {result['successful']}/{result['offered']} requests successfully",
          flush=True)
    return result


async def paired_requests(ports, rate, connections):
    return await asyncio.gather(*(fixed_rate(port, rate, 2, connections) for port in ports))


def quota_accuracy(args, out, redis, origin):
    capacity = 257
    ports = free_ports(2)
    with ExitStack() as stack:
        for index, port in enumerate(ports):
            text = config(port, origin, redis, "leased", "shared", capacity=capacity, rate=0, batch=16)
            stack.enter_context(launch_gateway(args, out, f"shared-{index}", text, port))
        data = asyncio.run(paired_requests(ports, args.rate, args.connections))
        gateways = [save_trial(out, f"shared-{index}", trial, 2) for index, trial in enumerate(data)]
    remaining = int(redis_cli(redis, "HGET", "shared:global", "tokens").strip())
    return {"capacity": capacity, "redis_remaining_millitokens": remaining, "gateways": gateways}


def disruption(args, out, name, redis, origin, *, pause=False, fallback=False):
    port = free_ports(1)[0]
    text = config(port, origin, redis, "leased", name, batch=8, fallback=fallback)
    with launch_gateway(args, out, name, text, port):
        if pause:
            redis_cli(redis, "CLIENT", "PAUSE", "350", "ALL")
        seconds = 1.5
        result = save_trial(out, name, asyncio.run(fixed_rate(port, args.rate, seconds, args.connections)),
                            seconds)
    result["diagnostics"] = stats_file(out / f"{name}.log")
    return result


def run(args):
    out = args.output.resolve()
    out.mkdir(parents=True)
    redis, origin, control, gateway = free_ports(4)
    origin_command = ["taskset", "-c", args.origin_cpu, str(args.simulator.resolve()), "-name", "fast",
                      "-listen", f"{HOST}:{origin}", "-control", f"{HOST}:{control}"]
    redis_command = ["taskset", "-c", args.redis_cpu, "redis-server", "--bind", HOST, "--port", str(redis),
                     "--dir", str(out), "--save", "", "--appendonly", "no"]
    result = {
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "working_tree": subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT,
                                                text=True).strip(),
        "os": platform.freedesktop_os_release()["PRETTY_NAME"],
        "kernel": platform.release(),
        "cpu": subprocess.check_output(["lscpu"], text=True),
        "memory": Path("/proc/meminfo").read_text().splitlines()[0],
        "rate": args.rate, "seconds": args.seconds, "repetitions": args.repetitions,
        "connection_slots": args.connections, "load_cpus": sorted(os.sched_getaffinity(0)),
        "redis_command": redis_command, "origin_command": origin_command, "gateway_cpus": args.gateway_cpus,
        "trials": [],
    }
    with running(origin_command, control, out / "origin.log") as backend:
        with running(redis_command, redis, out / "redis.log") as store:
            for index in range(args.repetitions):
                order = MODES[index % len(MODES):] + MODES[:index % len(MODES)]
                for mode in order:
                    row = compare(args, out, {"origin": backend, "redis": store},
                                  f"{index + 1:02}-{mode}", mode, (redis, origin, gateway))
                    result["trials"].append(row)
            result["quota_accuracy"] = quota_accuracy(args, out, redis, origin)
            result["slow_redis"] = disruption(args, out, "redis-paused", redis, origin, pause=True)
        result["fail_closed"] = disruption(args, out, "redis-down-closed", redis, origin)
        result["fallback"] = disruption(args, out, "redis-down-fallback", redis, origin, fallback=True)
    (out / "results.json").write_text(json.dumps(result, indent=2) + "\n")
    shared = result["quota_accuracy"]
    print(f"shared quota: {sum(row['successful'] for row in shared['gateways'])}/{shared['capacity']}",
          flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway", type=Path, required=True)
    parser.add_argument("--simulator", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rate", type=int, default=750)
    parser.add_argument("--seconds", type=float, default=4)
    parser.add_argument("--connections", type=int, default=128)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--gateway-cpus", default="4,6")
    parser.add_argument("--origin-cpu", default="2")
    parser.add_argument("--redis-cpu", default="0")
    args = parser.parse_args()
    if min(args.rate, args.connections, args.repetitions) < 1 or args.seconds < 1:
        parser.error("positive rate, connections and repetitions and seconds >= 1 required")
    run(args)
