#!/usr/bin/env python3
"""Send authenticated traffic and validate the live Prometheus/Grafana deployment."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import math
import os
import subprocess
import time
from urllib.parse import urlencode
from urllib.request import urlopen


def fetch(base, path):
    with urlopen(base + path, timeout=4) as response:
        return json.load(response)


def query(base, prefix, expression):
    result = fetch(base, f"{prefix}/api/v1/query?" + urlencode({"query": expression}))
    if result["status"] != "success":
        raise AssertionError(result)
    return [float(row["value"][1]) for row in result["data"]["result"]]


def eventually(test, description, seconds=20):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if test():
            return
        time.sleep(0.2)
    raise AssertionError(description)


def traffic(port, path, key, count):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=4)
    outcomes = []
    try:
        for _ in range(count):
            connection.request("GET", path, headers={"X-API-Key": key})
            response = connection.getresponse()
            response.read()
            outcomes.append(response.status)
    finally:
        connection.close()
    return outcomes


def change_origin(project, origin, plan):
    subprocess.run(
        ["docker", "compose", "-p", project, "exec", "-T", origin, "wget", "-q", "-O", "-",
         "--header", "Content-Type: application/json", "--post-data", json.dumps(plan),
         "http://127.0.0.1:9100/faults"],
        check=True, stdout=subprocess.DEVNULL, timeout=12,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="aegisgate")
    parser.add_argument("--prometheus", default="http://127.0.0.1:9090")
    parser.add_argument("--grafana", default="http://127.0.0.1:3000")
    args = parser.parse_args()
    key = os.environ["AEGISGATE_DEMO_TENANT_KEY"]

    def prom(expr):
        return query(args.prometheus, "", expr)

    def exercise(path, count):
        ports = (8080, 8081) * 4
        with ThreadPoolExecutor(max_workers=len(ports)) as pool:
            return list(pool.map(lambda port: traffic(port, path, key, count), ports))

    eventually(lambda: sorted(prom('up{job="aegisgate"}')) == [1, 1],
               "Prometheus did not scrape both gateway replicas")
    assert [traffic(port, "/data", key, 1)[0] for port in (8080, 8081)] == [200, 200]
    statuses = [status for lane in exercise("/limited", 12) for status in lane]
    assert 200 in statuses and 429 in statuses
    dashboard = fetch(args.grafana, "/api/dashboards/uid/aegisgate-overview")["dashboard"]
    panels = dashboard["panels"]
    assert len(panels) >= 10

    try:
        change_origin(args.project, "fast", {"health_status": 503})
        eventually(lambda: 0 in prom('aegisgate_backend_healthy{backend="fast"}'),
                   "Prometheus did not see active health isolation")
        change_origin(args.project, "slow", {"burst_503_every": 1, "burst_503_length": 1})
        statuses = [status for lane in exercise("/data", 12) for status in lane]
        assert 503 in statuses
        eventually(lambda: sum(prom('aegisgate_circuit_transitions_total{backend="slow",state="open"}')) > 0,
                   "Prometheus did not see an open circuit")
    finally:
        for origin in ("fast", "slow"):
            change_origin(args.project, origin, {"delay_ms": 1})

    def recovered():
        statuses = [status for lane in exercise("/data", 4) for status in lane]
        health = prom("aegisgate_backend_healthy")
        circuit = prom("aegisgate_circuit_state")
        return (all(s == 200 for s in statuses) and len(health) == 8 and min(health) == 1
                and len(circuit) == 8 and max(circuit) == 0)

    eventually(recovered, "gateway replicas did not recover")
    eventually(lambda: sum(prom("aegisgate_requests_successful_total")) >= 8,
               "successful counters did not reflect live traffic")
    proxy = "/api/datasources/proxy/uid/aegis-prometheus"
    for panel in panels:
        for target in panel["targets"]:
            values = query(args.grafana, proxy, target["expr"])
            if not any(math.isfinite(value) for value in values):
                raise AssertionError(f"no live samples for dashboard panel {panel['title']}")
    assert sum(prom("aegisgate_redis_commands_total")) > 0
    compose = ["docker", "compose", "-p", args.project, "exec", "-T"]
    sample = subprocess.check_output(
        [*compose, "gateway-a", "curl", "-fsS", "http://127.0.0.1:9091/metrics"], timeout=10)
    subprocess.run([*compose, "prometheus", "promtool", "check", "metrics"],
                   input=sample, check=True, timeout=10)
    print(f"Two live Prometheus targets, {len(panels)} Grafana panels, shared quota, "
          "backend isolation, and recovery verified.")


if __name__ == "__main__":
    main()
