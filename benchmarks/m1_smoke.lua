function done(summary, latency)
    print("AEGISGATE_RESULT " .. table.concat({
        summary.duration, summary.requests, summary.errors.status,
        summary.errors.connect, summary.errors.read, summary.errors.write, summary.errors.timeout,
        latency:percentile(50), latency:percentile(95),
        latency:percentile(99), latency:percentile(99.9)
    }, ","))
end
