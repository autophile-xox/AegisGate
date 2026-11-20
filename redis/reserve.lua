-- Redis 7 executes the entire read/refill/debit/write atomically.
local capacity, rate, wanted = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local function integer(value, low, high)
    return value and value == math.floor(value) and value >= low and value <= high
end
if #KEYS ~= 1 or #ARGV ~= 3 or not integer(capacity, 1, 1000000000)
    or not integer(rate, 0, 1000000000) or not integer(wanted, 1, capacity) then
    return redis.error_reply("invalid reservation arguments")
end

local clock = redis.call("TIME")
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local limit = capacity * 1000
local state = redis.call("HMGET", KEYS[1], "tokens", "time", "capacity", "rate")
local tokens, last = limit, now
if state[1] or state[2] or state[3] or state[4] then
    tokens, last = tonumber(state[1]), tonumber(state[2])
    if tonumber(state[3]) ~= capacity or tonumber(state[4]) ~= rate
        or not integer(tokens, 0, limit) or not integer(last, 0, 8999999999999) then
        return redis.error_reply("bucket state or configuration mismatch")
    end
end
-- Clamp clock regressions and elapsed time before multiplying to keep exact integers.
now = math.max(now, last)
if rate > 0 then
    local elapsed = math.min(now - last, math.ceil(limit / rate))
    tokens = math.min(limit, tokens + elapsed * rate)
end
local grant = math.min(wanted, math.floor(tokens / 1000))
tokens = tokens - grant * 1000
redis.call("HSET", KEYS[1], "tokens", tokens, "time", now, "capacity", capacity, "rate", rate)

local wait = 0
if tokens < 1000 then
    wait = rate == 0 and -1 or math.ceil((1000 - tokens) / rate)
end
return {grant, wait}
