"""Canonical Lua scripts for TaskQ rate-limit backends.

Script bodies are stored as ``Final[bytes]`` module-level constants so that
no file I/O occurs at import time and the byte-for-byte match against the
canonical source can be asserted in CI.  ``redis-py``'s ``register_script()``
consumes these directly — the bytes are sent verbatim to Redis.

Time domain: every script derives ``now`` from ``redis.call('TIME')`` — it
is NEVER passed as ARGV.  The admission state these scripts measure is
shared by the whole fleet, so the clock that stamps and measures it must be
shared too: a caller-supplied now lets one node's clock skew shift every
window boundary and TAT it writes (over-admission up to skew/window across
nodes).  ``TIME`` is non-deterministic, which is safe here because Redis
replicates script *effects* since Redis 5 (see the EVAL docs,
https://redis.io/docs/latest/commands/eval/).

ARGV contract for the token-bucket script:

* ``ARGV[1]`` = capacity      (float)
* ``ARGV[2]`` = refill_per_second (float; must be > 0 when the denial branch is reached)
* ``ARGV[3]`` = requested_tokens (float; default 1.0)
* ``ARGV[4]`` = ttl_seconds   (integer; ``math.ceil(capacity/refill*2)+60``)

ARGV contract for the sliding-window log script:

* ``ARGV[1]`` = window_ms    (integer milliseconds, e.g. 60000 for 60 s)
* ``ARGV[2]`` = limit        (integer, e.g. 60)
* ``ARGV[3]`` = request_id   (UUID7 string — unique member, prevents sub-ms collision)
* ``ARGV[4]`` = ttl_ms       (integer ms for PEXPIRE; default 2*window_ms + 60_000)

ARGV contract for the sliding-window GCRA script:

* ``ARGV[1]`` = emission_interval_ms (window_ms / limit, may be float)
* ``ARGV[2]`` = delay_tolerance_ms    (window_ms, integer)
* ``ARGV[3]`` = quantity_ms           (1 * emission_interval_ms for cost=1)
* ``ARGV[4]`` = ttl_ms                (window_ms + 60_000 default)

When allowed, the script returns {1, 0, remaining_estimate, pre_acquire_tat, post_acquire_tat}
where pre_acquire_tat and post_acquire_tat are string representations of the TAT
before and after the acquire, used for compare-and-set refunds.

ARGV contract for the token-bucket refund script:

* ``ARGV[1]`` = refund_amount (float — tokens to add back, NOT decision.remaining)
* ``ARGV[2]`` = capacity     (float — bucket cap; prevents over-refund)
* ``ARGV[3]`` = refill_per_second (float — mirrors the acquire script's
  elapsed-refill step so a refund does not lose accrued refill)
"""

from typing import Final

_LUA_SRC: Final[str] = """\
-- KEYS[1] = bucket key (format: taskq:{schema}:rl:tb:{bucket_name})
-- ARGV[1] = capacity    (float)
-- ARGV[2] = refill_per_second (float; must be > 0)
-- ARGV[3] = requested_tokens (float; default 1.0)
-- ARGV[4] = ttl_seconds (integer; math.ceil(capacity/refill*2)+60)
--
-- now is read from redis.call('TIME') — the shared admission state must
-- be stamped and measured by the store's own clock, never a caller's.
--
-- Returns: {allowed, tokens_remaining, retry_after_seconds}
--   allowed         = 1 if granted, 0 if denied
--   tokens_remaining = current token count after the operation (string to
--                      preserve fractional part; Redis truncates Lua numbers
--                      to integers on return)
--   retry_after_seconds = "0" when allowed; seconds until `requested_tokens`
--                         are available when denied (string for same reason)
local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill   = tonumber(ARGV[2])
local req      = tonumber(ARGV[3])
local ttl      = tonumber(ARGV[4])

-- TIME returns {seconds, microseconds}; combine into a float epoch.
local time = redis.call('TIME')
local now  = tonumber(time[1]) + tonumber(time[2]) / 1000000

-- Read current state. data[1]=tokens, data[2]=ts (last-refill timestamp).
local data   = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])

-- First-time initialization: empty key → full bucket at current time.
if tokens == nil then
  tokens = capacity
  ts = now
end

-- Refill: clamp elapsed to ≥0 to guard against backward clock jitter.
local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + elapsed * refill)

-- Attempt acquisition.
local allowed     = 0
local retry_after = 0
if tokens >= req then
  tokens  = tokens - req
  allowed = 1
else
  if refill > 0 then
    retry_after = (req - tokens) / refill
  else
    retry_after = 0
  end
end

-- tostring() on the stored values normalizes the stored encoding to a
-- decimal string and bounds precision at Lua's %.14g number formatting.
-- This is behaviorally inert on every supported store (Redis 5/6.2/7 and
-- Dragonfly pass number arguments through untruncated — the integer
-- truncation documented for EVAL applies only to RETURNED numbers, which
-- the return statement below already tostring()s for that reason).
redis.call('HMSET', key, 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('EXPIRE', key, ttl)
-- tostring() is required: Redis RESP2 converts Lua numbers to integers
-- by truncation (removing the decimal part). Returning floats as strings
-- preserves the fractional part so the Python caller receives accurate
-- tokens_remaining and retry_after_seconds values.  See Redis EVAL docs,
-- "Lua to RESP2 type conversion: Lua number -> integer reply".
return {allowed, tostring(tokens), tostring(retry_after)}
"""

TOKEN_BUCKET_SCRIPT: Final[bytes] = _LUA_SRC.encode("utf-8")

_SLIDING_WINDOW_LOG_SRC: Final[str] = """\
-- KEYS[1] = window_key  (e.g. "taskq:myschema:sw:{vendor_x_per_min}")
-- ARGV[1] = window_ms   (integer milliseconds, e.g. 60000 for 60 s)
-- ARGV[2] = limit       (integer, e.g. 60)
-- ARGV[3] = request_id  (UUID7 string — unique member, prevents sub-ms collision)
-- ARGV[4] = ttl_ms      (integer ms for PEXPIRE; default 2*window_ms + 60_000)
--
-- now_ms is derived from redis.call('TIME') — never a caller-supplied
-- ARGV — so every node's window boundary and ZADD score live in the same
-- clock domain as the shared sorted set.
--
-- Returns: {allowed, count, retry_after_ms}
--   allowed        = 1 if granted, 0 if denied
--   count          = window count after the operation (includes this acquire if allowed)
--   retry_after_ms = 0 when allowed; ms until oldest entry leaves the window when denied
local key    = KEYS[1]
local window = tonumber(ARGV[1])
local limit  = tonumber(ARGV[2])
local req_id = ARGV[3]
local ttl    = tonumber(ARGV[4])

-- TIME returns {seconds, microseconds}; combine into float milliseconds.
local time = redis.call('TIME')
local now  = (tonumber(time[1]) + tonumber(time[2]) / 1000000) * 1000

-- Step 1: evict entries older than the rolling window boundary.
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

-- Step 2: count entries currently in the window.
local count = redis.call('ZCARD', key)

-- Step 3: deny if at or over limit; compute retry_after from oldest entry.
if count >= limit then
  -- ZRANGE returns [member, score, ...]; oldest entry score is at index 2.
  local oldest         = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  local retry_after_ms = tonumber(oldest[2]) + window - now
  -- Refresh TTL even on denial so live entries are not orphaned by key expiry.
  redis.call('PEXPIRE', key, ttl)
  return {0, count, retry_after_ms}
end

-- Step 4: record this acquire (score = now_ms, member = unique request_id).
redis.call('ZADD', key, now, req_id)

-- Step 5: refresh TTL on every allowed acquire.
redis.call('PEXPIRE', key, ttl)
return {1, count + 1, 0}
"""

SLIDING_WINDOW_LOG_SCRIPT: Final[bytes] = _SLIDING_WINDOW_LOG_SRC.encode("utf-8")

_SLIDING_WINDOW_GCRA_SRC: Final[str] = """\
-- Source: https://github.com/Losant/redis-gcra/blob/main/lib/lua/gcra.lua
-- Algorithm: Brandur Leach, "Rate Limiting, Cells, and GCRA"
--   https://brandur.org/rate-limiting
-- Upstream commit: 4f0d73ce3a979ee917227e09faad4a0d357294be
-- TaskQ deviations from upstream: now is read from redis.call('TIME')
-- rather than a caller-supplied ARGV — the TAT is shared fleet state, so
-- the clock that advances it must be the store's own; a caller's skewed
-- now would poison the shared admission boundary for every other node.
-- TIME is non-deterministic, which is replication-safe because Redis
-- replicates script EFFECTS since Redis 5 (see the EVAL docs,
-- https://redis.io/docs/latest/commands/eval/). Further deviations:
-- millisecond arithmetic throughout; PEXPIRE instead of EXPIRE; return
-- shape includes pre/post TAT strings for compare-and-set refunds.
--
-- KEYS[1] = bucket key (format: taskq:{schema}:sw_gcra:{bucket_name})
-- ARGV[1] = emission_interval_ms (window_ms / limit, integer)
-- ARGV[2] = delay_tolerance_ms   (window_ms, integer)
-- ARGV[3] = quantity_ms          (1 * emission_interval_ms for cost=1)
-- ARGV[4] = ttl_ms               (window_ms + 60_000 default)
local key = KEYS[1]
local emission_interval = tonumber(ARGV[1])
local delay_tolerance   = tonumber(ARGV[2])
local quantity          = tonumber(ARGV[3])
local ttl               = tonumber(ARGV[4])

-- TIME returns {seconds, microseconds}; combine into float milliseconds.
local time = redis.call('TIME')
local now  = (tonumber(time[1]) + tonumber(time[2]) / 1000000) * 1000

local tat_str = redis.call('GET', key)
local tat
if not tat_str then
  tat = now
else
  tat = tonumber(tat_str)
end

if tat < now then tat = now end

local new_tat   = tat + quantity
local allow_at  = new_tat - delay_tolerance

if now < allow_at then
  -- denied: TAT not advanced; refresh TTL so the key doesn't expire mid-deny
  redis.call('PEXPIRE', key, ttl)
  return {0, allow_at - now, 0}
end

-- allowed: persist new_tat with TTL
redis.call('SET', key, tostring(new_tat), 'PX', ttl)
local remaining_estimate = math.floor((delay_tolerance - (new_tat - now)) / emission_interval)
if remaining_estimate < 0 then remaining_estimate = 0 end
return {1, 0, remaining_estimate, tostring(tat), tostring(new_tat)}
"""

SLIDING_WINDOW_GCRA_SCRIPT: Final[bytes] = _SLIDING_WINDOW_GCRA_SRC.encode("utf-8")

_REFUND_SRC: Final[str] = """\
-- Refund script (rollback path only — do NOT call after actor completes).
-- KEYS[1] = bucket key
-- ARGV[1] = refund_amount (float — tokens to add back)
-- ARGV[2] = capacity (float — bucket cap; prevents over-refund)
-- ARGV[3] = refill_per_second (float — must mirror the acquire script's
--           refill rate so a refund does not clobber accrued-but-unread
--           refill; parity with _InMemoryBucket.refund, which always
--           refunds against tokens computed with elapsed * refill applied)
--
-- now is read from redis.call('TIME') — the elapsed-refill step must run
-- in the same clock domain the acquire script stamped ts in.
local key      = KEYS[1]
local refund   = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local refill   = tonumber(ARGV[3])

local time = redis.call('TIME')
local now  = tonumber(time[1]) + tonumber(time[2]) / 1000000

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil then return {0, 0} end

-- Apply the same elapsed-refill step the acquire script applies, so a
-- refund landing after idle time does not lose the tokens that would
-- have accrued between the last write and now.
local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + elapsed * refill)

tokens = math.min(capacity, tokens + refund)
-- tostring() on the stored values normalizes the stored encoding to a
-- decimal string and bounds precision at Lua's %.14g number formatting;
-- behaviorally inert on every supported store (number arguments are
-- passed through untruncated — only RETURNED numbers are truncated to
-- integers, and the return value below is already tostring()'d).
redis.call('HMSET', key, 'tokens', tostring(tokens), 'ts', tostring(now))
return {1, tostring(tokens)}
"""

REFUND_SCRIPT: Final[bytes] = _REFUND_SRC.encode("utf-8")

_GCRA_REFUND_SRC: Final[str] = """\
-- GCRA refund script (compare-and-set for rollback path).
-- KEYS[1] = bucket key
-- ARGV[1] = pre_acquire_tat_str  (string: the TAT before our acquire)
-- ARGV[2] = post_acquire_tat_str (string: the TAT we set during our acquire)
-- ARGV[3] = ttl_ms               (integer ms for PEXPIRE)
local key = KEYS[1]
local pre_acquire  = ARGV[1]
local post_acquire = ARGV[2]
local ttl          = tonumber(ARGV[3])

local existing = redis.call('GET', key)
if not existing then
  return {0}
end

if existing ~= post_acquire then
  return {0}
end

redis.call('SET', key, pre_acquire, 'PX', ttl)
return {1}
"""

GCRA_REFUND_SCRIPT: Final[bytes] = _GCRA_REFUND_SRC.encode("utf-8")
