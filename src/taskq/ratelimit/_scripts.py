"""Canonical Lua scripts for TaskQ rate-limit backends.

Script bodies are stored as ``Final[bytes]`` module-level constants so that
no file I/O occurs at import time and the byte-for-byte match against the
canonical source can be asserted in CI.  ``redis-py``'s ``register_script()``
consumes these directly — the bytes are sent verbatim to Redis.

ARGV contract for the token-bucket script:

* ``ARGV[1]`` = now_seconds   (float, from Python ``Clock.now().timestamp()``)
* ``ARGV[2]`` = capacity      (float)
* ``ARGV[3]`` = refill_per_second (float; must be > 0 when the denial branch is reached)
* ``ARGV[4]`` = requested_tokens (float; default 1.0)
* ``ARGV[5]`` = ttl_seconds   (integer; ``math.ceil(capacity/refill*2)+60``)

ARGV contract for the sliding-window log script:

* ``ARGV[1]`` = now_ms       (integer milliseconds from Python Clock)
* ``ARGV[2]`` = window_ms    (integer milliseconds, e.g. 60000 for 60 s)
* ``ARGV[3]`` = limit        (integer, e.g. 60)
* ``ARGV[4]`` = request_id   (UUID7 string — unique member, prevents sub-ms collision)
* ``ARGV[5]`` = ttl_ms       (integer ms for PEXPIRE; default 2*window_ms + 60_000)

ARGV contract for the sliding-window GCRA script:

* ``ARGV[1]`` = emission_interval_ms (window_ms / limit, may be float)
* ``ARGV[2]`` = delay_tolerance_ms    (window_ms, integer)
* ``ARGV[3]`` = quantity_ms           (1 * emission_interval_ms for cost=1)
* ``ARGV[4]`` = ttl_ms                (window_ms + 60_000 default)
* ``ARGV[5]`` = now_ms                (integer milliseconds from Python Clock)

When allowed, the script returns {1, 0, remaining_estimate, pre_acquire_tat, post_acquire_tat}
where pre_acquire_tat and post_acquire_tat are string representations of the TAT
before and after the acquire, used for compare-and-set refunds.

ARGV contract for the token-bucket refund script:

* ``ARGV[1]`` = refund_amount (float — tokens to add back, NOT decision.remaining)
* ``ARGV[2]`` = now_seconds  (float, from Python Clock.now().timestamp())
* ``ARGV[3]`` = capacity     (float — bucket cap; prevents over-refund)
* ``ARGV[4]`` = refill_per_second (float — mirrors the acquire script's
  elapsed-refill step so a refund does not lose accrued refill)
"""

from typing import Final

_LUA_SRC: Final[str] = """\
-- KEYS[1] = bucket key (format: taskq:{schema}:rl:tb:{bucket_name})
-- ARGV[1] = now_seconds (float, from Python Clock.now().timestamp())
-- ARGV[2] = capacity    (float)
-- ARGV[3] = refill_per_second (float; must be > 0)
-- ARGV[4] = requested_tokens (float; default 1.0)
-- ARGV[5] = ttl_seconds (integer; math.ceil(capacity/refill*2)+60)
--
-- Returns: {allowed, tokens_remaining, retry_after_seconds}
--   allowed         = 1 if granted, 0 if denied
--   tokens_remaining = current token count after the operation (string to
--                      preserve fractional part; Redis truncates Lua numbers
--                      to integers on return)
--   retry_after_seconds = "0" when allowed; seconds until `requested_tokens`
--                         are available when denied (string for same reason)
local key      = KEYS[1]
local now      = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local refill   = tonumber(ARGV[3])
local req      = tonumber(ARGV[4])
local ttl      = tonumber(ARGV[5])

-- Read current state. data[1]=tokens, data[2]=ts (last-refill timestamp).
local data   = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])

-- First-time initialization: empty key → full bucket at current time.
if tokens == nil then
  tokens = capacity
  ts = now
end

local old_tokens = tokens

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

redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
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
-- ARGV[1] = now_ms      (integer milliseconds from Python Clock — never Redis TIME)
-- ARGV[2] = window_ms   (integer milliseconds, e.g. 60000 for 60 s)
-- ARGV[3] = limit       (integer, e.g. 60)
-- ARGV[4] = request_id  (UUID7 string — unique member, prevents sub-ms collision)
-- ARGV[5] = ttl_ms      (integer ms for PEXPIRE; default 2*window_ms + 60_000)
--
-- Returns: {allowed, count, retry_after_ms}
--   allowed        = 1 if granted, 0 if denied
--   count          = window count after the operation (includes this acquire if allowed)
--   retry_after_ms = 0 when allowed; ms until oldest entry leaves the window when denied
local key    = KEYS[1]
local now    = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit  = tonumber(ARGV[3])
local req_id = ARGV[4]
local ttl    = tonumber(ARGV[5])

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
-- TaskQ deviations: client-supplied now_ms (ARGV[5]) instead of TIME;
-- millisecond arithmetic throughout; PEXPIRE instead of EXPIRE; return
-- shape includes pre/post TAT strings for compare-and-set refunds.
--
-- KEYS[1] = bucket key (format: taskq:{schema}:sw_gcra:{bucket_name})
-- ARGV[1] = emission_interval_ms (window_ms / limit, integer)
-- ARGV[2] = delay_tolerance_ms   (window_ms, integer)
-- ARGV[3] = quantity_ms          (1 * emission_interval_ms for cost=1)
-- ARGV[4] = ttl_ms               (window_ms + 60_000 default)
-- ARGV[5] = now_ms               (integer milliseconds from Python Clock)
local key = KEYS[1]
local emission_interval = tonumber(ARGV[1])
local delay_tolerance   = tonumber(ARGV[2])
local quantity          = tonumber(ARGV[3])
local ttl               = tonumber(ARGV[4])
local now               = tonumber(ARGV[5])

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
-- ARGV[2] = now (float seconds, client clock)
-- ARGV[3] = capacity (float — bucket cap; prevents over-refund)
-- ARGV[4] = refill_per_second (float — must mirror the acquire script's
--           refill rate so a refund does not clobber accrued-but-unread
--           refill; parity with _InMemoryBucket.refund, which always
--           refunds against tokens computed with elapsed * refill applied)
local key      = KEYS[1]
local refund   = tonumber(ARGV[1])
local now      = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local refill   = tonumber(ARGV[4])

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
redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
return {1, tokens}
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
