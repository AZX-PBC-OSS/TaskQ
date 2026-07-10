"""Lock the canonical Lua script bytes against inline golden copies.

The script bodies are canonical constants in ``taskq.ratelimit._scripts``.
These tests embed an independent copy of each script as a module-level golden
string and assert a byte-for-byte match.  The test fails if either the
canonical script or the golden copy is edited independently, which is exactly
when we want it to break.  No external reference file is required.
"""

from taskq.ratelimit._scripts import (
    SLIDING_WINDOW_GCRA_SCRIPT,
    SLIDING_WINDOW_LOG_SCRIPT,
    TOKEN_BUCKET_SCRIPT,
)

_EXPECTED_TOKEN_BUCKET = """\
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

_EXPECTED_SLIDING_WINDOW_LOG = """\
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

_EXPECTED_SLIDING_WINDOW_GCRA = """\
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


def test_token_bucket_script_matches_golden() -> None:
    """TOKEN_BUCKET_SCRIPT must be byte-for-byte identical to the embedded golden copy."""
    assert TOKEN_BUCKET_SCRIPT.decode("utf-8") == _EXPECTED_TOKEN_BUCKET, (
        "TOKEN_BUCKET_SCRIPT does not match the embedded golden copy. "
        "If the script was intentionally changed, update the golden copy to match."
    )


def test_sliding_window_log_script_matches_golden() -> None:
    """SLIDING_WINDOW_LOG_SCRIPT must be byte-for-byte identical to the embedded golden copy."""
    assert SLIDING_WINDOW_LOG_SCRIPT.decode("utf-8") == _EXPECTED_SLIDING_WINDOW_LOG, (
        "SLIDING_WINDOW_LOG_SCRIPT does not match the embedded golden copy. "
        "If the script was intentionally changed, update the golden copy to match."
    )


def test_sliding_window_gcra_script_matches_golden() -> None:
    """SLIDING_WINDOW_GCRA_SCRIPT must be byte-for-byte identical to the embedded golden copy."""
    assert SLIDING_WINDOW_GCRA_SCRIPT.decode("utf-8") == _EXPECTED_SLIDING_WINDOW_GCRA, (
        "SLIDING_WINDOW_GCRA_SCRIPT does not match the embedded golden copy. "
        "If the script was intentionally changed, update the golden copy to match."
    )
