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
