-- KEYS[1] fraud:user:{user_id} -> window counter
-- KEYS[2] fraud:ip:{ip} -> window counter
-- KEYS[3] blocked:user:{user_id} -> block flag
-- KEYS[4] blocked:ip:{ip} ->
-- KEYS[5] alerted:{user_id} -> alert dedup flag
-- Returns: {user_count, ip_count, user_blocked, ip_blocked, should_alert}

local window          = tonumber(ARGV[1]) -- request window in seconds
local user_threshold  = tonumber(ARGV[2]) -- max requests with same user_id in window
local ip_threshold    = tonumber(ARGV[3]) -- max requests with same client_ip in window
local block_ttl       = tonumber(ARGV[4]) -- block TTL in seconds
local alert_ttl       = tonumber(ARGV[5]) -- alert dedup TTL in seconds


local user_count = redis.call('INCR', KEYS[1])
-- the first time set the timeout for the spam window
if user_count == 1 then
    redis.call('EXPIRE', KEYS[1], window)
end

local ip_count = redis.call('INCR', KEYS[2])
if ip_count == 1 then
    redis.call('EXPIRE', KEYS[2], window)
end


local user_blocked = 0
if user_count > user_threshold then
  redis.call('SET', KEYS[3], 'user_rate', 'EX', block_ttl)
  user_blocked = 1
end

local ip_blocked = 0
if ip_count > ip_threshold then
  redis.call('SET', KEYS[4], 'ip_rate', 'EX', block_ttl)
  ip_blocked = 1
end

-- the allert should be sent only once per user_id in a given time window
local should_alert = 0
if (user_blocked == 1 or ip_blocked == 1) then
  if redis.call('SET', KEYS[5], 1, 'NX', 'EX', alert_ttl) then -- set only if not exists
    should_alert = 1
  end
end

return {user_count, ip_count, user_blocked, ip_blocked, should_alert}