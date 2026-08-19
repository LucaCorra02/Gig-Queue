local seats_key = KEYS[1] -- available seats
local total_key = KEYS[2] -- total seats for the event
local dedup_key = KEYS[3] -- idempotency key for the order

local default_total = tonumber(ARGV[1]) -- default total seats for the event
local ttl = tonumber(ARGV[2]) -- TTL for the idempotency key, in seconds

local previous = redis.call('GET', dedup_key)
if previous then -- check if the order has already been processed
  local remaining = tonumber(redis.call('GET', seats_key)) or 0
  return {tonumber(previous), remaining}
end

if redis.call('EXISTS', total_key) == 0 then -- create the event if it doesn't exist
  redis.call('SET', total_key, default_total)
  redis.call('SET', seats_key, default_total)
end

local total = tonumber(redis.call('GET', total_key))
local remaining = tonumber(redis.call('GET', seats_key)) or 0

if remaining <= 0 then -- no seats left
  redis.call('SET', dedup_key, -1, 'EX', ttl)
  return {-1, 0}
end

remaining = redis.call('DECR', seats_key)
local seat = total - remaining -- assign the next available seat number
redis.call('SET', dedup_key, seat, 'EX', ttl)

return {seat, remaining}