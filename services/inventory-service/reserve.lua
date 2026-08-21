local seats_key = KEYS[1] -- available seats
local total_key = KEYS[2] -- total seats for the event
local dedup_key = KEYS[3] -- idempotency key for the order

local order_key = KEYS[4] -- for check the order status
local event_id  = ARGV[3]
local user_id   = ARGV[4]
local done_key = KEYS[5] -- current order that is being served

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
  redis.call('HSET', order_key, 'status', 'rejected', 'reason', 'sold_out', 'event_id', event_id, 'user_id', user_id) -- store the order status in a hash
  redis.call('EXPIRE', order_key, ttl)
  redis.call('INCR', done_key) -- process the next order in the queue
  redis.call('EXPIRE', done_key, ttl)
  return {-1, 0}
end

remaining = redis.call('DECR', seats_key)
local seat = total - remaining -- assign the next available seat number
redis.call('SET', dedup_key, seat, 'EX', ttl)
redis.call('HSET', order_key, 'status', 'confirmed', 'seat', seat, 'event_id', event_id, 'user_id', user_id)
redis.call('EXPIRE', order_key, ttl)
redis.call('INCR', done_key) -- process the next order in the queue
redis.call('EXPIRE', done_key, ttl)

return {seat, remaining}