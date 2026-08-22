local seats_key = KEYS[1] -- available seats
local total_key = KEYS[2] -- total seats for the event
local dedup_key = KEYS[3] -- idempotency key for the order

local order_key = KEYS[4] -- for check the order status
local done_key = KEYS[5] -- current order that is being served
local blocked_key = KEYS[6] -- set by fraud-detector

local default_total = tonumber(ARGV[1]) -- default total seats for the event
local ttl = tonumber(ARGV[2]) -- TTL for the idempotency key, in seconds
local event_id = ARGV[3]
local user_id = ARGV[4]
local qty = tonumber(ARGV[5]) -- how many tickets in this order

-- Returns {first_seat, last_seat, remaining}
  -- first_seat = -1  -> not enough seats
  -- first_seat = -2  ->  user is a bot

-- check if the order has already been processed
local previous = redis.call('GET', dedup_key)
if previous then
  local remaining = tonumber(redis.call('GET', seats_key)) or 0
  local first, last = string.match(previous, "^(-?%d+):(-?%d+)$")
  return {tonumber(first), tonumber(last), remaining}
end

-- check if the user is in the blocked list
if redis.call('GET', blocked_key) then
  local remaining = tonumber(redis.call('GET', seats_key)) or 0
  redis.call('SET', dedup_key, '-2:-2', 'EX', ttl)
  redis.call('HSET', order_key, 'status', 'rejected', 'reason', 'fraud_suspected',
             'event_id', event_id, 'user_id', user_id, 'quantity', qty)
  redis.call('EXPIRE', order_key, ttl)
  redis.call('INCR', done_key) -- queue need to also increase
  redis.call('EXPIRE', done_key, ttl)
  return {-2, -2, remaining}
end

if redis.call('EXISTS', total_key) == 0 then -- create the event if it doesn't exist
  redis.call('SET', total_key, default_total)
  redis.call('SET', seats_key, default_total)
end

local total = tonumber(redis.call('GET', total_key))
local remaining = tonumber(redis.call('GET', seats_key)) or 0

-- check if there are enough seats available for the order
if remaining < qty then
  local reason = 'sold_out'
  if remaining > 0 then
    reason = 'not_enough_seats'
  end
  redis.call('SET', dedup_key, '-1:-1', 'EX', ttl)
  redis.call('HSET', order_key, 'status', 'rejected', 'reason', reason,
             'event_id', event_id, 'user_id', user_id, 'quantity', qty)
  redis.call('EXPIRE', order_key, ttl)
  redis.call('INCR', done_key)
  redis.call('EXPIRE', done_key, ttl)
  return {-1, -1, remaining}
end

-- enough seats
remaining = redis.call('DECRBY', seats_key, qty)
-- seats are sequential
local first_seat = total - remaining - qty + 1
local last_seat  = total - remaining

redis.call('SET', dedup_key, first_seat .. ':' .. last_seat, 'EX', ttl)
redis.call('HSET', order_key, 'status', 'confirmed',
           'seat', first_seat, 'last_seat', last_seat, 'quantity', qty,
           'event_id', event_id, 'user_id', user_id)
redis.call('EXPIRE', order_key, ttl)
redis.call('INCR', done_key)
redis.call('EXPIRE', done_key, ttl)
return {first_seat, last_seat, remaining}