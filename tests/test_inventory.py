import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
import redis
from utils import (new_event, buy_id, run_tests, queue_done,
                   wait_for_outcomes, seats_left, send_raw, order_payload, wait_for_dlq)

API_URL = "http://localhost:8080"
REDIS_URL = "redis://localhost:6379/0"
KAFKA = "localhost:9092,localhost:9094,localhost:9096"
TOPIC_REQUESTS = "topic-requests"
TOPIC_ORDERS = "topic-orders"
TOPIC_DLQ = "topic-dlq"

rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def test_confirmed_order():
    event = new_event(seats=10)
    order_id = buy_id(event)

    outcomes = wait_for_outcomes([order_id])
    assert order_id in outcomes, "no elements in topic-orders"

    outcome = outcomes[order_id]
    assert outcome["status"] == "confirmed", outcome
    assert outcome["seat"] == 1, outcome
    assert outcome["seats_remaining"] == 9, outcome
    assert outcome["event_id"] == event
    assert seats_left(event) == 9

def test_rejected_order():
    event = new_event(seats=1)
    buy_id(event)
    order_id_failed = buy_id(event)
    outcome = wait_for_outcomes([order_id_failed])[order_id_failed]
    assert outcome["status"] == "rejected", outcome
    assert outcome["seat"] is None, outcome
    assert outcome["seats_remaining"] == 0, outcome

def test_multiple_orders():
    n_events = 5
    event = new_event(seats=n_events)
    order_ids = [buy_id(event) for _ in range(n_events)]
    assert n_events == len(order_ids)

    outcomes = wait_for_outcomes(order_ids)
    assert len(outcomes) == n_events, "not all orders processed"
    seats = [outcomes[order_id]["seat"] for order_id in order_ids]
    assert sorted(seats) == list(range(1, n_events + 1)), "seats not assigned correctly"
    assert seats_left(event) == 0, "seats left should be 0"

def test_sold_out():
    n_seats = 3
    event = new_event(seats=n_seats)
    order_ids = [buy_id(event) for _ in range(n_seats*2)]

    outcomes = wait_for_outcomes(order_ids)
    assert len(outcomes) == n_seats*2, "trovati %d esiti su %d" % (len(outcomes), n_seats * 2)
    confirmed = [o for o in outcomes.values() if o["status"] == "confirmed"]
    rejected = [o for o in outcomes.values() if o["status"] == "rejected"]

    assert len(confirmed) == 3, "confirmed %d instead of %d" % (len(confirmed), n_seats)
    assert len(rejected) == 3, "rejected %d instead of %d" % (len(rejected), n_seats)
    assert seats_left(event) == 0, "remaining %s" % seats_left(event)

    for outcome in rejected:
        assert outcome["seat"] is None, outcome

def test_no_oversell():
    n_seats = 20
    event = new_event(seats=n_seats)
    order_ids = []
    with ThreadPoolExecutor(max_workers=15) as pool:
        order_ids = list(pool.map(lambda i: buy_id(event), range(n_seats*2)))

    outcomes = wait_for_outcomes(order_ids, timeout=60)
    seats_confirmed = [o["seat"] for o in outcomes.values() if o["status"] == "confirmed"]
    assert len(outcomes) == n_seats*2, "find %d response" % len(outcomes)
    assert len(seats_confirmed) == n_seats, "confermid seats %d" % len(seats_confirmed)
    assert sorted(seats_confirmed) == list(range(1, 21))
    assert seats_left(event) == 0

def test_lua():
    event = new_event(seats=10)
    order_id = uuid.uuid4().hex
    payload = order_payload(order_id=order_id, event_id=event, user_id=None, quantity=1, ip=None)
    send_raw(topic=TOPIC_REQUESTS, key=event, payload_bytes=payload)
    time.sleep(1)
    first = seats_left(event)
    send_raw(topic=TOPIC_REQUESTS, key=event, payload_bytes=payload)
    time.sleep(1)
    second = seats_left(event)

    assert first == 9, "seats = %s" % first
    assert second == 9, "seats = %s" % second

def test_dlq_malformed_message():
    n_seats = 10
    event = new_event(seats=n_seats)
    marker = uuid.uuid4().hex
    send_raw(topic=TOPIC_REQUESTS, key=event, payload_bytes=f"this is not json {marker}".encode())
    entry = wait_for_dlq(marker)

    assert entry is not None, "malformed message never reached topic-dlq"
    assert marker in entry["raw_msg"], entry
    assert entry["reason"], "missing reason field"

    marker2 = uuid.uuid4().hex
    send_raw(topic=TOPIC_REQUESTS, key=event, payload_bytes=json.dumps({"order_id": marker2}).encode()) # missing fields
    assert wait_for_dlq(marker2) is not None, "missing-fields message not in topic-dlq"

    order_id = buy_id(event)
    outcomes = wait_for_outcomes([order_id])
    assert order_id in outcomes, "queue blocked after malformed messages"
    assert outcomes[order_id]["status"] == "confirmed", outcomes[order_id]

    assert seats_left(event) == n_seats-1, "a discarded message consumed a seat: %s" % seats_left(event)

def test_redis_hash_order_status():
    event = new_event(seats=5)
    order_id = buy_id(event, "user_redis")
    outcomes = wait_for_outcomes([order_id])
    assert order_id in outcomes, "no outcome for order"
    outcome = outcomes[order_id]
    assert outcome["status"] == "confirmed", outcome

    redis_key = f"order:{order_id}"
    redis_value = rdb.hgetall(redis_key)
    expired = rdb.ttl(redis_key)
    assert redis_value, f"{redis_key} does not exist in Redis"
    assert redis_value.get("status") == "confirmed", redis_value
    assert int(redis_value.get("seat")) == 1, redis_value
    assert expired > 86000, f"TTL is too short: {expired} seconds"

def test_queue_replay():
    event = new_event(seats=10)
    order_id = uuid.uuid4().hex
    payload = order_payload(event_id=event, order_id=order_id, user_id=None, quantity=1, ip=None)
    send_raw(topic=TOPIC_REQUESTS, key=event, payload_bytes=payload)
    time.sleep(2)
    done_first = queue_done(event_id=event) or 0
    seats_first = seats_left(event)
    send_raw(topic=TOPIC_REQUESTS, key=event, payload_bytes=payload)
    time.sleep(2)
    done_second = queue_done(event_id=event) or 0

    assert done_first == 1, "queue_done = %s after first order" % done_first
    assert done_second == 1, "queue_done = %s after second order" % done_second
    assert seats_left(event) == seats_first == 9, "replay consumed a seat"

def test_queue_done_rejected():
    event = new_event(seats=1)
    ids = [buy_id(event, "u1"), buy_id(event, "u2")]
    wait_for_outcomes(ids)
    time.sleep(1)
    assert queue_done(event) == 2, "rejected order did not increment queue_done"

def test_quantity_contiguous_seats():
    event = new_event(seats=20)
    order_id = buy_id(event_id = event, user_id = None, quantity = 4, ip = None)
    outcome = wait_for_outcomes([order_id])[order_id]
    assert outcome["status"] == "confirmed", outcome
    assert outcome["quantity"] == 4, outcome
    assert outcome["seat"] == 1 and outcome["last_seat"] == 4, outcome
    assert seats_left(event) == 16

def test_multiple_group_buys():
    event = new_event(seats=20)
    order_ids = [
        buy_id(event_id = event, user_id = None, quantity = 4, ip = None),
        buy_id(event_id = event, user_id = None, quantity = 3, ip = None),
        buy_id(event_id = event, user_id = None, quantity = 5, ip = None)
    ]

    outcomes = wait_for_outcomes(order_ids)
    assert len(outcomes) == 3, "not all orders processed"
    assert seats_left(event) == 8, "remaining seats should be 8"
    seats_block = [(outcomes[i]['seat'], outcomes[i]['last_seat']) for i in order_ids]
    assert seats_block == [(1,4), (5,7), (8,12)], "seats not assigned correctly: %s" % seats_block
    taken = [s for a, b in seats_block for s in range(a, b + 1)]
    assert len(set(taken)) == len(taken), "overlapping seats assigned: %s" % taken

def test_not_enough_seats():
    event = new_event(seats=5)
    id = buy_id(event_id = event, user_id = None, quantity = 3, ip = None)
    wait_for_outcomes([id])
    assert seats_left(event) == 2

    rejected =  buy_id(event_id = event, user_id = None, quantity = 5, ip = None)
    outcome = wait_for_outcomes([rejected])[rejected]
    assert outcome["status"] == "rejected", outcome
    assert outcome["reason"] == "not_enough_seats", outcome
    assert outcome["seat"] is None, outcome
    assert seats_left(event) == 2, "seats has been consumed"

    ok =  buy_id(event_id = event, user_id = None, quantity = 2, ip = None)
    assert wait_for_outcomes([ok])[ok]["status"] == "confirmed"
    assert seats_left(event) == 0

TESTS = [
    test_confirmed_order,
    test_rejected_order,
    test_multiple_orders,
    test_sold_out,
    test_no_oversell,
    test_lua,
    test_dlq_malformed_message,
    test_redis_hash_order_status,
    test_queue_replay,
    test_queue_done_rejected,
    test_quantity_contiguous_seats,
    test_multiple_group_buys,
    test_not_enough_seats,
]

if __name__ == "__main__":
    run_tests(TESTS)