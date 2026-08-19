import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
import redis
import requests
from confluent_kafka import Consumer, Producer

API_URL = "http://localhost:8080"
REDIS_URL = "redis://localhost:6379/0"
KAFKA = "localhost:9092,localhost:9094,localhost:9096"
TOPIC_REQUESTS = "topic-requests"
TOPIC_ORDERS = "topic-orders"

rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def new_event(seats=10):
    event_id = "test-%s" % uuid.uuid4().hex[:8]
    rdb.set("total:%s" % event_id, seats)
    rdb.set("seats:%s" % event_id, seats)
    return event_id

def buy(event_id, user_id="tester"):
    response = requests.post(
        "%s/buy" % API_URL,
        json={"event_id": event_id, "user_id": user_id},
        timeout=15,
    )
    assert response.status_code == 202, response.text
    return response.json()["order_id"]

def wait_for_outcomes(order_ids, timeout=40):
    wanted = set(order_ids)
    found = {}
    consumer = Consumer({
        "bootstrap.servers": KAFKA,
        "group.id": "test-%s" % uuid.uuid4().hex[:8],
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_ORDERS])
    deadline = time.time() + timeout
    try:
        while time.time() < deadline and wanted - set(found) != set():
            msg = consumer.poll(1.0)
            if msg is None or msg.error(): continue
            outcome = json.loads(msg.value())
            if outcome["order_id"] in wanted: found[outcome["order_id"]] = outcome
    finally:
        consumer.close()
    return found

def seats_left(event_id):
    value = rdb.get("seats:%s" % event_id)
    return int(value) if value is not None else None


def test_confirmed_order():
    event = new_event(seats=10)
    order_id = buy(event)

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
    buy(event)
    order_id_failed = buy(event)
    outcome = wait_for_outcomes([order_id_failed])[order_id_failed]
    #print(outcome)

    assert outcome["status"] == "rejected", outcome
    assert outcome["seat"] is None, outcome
    assert outcome["seats_remaining"] == 0, outcome

def test_multiple_orders():
    n_events = 5
    event = new_event(seats=n_events)
    order_ids = [buy(event, "user_%d" % i) for i in range(n_events)]
    assert n_events == len(order_ids)

    outcomes = wait_for_outcomes(order_ids)
    assert len(outcomes) == n_events, "not all orders processed"
    seats = [outcomes[order_id]["seat"] for order_id in order_ids]
    assert sorted(seats) == list(range(1, n_events + 1)), "seats not assigned correctly"
    assert seats_left(event) == 0, "seats left should be 0"

def test_sold_out():
    n_seats = 3
    event = new_event(seats=n_seats)
    order_ids = [buy(event, "user_%d" % i) for i in range(n_seats*2)]

    outcomes = wait_for_outcomes(order_ids)
    assert len(outcomes) == n_seats*2, "trovati %d esiti su %d" % len(outcomes) % n_seats*2
    confirmed = [o for o in outcomes.values() if o["status"] == "confirmed"]
    rejected = [o for o in outcomes.values() if o["status"] == "rejected"]

    assert len(confirmed) == 3, "confirmed %d instead of %d" % len(confirmed) % n_seats
    assert len(rejected) == 3, "rejected %d instead of %d" % len(rejected) % n_seats
    assert seats_left(event) == 0, "remaining %s" % seats_left(event)

    for outcome in rejected:
        assert outcome["seat"] is None, outcome

def test_no_oversell():
    n_seats = 20
    event = new_event(seats=n_seats)
    order_ids = []
    with ThreadPoolExecutor(max_workers=15) as pool:
        order_ids = list(pool.map(lambda i: buy(event, "user_%d" % i), range(n_seats*2)))

    outcomes = wait_for_outcomes(order_ids, timeout=60)
    seats_confirmed = [o["seat"] for o in outcomes.values() if o["status"] == "confirmed"]
    assert len(outcomes) == n_seats*2, "find %d response" % len(outcomes)
    assert len(seats_confirmed) == n_seats, "confermid seats %d" % len(seats_confirmed)
    assert sorted(seats_confirmed) == list(range(1, 21))
    assert seats_left(event) == 0


def send_raw(event_id, payload_bytes): # write without using the API, to simulate a replay of the same order_id
    producer = Producer({"bootstrap.servers": KAFKA, "acks": "all"})
    producer.produce(topic=TOPIC_REQUESTS, key=event_id.encode(), value=payload_bytes)
    producer.flush(10)

def test_lua():
    event = new_event(seats=10)
    order_id = uuid.uuid4().hex
    payload = json.dumps({
        "order_id": order_id,
        "event_id": event,
        "user_id": "mario",
        "ts_ms": int(time.time() * 1000),
    }).encode()

    send_raw(event, payload)
    time.sleep(1)
    first = seats_left(event)
    send_raw(event, payload)
    time.sleep(1)
    second = seats_left(event)

    assert first == 9, "seats = %" % first
    assert second == 9, "seats = %" % second


TESTS = [
    test_confirmed_order,
    test_rejected_order,
    test_multiple_orders,
    test_sold_out,
    test_no_oversell,
    test_lua,
]

if __name__ == "__main__":
    passed, failed = 0, 0
    for test in TESTS:
        name = test.__name__
        try:
            test()
            print(f"PASS {name}")
            passed += 1
        except AssertionError as exc:
            print(f"FAIL  {name,exc}")
            failed += 1
        except Exception as exc:
            print(f"ERROR {name}, {type(exc).__name__}, {exc}")
            failed += 1
    print(f"Passed {passed}, Failed {failed}")