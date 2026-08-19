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

TESTS = [
    test_confirmed_order,
    test_rejected_order
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