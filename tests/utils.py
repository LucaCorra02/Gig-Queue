import json
import os
import random
import time
import uuid
import redis
import requests
from confluent_kafka import Consumer, Producer

API_URL = "http://localhost:8080"
REDIS_URL = "redis://localhost:6379/0"
KAFKA = "localhost:9092,localhost:9094,localhost:9096"

TOPIC_REQUESTS = "topic-requests"
TOPIC_ORDERS = "topic-orders"
TOPIC_FRAUD = "topic-fraud"
TOPIC_DLQ = "topic-dlq"
USER_THRESHOLD = int(os.getenv("USER_THRESHOLD", 3)) # form docker compose
IP_THRESHOLD = int(os.getenv("IP_THRESHOLD", 15))
rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)


# user and event utils

def new_event(seats=10, prefix="test"):
    event_id = "%s-%s" % (prefix, uuid.uuid4().hex[:8])
    rdb.set("total:%s" % event_id, seats)
    rdb.set("seats:%s" % event_id, seats)
    return event_id


def new_user(name=None): return f"user-{name}-%s" % uuid.uuid4().hex[:8]

def new_ip(): return "10.%d.%d.%d" % (random.randint(0, 255), random.randint(0, 255), random.randint(1, 254))

def cleanup_fraud(): # remove blocks flags
    for pattern in ("blocked:*", "fraud:*", "alerted:*"):
        keys = rdb.keys(pattern)
        if keys:
            rdb.delete(*keys)

# api utils

def post_buy(event_id, user_id=None, quantity=1, ip=None): # only return http response
    return requests.post(
        f"{API_URL}/buy",
        json={"event_id": event_id, "user_id": user_id or new_user(),
              "quantity": quantity},
        headers={"X-Forwarded-For": ip or new_ip()},
        timeout=15,
    )


def buy(event_id, user_id=None, quantity=1, ip=None): # return json response
    r = post_buy(event_id, user_id, quantity, ip)
    assert r.status_code == 202, r.text
    return r.json()


def buy_id(event_id, user_id=None, quantity=1, ip=None):
    return buy(event_id, user_id, quantity, ip)["order_id"]


def status(order_id):
    return requests.get(f"{API_URL}/status", params={"order_id": order_id}, timeout=10)


def wait_for_status(order_id, wanted, timeout=40):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        r = status(order_id)
        if r.status_code == 200:
            last = r.json()
            if last["status"] in wanted:
                return last
        time.sleep(0.5)
    raise AssertionError(f"status never reached {wanted}, last was {last}")


# kafka utils

def send_raw(topic, payload_bytes, key=None):
    producer = Producer({"bootstrap.servers": KAFKA, "acks": "all"})
    producer.produce(topic=topic, key=key.encode() if key else None, value=payload_bytes)
    producer.flush(10)


def order_payload(event_id, order_id=None, user_id=None, quantity=1, ip=None):
    return json.dumps({
        "order_id": order_id or uuid.uuid4().hex,
        "event_id": event_id,
        "user_id": user_id or new_user(),
        "quantity": quantity,
        "client_ip": ip or new_ip(),
        "timestamp_ms": int(time.time() * 1000),
    }).encode()


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
        while time.time() < deadline and wanted - set(found):
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            outcome = json.loads(msg.value())
            if outcome["order_id"] in wanted:
                found[outcome["order_id"]] = outcome
    finally:
        consumer.close()
    return found


def _collect(topic, match, timeout, first_only):
    consumer = Consumer({
        "bootstrap.servers": KAFKA,
        "group.id": "test-%s" % uuid.uuid4().hex[:8],
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    deadline = time.time() + timeout
    found = []
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                entry = json.loads(msg.value())
            except ValueError:
                continue
            if match(entry):
                found.append(entry)
                if first_only:
                    break
    finally:
        consumer.close()
    return found


def wait_for_dlq(marker, timeout=30):
    found = _collect(TOPIC_DLQ, lambda e: marker in (e.get("raw_msg") or ""),
                     timeout, first_only=True)
    return found[0] if found else None


def wait_for_alerts(user_id, timeout=25):
    return _collect(TOPIC_FRAUD, lambda e: e.get("user_id") == user_id,
                    timeout, first_only=False)

# redis

def seats_left(event_id):
    value = rdb.get("seats:%s" % event_id)
    return int(value) if value is not None else None

def queue_seq(event_id):
    return int(rdb.get(f"queue_seq:{event_id}") or 0)

def queue_done(event_id):
    return int(rdb.get(f"queue_done:{event_id}") or 0)

def wait_until(predicate, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.5)
    return False

def run_tests(tests):
    cleanup_fraud()
    passed, failed = 0, 0
    for test in tests:
        name = test.__name__
        try:
            test()
            print(f"PASS  {name}")
            passed += 1
        except AssertionError as exc:
            print(f"FAIL  {name}: {exc}")
            failed += 1
        except Exception as exc:
            print(f"ERROR {name}: {type(exc).__name__}: {exc}")
            failed += 1
    print(f"\nPassed {passed}, Failed {failed}")
    return failed