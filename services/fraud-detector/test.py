import uuid
import random
import requests
import time
import redis
import json
from confluent_kafka import Consumer
import os


API_URL = "http://localhost:8080"
REDIS_URL = "redis://localhost:6379/0"
KAFKA = "localhost:9092,localhost:9094,localhost:9096"
TOPIC_FRAUD = "topic-fraud"
USER_THRESHOLD = int(os.getenv("USER_THRESHOLD", 3))
IP_THRESHOLD = int(os.getenv("IP_THRESHOLD", 15))
rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def new_event(seats=100):
    event_id = "fraud-%s" % uuid.uuid4().hex[:8]
    rdb.set("total:%s" % event_id, seats)
    rdb.set("seats:%s" % event_id, seats)
    return event_id

def new_user(name="test"): return f"{name}u-%s" % uuid.uuid4().hex[:8]

def new_ip():
    return "10.%d.%d.%d" % (random.randint(0, 255), random.randint(0, 255),
                            random.randint(1, 254))

def buy(event_id, user_id, ip=None):
    return requests.post(f"{API_URL}/buy",json={
        "event_id": event_id, "user_id": user_id},
        headers={"X-Forwarded-For": ip or new_ip()}
    ,timeout=15)

def wait_until(predicate, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate(): return True
        time.sleep(0.5)
    return False

def wait_for_alert(user_id, timeout=30):
    consumer = Consumer({
        "bootstrap.servers": KAFKA,
        "group.id": "test-fraud-%s" % uuid.uuid4().hex[:8],
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_FRAUD])
    deadline = time.time() + timeout
    found = []
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error(): continue
            alert = json.loads(msg.value())
            if alert.get("user_id") == user_id:
                found.append(alert)
    finally:
        consumer.close()
    return found


def test_user_is_not_blocked():
    event, user = new_event(), new_user()
    for i in range(USER_THRESHOLD):
        r = buy(event, user)
        assert r.status_code == 202, r.text

    time.sleep(3)
    assert not rdb.exists(f"blocked:user:{user}"), "a legitimate user was blocked"
    assert int(rdb.get(f"fraud:user:{user}")) == USER_THRESHOLD

def test_fraud_block_user_id():
    event_id = new_event()
    user_id = new_user("bot")
    blocked = False
    for i in range(1, IP_THRESHOLD + 3):
        response = buy(event_id, user_id, new_ip())
        if response.status_code == 403:
            print(f"User {user_id} has been blocked after {i} requests")
            blocked = True
            break
        else:
            assert response.status_code == 202, f"{response.text}"
            time.sleep(0.05)

    assert blocked, "the user has never been blocked"
    blocked_user = f"blocked:user:{user_id}"
    assert rdb.exists(blocked_user), "redis key for blocked user does not exist"
    assert rdb.ttl(blocked_user) > 0, "ttl not set"
    alert_key = f"alerted:{user_id}"
    assert rdb.get(alert_key) == "1", "alert kay not set"

def test_api_rejects_blocked_user():
    event, user = new_event(), new_user()
    for _ in range(USER_THRESHOLD + 3):
        buy(event, user, new_ip())
    assert wait_until(lambda: rdb.exists(f"blocked:user:{user}")), "user never blocked"

    r = buy(event, user, new_ip())
    assert r.status_code == 403, f"expected 403, got {r.status_code}: {r.text}"
    assert "user" not in r.text.lower() or "block" not in r.text.lower(), \
        "the error message leaks the block reason to the attacker"

def test_one_alert_per_block():
    event, user = new_event(), new_user()
    for _ in range(USER_THRESHOLD + 8):
        buy(event, user, new_ip())
    assert wait_until(lambda: rdb.exists(f"blocked:user:{user}")), "user never blocked"
    time.sleep(1)

    alerts = wait_for_alert(user, timeout=20)
    assert len(alerts) == 1, "expected exactly 1 alert, got %d" % len(alerts)

    alert = alerts[0]
    assert alert["reason"] == "user_rate", alert
    assert alert["user_count"] > USER_THRESHOLD, alert
    assert alert["blocked_for_s"] > 0, alert
    assert alert["client_ip"], "missing client_ip in the alert"

def test_fraud_block_user_ip():
    event_id = new_event()
    user_ip = new_ip()
    blocked = False
    users = []
    for i in range(1, IP_THRESHOLD + 3):
        user_tmp = new_user()
        users.append(user_tmp)
        response = buy(event_id, user_tmp, user_ip)
        if response.status_code == 403:
            print(f"ip {user_ip} has been blocked after {i} requests")
            blocked = True
            break
        else:
            assert response.status_code == 202, f"{response.text}"
            time.sleep(0.05)

    assert blocked, "the ip has never been blocked"
    blocked_ip = f"blocked:ip:{user_ip}"
    assert rdb.exists(blocked_ip), "redis key for blocked ip does not exist"
    assert rdb.ttl(blocked_ip) > 0, "ttl not set"

    for user in users:
        assert not rdb.exists(f"blocked:user:{user}"), \
            f"{user} was blocked because of a shared ip"

TESTS = [
    test_user_is_not_blocked,
    test_fraud_block_user_id,
    test_fraud_block_user_ip,
    test_api_rejects_blocked_user,
    test_one_alert_per_block
]

def cleanup():
    for pattern in ("blocked:*", "fraud:*", "alerted:*"):
        keys = rdb.keys(pattern)
        if keys:
            rdb.delete(*keys)

if __name__ == "__main__":
    cleanup()
    passed, failed = 0, 0
    for test in TESTS:
        name = test.__name__
        try:
            test()
            print(f"PASS {name}")
            passed += 1
        except AssertionError as exc:
            print(f"FAIL  {name}: {exc}")
            failed += 1
        except Exception as exc:
            print(f"ERROR {name}, {type(exc).__name__}, {exc}")
            failed += 1
    print(f"Passed {passed}, Failed {failed}")