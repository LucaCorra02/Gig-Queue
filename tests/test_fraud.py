import uuid
import random
import requests
import time
import redis
import json
from confluent_kafka import Consumer
import os
from utils import (new_event, new_user, new_ip, buy, buy_id, wait_until, wait_for_alerts,
                   run_tests, exist_blocked_user, get_fraud_count, post_buy, get_allert_count)

API_URL = "http://localhost:8080"
REDIS_URL = "redis://localhost:6379/0"
KAFKA = "localhost:9092,localhost:9094,localhost:9096"
TOPIC_FRAUD = "topic-fraud"
USER_THRESHOLD = int(os.getenv("USER_THRESHOLD", 3))
IP_THRESHOLD = int(os.getenv("IP_THRESHOLD", 15))
rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def test_user_is_not_blocked():
    event, user = new_event(), new_user()
    for i in range(USER_THRESHOLD):
        r = post_buy(event_id=event, user_id=user)
        assert r.status_code == 202, r.text

    time.sleep(3)
    assert not exist_blocked_user(user), "a legitimate user was blocked"
    assert get_fraud_count(user) == USER_THRESHOLD

def test_fraud_block_user_id():
    event_id = new_event()
    user_id = new_user("bot")
    blocked = False
    for i in range(1, IP_THRESHOLD + 3):
        response = post_buy(event_id=event_id, user_id=user_id)
        if response.status_code == 403:
            print(f"User {user_id} has been blocked after {i} requests")
            blocked = True
            break
        else:
            assert response.status_code == 202, f"{response.text}"
            time.sleep(0.05)

    assert blocked, "the user has never been blocked"
    assert exist_blocked_user(user_id=user_id), "redis key for blocked user does not exist"
    blocked_user = f"blocked:user:{user_id}"
    assert rdb.ttl(blocked_user) > 0, "ttl not set"
    assert get_allert_count(user_id) == 1, "alert key not set"

def test_api_rejects_blocked_user():
    event, user = new_event(), new_user()
    for _ in range(USER_THRESHOLD + 3):
        post_buy(event_id=event, user_id=user)
    assert wait_until(lambda: exist_blocked_user(user_id=user)), "user never blocked"

    r = post_buy(event_id=event, user_id=user)
    assert r.status_code == 403, f"expected 403, got {r.status_code}: {r.text}"
    assert "user" not in r.text.lower() or "block" not in r.text.lower(), \
        "the error message leaks the block reason to the attacker"

def test_one_alert_per_block():
    event, user = new_event(), new_user()
    for _ in range(USER_THRESHOLD + 8):
        post_buy(event_id=event, user_id=user)
    assert wait_until(lambda: exist_blocked_user(user_id=user)), "user never blocked"
    time.sleep(1)

    alerts = wait_for_alerts(user, timeout=20)
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
        response = post_buy(event_id=event_id, user_id=user_tmp, ip=user_ip)
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
        assert not exist_blocked_user(user), \
            f"{user} was blocked because of a shared ip"

TESTS = [
    test_user_is_not_blocked,
    test_fraud_block_user_id,
    test_fraud_block_user_ip,
    test_api_rejects_blocked_user,
    test_one_alert_per_block
]

if __name__ == "__main__":
    run_tests(TESTS)