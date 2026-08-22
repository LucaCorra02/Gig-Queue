import uuid
import random
import requests
import time
import redis

API_URL = "http://localhost:8080"
REDIS_URL = "redis://localhost:6379/0"
USER_THRESHOLD = 3
rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def new_event(seats=100):
    event_id = "fraud-%s" % uuid.uuid4().hex[:8]
    rdb.set("total:%s" % event_id, seats)
    rdb.set("seats:%s" % event_id, seats)
    return event_id

def new_user(name="test"): return f"{name}u-%s" % uuid.uuid4().hex[:8]

def buy(event_id, user_id, headers=None):
    return requests.post(f"{API_URL}/buy",json={"event_id": event_id, "user_id": user_id},headers=headers,timeout=15)


def test_fraud_block_user_id():
    event_id = new_event()
    user_id = new_user("bot")
    blocked = False
    for i in range(1, 7):
        fake_ip = f"203.0.113.{random.randint(1, 250)}"
        headers = {"X-Forwarded-For": fake_ip}
        response = buy(event_id, user_id, headers=headers)
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

if __name__ == "__main__":
    test_fraud_block_user_id()