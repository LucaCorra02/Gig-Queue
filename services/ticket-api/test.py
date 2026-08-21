import requests
import redis
import time
import json

API_URL = "http://localhost:8080"
REDIS_URL = "redis://localhost:6379/0"
rdb = redis.from_url(REDIS_URL, decode_responses=True)

def test_buy_ticket_success():
    payload = {"event_id": "live-verdena", "user_id": "user_01"}
    response = requests.post(f"{API_URL}/buy", json=payload)

    assert response.status_code == 202, response.text
    data = response.json()
    assert data["order_id"]
    assert data["status"] == "queued"
    assert isinstance(data["partition"], int)
    assert isinstance(data["offset"], int)

def test_buy_ticket_missing_event():
    payload = {"user_id": "user_01"}
    response = requests.post(f"{API_URL}/buy", json=payload)
    assert response.status_code == 422

def test_buy_ticket_empty_user():
    payload = {"event_id": "live-verdena", "user_id": "   "}
    response = requests.post(f"{API_URL}/buy", json=payload)
    assert response.status_code == 422

def test_health_check():
    response = requests.get(f"{API_URL}/healthz")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"

def test_buy_ticket_success_and_redis():
    payload = {"event_id": "live-verdena", "user_id": "user-test-redis"}
    response = requests.post(f"{API_URL}/buy", json=payload)

    assert response.status_code == 202, response.text
    data = response.json()
    order_id = data["order_id"]
    partition = data["partition"]
    offset = data["offset"]

    assert order_id
    assert data["status"] == "queued"
    redis_key = f"queue:{order_id}"
    time.sleep(0.5)
    redis_value = rdb.get(redis_key)
    json_value = json.loads(redis_value)
    partition_redis = json_value.get("partition")
    offset_redis = json_value.get("offset")

    assert redis_value is not None, f"{redis_key} does not exist in Redis"
    assert partition_redis == partition, f"Wrong Redis partition {partition_redis} != {partition}"
    assert offset_redis == offset, f"Wrong Redis offset {offset_redis} != {offset}"
    rdb.delete(redis_key)

TESTS = [
    test_buy_ticket_success,
    test_buy_ticket_missing_event,
    test_buy_ticket_empty_user,
    test_health_check,
    test_buy_ticket_success_and_redis
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
