import requests
import redis
import time
import json
import uuid
import subprocess
from utils import (new_event, new_user, new_ip, buy, buy_id, status, 
                   wait_for_status, post_buy, run_tests, queue_seq, queue_done
                   )

API_URL = "http://localhost:8080"
REDIS_URL = "redis://localhost:6379/0"
rdb = redis.from_url(REDIS_URL, decode_responses=True)

def test_buy_ticket_success():
    event_id = new_event()
    data = buy(event_id)
    assert data["order_id"]
    assert data["status"] == "queued"
    assert isinstance(data["partition"], int)
    assert isinstance(data["offset"], int)

def test_buy_ticket_missing_event():
    response = post_buy(event_id = None, user_id= None, quantity=1, ip=None)
    assert response.status_code == 422

def test_buy_ticket_empty_user():
    response = post_buy(event_id = "live-verdena", user_id="    ", quantity=1, ip=None)
    assert response.status_code == 422

def test_health_check():
    response = requests.get(f"{API_URL}/healthz")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"

def test_buy_ticket_success_and_redis():
    event_id = new_event()
    data= buy(event_id, "user-test-redis")
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

def test_status_lifecycle():
    event = new_event(seats=5)
    order_id = buy_id(event)

    first = status(order_id)
    body = first.json()
    assert body["order_id"] == order_id, body
    assert body["status"] in ("queued", "processing", "confirmed"), body
    assert body["queue_ahead"] is not None and body["queue_ahead"] >= 0, body
    if body["eta_seconds"] is not None:
        assert body["eta_seconds"] >= 0, body

    final = wait_for_status(order_id, ("confirmed", "rejected"))
    assert final["status"] == "confirmed", final
    assert final["queue_ahead"] == 0, final
    assert final["seat"] == 1, final
    assert final["reason"] is None, final

def test_status_rejected_order():
    event = new_event(seats=1)
    buy(event, "first")
    second = buy(event, "second")

    final = wait_for_status(second["order_id"], ("confirmed", "rejected"))
    assert final["status"] == "rejected", final
    assert final["reason"] == "sold_out", final
    assert final["seat"] is None, final
    assert final["queue_ahead"] == 0, final

def test_queue_position_per_event():
    event = new_event(seats=50)
    n = 8
    subprocess.run(["docker", "compose", "stop", "inventory-service"], check=True)
    print("Inventory-service stopped")
    try:
        responses = [buy_id(event, "user_test_queue_%d" % i) for i in range(n)]
        last_order = responses[-1]
        st = status(last_order).json()

        assert st["status"] == "queued", f"status should be queued, got {st['status']}"
        assert st["queue_ahead"] == n - 1, f"expected {n-1} ahead, got {st['queue_ahead']}"
        assert queue_seq(event) == n
        done = queue_done(event)
        assert done is None or int(done) == 0, f"current serving order should be 0, got {done}"
    finally:
        print("Inventory-service started")
        subprocess.run(["docker", "compose", "start", "inventory-service"], check=True)

    final = wait_for_status(last_order, ("confirmed", "rejected"))
    assert final["status"] == "confirmed", final
    assert final["queue_ahead"] == 0, final
    assert queue_seq(event) == queue_done(event) == n

def test_ticket_limit():
    event = new_event(seats=100)
    r = post_buy(event_id=event, user_id=None, quantity=99)
    assert r.status_code == 422, r.text

def test_quantity_zero():
    event = new_event(seats=100)
    r = post_buy(event_id=event, user_id=None, quantity=0)
    assert r.status_code == 422, r.text

def test_default_quantity():
    event = new_event(seats=10)
    data = buy(event, "u")
    assert data["quantity"] == 1, data
    final = wait_for_status(data["order_id"], ("confirmed", "rejected"))
    assert final["status"] == "confirmed", final
    assert final["seat"] == 1 and final["last_seat"] == 1, final


TESTS = [
    test_buy_ticket_success,
    test_buy_ticket_missing_event,
    test_buy_ticket_empty_user,
    test_health_check,
    test_buy_ticket_success_and_redis,
    test_status_lifecycle,
    test_status_rejected_order,
    test_queue_position_per_event,
    test_ticket_limit,
    test_quantity_zero,
    test_default_quantity
]


if __name__ == "__main__":
    run_tests(TESTS)