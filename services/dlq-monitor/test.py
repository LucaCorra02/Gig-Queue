from confluent_kafka import Producer
import json
import time
import uuid
import redis

API_URL = "http://localhost:8080"
REDIS_URL = "redis://localhost:6379/0"
KAFKA = "localhost:9092,localhost:9094,localhost:9096"
TOPIC_REQUESTS = "topic-requests"

rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def new_event(seats=10):
    event_id = "dlq-%s" % uuid.uuid4().hex[:8]
    rdb.set("total:%s" % event_id, seats)
    rdb.set("seats:%s" % event_id, seats)
    return event_id

def send_raw(event_id, payload_bytes):
    producer = Producer({"bootstrap.servers": KAFKA, "acks": "all"})
    producer.produce(topic=TOPIC_REQUESTS, key=event_id.encode(), value=payload_bytes)
    producer.flush(10)


def dlq_count():
    return int(rdb.get("dlq:count") or 0)


def recent_entries(n=10):
    return [json.loads(x) for x in rdb.lrange("dlq:recent", 0, n - 1)]


def wait_until(predicate, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate(): return True
        time.sleep(0.5)
    return False


def test_counter_increments():
    event = new_event()
    before = dlq_count()
    marker = uuid.uuid4().hex
    #print(before)
    send_raw(event, f"not json {marker}".encode())
    assert wait_until(lambda: dlq_count() == before + 1), "dlq:count did not increment: %s -> %s" % (before, dlq_count())

def test_recent_list():
    event = new_event()
    marker = uuid.uuid4().hex
    send_raw(event, f"broken {marker}".encode())
    assert wait_until(lambda: any(marker in (e.get("raw_msg") or "") for e in recent_entries(20))), "entry not found in dlq:recent"

    last = recent_entries(20)[0]["raw_msg"]
    assert marker in last, "last entry in dlq %s" % last


TESTS = [
    test_counter_increments,
    test_recent_list
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
            print(f"FAIL  {name}: {exc}")
            failed += 1
        except Exception as exc:
            print(f"ERROR {name}, {type(exc).__name__}, {exc}")
            failed += 1
    print(f"Passed {passed}, Failed {failed}")