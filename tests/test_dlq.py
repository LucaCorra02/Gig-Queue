import json
import time
import uuid
import redis
import subprocess
from utils import (new_event, send_raw, wait_until, run_tests)

API_URL = "http://localhost:8080"
REDIS_URL = "redis://localhost:6379/0"
KAFKA = "localhost:9092,localhost:9094,localhost:9096"
TOPIC_REQUESTS = "topic-requests"

rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def dlq_count(): return int(rdb.get("dlq:count") or 0)

def recent_entries(n=10):
    return [json.loads(x) for x in rdb.lrange("dlq:recent", 0, n - 1)]

def test_counter_increments():
    event = new_event()
    before = dlq_count()
    marker = uuid.uuid4().hex
    send_raw(topic= TOPIC_REQUESTS, key=event, payload_bytes=f"not json {marker}".encode())
    assert wait_until(lambda: dlq_count() == before + 1), "dlq:count did not increment: %s -> %s" % (before, dlq_count())

def test_recent_list():
    event = new_event()
    marker = uuid.uuid4().hex
    send_raw(topic=TOPIC_REQUESTS, key=event, payload_bytes=f"broken {marker}".encode())
    assert wait_until(lambda: any(marker in (e.get("raw_msg") or "") for e in recent_entries(20))), "entry not found in dlq:recent"
    last = recent_entries(20)[0]["raw_msg"]
    assert marker in last, "last entry in dlq %s" % last

def test_per_service_counter():
    event = new_event()
    before = int(rdb.get("dlq:by_service:inventory") or 0)
    send_raw(topic=TOPIC_REQUESTS,key=event, payload_bytes=b"still not json")
    assert wait_until(lambda: int(rdb.get("dlq:by_service:inventory") or 0) == before + 1), "inventory did not increment"

def test_counter_on_replay():
    event = new_event()
    send_raw(topic=TOPIC_REQUESTS,key=event, payload_bytes=f"replay test {uuid.uuid4().hex}".encode())
    assert wait_until(lambda: dlq_count() > 0), "no dlq entry"
    time.sleep(3)
    before = dlq_count()

    # Stop and re-read the whole topic
    subprocess.run(["docker", "compose", "stop", "dlq-monitor"], check=True)
    subprocess.run([
        "docker", "exec", "kafka-1", "kafka-consumer-groups",
        "--bootstrap-server", "kafka-1:9093", "--group", "group-dlq",
        "--topic", "topic-dlq", "--reset-offsets", "--to-earliest", "--execute",
    ], check=True, capture_output=True)
    subprocess.run(["docker", "compose", "start", "dlq-monitor"], check=True)

    time.sleep(10)
    after = dlq_count()
    assert after == before, "counter grew from %d to %d after replaying the whole topic: " % (before, after)

TESTS = [
    test_counter_increments,
    test_recent_list,
    test_per_service_counter,
    test_counter_on_replay
]

if __name__ == "__main__":
    run_tests(TESTS)