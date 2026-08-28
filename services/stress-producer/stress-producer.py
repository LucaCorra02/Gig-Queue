import os
import random
import time
import uuid
import requests
import zlib

API_URL = os.getenv("API_URL", "http://localhost:8080")
PARTITIONS = int(os.getenv("PARTITIONS", 3))
session = requests.Session()

def new_ip():
    return f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"

def new_user():
    return f"user-{uuid.uuid4().hex[:10]}"

def new_event(prefix="ev"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"

def random_quantity(min_qty=1, max_qty=5):
    return random.randint(min_qty, max_qty)


"""
    Buy tickets for an event the result is a dict with the following keys:
    - event
    - user
    - ip
    - quantity
    - status_code: HTTP status code returned by the API
    - order_id: order id (if accepted)
    - latency: the time taken for the request in seconds
    - error: the error message if the request failed
"""
def buy(event_id, user_id=None, ip=None, quantity=1):
    record = {
        "event": event_id,
        "user": user_id or new_user(), # defaul is a new user avoid fradu
        "ip": ip or new_ip(),
        "quantity": quantity or random_quantity(),
        "status_code": None,
        "order_id": None,
    }
    started = time.time()
    try:
        response = session.post(
            f"{API_URL}/buy",
            json={"event_id": record["event"],
                  "user_id": record["user"],
                  "quantity": record["quantity"]},
            headers={"X-Forwarded-For": record["ip"]},
            timeout=15,
        )
    except requests.RequestException as exc:
        record["latency"] = time.time() - started
        record["error"] = type(exc).__name__
        return record

    record["latency"] = time.time() - started
    record["status_code"] = response.status_code
    if response.status_code == 202:
        body = response.json()
        record["order_id"] = body["order_id"]
        record["partition"] = body["partition"]
        record["offset"] = body["offset"]
    return record

def get_partition(event_id, partitions=PARTITIONS):
    return zlib.crc32(event_id.encode()) % partitions

"""
    Create 4 events two in the same partition: onnce is the flash sale event and the other is the victim.
    Other partitions contain only normal traffic events
    return a list of event id with their role
"""
def plan_events(partitions=PARTITIONS):
    partition = [[] for _ in range(partitions)]
    n_ev = 0
    while n_ev < partitions + 1:
        event_id = new_event("ev")
        partition_id = get_partition(event_id, partitions)
        if len(partition[partition_id]) == 0:
            partition[partition_id].append(event_id)
        elif len(partition[partition_id]) == 1 and not any(len(p) == 2 for p in partition):
            partition[partition_id].append(event_id)
        n_ev = sum(len(p) for p in partition)

    orders = []
    for i, p in enumerate(partition):
        if len(p) == 2:
            orders.append({"partition": i,"event_id": p[0],"role": "flash_sale"})
            orders.append({"partition": i,"event_id": p[1], "role": "victim"})
        else:
            orders.append({"partition": i,"event_id": p[0], "role": "normal"})

    calculated_order = partition[0][0]
    assert buy(calculated_order)["partition"] == 0, f"partition formula has an error"
    return orders

if __name__ == "__main__":
    plan = plan_events()
    print(plan)