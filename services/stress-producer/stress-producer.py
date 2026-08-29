import os
import random
import time
import uuid
import requests
import zlib
import threading
from concurrent.futures import ThreadPoolExecutor

API_URL = os.getenv("API_URL", "http://localhost:8080")
PARTITIONS = int(os.getenv("PARTITIONS", 3))
CONCURRENCY = int(os.getenv("CONCURRENCY", 40))
SETTLE_TIMEOUT_S = int(os.getenv("SETTLE_TIMEOUT_S", 180))
POLL_INTERVAL_S = 0.5
LIGHT_REQUESTS = int(os.getenv("LIGHT_REQUESTS", 50))
LIGHT_INTERVAL_S = float(os.getenv("LIGHT_INTERVAL_S", 0.5))
FLASH_REQUESTS = int(os.getenv("FLASH_REQUESTS", 500))
FLASH_DELAY_S = float(os.getenv("FLASH_DELAY_S", 3))


def new_ip():
    return f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"

def new_user():
    return f"user-{uuid.uuid4().hex[:10]}"

def new_event(prefix="ev"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"

def random_quantity(min_qty=1, max_qty=4):
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
def buy(event_id, user_id=None, ip=None, quantity=None):
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
        response = get_session().post(
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
        record["acked_at"] = time.time()
    return record

_local = threading.local()

def get_session():
    if not hasattr(_local, "session"): #each thread has its own session to avoid conflicts
        _local.session = requests.Session()
        _local.session.mount(
            "http://", requests.adapters.HTTPAdapter(pool_maxsize=64)
        )
    return _local.session

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

def buyers_pool(jobs, records, concurrency=CONCURRENCY): # multiple costumers buying tickets concurrently
    def one(job):
        record = buy(**job)
        records.append(record) #record contain all the buyer's request
        return record

    started = time.time()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        list(executor.map(one, jobs))
    return time.time() - started

def order_status(order_id):
    try:
        response = get_session().get(
            f"{API_URL}/status",
            params={"order_id": order_id}, 
            timeout=10
        )
        return response.json() if response.status_code == 200 else None
    except requests.RequestException:
        return None

"""
    Background gropu of threads that check the status of the orders and store the outcomes
"""
def tracker(records, outcomes, stop):
    with ThreadPoolExecutor(max_workers=16) as pool:
        while not stop.is_set():
            pending = [r["order_id"] for r in list(records)
                       if r.get("order_id") and r["order_id"] not in outcomes]
            for order_id, body in zip(pending, pool.map(order_status, pending)):
                if body and body["status"] in ("confirmed", "rejected"):
                    body["settled_at"] = time.time() # timestamp when the order is confirmed or rejected
                    outcomes[order_id] = body
            time.sleep(POLL_INTERVAL_S)
"""
    Compute the waiting time for each order and group them by role
"""
def queue_waits(records, outcomes):
    """Seconds between the 202 and the final outcome, grouped by event."""
    waits = {}
    for record in records:
        body = outcomes.get(record.get("order_id"))
        if body and "acked_at" in record:
            waits.setdefault(record["event"], []).append(
                body["settled_at"] - record["acked_at"])
    return waits

"""
    Generate light traffic for the given event ids (round-robin)
"""
def light_traffic(event_ids, count, interval, records):
    for index in range(count):
        event_id = event_ids[index % len(event_ids)]
        records.append(buy(event_id))
        time.sleep(interval)

def print_waits(plan, waits):
    for row in plan:
        values = sorted(waits.get(row["event_id"], []))
        if not values:
            print(f"{row['role']}: no orders")
            continue
        median = values[len(values) // 2]
        print(f"{row['role']} (partition {row['partition']}): "
              f"{len(values)} orders, median {median:.1f}s, max {values[-1]:.1f}s")

"""
    For each event return a list of tuples with
    the first and last seat for each confirmed order
"""
def get_seats_per_event(records, outcomes):
    seats_per_event = {}
    rejected_order_per_event = {}
    for record in records:
        event_id = record.get("event")
        record_order_id = record.get("order_id")
        order_info = outcomes.get(record_order_id)
        if not order_info: continue
        if order_info["status"] == "confirmed":
            seats_per_event.setdefault(event_id, []).append((order_info["seat"], order_info["last_seat"]))
        elif order_info["status"] == "rejected":
            rejected_order_per_event.setdefault(event_id, []).append(record["order_id"])
    return seats_per_event, rejected_order_per_event

def check_progressive_seats(seats_per_event):
    ok = True
    for event_id, seats in seats_per_event.items():
        seats.sort(key=lambda x: x[0]) # sort by first seat
        for i in range(1, len(seats)):
            if seats[i][0] != seats[i-1][1] + 1:
                print(f"Non-progressive seats for event {event_id}: {seats}")
                ok = False
                break
    if ok:
        print("All events have progressive seats")


if __name__ == "__main__":
    plan = plan_events()
    flash_event_id = [p["event_id"] for p in plan if p["role"] == "flash_sale"][0]
    light_event_ids = [p["event_id"] for p in plan if p["role"] != "flash_sale"]

    records = []
    outcomes = {}
    stop = threading.Event()

    # Start a background thread to track the status of the orders
    threading.Thread(target=tracker, args=(records, outcomes, stop),
                     daemon=True).start()

    # Start a background thread to generate light traffic for the other events
    light = threading.Thread(target=light_traffic,
                             args=(light_event_ids, LIGHT_REQUESTS,
                                   LIGHT_INTERVAL_S, records))
    light.start()
    time.sleep(FLASH_DELAY_S)

    print(f"flash sale: {FLASH_REQUESTS} requests on {flash_event_id}")
    jobs = [{"event_id": flash_event_id} for _ in range(FLASH_REQUESTS)]
    elapsed = buyers_pool(jobs, records)
    print(f"    sent in {elapsed:.1f}s")

    light.join() # wait for the light traffic to finish

    # wait for missing orders to be settled or timeout
    deadline = time.time() + SETTLE_TIMEOUT_S
    while time.time() < deadline:
        missing = [r for r in records
                   if r["order_id"] and r["order_id"] not in outcomes]
        if not missing: break
        time.sleep(1)
    stop.set()

    print(f"missing: {missing}")
    print_waits(plan, queue_waits(records, outcomes))
    seats_per_event, rejected_order_per_event = get_seats_per_event(records, outcomes)
    check_progressive_seats(seats_per_event)