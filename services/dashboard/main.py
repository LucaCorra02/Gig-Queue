import asyncio
import os
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI
from fastapi.responses import FileResponse
import redis.asyncio as redis
import time
from collections import deque
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9093")
KAFKA_SECURITY = {}
if os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper() == "SSL":
    KAFKA_SECURITY = {
        "security.protocol": "SSL",
        "ssl.ca.location": os.getenv("KAFKA_SSL_CA", "/certs/ca.crt"),
        "ssl.certificate.location": os.getenv("KAFKA_SSL_CERT", "/certs/client.crt"),
        "ssl.key.location": os.getenv("KAFKA_SSL_KEY", "/certs/client.key"),
    }
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
MAX_EVENTS = int(os.getenv("MAX_EVENTS", 4)) # maximum number of events to display in the dashboard
SAMPLE_INTERVAL_S = float(os.getenv("SAMPLE_INTERVAL_S", 2)) # update the samples every N seconds
HISTORY_POINTS = int(os.getenv("HISTORY_POINTS", 90)) # number of history points to keep for each event used for eta and rate
admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP, **KAFKA_SECURITY})

@asynccontextmanager
async def lifespan(app: FastAPI):
    sampler_task = asyncio.create_task(get_sample()) #startup
    yield
    sampler_task.cancel()

app = FastAPI(title="Gig-Queue console", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
rdb = redis.from_url(REDIS_URL, decode_responses=True)

"""
    Take cluster metadata from kafka api. The info is used to display the cluster state in the dashboard
"""
def read_cluster():
    metadata = admin.list_topics(timeout=10)
    online = set(metadata.brokers)
    topics, expected, leading = [], set(), {}
    under_replicated = offline_partitions = total_partitions = 0

    for name, topic in sorted(metadata.topics.items()):
        if not name.startswith("topic-"): continue
        partitions = []
        for partition in sorted(topic.partitions.values(), key=lambda p: p.id):
            replicas, isr = list(partition.replicas), list(partition.isrs)
            expected.update(replicas)
            total_partitions += 1
            if partition.leader >= 0:
                leading[partition.leader] = leading.get(partition.leader, 0) + 1
            else:
                offline_partitions += 1
            if len(isr) < len(replicas):
                under_replicated += 1
            partitions.append({
                "id": partition.id,
                "leader": partition.leader,
                "replicas": replicas,
                "isr": isr,
                "healthy": len(isr) == len(replicas),
            })
        topics.append({"name": name, "partitions": partitions,
                       "partition_count": len(partitions)})

    brokers = []
    for broker_id in sorted(expected | online):
        info = metadata.brokers.get(broker_id)
        brokers.append({
            "id": broker_id,
            "host": f"{info.host}:{info.port}" if info else None,
            "online": broker_id in online,
            "leading": leading.get(broker_id, 0)
        })

    return {
        "cluster_id": metadata.cluster_id,
        "brokers": brokers,
        "topics": topics,
        "totals": {
            "brokers_online": len(online),
            "brokers_expected": len(expected | online),
            "partitions": total_partitions,
            "under_replicated": under_replicated,
            "offline": offline_partitions,
        },
    }

"""
    read specific redis keys in chucks to avoid blocking the event loop
    only return the first 500 keys to avoid overloading
"""
async def scan_keys(pattern, limit=500):
    keys = []
    async for key in rdb.scan_iter(match=pattern, count=200):
        keys.append(key)
        if len(keys) >= limit: break
    return keys

# Contains the last current order status for each event, used to calculate the processing rate and ETA
samples = {}
last_rate = {}

async def read_events():
    # get the active event ids from redis
    ids = [key.split(":", 1)[1] for key in await scan_keys("queue_seq:*")]
    if not ids: return []

    pipe = rdb.pipeline()
    for event_id in ids:
        # queue_seq: total order
        # queue_done: current state
        pipe.mget(f"queue_seq:{event_id}", f"queue_done:{event_id}",
                  f"seats:{event_id}", f"total:{event_id}")
    rows = await pipe.execute() # each row is a tuple of (seq, done, seats, total)

    now = time.time()
    events = []
    for event_id, (seq, done, seats, total) in zip(ids, rows):
        seq, done = int(seq or 0), int(done or 0)
        seats, total = int(seats or 0), int(total or 0)
        window = samples.setdefault(event_id, deque(maxlen=10)) # last 10 samples of (timestamp, done) for this event
        window.append((now, done))

        rate = 0.0
        if len(window) >= 2:
            elapsed = window[-1][0] - window[0][0]  # time elapsed between the first and last sample
            processed = window[-1][1] - window[0][1] #new done - old done
            rate = processed / elapsed if elapsed > 0 else 0.0
        if rate > 0.05: # rate is significant
            last_rate[event_id] = rate
        estimate = rate if rate > 0.05 else last_rate.get(event_id, 0.0)

        ahead = max(seq - done, 0) # total orders - processed orders
        events.append({
            "id": event_id,
            "seq": seq,
            "done": done,
            "ahead": ahead,
            "seats_left": seats,
            "total": total,
            "sold": max(total - seats, 0),
            "rate": round(rate, 2),
            "eta": round(ahead / estimate, 1) if ahead and estimate > 0.05 else None,
            "active": ahead > 0 or rate > 0.05,
        })
    # recent attivity first
    events.sort(key=lambda e: (not e["active"], -e["ahead"], -e["rate"], e["id"]))
    return events[:MAX_EVENTS]

"""
    Read the counters from redis system, like total emailed, total replays, total blocked users and ips, etc.
"""
async def read_counters():
    dlq_keys = await scan_keys("dlq:by_service:*")
    kind_keys = await scan_keys("notifications:by_kind:*")

    pipe = rdb.pipeline()
    pipe.get("dlq:count")
    pipe.lrange("dlq:recent", 0, 9)
    pipe.get("notifications:count")
    pipe.get("replays:total")
    for key in dlq_keys + kind_keys: pipe.get(key)
    values = await pipe.execute() #wait for all the values to be fetched from redis

    dlq_count, dlq_recent, notified, replays = values[:4] # fixed values
    tail = values[4:] # dynamic values
    by_service = {k.rsplit(":", 1)[1]: int(v or 0)
                  for k, v in zip(dlq_keys, tail[:len(dlq_keys)])}
    by_kind = {k.rsplit(":", 1)[1]: int(v or 0)
               for k, v in zip(kind_keys, tail[len(dlq_keys):])}
    return {
        "dlq": {"count": int(dlq_count or 0), "by_service": by_service, "recent": dlq_recent},
        "notifications": {"count": int(notified or 0), "by_kind": by_kind},
        "fraud": {"blocked_users": len(await scan_keys("blocked:user:*")),
                  "blocked_ips": len(await scan_keys("blocked:ip:*"))},
        "replays": int(replays or 0),
    }

"""
    return group membership information from kafka
"""
def read_groups():
    try:
        listing = admin.list_consumer_groups(timeout=10).result()
        ids = [g.group_id for g in listing.valid if g.group_id.startswith("group-")]
        if not ids: return []
        described = admin.describe_consumer_groups(ids)
        groups = []
        for group_id, future in described.items():
            info = future.result(timeout=10)
            groups.append({"id": group_id, "members": len(info.members)})
        return sorted(groups, key=lambda g: g["id"])
    except Exception as exc:
        print(f"read_groups failed: {exc!r}", flush=True)
        return []

throughput_history = deque(maxlen=HISTORY_POINTS) # keep the last N throughput samples for the dashboard
snapshot = {"ts": None} # current state of the system

async def read_data():
    cluster, groups, events, counters = await asyncio.gather(
        asyncio.to_thread(read_cluster),
        asyncio.to_thread(read_groups),
        read_events(),
        read_counters(),
    )

    throughput_history.append({
        "t": time.time(),
        "rate": round(sum(e["rate"] for e in events), 2),
    })

    snapshot.update({
        "ts": time.time(),
        "cluster": cluster,
        "groups": groups,
        "events": events,
        "throughput": list(throughput_history),
        **counters,
    })

"""
    Background task that reads the cluster and events data every SAMPLE_INTERVAL_S seconds
    and updates the snapshot dictionary with the latest state of the system
"""
async def get_sample():
    while True:
        try:
            await read_data()
        except Exception as exc:
            snapshot["error"] = str(exc)
        else:
            snapshot.pop("error", None)
        await asyncio.sleep(SAMPLE_INTERVAL_S)


@app.get("/api/state")
async def state():
    if snapshot["ts"] is None:
        return {"ts": None, "warming_up": True, "error": snapshot.get("error")}
    return snapshot

@app.get("/healthz")
async def healthz():
    await rdb.ping()
    return {"status": "ok"}

@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))