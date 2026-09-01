import asyncio
import os
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI
from fastapi.responses import FileResponse

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
admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP, **KAFKA_SECURITY})
app = FastAPI(title="Gig-Queue console")

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


@app.get("/api/state")
async def state():
    cluster = await asyncio.to_thread(read_cluster)
    return {"cluster": cluster}

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}

@app.get("/")
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))