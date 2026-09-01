import asyncio
import os
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9093")
KAFKA_SECURITY = {}
if os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper() == "SSL":
    KAFKA_SECURITY = {
        "security.protocol": "SSL",
        "ssl.ca.location": os.getenv("KAFKA_SSL_CA", "/certs/ca.crt"),
        "ssl.certificate.location": os.getenv("KAFKA_SSL_CERT", "/certs/client.crt"),
        "ssl.key.location": os.getenv("KAFKA_SSL_KEY", "/certs/client.key"),
    }
admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP, **KAFKA_SECURITY})
app = FastAPI(title="Gig-Queue console")

"""
    Take cluster metadata from kafka api. The info is used to display the cluster state in the dashboard
"""
def read_cluster():
    metadata = admin.list_topics(timeout=10)
    brokers = sorted(
        ({"id": b.id, "host": b.host} for b in metadata.brokers.values()),
        key=lambda b: b["id"],
    )
    topics = []
    for name, topic in sorted(metadata.topics.items()):
        if not name.startswith("topic-"): continue
        partitions = []
        for partition in sorted(topic.partitions.values(), key=lambda p: p.id):
            replicas, isr = list(partition.replicas), list(partition.isrs)
            partitions.append({
                "id": partition.id,
                "leader": partition.leader,
                "replicas": replicas,
                "isr": isr,
                "healthy": len(isr) == len(replicas),
            })
        topics.append({"name": name, "partitions": partitions})
    return {"brokers": brokers, "topics": topics}


@app.get("/api/state")
async def state():
    cluster = await asyncio.to_thread(read_cluster)
    return {"cluster": cluster}

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}