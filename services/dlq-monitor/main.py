import os
import redis
import sys
from confluent_kafka import Consumer
from loguru import logger
import signal
import json

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "topic-dlq")
GROUP_ID = os.getenv("GROUP_ID", "group-dlq")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
RECENT_MAX = int(os.getenv("RECENT_MAX", "50"))
SEEN_TTL_S = int(os.getenv("SEEN_TTL_S", "86400"))
KAFKA_SECURITY: dict[str, str] = {}
if os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper() == "SSL":
    KAFKA_SECURITY = {
        "security.protocol": "SSL",
        "ssl.ca.location": os.getenv("KAFKA_SSL_CA", "/certs/ca.crt"),
        "ssl.certificate.location": os.getenv("KAFKA_SSL_CERT", "/certs/client.crt"),
        "ssl.key.location": os.getenv("KAFKA_SSL_KEY", "/certs/client.key"),
    }

logger.remove()
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    **KAFKA_SECURITY,
})
rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)

running = True

def handle_stop(signum, frame):
    global running
    logger.info(f"Received signal {signum}, stopping")
    running = False

signal.signal(signal.SIGINT, handle_stop)
signal.signal(signal.SIGTERM, handle_stop)


def record(entry, partition, offset):
    seen_key = f"dlq:seen:{partition}:{offset}"
    if not rdb.set(seen_key, 1, nx=True, ex=SEEN_TTL_S): return False # Key already exists

    service = entry.get("service", "unknown")
    pipe = rdb.pipeline()
    pipe.incr("dlq:count") # total count
    pipe.incr(f"dlq:by_service:{service}")
    pipe.lpush("dlq:recent", json.dumps({
        "reason": entry.get("reason"),
        "service": service,
        "raw_msg": (entry.get("raw_msg") or "")[:200],
        "source_partition": entry.get("source_partition"),
        "source_offset": entry.get("source_offset"),
        "ts_ms": entry.get("ts_ms"),
    }))
    pipe.ltrim("dlq:recent", 0, RECENT_MAX - 1) # keep the list bounded
    pipe.execute()
    return True


def read_from_topic():
    consumer.subscribe([TOPIC_DLQ])
    logger.info(f"dlq monitor subscribed to topic {TOPIC_DLQ}")

    exit_code = 0
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            try:
                entry = json.loads(msg.value())
            except ValueError as e:
                logger.error(f"Unreadable DLQ entry at {msg.partition()}@{msg.offset()}: {e}")
                consumer.commit(message=msg, asynchronous=False)
                continue

            logger.info(entry)
            try:
                counted = record(entry, msg.partition(), msg.offset())
            except redis.RedisError as e:
                logger.error(f"Redis unavailable, stopping without commit: {e}")
                exit_code = 1
                break
            if counted:
                total = rdb.get("dlq:count")
                logger.warning(
                    f"DLQ #{total} from {entry.get('service')}: {entry.get('reason')}"
                )
            else:
                logger.info(f"Replay at {msg.partition()}@{msg.offset()}, not counted twice")

            consumer.commit(message=msg, asynchronous=False)
    finally:
        logger.info("Closing consumer")
        consumer.close()
    return exit_code

if __name__ == "__main__":
    sys.exit(read_from_topic())