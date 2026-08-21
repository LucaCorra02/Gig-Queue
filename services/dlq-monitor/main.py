import os
import redis
import sys
from confluent_kafka import Consumer
from loguru import logger
import signal

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "topic-dlq")
GROUP_ID = os.getenv("GROUP_ID", "group-dlq")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

logger.remove()
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})
rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)

running = True

def handle_stop(signum, frame):
    global running
    logger.info(f"Received signal {signum}, stopping")
    running = False

signal.signal(signal.SIGINT, handle_stop)
signal.signal(signal.SIGTERM, handle_stop)

def read_from_topic():
    consumer.subscribe([TOPIC_DLQ])
    logger.info(f"dlq monitor subscribed to topic {TOPIC_DLQ}")

    while running:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue
        logger.info(f"Received message: {msg.value().decode('utf-8')}")

        consumer.commit(message=msg, asynchronous=False)


if __name__ == "__main__":
    sys.exit(read_from_topic())