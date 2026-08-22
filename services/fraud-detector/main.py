import os
from confluent_kafka import Consumer
from loguru import logger
import signal
import sys

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_FRAUD = os.getenv("TOPIC_FRAUD", "topic-fraud")
TOPIC_REQUESTS = os.getenv("TOPIC_REQUESTS", "topic-requests")
GROUP_ID = os.getenv("GROUP_ID", "group-fraud")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
WINDOW_S = int(os.getenv("WINDOW_S", 60))
USER_THRESHOLD = int(os.getenv("USER_THRESHOLD", 3))
IP_THRESHOLD = int(os.getenv("IP_THRESHOLD", 15))
BLOCK_TTL_S = int(os.getenv("BLOCK_TTL_S", "300"))

logger.remove()
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
     "client.id": f"fraud-detector-{os.uname().nodename}",
})

running = True
def handle_stop(signum, frame):
    global running
    logger.info(f"Received signal {signum}, stopping")
    running = False

signal.signal(signal.SIGINT, handle_stop)
signal.signal(signal.SIGTERM, handle_stop)

def read_from_topic():
    exit_code = 0
    consumer.subscribe([TOPIC_REQUESTS])
    logger.info(f"Subscribed to topic {TOPIC_REQUESTS}")
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error(): logger.error(f"Consumer error: {msg.error()}"); continue
            logger.info(f"Received message: {msg.value().decode('utf-8')}")

            consumer.commit(msg, asynchronous=False)
    finally:
        consumer.close()

    return exit_code


if __name__ == "__main__":
    sys.exit(read_from_topic())