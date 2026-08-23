import os
from loguru import logger
import sys
from confluent_kafka import Consumer
import signal

KAFKA_BOOTSTRAP= os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_ORDERS= os.getenv("TOPIC_ORDERS", "topic-orders")
TOPIC_FRAUD= os.getenv("TOPIC_FRAUD", "topic-fraud")
GROUP_ID= os.getenv("GROUP_ID", "group-notifier")
REDIS_URL= os.getenv("REDIS_URL", "redis://redis:6379/0")
SMTP_HOST= os.getenv("SMTP_HOST", "mailpit")
SMTP_PORT= os.getenv("SMTP_PORT", 1025)
MAIL_FROM= os.getenv("MAIL_FROM", "noreply@gig-queue.test")
ADMIN_EMAIL= os.getenv("ADMIN_EMAIL", "security@gig-queue.test")
USER_DOMAIN= os.getenv("USER_DOMAIN", "gig-queue.test")

logger.remove()
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "client.id": f"notifier-{os.uname().nodename}"
})

running = True
def handle_stop(signum, frame):
    global running
    logger.info(f"Received signal {signum}, stopping")
    running = False

signal.signal(signal.SIGINT, handle_stop)
signal.signal(signal.SIGTERM, handle_stop)

def read_topics():
    consumer.subscribe([TOPIC_ORDERS, TOPIC_FRAUD])
    logger.info(f"Subscribed to topics: {TOPIC_ORDERS}, {TOPIC_FRAUD}")
    exit_code = 0
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            logger.info(f"Received message from topic {msg.topic()}: {msg.value().decode('utf-8')}")


            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
    return exit_code

if __name__ == "__main__":
    sys.exit(read_topics())