import os
import sys
from confluent_kafka import Consumer
from loguru import logger

# TODO: fix requirements.txt

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_REQUESTS = os.getenv("TOPIC_REQUESTS", "topic-requests")
GROUP_ID = os.getenv("GROUP_ID", "group-debug-monitor") # TODO: change to group-debug

logger.remove()
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest", #Start from the begninning of the topic if no previous offset
})


def read_from_topic():
    consumer.subscribe([TOPIC_REQUESTS])
    try:
        while True:
            print("Waiting for messages...")
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            raw_value = msg.value().decode("utf-8")
            logger.info(f"Recived Order: {raw_value}")
    except KeyboardInterrupt:
        logger.info("closing")
    finally:
        consumer.close()

if __name__ == "__main__":
    read_from_topic()