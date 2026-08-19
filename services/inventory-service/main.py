import os
import sys
import json
import redis
from confluent_kafka import Consumer
from loguru import logger

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_REQUESTS = os.getenv("TOPIC_REQUESTS", "topic-requests")
GROUP_ID = os.getenv("GROUP_ID", "group-debug-monitor") # TODO: change to group-inventory
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SEATS_PER_EVENT = int(os.getenv("SEATS_PER_EVENT", "100")) # TODO: add some redis records for testing

logger.remove()
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest", #Start from the begninning of the topic if no previous offset
    "enable.auto.commit": False # No data loss
})
rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def reserve(event_id):
    seats_key = f"seats:{event_id}"
    total_key = f"total:{event_id}"

    if rdb.set(total_key, SEATS_PER_EVENT, nx=True):
        rdb.set(seats_key, SEATS_PER_EVENT)

    total = int(rdb.get(total_key))
    remaining = rdb.decr(seats_key)

    if remaining < 0:
        rdb.incr(seats_key)
        return None, 0

    return total - remaining, remaining

def read_from_topic():
    consumer.subscribe([TOPIC_REQUESTS])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            try:
                order = json.loads(msg.value())
                order_id = order["order_id"]
                event_id = order["event_id"]
                user_id  = order["user_id"]
            except Exception as e:
                logger.error(f"Error parsing message: {e}")
                continue
            logger.info(f"Order {order_id[:8]}, events={event_id}, user={user_id}")
            seat, seats_remaining = reserve(event_id)
            if seat:
                logger.success(f"Confirmed {order_id[:8]} seat={seat} remaining={seats_remaining}")
            else:
                logger.warning(f"Rejected {order_id[:8]} - sold out")
            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        logger.info("closing")
    finally:
        consumer.close()

if __name__ == "__main__":
    read_from_topic()