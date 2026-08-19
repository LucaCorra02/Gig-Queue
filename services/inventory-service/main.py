import os
import sys
import json
import redis
from confluent_kafka import Consumer, Producer
from loguru import logger
from pathlib import Path

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_REQUESTS = os.getenv("TOPIC_REQUESTS", "topic-requests")
GROUP_ID = os.getenv("GROUP_ID", "group-inventory") # TODO: change to group-inventory
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SEATS_PER_EVENT = int(os.getenv("SEATS_PER_EVENT", "100")) # TODO: add some redis records for testing
TOPIC_ORDERS = os.getenv("TOPIC_ORDERS", "topic-orders")
DEDUP_TTL_S = int(os.getenv("DEDUP_TTL_S", "86400"))

logger.remove()
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest", #Start from the begninning of the topic if no previous offset
    "enable.auto.commit": False # No data loss
})
rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks": "all",
    "enable.idempotence": True,
})

RESERVE = rdb.register_script(
    (Path(__file__).parent / "reserve.lua").read_text(encoding="utf-8")
)

def reserve(order_id, event_id):
    seat, remaining = RESERVE(
        keys=[f"seats:{event_id}", f"total:{event_id}", f"processed:{order_id}"],
        args=[SEATS_PER_EVENT, DEDUP_TTL_S],
    )
    if seat == -1: return None, 0
    return seat, remaining

delivery_errors = []

def on_delivery(err, msg):
    if err is not None:
        delivery_errors.append(err)

def read_from_topic():
    consumer.subscribe([TOPIC_REQUESTS])
    logger.info(f"Listen on {TOPIC_REQUESTS} | group={GROUP_ID} | kafka={KAFKA_BOOTSTRAP}")
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
            seat, seats_remaining = reserve(order_id, event_id)
            if seat:
                logger.success(f"Confirmed {order_id[:8]} seat={seat} remaining={seats_remaining}")
            else:
                logger.warning(f"Rejected {order_id[:8]} - sold out")

            response = {
                "order_id": order_id,
                "event_id": event_id,
                "user_id": user_id,
                "status": "confirmed" if seat else "rejected",
                "seat": seat,
                "seats_remaining": seats_remaining,
            }
            delivery_errors.clear()
            producer.produce(
                topic=TOPIC_ORDERS,
                key=order_id.encode("utf-8"),
                value=json.dumps(response).encode("utf-8"),
                callback=on_delivery
            )
            pending = producer.flush(10) # TODO: add batch processing to avoid waiting for each message
            if pending or delivery_errors:
                logger.error(f"Result not committed ({delivery_errors})")
                break# TODO: Fix multiple decrease
            consumer.commit(message=msg, asynchronous=False)
            logger.info(f"Sent response for order {order_id[:8]} to topic {TOPIC_ORDERS}, {response['status']}, seat={response['seat']}, remaining={response['seats_remaining']}")

    except KeyboardInterrupt:
        logger.info("closing")
    finally:
        producer.flush(10)
        consumer.close()

if __name__ == "__main__":
    read_from_topic()