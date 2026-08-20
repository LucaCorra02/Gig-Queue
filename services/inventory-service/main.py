import os
import sys
import json
import redis
import signal
import time
from confluent_kafka import Consumer, Producer
from loguru import logger
from pathlib import Path

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_REQUESTS = os.getenv("TOPIC_REQUESTS", "topic-requests")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "topic-dlq")
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

running = True

def handle_stop(signum, frame):
    global running
    logger.info(f"Received signal {signum}, stopping")
    running = False

signal.signal(signal.SIGINT, handle_stop)
signal.signal(signal.SIGTERM, handle_stop)

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

def deliver_message(topic, value, key=None): # Produce a message and wait for delivery confirmation
    delivery_errors.clear()
    producer.produce(
        topic=topic,
        key=key.encode("utf-8") if key else None,
        value=json.dumps(value).encode("utf-8"),
        callback=on_delivery
    )
    pending = producer.flush(10) # TODO: add batch processing to avoid waiting for each message
    if pending or delivery_errors:
        logger.error(f"Result not committed ({delivery_errors})")
        return False
    return True


def read_from_topic():
    consumer.subscribe([TOPIC_REQUESTS])
    logger.info(f"Listen on {TOPIC_REQUESTS} | group={GROUP_ID} | kafka={KAFKA_BOOTSTRAP}")

    exit_code = 0
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            raw_msg = msg.value()
            try:
                order = json.loads(raw_msg)
                order_id = order["order_id"]
                event_id = order["event_id"]
                user_id  = order["user_id"]
            except (ValueError, KeyError, TypeError) as e:
                logger.error(f"Error parsing message: {e}")
                dlq_msg = {
                    "reason": str(e),
                    "raw_msg": raw_msg.decode("utf-8", errors="replace") if raw_msg else None,
                    "service": "inventory",
                    "ts_ms": int(time.time() * 1000),
                    "source_partition": msg.partition(),
                    "source_offset": msg.offset(),
                }
                if not deliver_message(topic=TOPIC_DLQ, value=dlq_msg):
                    exit_code = 1
                    break
                consumer.commit(message=msg, asynchronous=False)
                continue

            seat, seats_remaining = reserve(order_id, event_id)
            if seat is not None:
                logger.success(f"Confirmed {order_id[:8]} seat={seat} remaining={seats_remaining}")
            else:
                logger.warning(f"Rejected {order_id[:8]} - sold out")

            response = {
                "order_id": order_id,
                "event_id": event_id,
                "user_id": user_id,
                "status": "confirmed" if seat is not None else "rejected",
                "seat": seat,
                "seats_remaining": seats_remaining,
                "source_partition": msg.partition(),
                "source_offset": msg.offset(),
            }
            if not deliver_message(topic=TOPIC_ORDERS, value=response, key=order_id):
                exit_code = 1
                break # TODO: Add kafka transaction support to avoid duplicate messages in case of special case failure
            consumer.commit(message=msg, asynchronous=False)
            logger.info(f"Sent response for orderrr {order_id[:8]} to topic {TOPIC_ORDERS}, {response['status']}, seat={response['seat']}, remaining={response['seats_remaining']}")
    finally:
        logger.info("Flushing producer and closing consumer")
        producer.flush(10)
        consumer.close()
    return exit_code

if __name__ == "__main__":
    sys.exit(read_from_topic())