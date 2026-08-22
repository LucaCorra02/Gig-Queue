import os
from confluent_kafka import Consumer
from loguru import logger
import signal
import sys
import redis
import json
from pathlib import Path

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_FRAUD = os.getenv("TOPIC_FRAUD", "topic-fraud")
TOPIC_REQUESTS = os.getenv("TOPIC_REQUESTS", "topic-requests")
GROUP_ID = os.getenv("GROUP_ID", "group-fraud")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
WINDOW_S = int(os.getenv("WINDOW_S", 60))
USER_THRESHOLD = int(os.getenv("USER_THRESHOLD", 3))
IP_THRESHOLD = int(os.getenv("IP_THRESHOLD", 15))
BLOCK_TTL_S = int(os.getenv("BLOCK_TTL_S", "300"))
ALERT_TTL_S = int(os.getenv("ALERT_TTL_S", "300"))


logger.remove()
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>")
rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)
DETECT = rdb.register_script(
    (Path(__file__).parent / "detect.lua").read_text(encoding="utf-8")
)

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

def detect(user_id, ip):
    user_count, ip_count, user_blocked, ip_blocked, should_alert = DETECT(
        keys=[f"fraud:user:{user_id}", f"fraud:ip:{ip}",
              f"blocked:user:{user_id}", f"blocked:ip:{ip}",
              f"alerted:{user_id}"],
        args=[WINDOW_S, USER_THRESHOLD, IP_THRESHOLD, BLOCK_TTL_S, ALERT_TTL_S],
    )
    return int(user_count), int(ip_count), int(user_blocked), int(ip_blocked), int(should_alert)

def read_from_topic():
    exit_code = 0
    consumer.subscribe([TOPIC_REQUESTS])
    logger.info(f"Subscribed to topic {TOPIC_REQUESTS}")
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error(): logger.error(f"Consumer error: {msg.error()}"); continue
            try: 
                order = json.loads(msg.value())
                order_id = order["order_id"]
                event_id = order["event_id"]
                user_id = order["user_id"]
                client_ip = order.get("client_ip", "unknown")
            except (ValueError, KeyError, TypeError) as e:
                logger.warning(f"Invalid message format: {msg.value()}. Error: {e}")
                consumer.commit(message=msg, asynchronous=False) # Not produce on topic-dlq, inventory service job
                continue
            try:
                u_count, i_count, u_blocked, i_blocked, should_alert = detect(user_id, client_ip)
            except redis.RedisError as e:
                logger.error(f"Redis unavailable, stopping without commit: {e}")
                exit_code = 1
                break

            if u_blocked or i_blocked:
                reason = "user_rate" if u_blocked else "ip_rate"
                logger.warning(
                    f"SUSPECT {user_id} ip={client_ip} reason={reason} "
                    f"user_count={u_count} ip_count={i_count}"
                )
            else:
                logger.info(
                    f"OK {user_id} ip={client_ip} "
                    f"user_count={u_count} ip_count={i_count}"
                )

            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()

    return exit_code


if __name__ == "__main__":
    sys.exit(read_from_topic())