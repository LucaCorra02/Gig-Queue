import os
from loguru import logger
import sys
from confluent_kafka import Consumer
import signal
import json
import redis
from email.message import EmailMessage
import smtplib

KAFKA_BOOTSTRAP= os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_ORDERS= os.getenv("TOPIC_ORDERS", "topic-orders")
TOPIC_FRAUD= os.getenv("TOPIC_FRAUD", "topic-fraud")
GROUP_ID= os.getenv("GROUP_ID", "group-notifier")
REDIS_URL= os.getenv("REDIS_URL", "redis://redis:6379/0")
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "1025"))
MAIL_FROM = os.getenv("MAIL_FROM", "noreply@gig-queue.test")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "security@gig-queue.test")
USER_DOMAIN = os.getenv("USER_DOMAIN", "gig-queue.test")
NOTIFY_TTL_S = int(os.getenv("NOTIFY_TTL_S", "86400"))

logger.remove()
logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "client.id": f"notifier-{os.uname().nodename}"
})
rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)

running = True
def handle_stop(signum, frame):
    global running
    logger.info(f"Received signal {signum}, stopping")
    running = False

signal.signal(signal.SIGINT, handle_stop)
signal.signal(signal.SIGTERM, handle_stop)

def seat_range(outcome):
    first, last = outcome.get("seat"), outcome.get("last_seat")
    if last is None or last == first: return str(first)
    return f"{first}-{last}"

def parse_reason(outcome):
    raw_reason = outcome.get("reason", "unknown")
    reasons = {
        "sold_out": "the event is sold out",
        "not_enough_seats": "there are not enough adjacent seats left",
        "fraud_suspected": "the request was blocked by our security checks",
    }
    reason = raw_reason
    if raw_reason in reasons: reason = reasons[raw_reason]
    return reason

def build_order_email(payload):
    event = payload["event_id"]
    order_id = payload["order_id"]
    if payload["status"] == "confirmed":
        subject = f"Order Confirmed for {event}"
        body = (
            f"Your order has been confirmed.\n\n"
            f"Order ID : {order_id}\n"
            f"Event    : {event}\n"
            f"Tickets  : {payload.get('quantity', 1)}\n"
            f"Seats    : {seat_range(payload)}\n\n"
            f"Thank you for your purchase!"
        )
    else:
        subject = f"Order Rejected for {event}"
        body = (
            f"We could not complete your order.\n\n"
            f"Order ID : {order_id}\n"
            f"Event    : {event}\n"
            f"Tickets  : {payload.get('quantity', 1)}\n"
            f"Reason   : {parse_reason(payload)}\n"
        )
    user_address = f"{payload["user_id"]}@{USER_DOMAIN}"
    return user_address, subject, body

def build_fraud_email(alert):
    subject = f"SECURITY ALERT - {alert['user_id']}"
    body = (
        f"Suspicious activity detected.\n\n"
        f"User      : {alert['user_id']}\n"
        f"IP        : {alert['client_ip']}\n"
        f"Reason    : {alert['reason']}\n"
        f"Requests  : {alert['user_count']} by user, {alert['ip_count']} by IP\n"
        f"Window    : {alert['window_s']}s\n"
        f"Blocked   : {alert['blocked_for_s']}s\n"
        f"Triggered by order {alert.get('trigger_order_id')} on {alert.get('event_id')}\n"
    )
    return ADMIN_EMAIL, subject, body

def send_email(to_address, subject, body):
    msg = EmailMessage()
    msg["From"] = MAIL_FROM
    msg["To"] = to_address
    msg["Subject"] = subject
    msg.set_content(body)
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as smtp:
        smtp.send_message(msg)

def remember(kind):
    pipe = rdb.pipeline()
    pipe.incr("notifications:count")
    pipe.incr(f"notifications:by_kind:{kind}")
    pipe.execute()

def handle_message(msg):
    try:
        payload = json.loads(msg.value())
    except ValueError as e: # already in dlq topic
        logger.error(f"failed to decode json from topic {msg.topic()}: {e}")
        return True
    try:
        if msg.topic() == TOPIC_ORDERS:
            kind = "order"
            dedup_key = f"notified:order:{payload['order_id']}"
            to_address, subject, body = build_order_email(payload)
        else:
            kind = "fraud"
            dedup_key = f"notified:fraud:{payload['user_id']}:{payload.get('trigger_order_id')}"
            to_address, subject, body = build_fraud_email(payload)
    except KeyError as e:
        logger.error(f"missing key in message from topic {msg.topic()}: {e}")
        return True

    if rdb.get(dedup_key): #email already sent for this order
        logger.info(f"duplicate {kind} notification for key {dedup_key}, skipping")
        return True

    send_email(to_address, subject, body)
    rdb.set(dedup_key, "1", ex=NOTIFY_TTL_S) # avoid duplicate notifications
    remember(kind) # increase counters for metrics
    logger.success(f"sent {kind} notification to {to_address}:{subject}")
    return True

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
            try:
                handled = handle_message(msg)
            except smtplib.SMTPException as e:
                logger.error(f"smtp delivery failed {e}")
                exit_code = 1
                break
            except (OSError, redis.RedisError) as e:
                logger.error(f"internal error {e}")
                exit_code = 1
                break
            if handled: consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
    return exit_code

if __name__ == "__main__":
    sys.exit(read_topics())