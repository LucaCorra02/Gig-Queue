import json
import os
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "client.id": "test-producer"
})

def delivery_report(err, msg):
    if err is not None:
        print(f"err {err}")
    else:
        print(f"ok {msg.topic()}")

order_payload = {
    "order_id": "test-order-12345",
    "event_id": "concerto-indie",
    "user_id": "mario.rossi",
    "status": "confirmed",
    "quantity": 2,
    "seat": 42,
    "last_seat": 43
}
fraud_payload = {
    "user_id": "bot-spammer",
    "client_ip": "192.168.1.100",
    "reason": "ip_rate",
    "user_count": 2,
    "ip_count": 55,
    "window_s": 60,
    "blocked_for_s": 300,
    "trigger_order_id": "hack-order-999"
}
producer.produce(
    "topic-orders",
    value=json.dumps(order_payload).encode('utf-8'),
    callback=delivery_report
)
producer.produce(
    "topic-fraud",
    value=json.dumps(fraud_payload).encode('utf-8'),
    callback=delivery_report
)
producer.flush()
print("Messages sent to Kafka topics: topic-orders and topic-fraud")