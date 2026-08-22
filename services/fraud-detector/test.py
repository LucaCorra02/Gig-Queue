import json
from confluent_kafka import Producer
import uuid

producer = Producer({"bootstrap.servers": "localhost:9092"})

payload = {
    "order_id": uuid.uuid4().hex,
    "event_id": "verdena-concert",
    "user_id": "test-fraud",
    "client_ip": "192.168.1.100",
    "quantity": 2
}
producer.produce("topic-requests", json.dumps(payload).encode("utf-8"))
producer.flush()
print("send on topic-request")