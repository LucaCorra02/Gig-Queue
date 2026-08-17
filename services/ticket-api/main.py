import json
import os
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_REQUESTS = os.getenv("TOPIC_REQUESTS", "topic-requests")
print(KAFKA_BOOTSTRAP, TOPIC_REQUESTS)

conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP
}
producer = Producer(conf)

def check_delivery_error(err,msg):
    if err is not None:
        print(f"Error: {err}")
    else:
        print(f"Succeded, topic: {msg.topic()}, offset: {msg.offset()}")

event_id = "gig-1"
payload = {
    "event_id": event_id,
    "user_id": "mario"
}
producer.produce(
    topic= TOPIC_REQUESTS,
    key=event_id.encode('utf-8'),
    value=json.dumps(payload).encode('utf-8'),
    callback=check_delivery_error
)
producer.flush()
print("exit")