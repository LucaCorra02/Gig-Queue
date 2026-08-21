from confluent_kafka import Producer
import json

p = Producer({"bootstrap.servers": "localhost:9092"})
msg = {"service": "test", "reason": "test"}

p.produce("topic-dlq", json.dumps(msg).encode("utf-8"))
p.flush()
print("message sent")