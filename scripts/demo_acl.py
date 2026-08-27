from confluent_kafka import Consumer, TopicPartition
import time
from demo_utils import BOOTSTRAP, return_valid_ssl_conf, connect, rogue_certificate, QUIET


"""
    Try to read from a topic
    The client is configured with a valid cert
    I use assign instead of subscribe to avoid consumer group coordination (overhead)
"""
def try_consume(conf, topic, group_id = "demo", seconds=8):
    errors = set()
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group_id,
        "enable.auto.commit": False,
        "error_cb": lambda err: errors.add(err.name()),
        **conf,
    }, logger=QUIET)
    consumer.assign([TopicPartition(topic, 0)])

    deadline = time.time() + seconds
    while time.time() < deadline and not errors:
        message = consumer.poll(1.0)
        if message is not None and message.error():
            errors.add(message.error().name())
    consumer.close()
    return errors

def denied(errors):
    return any("AUTHORIZATION" in name for name in errors)

def try_dlq_read_permission():
    print("dlq-service should be able to read from topic-dlq")
    conf = return_valid_ssl_conf("dlq-monitor")
    errors = try_consume(conf, "topic-dlq", group_id="group-dlq")
    if denied(errors):
        print(f"    refused, but it should be allowed: {sorted(errors)}")
    else:
        print("     allowed: the service can do its job")

def try_dlq_read_rejection():
    print("dlq-serive should NOT be able to read from topic-inventory")
    conf = return_valid_ssl_conf("dlq-monitor")
    errors = try_consume(conf, "topic-inventory", group_id="group-dlq")
    if denied(errors):
        print(f"    refused: no auth {sorted(errors)}")
    else:
        print("     allowed:  but it should be allowed")



if __name__ == "__main__":
    try_dlq_read_permission()
    print()
    try_dlq_read_rejection()