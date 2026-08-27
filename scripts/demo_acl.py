import time
from demo_utils import BOOTSTRAP, return_valid_ssl_conf, connect, rogue_certificate, QUIET, try_consume, denied, try_produce

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

def try_produce_rejection():
    print("dlq-service should NOT be able to write to topic-orders")
    payload = b'{"order_id": "fake-001", "status": "confirmed", "seat": 1}'
    conf = return_valid_ssl_conf("dlq-monitor")
    delivered, error = try_produce(conf, "topic-orders", payload=payload)
    if delivered:
        print("     Error: order was accepted by the broker")
    elif error is not None:
        print(f"    refused: {error.name()}")
    else:
        print("     no delivery report: inconclusive")

def try_dashboard_metadata():
    print("dashboard should be able to read metadata from the broker but not to write to any topic")
    conf = return_valid_ssl_conf("dashboard")
    metadata, error = connect(conf, bootstrap=BOOTSTRAP, timeout=5)
    if metadata is not None:
        print(f"     allowed: {len(metadata.topics)} topics found")
    elif error is not None:
        print(f"     refused: {error}")
    else:
        print("     no response: inconclusive")

    payload = b'{"order_id": "fake-001", "status": "confirmed", "seat": 1}'
    delivered, error = try_produce(conf, "topic-orders", payload=payload)
    if delivered:
        print("     Error: order was accepted by the broker")
    elif error is not None:
        print(f"    refused: {error.name()}")
    else:
        print("     no delivery report: inconclusive")


if __name__ == "__main__":
    try_dlq_read_permission()
    print()
    try_dlq_read_rejection()
    print()
    try_produce_rejection()
    print()
    try_dashboard_metadata()