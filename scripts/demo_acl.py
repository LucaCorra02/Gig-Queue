import time
from demo_utils import BOOTSTRAP, return_valid_ssl_conf, connect, rogue_certificate, QUIET, try_consume, denied

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