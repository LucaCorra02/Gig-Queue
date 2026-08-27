from confluent_kafka import Producer, KafkaException
import logging

BOOTSTRAP = "localhost:9092,localhost:9094,localhost:9096"
EXTERNAL_PORTS = [9092, 9094, 9096]
CERT_DIR = "../security"

QUIET = logging.getLogger("rdkafka-quiet")
QUIET.addHandler(logging.NullHandler())
QUIET.propagate = False

def return_valid_ssl_conf(service_name):
    return {
        "security.protocol": "SSL",
        "ssl.ca.location": f"{CERT_DIR}/ca.crt",
        "ssl.certificate.location": f"{CERT_DIR}/{service_name}.crt",
        "ssl.key.location": f"{CERT_DIR}/{service_name}.key",
    }


def connect(conf, bootstrap=BOOTSTRAP, timeout=8):
    producer = Producer(
        {"bootstrap.servers": bootstrap, "socket.timeout.ms": 3000, **conf},
        logger = QUIET, # too many logs
    )
    try:
        return producer.list_topics(timeout=timeout), None
    except KafkaException as exc:
        return None, str(exc.args[0])
