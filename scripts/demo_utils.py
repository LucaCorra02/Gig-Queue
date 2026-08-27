from confluent_kafka import Producer, KafkaException
import logging
import subprocess
import tempfile
import os

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

_rogue = {}

def rogue_certificate(common_name="ticket-api"): # Return a valid certificate signed by another CA
    if not _rogue:
        directory = tempfile.mkdtemp(prefix="gigqueue-fake-")
        crt = os.path.join(directory, "fake.crt")
        key = os.path.join(directory, "fake.key")
        subprocess.run(
            ["openssl", "req", "-new", "-x509", "-nodes", "-days", "1",
             "-subj", f"/CN={common_name},O=GigQueue,C=IT",
             "-keyout", key, "-out", crt],
            check=True, capture_output=True,
        )
        _rogue["crt"], _rogue["key"] = crt, key
    return _rogue["crt"], _rogue["key"]