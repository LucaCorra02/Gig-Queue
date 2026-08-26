from confluent_kafka import Producer, KafkaException
from utils import run_tests, KAFKA_SECURITY, CERT_DIR
import logging
import subprocess
import tempfile
import os

BOOTSTRAP = "localhost:9092,localhost:9094,localhost:9096"
EXTERNAL_PORTS = [9092, 9094, 9096]

SERVICE_CERTS = ["ticket-api", "inventory", "fraud-detector",
                 "dlq-monitor", "notifier", "admin", "test"] # TODO: add dashboard

QUIET = logging.getLogger("rdkafka-quiet")
QUIET.addHandler(logging.NullHandler())
QUIET.propagate = False

def try_connection(conf, bootstrap=BOOTSTRAP, timeout=5):
    producer = Producer(
        {"bootstrap.servers": bootstrap, "socket.timeout.ms": 3000, **conf},
        logger = QUIET,
    )
    try:
        return producer.list_topics(timeout=timeout)
    except KafkaException:
        return None

def broker_cli(*args, timeout=90):
    # not --command-config with ssl conf
    return subprocess.run(
        ["docker", "exec", "kafka-1", *args],
        capture_output=True, text=True, timeout=timeout,
    )

rogue = {}

# Return a valida certificate signed by another CA
def rogue_certificate():
    if not rogue:
        directory = tempfile.mkdtemp(prefix="gigqueue-fake-")
        crt = os.path.join(directory, "fake.crt")
        key = os.path.join(directory, "fake.key")
        subprocess.run(
            ["openssl", "req", "-new", "-x509", "-nodes", "-days", "1",
             "-subj", "/CN=ticket-api,O=GigQueue,C=IT",
             "-keyout", key, "-out", crt],
            check=True, capture_output=True,
        )
        rogue["crt"], rogue["key"] = crt, key
    return rogue["crt"], rogue["key"]

def test_valid_certificate():
    metadata = try_connection(KAFKA_SECURITY, timeout=10)
    assert metadata is not None, "certificate was rejected"
    assert len(metadata.brokers) == 3, "wrong len brokers"
    assert "topic-requests" in metadata.topics

def test_no_cert():
    for port in EXTERNAL_PORTS:
        metadata = try_connection({}, bootstrap=f"localhost:{port}")
        assert metadata is None, f"broker on port:{port} accept no cert client"


def test_internal_listener():
    result = broker_cli("kafka-broker-api-versions", "--bootstrap-server", "kafka-1:9093")
    assert "(id:" not in result.stdout, "INTERNAL listener answered without a certificate"
    assert "ERROR" in result.stderr, "INTERNAL listener did not reject a no-cert client"

def test_listener_ssl():
    result = broker_cli("cat", "/etc/kafka/kafka.properties")
    assert result.returncode == 0, "kafka.properties not found"
    mapping = [l for l in result.stdout.splitlines()
               if l.startswith("listener.security.protocol.map")]
    assert mapping
    for i in mapping: assert "SSL" in i, i

def test_client_no_cert():
    metadata = try_connection({
        "security.protocol": "SSL",
        "ssl.ca.location": f"{CERT_DIR}/ca.crt",
    })
    assert metadata is None, "clinet have no auth"


def test_foreign_ca():
    crt, key = rogue_certificate()
    metadata = try_connection({
        "security.protocol": "SSL",
        "ssl.ca.location": f"{CERT_DIR}/ca.crt",
        "ssl.certificate.location": crt,
        "ssl.key.location": key,
    })
    assert metadata is None, "certificate signed by an unknown CA accepted"

def test_fake_ca_location():
    crt, _ = rogue_certificate()
    conf = dict(KAFKA_SECURITY)
    conf["ssl.ca.location"] = crt
    metadata = try_connection(conf)
    assert metadata is None, "client trusted a broker not signed by the real CA"


TESTS  = [
    test_valid_certificate,
    test_internal_listener,
    test_listener_ssl,
    test_client_no_cert,
    test_foreign_ca,
    test_fake_ca_location
]

if __name__ == "__main__":
    raise SystemExit(run_tests(TESTS))


