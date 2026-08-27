from demo_utils import connect, return_valid_ssl_conf, rogue_certificate, BOOTSTRAP, EXTERNAL_PORTS, CERT_DIR

"""
    The demo test the connection to kafka cluster with mtls enabled.
    a valid certificate is required to connect.
"""

def demo_valid_certificate():
    print("Valid ssl certificate (with ACL not all topics are visible):")
    valid_conf = return_valid_ssl_conf("inventory")
    metadata, error = connect(valid_conf, timeout=10)
    if metadata is None:
        print(f"    Failed to connect to kafka cluster: {error}")
    else:
        print(f"    Connected to kafka cluster with {len(metadata.brokers)} brokers")
        print(f"    Topics: {metadata.topics.keys()}")

def client_no_cert():
    print("Client with no cert try to connect to a broker:")
    for port in EXTERNAL_PORTS:
        metadata, reason = connect({}, bootstrap=f"localhost:{port}", timeout=5)
        if metadata:
            print(f"    port {port} answered a plaintext client")
        else:
            print(f"    port {port} refused the connection")

def client_use_tls():
    print("Client with TLS but no cert try to connect to a broker")
    valid_conf = return_valid_ssl_conf("inventory")
    conf = {
        "security.protocol": "SSL",
        "ssl.ca.location": valid_conf["ssl.ca.location"], #only trust store
    }
    metadata, reason = connect(conf, timeout=5)
    if metadata:
        print(f"    broker answered a TLS client with no cert")
    else:
        print(f"    broker refused the connection: {reason}")

def fake_ca():
    print("Client with a valid certificate signed by a foreign CA try to connect to a broker:")
    fake_crt, fake_key = rogue_certificate(common_name="ticket-api") #ticket-api valid cert but not CA
    metadata, reason = connect({
        "security.protocol": "SSL",
        "ssl.ca.location": f"{return_valid_ssl_conf('ticket-api')['ssl.ca.location']}",
        "ssl.certificate.location": fake_crt,
        "ssl.key.location": fake_key,
    })
    if metadata:
        print("     Fake certificate was accepted by the broker")
    else:
        print("     Refused: client has a valid certificate but signed by a foreign CA")

def client_refuse_server():
    print("Client trust store contains foreing CA")
    conf = return_valid_ssl_conf("inventory")
    fake_crt, fake_key = rogue_certificate(common_name="inventory")
    conf["ssl.ca.location"] = fake_crt # client trust store is a fake CA
    metadata, reason = connect(conf)
    if metadata:
        print("     Error, broker accepted a fake CA")
    else:
        print("     Refused: client has a valid certificate but trust store is a foreign CA")


if __name__ == "__main__":
    demo_valid_certificate()
    print()
    client_no_cert()
    print()
    client_use_tls()
    print()
    fake_ca()
    print()
    client_refuse_server()


