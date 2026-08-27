from demo_utils import connect, return_valid_ssl_conf, BOOTSTRAP, EXTERNAL_PORTS, CERT_DIR

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

if __name__ == "__main__":
    demo_valid_certificate()
    print()
    client_no_cert()


