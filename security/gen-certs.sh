#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
echo Secirity directory: $SCRIPT_DIR

# Load SSL password from .env
if [[ -f ../.env ]]; then
    export $(grep -v '^#' ../.env | xargs)
fi
KEYSTORE_PWD="${KAFKA_SSL_PASSWORD}"
TRUSTSTORE_PWD="${KAFKA_SSL_PASSWORD}"
CLIENT_KEYSTORE_PWD="${KAFKA_SSL_PASSWORD}"
CLIENT_TRUSTSTORE_PWD="${KAFKA_SSL_PASSWORD}"

# Create CA key and certificate
CA_KEY="ca.key"
CA_CERT="ca.crt"
CA_SUBJECT="/CN=GigQueue-CA/O=GigQueue/C=IT"
CA_VALIDITY=3650 # ten years

FORCE=false
if [[ "${1:-}" == "--force" ]]; then
    FORCE=true
    rm -f *.key *.crt *.jks *.csr *.cert-signed *.p12 *-san.cnf ca.srl *_creds
    echo "old certificates removed"
fi

# Create CA key and certificate if they don't exist
if [[ ! -f "$CA_KEY" || ! -f "$CA_CERT" ]]; then
    openssl genrsa -out "$CA_KEY" 4096 # 4096 bit RSA key
    chmod 600 "$CA_KEY"

    # Self-signed CA certificates
    openssl req -new -x509 \
        -key "$CA_KEY" \
        -days "$CA_VALIDITY" \
        -subj "$CA_SUBJECT" \
        -out "$CA_CERT"
    chmod 644 "$CA_CERT"
    echo "CA created"
else
    echo "CA already exists"
fi
#openssl x509 -in "$CA_CERT" -text -noout

BROKER_NAMES=("kafka-1" "kafka-2" "kafka-3")
BROKER_VALIDITY=3650
BROKER_KEY_SIZE=2048
CLIENT_NAMES=("ticket-api" "inventory" "fraud-detector" "dlq-monitor" "notifier" "dashboard" "akhq" "test" "stress-producer")
CLIENT_TRUSTSTORE="client.truststore.jks" # TODO: modify in python

# Check if a file exists, return 1 if not
check_existing() {
    local file="$1"
    if [[ -f "$file" ]]; then
        if $FORCE; then
            rm -f "$file"
            return 1
        else
            return 0
        fi
    fi
    return 1
}

# Create the truststore for kafka brokers by importing the CA certificate
for broker in "${BROKER_NAMES[@]}"; do
    BROKER_TRUSTSTORE="${broker}.server.truststore.jks"
    if ! check_existing "$BROKER_TRUSTSTORE"; then
        keytool -keystore "$BROKER_TRUSTSTORE" \
            -alias CARoot \
            -importcert \
            -file "$CA_CERT" \
            -storepass "$TRUSTSTORE_PWD" \
            -noprompt
        chmod 644 "$BROKER_TRUSTSTORE"
        echo "created truststore for $broker"
    else
        echo "truststore for $broker already exists"
    fi
done

# Create the trust store for java clients (AKHQ)
if ! check_existing "$CLIENT_TRUSTSTORE"; then
    keytool -keystore "$CLIENT_TRUSTSTORE" \
        -alias CARoot \
        -importcert \
        -file "$CA_CERT" \
        -storepass "$CLIENT_TRUSTSTORE_PWD" \
        -noprompt
    chmod 644 "$CLIENT_TRUSTSTORE"
    echo "created java client truststore"
fi
# check if the keystore of the broker 1
# keytool -list -v -keystore security/kafka-1.server.truststore.jks -storepass gigQueueSecret

