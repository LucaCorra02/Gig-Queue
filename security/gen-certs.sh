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

# Check if a file exists, return 1 if exists
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

# Create the keystore and certificate for each broker
for broker in "${BROKER_NAMES[@]}"; do
    BROKER_KEYSTORE="${broker}.server.keystore.jks"
    if check_existing "$BROKER_KEYSTORE"; then
        echo "keystore for $broker already exist"
        continue;
    fi # already exists

    # Crete the coupled key for JKS
    keytool -genkeypair -keystore "$BROKER_KEYSTORE" -alias "$broker" -validity "$BROKER_VALIDITY" -keyalg RSA -keysize "$BROKER_KEY_SIZE" \
        -dname "CN=${broker},O=GigQueue,C=IT" \
        -ext "SAN=DNS:${broker},DNS:localhost,IP:127.0.0.1" \
        -storepass "$KEYSTORE_PWD" -keypass "$KEYSTORE_PWD" -noprompt

    # Create a CSR request for brokers certificates
    CSR_FILE="${broker}.csr"
    keytool -certreq -keystore "$BROKER_KEYSTORE" -alias "$broker" -file "$CSR_FILE" \
        -storepass "$KEYSTORE_PWD" -keypass "$KEYSTORE_PWD" \
        -ext "SAN=DNS:${broker},DNS:localhost,IP:127.0.0.1"

    # Sign the CSR with the CA to create a signed certificate for the broker
    SIGNED_CERT="${broker}.cert-signed"
    SAN_EXT_FILE="${broker}-san.cnf"
    cat > "$SAN_EXT_FILE" <<EOF
[san_ext]
subjectAltName = DNS:${broker},DNS:localhost,IP:127.0.0.1
EOF

    openssl x509 -req -CA "$CA_CERT" -CAkey "$CA_KEY" -in "$CSR_FILE" -out "$SIGNED_CERT" \
        -days "$BROKER_VALIDITY" -CAcreateserial -extfile "$SAN_EXT_FILE" -extensions san_ext

    # Import the CA certificate and the signed broker certificate into the broker kaystore
    keytool -importcert -keystore "$BROKER_KEYSTORE" -alias CARoot -file "$CA_CERT" -storepass "$KEYSTORE_PWD" -noprompt
    keytool -importcert -keystore "$BROKER_KEYSTORE" -alias "$broker" -file "$SIGNED_CERT" -storepass "$KEYSTORE_PWD" -keypass "$KEYSTORE_PWD" -noprompt
    chmod 644 "$BROKER_KEYSTORE"
    rm -f "$CSR_FILE" "$SIGNED_CERT" "$SAN_EXT_FILE"
    echo "created keytore for $broker broker"
done
# For check the keystore of the broker 1:
# keytool -list -v -keystore kafka-1.server.keystore.jks -storepass gigQueueSecret

# Create the keystore and certificate for docker containers
echo -n "$KEYSTORE_PWD" > "keystore_creds"
echo -n "$KEYSTORE_PWD" > "key_creds"
echo -n "$TRUSTSTORE_PWD" > "truststore_creds"

for broker in "${BROKER_NAMES[@]}"; do
    echo -n "$KEYSTORE_PWD" > "${broker}.keystore_creds"
    echo -n "$KEYSTORE_PWD" > "${broker}.key_creds"
    echo -n "$TRUSTSTORE_PWD" > "${broker}.truststore_creds"
done