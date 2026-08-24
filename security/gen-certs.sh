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