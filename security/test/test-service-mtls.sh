docker compose up -d --build dlq-monitor

echo "check if the mTLS handshake have been done"

docker exec kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:9093 --command-config /etc/kafka/secrets/admin.properties --describe --group group-dlq

echo "check for broker error"

docker logs kafka-1 2>&1 | grep -i "failed authentication" | tail -3
