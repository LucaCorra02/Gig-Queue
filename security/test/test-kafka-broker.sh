docker compose down
docker compose up -d kafka-1 kafka-2 kafka-3
docker compose ps

CONTAINERS=("kafka-1" "kafka-2" "kafka-3")
for container in "${CONTAINERS[@]}"; do
    while [ "$(docker inspect -f '{{.State.Health.Status}}' "$container" 2>/dev/null)" != "healthy" ]; do
        sleep 3
    done
    echo "$container is healthy"
done
echo "alla brokers are healthy"

# should response, certs is valid
docker exec kafka-1 kafka-broker-api-versions \
  --bootstrap-server kafka-1:9093 \
  --command-config /etc/kafka/secrets/client.properties

# should not response, certs not applied
docker exec kafka-1 kafka-broker-api-versions --bootstrap-server kafka-1:9093
docker logs kafka-1 2>&1 | grep -iE "failed authentication|SSLHandshake" | tail -3

# quorom traffic should be encrypted
docker exec kafka-1 kafka-metadata-quorum \
  --bootstrap-server kafka-1:9093 \
  --command-config /etc/kafka/secrets/client.properties \
  describe --status