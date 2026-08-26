set -uo pipefail

BS=kafka-1:9093
CFG=/etc/kafka/secrets/admin.properties

acl() {
    docker exec kafka-1 kafka-acls \
        --bootstrap-server "$BS" --command-config "$CFG" --add "$@" >/dev/null
}

echo "ticket-api"
acl --allow-principal User:ticket-api --operation Write --topic topic-requests
acl --allow-principal User:ticket-api --operation Read  --group group-inventory

echo "inventory"
acl --allow-principal User:inventory --operation Read  --topic topic-requests
acl --allow-principal User:inventory --operation Read  --group group-inventory
acl --allow-principal User:inventory --operation Write --topic topic-orders
acl --allow-principal User:inventory --operation Write --topic topic-dlq

echo "fraud-detector"
acl --allow-principal User:fraud-detector --operation Read  --topic topic-requests
acl --allow-principal User:fraud-detector --operation Read  --group group-fraud
acl --allow-principal User:fraud-detector --operation Write --topic topic-fraud

echo "notifier"
acl --allow-principal User:notifier --operation Read --topic topic-orders
acl --allow-principal User:notifier --operation Read --topic topic-fraud
acl --allow-principal User:notifier --operation Read --group group-notifier

echo "dlq-monitor"
acl --allow-principal User:dlq-monitor --operation Read --topic topic-dlq
acl --allow-principal User:dlq-monitor --operation Read --group group-dlq

echo "dashboard (observer only)"
acl --allow-principal User:dashboard --operation Describe --topic '*'
acl --allow-principal User:dashboard --operation Describe --group '*'
acl --allow-principal User:dashboard --operation Describe --cluster

echo "akhq (read-only GUI)"
acl --allow-principal User:akhq --operation Describe        --topic '*'
acl --allow-principal User:akhq --operation DescribeConfigs --topic '*'
acl --allow-principal User:akhq --operation Read            --topic '*'
acl --allow-principal User:akhq --operation Describe        --group '*'
acl --allow-principal User:akhq --operation Read            --group '*'
acl --allow-principal User:akhq --operation Describe        --cluster

echo "test harness"
acl --allow-principal User:test --operation Read  --topic '*'
acl --allow-principal User:test --operation Write --topic '*'
acl --allow-principal User:test --operation Read  --group '*'

echo
echo "Current acl:"
docker exec kafka-1 kafka-acls --bootstrap-server "$BS" --command-config "$CFG" --list