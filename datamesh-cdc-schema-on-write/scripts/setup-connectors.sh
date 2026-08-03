#!/bin/bash
set -e

echo "Waiting for Kafka Connect..."
until curl -s http://localhost:8083/connectors > /dev/null; do
  sleep 2
done

echo "Registering connectors..."

curl -X POST -H "Content-Type: application/json" \
  --data @debezium/connectors/customers-connector.json \
  http://localhost:8083/connectors || echo "customers connector may already exist"

curl -X POST -H "Content-Type: application/json" \
  --data @debezium/connectors/orders-connector.json \
  http://localhost:8083/connectors || echo "orders connector may already exist"

echo "Done!"