#!/bin/bash
set -e

echo "Waiting for Kafka Connect to be ready..."
while ! curl -s http://localhost:8083/connectors > /dev/null 2>&1; do
    sleep 2
done

echo "Registering Debezium connectors..."

curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/connectors/orders-connector.json

echo "Orders connector registered"

curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/connectors/customers-connector.json

echo "Customers connector registered"

echo "All connectors registered successfully!"
