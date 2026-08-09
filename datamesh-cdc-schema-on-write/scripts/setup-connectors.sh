#!/bin/bash

set -e

KAFKA_CONNECT_URL="http://localhost:8083"

function wait_for_connect() {
    echo "Waiting for Kafka Connect..."
    until curl -s "$KAFKA_CONNECT_URL/" | grep -q "version"; do
        sleep 2
    done
    echo "Kafka Connect is up."
}

function register_connector() {
    local name=$1
    local config=$2

    echo "Registering connector: $name"
    curl -s -X POST "$KAFKA_CONNECT_URL/connectors"         -H "Content-Type: application/json"         -d "$config" || echo "Connector $name may already exist"
}

wait_for_connect

# ==================== SOURCE CONNECTORS ====================

echo "Registering source connectors..."

register_connector "orders-cdc-connector" '{
  "name": "orders-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres-orders",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "orders_db",
    "topic.prefix": "orders-server",
    "table.include.list": "public.orders",
    "plugin.name": "pgoutput",
    "slot.name": "debezium",
    "publication.name": "dbz_publication",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "true",
    "decimal.handling.mode": "string"
  }
}'

register_connector "customers-cdc-connector" '{
  "name": "customers-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres-customers",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "customers_db",
    "topic.prefix": "customers-server",
    "table.include.list": "public.customers",
    "plugin.name": "pgoutput",
    "slot.name": "debezium",
    "publication.name": "dbz_publication",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "true",
    "decimal.handling.mode": "string"
  }
}'

# ==================== SINK CONNECTORS ====================

echo "Registering JDBC sink connectors..."

register_connector "orders-jdbc-sink" '{
  "name": "orders-jdbc-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "orders-server.public.orders",
    "connection.url": "jdbc:postgresql://postgres-dwh:5432/datamesh_dwh",
    "connection.user": "dwh",
    "connection.password": "dwh",
    "auto.create": "true",
    "auto.evolve": "true",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "pk.fields": "id",
    "table.name.format": "raw.orders_cdc",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081"
  }
}'

register_connector "customers-jdbc-sink" '{
  "name": "customers-jdbc-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "customers-server.public.customers",
    "connection.url": "jdbc:postgresql://postgres-dwh:5432/datamesh_dwh",
    "connection.user": "dwh",
    "connection.password": "dwh",
    "auto.create": "true",
    "auto.evolve": "true",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "pk.fields": "id",
    "table.name.format": "raw.customers_cdc",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081"
  }
}'

echo "Done! All connectors registered."
echo ""
echo "Active connectors:"
curl -s "$KAFKA_CONNECT_URL/connectors" | jq .
