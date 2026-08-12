#!/bin/bash
set -euo pipefail

KAFKA_CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
MAX_RETRIES="${MAX_RETRIES:-30}"
SLEEP_SEC="${SLEEP_SEC:-2}"

# ─── Helpers ──────────────────────────────────────────────────────────

log_info()  { echo "[INFO]  $*"; }
log_warn()  { echo "[WARN]  $*"; }
log_error() { echo "[ERROR] $*"; }

wait_for_connect() {
    log_info "Waiting for Kafka Connect at ${KAFKA_CONNECT_URL} ..."
    local i=0
    until curl -sf "${KAFKA_CONNECT_URL}/" >/dev/null 2>&1; do
        i=$((i + 1))
        if [[ $i -ge $MAX_RETRIES ]]; then
            log_error "Kafka Connect did not become ready after ${MAX_RETRIES} attempts"
            exit 1
        fi
        sleep "${SLEEP_SEC}"
    done
    log_info "Kafka Connect is ready."
}

connector_exists() {
    local name=$1
    local code
    code=$(curl -s -o /dev/null -w "%{http_code}" "${KAFKA_CONNECT_URL}/connectors/${name}")
    [[ "$code" == "200" ]]
}

register_connector() {
    local name=$1
    local config=$2

    if connector_exists "$name"; then
        log_warn "Connector '${name}' already exists — skipping."
        return 0
    fi

    log_info "Registering connector: ${name}"
    local http_code body
    body=$(curl -s -w "\n%{http_code}" \
        -X POST "${KAFKA_CONNECT_URL}/connectors" \
        -H "Content-Type: application/json" \
        -d "$config")

    http_code=$(echo "$body" | tail -n1)
    body=$(echo "$body" | sed '$d')

    if [[ "$http_code" == "201" ]]; then
        log_info "Connector '${name}' registered successfully."
    elif [[ "$http_code" == "409" ]]; then
        log_warn "Connector '${name}' conflict (already exists)."
    else
        log_error "Failed to register '${name}' (HTTP ${http_code}): ${body}"
        return 1
    fi
}

# ─── Main ─────────────────────────────────────────────────────────────

wait_for_connect

# ==================== SOURCE CONNECTORS ONLY ====================

log_info "Registering source connectors..."

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
    "slot.name": "debezium_orders",
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
    "delete.enabled": "true",
    "time.precision.mode": "connect",
    "decimal.handling.mode": "precise"
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
    "slot.name": "debezium_customers",
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
    "delete.enabled": "true",
    "time.precision.mode": "connect",
    "decimal.handling.mode": "precise"
  }
}'

log_info "Done! Active connectors:"
curl -s "${KAFKA_CONNECT_URL}/connectors" | python3 -m json.tool 2>/dev/null || curl -s "${KAFKA_CONNECT_URL}/connectors"
echo
