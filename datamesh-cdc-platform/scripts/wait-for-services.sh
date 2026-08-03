#!/bin/bash
set -e

echo "Waiting for services to be ready..."

# Wait for PostgreSQL
until pg_isready -h localhost -p 5432 -U postgres > /dev/null 2>&1; do
    echo "Waiting for postgres-orders..."
    sleep 2
done
echo "✅ postgres-orders ready"

until pg_isready -h localhost -p 5433 -U postgres > /dev/null 2>&1; do
    echo "Waiting for postgres-customers..."
    sleep 2
done
echo "✅ postgres-customers ready"

# Wait for Kafka
until kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    echo "Waiting for kafka..."
    sleep 2
done
echo "✅ kafka ready"

# Wait for Schema Registry
until curl -s http://localhost:8081/subjects > /dev/null 2>&1; do
    echo "Waiting for schema-registry..."
    sleep 2
done
echo "✅ schema-registry ready"

# Wait for Kafka Connect
until curl -s http://localhost:8083/connectors > /dev/null 2>&1; do
    echo "Waiting for kafka-connect..."
    sleep 2
done
echo "✅ kafka-connect ready"

# Wait for MinIO
until curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; do
    echo "Waiting for minio..."
    sleep 2
done
echo "✅ minio ready"

# Wait for Iceberg REST
until curl -s http://localhost:8181/v1/config > /dev/null 2>&1; do
    echo "Waiting for iceberg-rest..."
    sleep 2
done
echo "✅ iceberg-rest ready"

echo ""
echo "🎉 All services are ready!"
echo ""
echo "Available endpoints:"
echo "  PostgreSQL Orders:    localhost:5432"
echo "  PostgreSQL Customers: localhost:5433"
echo "  Kafka:                localhost:9092"
echo "  Schema Registry:      http://localhost:8081"
echo "  Kafka Connect:        http://localhost:8083"
echo "  MinIO Console:        http://localhost:9001 (minio / minio123)"
echo "  Iceberg REST:         http://localhost:8181"
echo "  Trino:                http://localhost:8080"
echo "  Grafana:              http://localhost:3000 (admin / admin)"
echo "  Prometheus:           http://localhost:9090"
