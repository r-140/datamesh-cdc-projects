#!/bin/bash

echo "=== 1. Connector Detailed Status ==="
for conn in orders-cdc-connector customers-cdc-connector orders-jdbc-sink customers-jdbc-sink; do
    echo "--- $conn ---"
    curl -s http://localhost:8083/connectors/$conn/status | jq .
done

echo ""
echo "=== 2. Kafka Topics ==="
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

echo ""
echo "=== 3. Kafka Topic Offsets (beginning vs end) ==="
for topic in orders-server.public.orders customers-server.public.customers; do
    echo "--- $topic ---"
    docker exec kafka kafka-run-class kafka.tools.GetOffsetShell         --broker-list localhost:29092 --topic $topic --time -2
    docker exec kafka kafka-run-class kafka.tools.GetOffsetShell         --broker-list localhost:29092 --topic $topic --time -1
done

echo ""
echo "=== 4. Replication Slots (source DBs) ==="
docker exec postgres-orders psql -U postgres -d orders_db -c "SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots;"
docker exec postgres-customers psql -U postgres -d customers_db -c "SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots;"

echo ""
echo "=== 5. Kafka Connect Logs (last 50 lines, errors only) ==="
docker compose logs kafka-connect --tail=50 | grep -E "ERROR|WARN|Exception|failed|Failed" | tail -20

echo ""
echo "=== 6. Check if JDBC Sink is actually writing ==="
docker exec postgres-dwh psql -U dwh -d datamesh_dwh -c "SELECT schemaname, tablename, n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_user_tables WHERE tablename IN ('orders_cdc', 'customers_cdc');"

echo ""
echo "=== 7. Sample data from DWH ==="
docker exec postgres-dwh psql -U dwh -d datamesh_dwh -c "SELECT * FROM raw.orders_cdc ORDER BY id DESC LIMIT 5;"
docker exec postgres-dwh psql -U dwh -d datamesh_dwh -c "SELECT * FROM raw.customers_cdc ORDER BY id DESC LIMIT 5;"
