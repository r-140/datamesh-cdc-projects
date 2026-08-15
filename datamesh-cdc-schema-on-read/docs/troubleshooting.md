# Troubleshooting Guide

## Quick Diagnostics

```bash
# Check all services are running
docker ps

# Check connector status
make connectors

# Check consumer logs
tail -f logs/consumer.log

# Verify data flow
python scripts/data_generator.py --mode verify
```

## Common Issues

### `make up` hangs on "Waiting for Kafka Connect"

**Cause**: Kafka Connect takes time to start on first run.

**Solution**:
- Wait up to 2 minutes
- Check logs: `docker logs kafka-connect`
- Verify Kafka is healthy: `docker logs kafka | tail -n 20`

### Consumer not writing to DWH

**Cause**: Consumer not started or crashed.

**Solution**:
```bash
# Start consumer
make consumer

# Check logs
tail -f logs/consumer.log

# Check DWH connectivity
psql -h localhost -p 5434 -U dwh -d datamesh_dwh -c "SELECT 1;"
```

### `ModuleNotFoundError: confluent_kafka`

**Cause**: Missing Python dependency.

**Solution**:
```bash
pip install "confluent-kafka[avro]"
# Or install all dev dependencies
pip install -e ".[dev]"
```

### `Connector X may already exist` / HTTP 409

**Cause**: Script is idempotent — this is not an error.

**Solution**: Safe to ignore. Run `make connectors` again safely.

### `curl: (22) The requested URL returned error: 400` on connector registration

**Cause**: Invalid JSON in connector config.

**Solution**:
- Check `scripts/setup-connectors.sh` for trailing commas
- Validate JSON: `cat scripts/setup-connectors.sh | python -m json.tool`
- Check for duplicate keys in connector JSON

### Connectors disappear after `make down`

**Cause**: Expected behavior. Connectors live in Kafka topics (`connect-configs`, `connect-offsets`, `connect-status`) which survive `make down`.

**Solution**:
```bash
# Re-register connectors
make connectors

# Or full reset
make reset && make up
```

### Connectors disappear after `make reset`

**Cause**: `down -v` wipes all Kafka data including Connect internal topics.

**Solution**: Run `make up` — the `setup-connectors` service will auto-register them.

### `relation "customers" does not exist`

**Cause**: Init SQL scripts not mounted properly.

**Solution**:
- Check `docker-compose.yml` volumes:
  ```yaml
  volumes:
    - ./scripts/init_customers.sql:/docker-entrypoint-initdb.d/init.sql:ro
  ```
- Restart source DBs: `docker restart postgres-customers`

### Prometheus not scraping Kafka Connect

**Cause**: JMX Exporter agent missing or `KAFKA_OPTS` misconfigured.

**Solution**:
```bash
# Verify JMX Exporter is running
curl http://localhost:7071/metrics | head

# Check Dockerfile
# kafka-connect/Dockerfile should include:
# ENV KAFKA_OPTS="-javaagent:/opt/jmx_prometheus_javaagent.jar=7071:/opt/jmx_exporter_config.yml"
```

### Grafana alert not firing

**Cause**: Datasource or alert rule issue.

**Solution**:
1. Verify Prometheus is configured as datasource in Grafana
2. Check alert rule evaluation interval
3. Test alert query in Prometheus UI first
4. Check Grafana alert state: `Alerting → Alert Rules`

### `prometheus.yml is a directory`

**Cause**: File/directory name collision.

**Solution**:
```bash
rm -rf prometheus/prometheus.yml
# Ensure prometheus/prometheus.yml is a file, not directory
```

### MinIO port conflict

**Cause**: Existing container on port 9000/9001.

**Solution**:
```bash
docker rm -f quantum-sim-minio-1
docker-compose up -d
```

### Kafka `CLUSTER_ID` invalid

**Cause**: Wrong format for KRaft cluster ID.

**Solution**: Use base64 UUID format:
```bash
export CLUSTER_ID="MkU3OEVBNTYwNTUENDI2Qg"
```

### dbt test fails after breaking change demo

**Cause**: Schema-on-Read behavior — Silver model expects column that was dropped.

**Solution**: This is **expected**! Restore the column:
```bash
# For orders table
PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -d orders -c   "ALTER TABLE orders ADD COLUMN total_amount DECIMAL(12,2);"

# For customers table
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d customers -c   "ALTER TABLE customers ADD COLUMN email VARCHAR(255);"
```

Then re-run dbt:
```bash
cd dbt_datamesh && dbt run && dbt test
```

### High consumer lag

**Cause**: Consumer cannot keep up with production rate.

**Solution**:
1. Check consumer CPU/memory usage
2. Check DWH performance (slow writes)
3. Consider scaling consumer instances (multiple consumers in same group)
4. Check for DWH locks: `SELECT * FROM pg_locks WHERE NOT granted;`

### DWH disk space growing rapidly

**Cause**: JSONB tables accumulate all CDC history.

**Solution**:
```sql
-- Check table sizes
SELECT schemaname, relname, pg_size_pretty(pg_total_relation_size(relid))
FROM pg_stat_user_tables
WHERE schemaname = 'raw';

-- Optional: vacuum to reclaim space
VACUUM FULL raw.orders_cdc;
```

Consider implementing retention policy for raw CDC data.

### Schema Registry connection errors

**Cause**: Schema Registry not ready or wrong URL.

**Solution**:
```bash
# Check Schema Registry health
curl http://localhost:8081/subjects

# Verify URL in consumer config
# Should be: http://schema-registry:8081 (inside Docker network)
# Or: http://localhost:8081 (from host)
```

## Getting Help

1. Check logs: `docker logs <container_name>`
2. Run verification: `python scripts/data_generator.py --mode verify`
3. Check monitoring: http://localhost:3000 (Grafana)
4. Review README for latest updates
