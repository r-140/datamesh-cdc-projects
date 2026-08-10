# Grafana Monitoring Guide

## Access

- **URL**: http://localhost:3000
- **Login**: `admin`
- **Password**: `admin`

## Auto-Provisioning

Datasource and dashboards are provisioned automatically on startup via:

```
grafana/provisioning/datasources/prometheus.yml   → Prometheus datasource
grafana/provisioning/dashboards/dashboard.yml     → Dashboard provider
grafana/dashboards/datamesh-cdc.json              → CDC Monitoring dashboard
```

If dashboards are missing after restart:

```bash
# Reload provisioning
curl -X POST http://admin:admin@localhost:3000/api/admin/provisioning/dashboards/reload

# Or restart Grafana container
docker compose restart grafana
```

## Dashboard: Data Mesh CDC Monitoring

Located in **Browse → Dashboards → Data Mesh → Data Mesh CDC Monitoring**

### Overview Row

| Panel | Type | Metric | Interpretation |
|-------|------|--------|----------------|
| **Connector Task State** | Stat | `kafka_connect_connector_task_state` | `1` = RUNNING (green), `0` = FAILED (red). *Note: If showing "No data", the JMX metric name may need adjustment in `jmx_exporter_config.yml`.* |
| **Debezium Events Total** | Stat | `debezium_events_total` | Cumulative CDC events. `customers-server` (snapshot vs streaming) and `orders-server` shown separately. |
| **DWH Orders (raw)** | Stat | `pg_stat_user_tables_n_live_tup` | Live rows in `raw.orders_cdc`. Should grow as new orders are inserted. |
| **DWH Customers (raw)** | Stat | `pg_stat_user_tables_n_live_tup` | Live rows in `raw.customers_cdc`. Should grow as new customers are inserted. |

### CDC Source (Debezium) Row

| Panel | Type | Metric | Normal / Alert |
|-------|------|--------|----------------|
| **Events per Second** | Time Series | `rate(debezium_events_total[1m])` | Spikes during data generation, flat at 0 during idle. |
| **Replication Lag (ms)** | Time Series | `debezium_lag_ms` | **🟡 Fires `CDC_HighLag`** when > 60s. High lag during idle is expected (last event was long ago). |
| **Snapshot Running** | Time Series | `debezium_snapshot_running` | `1` during initial snapshot, `0` during streaming. |
| **Source Record Poll Rate** | Time Series | `kafka_connect_source_record_poll_rate` | Records/sec polled from WAL. |
| **Source Active Records (max)** | Time Series | `kafka_connect_source_record_active_count_max` | Buffered records in connector. |

### JDBC Sink Row

| Panel | Type | Metric | Normal / Alert |
|-------|------|--------|----------------|
| **Sink Record Send Rate** | Time Series | `kafka_connect_sink_record_send_rate` | Records/sec written to DWH. **🟡 Fires `CDC_SinkStall`** when flat at 0 for 2m. |
| **Sink Active Records (max)** | Time Series | `kafka_connect_sink_record_active_count_max` | Backpressure indicator. Growing = sink can't keep up. |

### JVM (Kafka Connect) Row

| Panel | Type | Metric | Interpretation |
|-------|------|--------|----------------|
| **Heap Memory Usage** | Time Series | `jvm_memory_heap_used_bytes` / `max` | Memory pressure. If used approaches max → increase `-Xmx`. |
| **GC Collection Count** | Time Series | `rate(jvm_gc_collection_count[1m])` | Frequent GC = memory pressure or suboptimal heap size. |

### PostgreSQL DWH Row

| Panel | Type | Metric | Interpretation |
|-------|------|--------|----------------|
| **Table Row Counts (raw)** | Time Series | `pg_stat_user_tables_n_live_tup{schemaname="raw"}` | Growth of CDC tables over time. |
| **Transactions Committed/sec** | Time Series | `rate(pg_stat_database_xact_commit[1m])` | DWH write throughput. Spikes during CDC sink or dbt runs. |
| **DB Connections** | Time Series | `pg_stat_activity_count` | Active connections. **🟡 Fires `PostgresHighConnections`** when > 20. |
| **Cache Hit Ratio** | Time Series | `blks_hit / (blks_hit + blks_read)` | Should be > 99%. Low = insufficient shared_buffers. |
| **Deadlocks/min** | Time Series | `rate(pg_stat_database_deadlocks[1m])` | Should be 0. Any deadlocks indicate application bug. |

### Active Alerts Row

| Panel | Type | Query | Purpose |
|-------|------|-------|---------|
| **Prometheus Alerts** | Table | `ALERTS{alertstate="firing"}` | Lists all currently firing alerts with labels and annotations. |

## Alert Rules

Prometheus evaluates rules from `prometheus/alerts.yml`. View at:
- **Prometheus**: http://localhost:9090/alerts
- **Grafana**: http://localhost:3000/alerting/list

### Existing Alerts (from your alerts.yml)

| Alert | Condition | Severity | Typical State | Action if Firing |
|-------|-----------|----------|---------------|------------------|
| **PostgresDWHDown** | `up{job="postgres-exporter"} == 0` | critical | Normal | Check `docker compose ps postgres-dwh` |
| **PostgresHighConnections** | `pg_stat_activity_count{datname="datamesh_dwh"} > 20` | warning | Normal | Find idle sessions: `SELECT * FROM pg_stat_activity WHERE state = 'idle';` |
| **PostgresDeadTuplesHigh** | `pg_stat_user_tables_n_dead_tup > 10000` | warning | Normal | Run `VACUUM ANALYZE;` |
| **KafkaConnectDown** | `up{job="kafka-connect"} == 0` | critical | **🔴 Firing** | REST API job is deprecated — remove from `prometheus.yml` |

### New CDC Alerts (JMX-based)

| Alert | Condition | Severity | Typical State | Action if Firing |
|-------|-----------|----------|---------------|------------------|
| **CDC_ConnectorTaskFailed** | `kafka_connect_connector_task_state != 1` | critical | Normal | Check `docker compose logs kafka-connect`. Likely schema mismatch. |
| **CDC_HighLag** | `debezium_lag_ms > 60000` | warning | **🟡 Firing** (idle) | Expected when no new events. **Not actionable during idle.** |
| **CDC_PipelineStall** | `rate(debezium_events_total[5m]) == 0 AND snapshot == 0` | warning | **🟡 Firing** (idle) | Expected when no data changes. **Not actionable during idle.** |
| **CDC_SinkStall** | `rate(kafka_connect_sink_record_send_rate[5m]) == 0` | warning | **🟡 Firing** (idle) | Expected when no Kafka messages. **Not actionable during idle.** |

> **Note on idle firing**: `CDC_HighLag`, `CDC_PipelineStall`, and `CDC_SinkStall` fire when the pipeline is idle (no data changes). This is expected behavior — lag grows because the last event was a long time ago. To avoid noise, either:
> - Silence these alerts during known idle periods
> - Increase `for:` duration to 30m
> - Add a condition `AND on() vector(1) == 1` only when source DB has recent writes (requires additional metric)

## Troubleshooting

### "No data" in Connector Task State panel

The JMX metric `kafka_connect_connector_task_state` may not be exposed with the expected pattern. Check:

```bash
# Verify JMX exporter is serving metrics
curl -s http://localhost:7071/metrics | grep connector | head -20

# If missing, check JMX config
cat prometheus/jmx_exporter_config.yml
```

**Fix**: Update `jmx_exporter_config.yml` pattern to match your Kafka Connect version's MBean names.

### Dashboard shows "No data sources found"

```bash
# Verify datasource provisioning file exists
cat grafana/provisioning/datasources/prometheus.yml

# Should show:
# apiVersion: 1
# datasources:
#   - name: Prometheus
#     type: prometheus
#     url: http://prometheus:9090

# If file is at grafana/datasources.yml instead, move it:
mkdir -p grafana/provisioning/datasources
mv grafana/datasources.yml grafana/provisioning/datasources/prometheus.yml
docker compose restart grafana
```

### Prometheus targets down

```bash
# Check all targets
curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job, health, lastError}'

# Expected:
# kafka-connect-jmx: UP
# postgres-exporter: UP
# prometheus: UP
```

If `kafka-connect` (REST, port 8083) shows `INVALID` error — **remove this job** from `prometheus.yml`, it serves JSON not metrics.

### Alerts firing after breaking change demo

After running `breaking_change_demo.py`:

```bash
# 1. Check connector status
curl http://localhost:8083/connectors/orders-cdc-connector/status

# 2. If FAILED, check logs
docker compose logs kafka-connect --tail=50 | grep ERROR

# 3. Restart connector
curl -X POST http://localhost:8083/connectors/orders-cdc-connector/restart?includeTasks=true

# 4. Verify alert clears in Grafana → Alerting → Alert Rules
```

## Data Generator & Live Testing

Generate data and watch panels update in real-time:

```bash
# Batch insert (watch Debezium Events Total and DWH row counts grow)
python scripts/data_generator.py --mode batch --count 20

# Continuous streaming (watch Events per Second and Sink Send Rate)
python scripts/data_generator.py --mode continuous --interval 2

# Verify state
python scripts/data_generator.py --mode verify
```

## Useful URLs

| Service | URL | Purpose |
|---------|-----|---------|
| Grafana | http://localhost:3000 | Dashboards & alerting |
| Prometheus | http://localhost:9090 | Metrics & alert evaluation |
| Prometheus Targets | http://localhost:9090/targets | Scrape health |
| Prometheus Alerts | http://localhost:9090/alerts | Firing alerts |
| Kafka Connect JMX | http://localhost:7071/metrics | Raw JMX metrics |
| Kafka Connect REST | http://localhost:8083/connectors | Connector management |
