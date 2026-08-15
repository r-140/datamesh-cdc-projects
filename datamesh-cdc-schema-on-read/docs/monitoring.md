# Monitoring & Alerting

## Overview

This project uses **Prometheus** + **Grafana** for monitoring the CDC pipeline, DWH and Python consumer.

## Metrics Architecture

```
┌─────────────────┐     JMX      ┌──────────────────┐     HTTP     ┌─────────────┐
│  Kafka Connect  │ ───────────> │  JMX Exporter    │ ───────────> │  Prometheus │
│  (Debezium)     │  (port 9999) │  (port 7071)     │  /metrics    │  (port 9090)│
└─────────────────┘              └──────────────────┘              └─────────────┘
                                                                          │
┌─────────────────┐     HTTP     ┌──────────────────┐                   │
│  postgres-dwh   │ ───────────> │ postgres-exporter│ ─────────────────┘
│                 │  (port 9187) │  (port 9187)     │   /metrics
└─────────────────┘              └──────────────────┘
                                                                          │
┌─────────────────┐     HTTP     ┌──────────────────┐                   │
│  Python CDC     │ ───────────> │  Consumer        │ ─────────────────┘
│  Consumer       │  (port 8000) │  /metrics        │   /metrics
│  (prometheus_   │              │  (optional)      │
│   client)       │              │                  │
└─────────────────┘              └──────────────────┘
```

## Components

### JMX Exporter

Java agent attached to Kafka Connect JVM. Exposes Debezium metrics on port `7071`.

Configuration: `kafka-connect/jmx_exporter_config.yml`

Key metrics exposed:
- `debezium_events_total` — Total CDC events captured
- `debezium_lag_ms` — Replication lag in milliseconds
- `debezium_snapshot_running` — Snapshot status (0/1)
- `kafka_connect_connector_task_state` — Connector task state

### postgres-exporter

Sidecar container scraping PostgreSQL DWH metrics on port `9187`.

Key metrics:
- `pg_stat_database_xact_commit` — Transactions committed
- `pg_stat_database_xact_rollback` — Transactions rolled back
- `pg_stat_database_blks_hit` — Cache hits
- `pg_stat_database_blks_read` — Disk reads
- `pg_stat_user_tables_n_live_tup` — Live tuples (row counts)
- `pg_stat_user_tables_n_dead_tup` — Dead tuples
- `pg_database_size_bytes` — Database size

### Python Consumer Metrics (Optional)

If `prometheus-client` is enabled in the consumer:
- `cdc_consumer_messages_total` — Messages processed
- `cdc_consumer_lag` — Consumer lag
- `cdc_consumer_errors_total` — Processing errors

## Prometheus Configuration

`prometheus/prometheus.yml`:

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'kafka-connect'
    static_configs:
      - targets: ['kafka-connect:7071']

  - job_name: 'postgres-exporter'
    static_configs:
      - targets: ['postgres-exporter:9187']

  - job_name: 'cdc-consumer'
    static_configs:
      - targets: ['cdc-consumer:8000']
```

## Alert Rules

Defined in `prometheus/alerts.yml`:

| Alert | Condition | Severity |
|-------|-----------|----------|
| `CDC_Connector_Down` | Connector task not RUNNING for 30s | **critical** |
| `CDC_High_Lag` | Replication lag > 60s for 2m | warning |
| `CDC_Pipeline_Stall` | No events for 5m (not snapshotting) | warning |
| `CDC_Consumer_Down` | Consumer not processing messages | **critical** |
| `DWH_High_Rollback_Rate` | Rollback rate > 5% | warning |
| `DWH_Low_Cache_Hit_Ratio` | Cache hit ratio < 90% | warning |

## Grafana Dashboard

Pre-built dashboard auto-imported via provisioning.

### Dashboard Sections

1. **Overview** — Pipeline health at a glance
   - Connector status (UP/DOWN)
   - Total events processed
   - DWH row counts
   - Consumer lag

2. **CDC Source** — Debezium metrics
   - Events/sec per connector
   - Replication lag
   - Snapshot progress
   - Poll rate

3. **CDC Consumer** — Python consumer metrics
   - Messages consumed/sec
   - Processing latency
   - Error rate
   - Lag per partition

4. **JVM** — Kafka Connect JVM health
   - Heap memory usage
   - GC pressure
   - Thread count

5. **PostgreSQL DWH** — Database health
   - Table sizes
   - Transactions/sec
   - Cache hit ratio
   - Deadlocks
   - Connection count

6. **Alerts** — Firing Prometheus alerts in real-time

## Accessing Monitoring

| Service | URL | Credentials |
|---------|-----|-------------|
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |

## Useful Queries

### Prometheus

```promql
# Connector task state (1 = RUNNING, 0 = FAILED)
kafka_connect_connector_task_state{connector="orders-cdc-connector"}

# Events per second
rate(debezium_events_total[1m])

# Replication lag
debezium_lag_ms / 1000

# DWH row count
pg_stat_user_tables_n_live_tup{relname="orders_cdc"}

# Cache hit ratio
pg_stat_database_blks_hit / (pg_stat_database_blks_hit + pg_stat_database_blks_read)
```

### Grafana

Dashboard is available at: `Dashboards → Browse → Data Mesh CDC (Schema-on-Read)`

## Alertmanager Integration (Optional)

To send alerts to Slack/PagerDuty:

1. Configure `alertmanager.yml`:
```yaml
route:
  receiver: 'slack'
receivers:
  - name: 'slack'
    slack_configs:
      - api_url: 'https://hooks.slack.com/services/...'
        channel: '#alerts'
```

2. Add to `prometheus/prometheus.yml`:
```yaml
alerting:
  alertmanagers:
    - static_configs:
        - targets: ['alertmanager:9093']
```
