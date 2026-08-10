# Grafana Setup Guide

## Import Dashboard

1. Open Grafana: http://localhost:3000 (admin / admin)
2. Left menu → **Dashboards** → **Import**
3. Upload `grafana/dashboards/datamesh-cdc.json` or paste JSON
4. Select **Prometheus** datasource
5. Click **Import**

## Configure Prometheus Datasource

If not auto-configured via provisioning:

1. Left menu → **Connections** → **Data sources**
2. Click **Add data source** → **Prometheus**
3. Set URL: `http://prometheus:9090`
4. Click **Save & test**

## Dashboard Panels

### Overview Row
| Panel | Metric | What it shows |
|-------|--------|---------------|
| Connector Task State | `kafka_connect_connector_task_state` | 1=RUNNING (green), 0=FAILED (red) |
| Debezium Events Total | `debezium_events_total` | Cumulative CDC events captured |
| DWH Orders (raw) | `pg_stat_user_tables_n_live_tup` | Live rows in `raw.orders_cdc` |
| DWH Customers (raw) | `pg_stat_user_tables_n_live_tup` | Live rows in `raw.customers_cdc` |

### CDC Source (Debezium)
| Panel | Metric | Alert threshold |
|-------|--------|-----------------|
| Events per Second | `rate(debezium_events_total[1m])` | — |
| Replication Lag (ms) | `debezium_lag_ms` | >60s = warning |
| Snapshot Running | `debezium_snapshot_running` | Stuck snapshot = investigate |
| Source Record Poll Rate | `kafka_connect_source_record_poll_rate` | — |
| Source Active Records | `kafka_connect_source_record_active_count_max` | Backpressure indicator |

### JDBC Sink
| Panel | Metric | What it shows |
|-------|--------|---------------|
| Sink Record Send Rate | `kafka_connect_sink_record_send_rate` | Records/sec written to DWH |
| Sink Active Records | `kafka_connect_sink_record_active_count_max` | Buffered records waiting to sink |

### JVM (Kafka Connect)
| Panel | Metric | What it shows |
|-------|--------|---------------|
| Heap Memory Usage | `jvm_memory_heap_used_bytes` / `max` | Memory pressure |
| GC Collection Count | `rate(jvm_gc_collection_count[1m])` | GC pressure |

### PostgreSQL DWH
| Panel | Metric | What it shows |
|-------|--------|---------------|
| Table Row Counts | `pg_stat_user_tables_n_live_tup{schemaname="raw"}` | Growth of CDC tables |
| Transactions Committed/sec | `rate(pg_stat_database_xact_commit[1m])` | DWH write throughput |
| DB Connections | `pg_stat_activity_count` | Connection pool usage |
| Cache Hit Ratio | `blks_hit / (blks_hit + blks_read)` | Should be >99% |
| Deadlocks/min | `rate(pg_stat_database_deadlocks[1m])` | Should be 0 |

### Active Alerts
| Panel | Query | What it shows |
|-------|-------|---------------|
| Prometheus Alerts | `ALERTS{alertstate="firing"}` | All currently firing alerts |

## Alerting in Grafana

### Option A: Use Prometheus Alertmanager (recommended)

Prometheus evaluates rules from `prometheus/alerts.yml` and sends to Alertmanager.

```yaml
# docker-compose.yml addition
  alertmanager:
    image: prom/alertmanager:v0.26.0
    container_name: alertmanager
    ports:
      - "9093:9093"
    volumes:
      - ./alertmanager/alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro
    networks:
      - datamesh
```

```yaml
# alertmanager/alertmanager.yml
global:
  smtp_smarthost: 'localhost:587'
  smtp_from: 'alerts@datamesh.local'

route:
  receiver: 'default'
  routes:
    - match:
        severity: critical
      receiver: 'pagerduty'
    - match:
        severity: warning
      receiver: 'slack'

receivers:
  - name: 'default'
    slack_configs:
      - api_url: 'YOUR_SLACK_WEBHOOK_URL'
        channel: '#datamesh-alerts'
        title: '{{ .GroupLabels.alertname }}'
        text: '{{ range .Alerts }}{{ .Annotations.description }}\n{{ end }}'

  - name: 'pagerduty'
    pagerduty_configs:
      - service_key: 'YOUR_PAGERDUTY_KEY'

  - name: 'slack'
    slack_configs:
      - api_url: 'YOUR_SLACK_WEBHOOK_URL'
        channel: '#datamesh-warnings'
```

### Option B: Use Grafana Unified Alerting (simpler)

1. Left menu → **Alerting** → **Alert rules**
2. Click **New alert rule**
3. Query: `kafka_connect_connector_task_state < 1`
4. Condition: IS BELOW 1
5. Evaluation: every 30s for 30s
6. Add label: `severity=critical`
7. Contact point: Slack/Email/Webhook

### Webhook Example (for custom notifications)

```python
# scripts/alert_webhook.py
from flask import Flask, request
import json

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    alert = request.json
    for a in alert.get('alerts', []):
        print(f"🚨 {a['labels']['alertname']}: {a['annotations']['summary']}")
    return 'OK'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
```

## Troubleshooting

| Problem | Solution |
|---------|----------|
| "No data" in panels | Check Prometheus targets: http://localhost:9090/targets. Ensure `kafka-connect:7071` and `postgres-exporter:9187` are UP |
| JMX metrics missing | Verify `KAFKA_OPTS` in docker-compose.yml points to jmx_prometheus_javaagent.jar. Check `docker compose logs kafka-connect` for agent load errors |
| postgres_exporter shows no tables | Ensure `PG_EXPORTER_AUTO_DISCOVER_DATABASES=true` or explicit `DATA_SOURCE_NAME` includes `datamesh_dwh` |
| Dashboard import fails | Check Grafana version ≥10.0. Older versions may need manual panel conversion |
| Alerts not firing | Verify evaluation interval in `prometheus.yml` (default 15s). Check `prometheus/alerts.yml` syntax with `promtool check rules` |
| Slow dashboard refresh | Increase scrape interval to 30s or reduce panel query range |

## Useful Links

- Prometheus: http://localhost:9090
- Prometheus Targets: http://localhost:9090/targets
- Prometheus Alerts: http://localhost:9090/alerts
- Grafana: http://localhost:3000
- Kafka Connect JMX: http://localhost:7071/metrics
