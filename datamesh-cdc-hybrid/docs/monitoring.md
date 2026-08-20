# Prometheus and Grafana Monitoring

## Access

| Service | URL | Credentials |
| --- | --- | --- |
| Grafana | `http://localhost:3000` | `admin` / `admin` |
| Prometheus | `http://localhost:9090` | None |
| Hybrid consumer metrics | `http://localhost:8000/metrics` | None |
| Kafka Connect JMX metrics | `http://localhost:7071/metrics` | None |
| PostgreSQL exporter | `http://localhost:9187/metrics` | None |

The dashboard is provisioned automatically under **Data Mesh → Data Mesh CDC - Hybrid**.

## Metric Boundaries

Prometheus collects three kinds of evidence:

- application metrics from `hybrid-consumer` describe Bronze/Silver outcomes;
- JMX metrics from Kafka Connect describe Debezium capture health;
- PostgreSQL exporter metrics describe warehouse availability and behavior.

## Hybrid Consumer Metrics

| Metric | Meaning |
| --- | --- |
| `hybrid_cdc_events_total` | Processed events labeled by topic, source table and outcome (`promoted`, `bronze_only`, `duplicate`). |
| `hybrid_cdc_projection_failures_total` | Cumulative events accepted by Bronze but rejected by Silver. |
| `hybrid_cdc_unresolved_projection_failures` | Current failures with no later successful Silver event for the same entity. |
| `hybrid_cdc_observed_schema_shapes` | Number of distinct field-set fingerprints per source table. |
| `hybrid_cdc_last_processed_timestamp_seconds` | Timestamp of the most recently processed event per topic. |

Counters describe activity since the consumer started. Stateful gauges are refreshed from PostgreSQL, so unresolved-failure and schema-shape values survive consumer restarts.

## Dashboard Panels

### Unresolved Silver Projections

The current number of Bronze events still excluded from Silver. Zero is healthy. During step 3 of `make demo`, it should rise to one; after the corrective event, it should return to zero.

### Hybrid Consumer Throughput

Events processed per second, separated by source domain. A quiet demo environment naturally reaches zero when no new rows are written; this alone is not an error.

### Seconds Since Last Processed Event

Age of the newest processed event per topic. Interpret it with expected source activity—high age is normal for an idle source but suspicious during active generation.

### Processing Outcomes: Promoted vs Bronze-only

Compares records successfully written to Silver with quarantined Bronze-only records. A Bronze-only series is the distinctive hybrid signal that acquisition succeeded while the consumer contract rejected promotion.

### Silver Projection Failure Rate

Rate of new quarantined records. A spike after a deployment or source migration suggests an incompatible type, renamed field, or removed required field.

### Observed Bronze Schema Shapes

Distinct field sets seen per table. Growth is not automatically bad—an additive field legitimately creates a new shape—but it is a prompt to inspect source evolution.

### Pipeline Component Health

Prometheus scrape health for the hybrid consumer, Kafka Connect JMX exporter and PostgreSQL exporter. `UP` confirms visibility, not necessarily correctness; combine it with failure and throughput panels.

### PostgreSQL DWH Connections

Current warehouse connection count. Use it to spot connection leaks or unexpected load from consumers and dbt.

## Alerts

The provisioned rules include:

- `HybridConsumerDown`: application metrics unavailable for 30 seconds;
- `HybridUnresolvedProjectionFailures`: unresolved Silver failures remain for one minute;
- `HybridProjectionFailureRateHigh`: sustained projection failures above 0.1 records/second;
- `KafkaConnectMetricsDown`: Debezium JMX visibility unavailable;
- `PostgresDWHDown`: warehouse exporter unavailable.

## Demo Walkthrough

Open Grafana before running:

```bash
make demo
```

Watch these transitions:

1. Baseline: `promoted` increases and unresolved failures remain zero.
2. Additive field: observed schema shapes increases; promotion continues.
3. Dropped required field: `bronze_only` and projection failures increase; unresolved becomes one.
4. Corrective update: `promoted` increases and unresolved returns to zero.

The historical failure counter does not decrease because counters record events. The unresolved gauge does decrease because it represents current recovery state.

## Useful PromQL

```promql
sum by (outcome) (rate(hybrid_cdc_events_total[5m]))
hybrid_cdc_unresolved_projection_failures
sum by (source_table) (rate(hybrid_cdc_projection_failures_total[5m]))
hybrid_cdc_observed_schema_shapes
time() - hybrid_cdc_last_processed_timestamp_seconds
up{job=~"hybrid-consumer|kafka-connect-jmx|postgres-exporter"}
```

## Troubleshooting No Data

```bash
curl -fsS http://localhost:8000/metrics | grep hybrid_cdc
curl -fsS http://localhost:9090/api/v1/targets
docker compose logs prometheus hybrid-consumer kafka-connect
```

Counters with labels appear only after the first event. Stateful gauges should appear after the consumer starts and connects to the DWH.
