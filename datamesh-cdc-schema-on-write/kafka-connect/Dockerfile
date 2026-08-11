FROM confluentinc/cp-kafka-connect:7.5.3

USER root

# ─── JMX Prometheus Exporter agent ────────────────────────────────────
# Config is mounted via docker-compose volume:
#   ./prometheus/jmx_exporter_config.yml:/usr/share/jmx_exporter/config.yml:ro
RUN mkdir -p /usr/share/jmx_exporter && \
    curl -fsSL \
    https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar \
    -o /usr/share/jmx_exporter/jmx_prometheus_javaagent.jar

# ─── Debezium PostgreSQL Connector ────────────────────────────────────
RUN mkdir -p /usr/share/confluent-hub-components/debezium-connector-postgresql && \
    curl -fsSL \
    https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.5.0.Final/debezium-connector-postgres-2.5.0.Final-plugin.tar.gz \
    | tar -xz -C /usr/share/confluent-hub-components/debezium-connector-postgresql --strip-components=1

# ─── Confluent JDBC Sink Connector ────────────────────────────────────
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.6

USER appuser
