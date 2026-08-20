FROM confluentinc/cp-kafka-connect:7.5.3
USER root
RUN mkdir -p /usr/share/confluent-hub-components/debezium-postgres && curl -fsSL https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.5.0.Final/debezium-connector-postgres-2.5.0.Final-plugin.tar.gz | tar -xz -C /usr/share/confluent-hub-components/debezium-postgres --strip-components=1
RUN mkdir -p /usr/share/jmx_exporter && curl -fsSL https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar -o /usr/share/jmx_exporter/jmx_prometheus_javaagent.jar
USER appuser
