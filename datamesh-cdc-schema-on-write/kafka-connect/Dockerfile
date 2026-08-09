FROM confluentinc/cp-kafka-connect:7.5.3

USER root

# Debezium PostgreSQL Connector
RUN mkdir -p /usr/share/confluent-hub-components/debezium-connector-postgresql && \
    curl -fsSL \
    https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.5.0.Final/debezium-connector-postgres-2.5.0.Final-plugin.tar.gz \
    | tar -xz -C /usr/share/confluent-hub-components/debezium-connector-postgresql --strip-components=1

# Confluent JDBC Sink Connector (через confluent-hub — самый надежный способ)
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.6

USER appuser