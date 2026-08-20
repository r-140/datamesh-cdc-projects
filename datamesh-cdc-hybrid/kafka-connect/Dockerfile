FROM confluentinc/cp-kafka-connect:7.5.3
USER root
RUN mkdir -p /usr/share/confluent-hub-components/debezium-postgres && curl -fsSL https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.5.0.Final/debezium-connector-postgres-2.5.0.Final-plugin.tar.gz | tar -xz -C /usr/share/confluent-hub-components/debezium-postgres --strip-components=1
USER appuser
