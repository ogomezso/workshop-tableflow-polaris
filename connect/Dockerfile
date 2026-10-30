# Kafka Connect with the Debezium MySQL and PostgreSQL connectors and the Confluent Avro converter (bundled in cp-kafka-connect).
ARG CP_VERSION=7.9.0
FROM alpine:3.20 AS plugins
ARG DEBEZIUM_VERSION=2.7.4.Final
RUN mkdir /plugins && cd /plugins && \
    for c in mysql postgres; do \
      wget -qO- "https://repo1.maven.org/maven2/io/debezium/debezium-connector-${c}/${DEBEZIUM_VERSION}/debezium-connector-${c}-${DEBEZIUM_VERSION}-plugin.tar.gz" | tar xz; \
    done

FROM confluentinc/cp-kafka-connect:${CP_VERSION}
COPY --from=plugins /plugins /opt/connect-plugins
