# TITAN derives from the Apache-maintained Spark 4.0.1 image, pinned to its
# linux/amd64 manifest digest. It contains Spark 4.0.1, Scala 2.13, Python 3
# and Java 17. Only TITAN's missing S3A, Kafka, JDBC and pool jars are added.
FROM apache/spark@sha256:989054f2e005c50d4a2f78bd054b0a32de8ee7619a5658e5f4280c61241c9f6d

ARG SPARK_VERSION=4.0.1
ARG HADOOP_VERSION=3.4.1
ARG AWS_SDK_BUNDLE=2.24.6
ARG POSTGRES_JDBC=42.7.7

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Compile-scoped Kafka connector artifacts. spark-sql and Hadoop stay provided
# by the Spark distribution. lz4, snappy, and slf4j are already in that binary.
ARG KAFKA_CLIENTS=3.9.1
ARG COMMONS_POOL=2.12.0

RUN set -eu \
    && download() { \
        file="$1"; \
        url="$2"; \
        curl --fail --show-error --location --retry 3 --retry-all-errors --connect-timeout 20 --max-time 600 --continue-at - --progress-bar -o "/opt/spark/jars/${file}" "${url}"; \
        curl --fail --show-error --location --retry 3 --retry-all-errors --connect-timeout 20 --max-time 30 -o "/tmp/${file}.sha1" "${url}.sha1"; \
        expected="$(tr -d '[:space:]' < "/tmp/${file}.sha1" | cut -c1-40)"; \
        echo "${expected}  /opt/spark/jars/${file}" | sha1sum -c -; \
        rm "/tmp/${file}.sha1"; \
    } \
    && download "hadoop-aws-${HADOOP_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" \
    && download "bundle-${AWS_SDK_BUNDLE}.jar" \
        "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_BUNDLE}/bundle-${AWS_SDK_BUNDLE}.jar" \
    && download "postgresql-${POSTGRES_JDBC}.jar" \
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC}/postgresql-${POSTGRES_JDBC}.jar" \
    && download "spark-sql-kafka-0-10_2.13-${SPARK_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/${SPARK_VERSION}/spark-sql-kafka-0-10_2.13-${SPARK_VERSION}.jar" \
    && download "spark-token-provider-kafka-0-10_2.13-${SPARK_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.13-${SPARK_VERSION}.jar" \
    && download "kafka-clients-${KAFKA_CLIENTS}.jar" \
        "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS}/kafka-clients-${KAFKA_CLIENTS}.jar" \
    && download "commons-pool2-${COMMONS_POOL}.jar" \
        "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL}/commons-pool2-${COMMONS_POOL}.jar"

RUN pip3 install --no-cache-dir \
        "psycopg[binary]==3.3.9" \
        "minio==7.2.20" \
        "pytest==9.2.0"

ENV SPARK_HOME=/opt/spark \
    PATH="/opt/spark/bin:${PATH}" \
    PYSPARK_PYTHON=python3 \
    SPARK_DRIVER_MEMORY=768m

COPY pipelines/spark /opt/titan/pipelines/spark
WORKDIR /opt/titan

RUN printf '%s\n' \
        '#!/bin/sh' \
        'set -eu' \
        'py4j=$(echo "$SPARK_HOME"/python/lib/py4j-*.zip)' \
        'export PYTHONPATH="/opt/titan/pipelines/spark/src:$SPARK_HOME/python:${py4j}${PYTHONPATH:+:$PYTHONPATH}"' \
        'exec "$@"' \
        > /opt/titan/entrypoint.sh \
    && chmod +x /opt/titan/entrypoint.sh

ENTRYPOINT ["/opt/titan/entrypoint.sh"]
CMD ["python", "-m", "titan_spark.bronze", "--trigger", "availableNow"]
