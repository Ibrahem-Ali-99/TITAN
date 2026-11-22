# Spark 4.0.1, Java 21, Hadoop 3.4.1.
# Spark 4.0.1 requires Java 17 or 21 and is built for Hadoop 3.4.1.
# hadoop-aws 3.4.1 is the matching S3A client. Its compile dependency is
# software.amazon.awssdk:bundle 2.24.6. The hadoop3 binary omits the Kafka
# connector, so the image adds the Spark 4.0.1 POM compile jars by SHA-1.
FROM eclipse-temurin:21-jdk-jammy

ARG SPARK_VERSION=4.0.1
ARG HADOOP_VERSION=3.4.1
ARG AWS_SDK_BUNDLE=2.24.6
ARG POSTGRES_JDBC=42.7.7

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl python3 python3-pip \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o "/tmp/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
        "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    && curl -fsSL -o /tmp/spark.sha512 \
        "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz.sha512" \
    && cd /tmp && sha512sum -c spark.sha512 \
    && tar -xzf "/tmp/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -C /opt \
    && mv "/opt/spark-${SPARK_VERSION}-bin-hadoop3" /opt/spark \
    && rm "/tmp/spark-${SPARK_VERSION}-bin-hadoop3.tgz" /tmp/spark.sha512

# Compile-scoped Kafka connector artifacts. spark-sql and Hadoop stay provided
# by the Spark distribution. lz4, snappy, and slf4j are already in that binary.
ARG KAFKA_CLIENTS=3.9.1
ARG COMMONS_POOL=2.12.0

RUN set -eu \
    && download() { \
        file="$1"; \
        url="$2"; \
        curl -fsSL -o "/opt/spark/jars/${file}" "${url}"; \
        curl -fsSL -o "/tmp/${file}.sha1" "${url}.sha1"; \
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
