# Use a Flink 1.18 base image
FROM flink:1.18-scala_2.12-java11
RUN apt-get update && apt-get install -y netcat-openbsd --no-install-recommends && rm -rf /var/lib/apt/lists/*
# Set the Flink version to match the base image
ENV FLINK_VERSION 1.18

# Define the stable and compatible versions for the connectors
ENV ICEBERG_VERSION 1.10.0
ENV KAFKA_CONNECTOR_VERSION 3.1.0
ENV HADOOP_VERSION 2.8.3
ENV AWS_SDK_VERSION 1.11.1026

# --- Flink and Iceberg Connectors ---

RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/2.8.3/hadoop-common-2.8.3.jar -O ${FLINK_HOME}/lib/hadoop-common-2.8.3.jar && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs/2.8.3/hadoop-hdfs-2.8.3.jar -O ${FLINK_HOME}/lib/hadoop-hdfs-2.8.3.jar && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client/2.8.3/hadoop-client-2.8.3.jar -O ${FLINK_HOME}/lib/hadoop-client-2.8.3.jar

RUN wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
    -O ${FLINK_HOME}/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

RUN wget https://repo1.maven.org/maven2/com/google/guava/guava/20.0/guava-20.0.jar \
    -O ${FLINK_HOME}/lib/guava-20.0.jar

# 2. Download hadoop-aws JAR (Provides S3AFileSystem class, fixing ClsssNotFound issues)
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar \
    -O $FLINK_HOME/lib/hadoop-aws-${HADOOP_VERSION}.jar

# 3. Download AWS SDK Bundle (Required S3A dependency)
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -O $FLINK_HOME/lib/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar


RUN mkdir -p $FLINK_HOME/plugins/s3-fs-hadoop \
    && wget https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/${FLINK_VERSION}.1/flink-s3-fs-hadoop-${FLINK_VERSION}.1.jar \
        -O $FLINK_HOME/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-${FLINK_VERSION}.1.jar


RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.5.0/iceberg-flink-runtime-1.18-1.5.0.jar \
  -O $FLINK_HOME/lib/iceberg-flink-runtime-2.0-${ICEBERG_VERSION}.jar

# 6. Download Iceberg Core Dependency
RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-core/${ICEBERG_VERSION}/iceberg-core-${ICEBERG_VERSION}.jar \
    -O $FLINK_HOME/lib/iceberg-core-${ICEBERG_VERSION}.jar

# 7. Download Flink Kafka Connector (Source Connector)
# Using the highest compatible connector version for Flink 1.18.
RUN wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}-1.18/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}-1.18.jar \
    -O $FLINK_HOME/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}-1.18.jar

# Copy the Flink SQL script
COPY sql/flink_job.sql /tmp/flink_job.sql