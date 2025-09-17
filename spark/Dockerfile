FROM bitnami/spark:3.4.1

# Ensure python3 and pip are available in the Spark image so PySpark can import Python dependencies
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
	python3 \
	python3-pip \
 && rm -rf /var/lib/apt/lists/*

# Download Kafka connector and Cassandra connector jars and add them to Spark's jars
RUN mkdir -p /opt/bitnami/spark/jars && \
	curl --retry 3 -fSL -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.13-3.4.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.4.1/spark-sql-kafka-0-10_2.13-3.4.1.jar && \
	curl --retry 3 -fSL -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.13-3.4.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/3.4.1/spark-token-provider-kafka-0-10_2.13-3.4.1.jar && \
	curl --retry 3 -fSL -o /opt/bitnami/spark/jars/kafka-clients-3.3.2.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar && \
	curl --retry 3 -fSL -o /opt/bitnami/spark/jars/spark-cassandra-connector_2.13-3.4.0.jar https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.13/3.4.0/spark-cassandra-connector_2.13-3.4.0.jar && \
	ls -la /opt/bitnami/spark/jars | grep -iE 'kafka|cassandra' || true

COPY streaming_job.py /opt/spark/app/streaming_job.py

# Install Python Cassandra driver into the system python used by PySpark
RUN python3 -m pip install --no-cache-dir cassandra-driver==3.25.0

# Make sure PySpark uses python3 by default
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
USER 1001
