FROM bitnami/spark:3.4.1

USER root

# Install dependencies (curl to download driver, python dependencies)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Download PostgreSQL JDBC driver
RUN curl -o /opt/bitnami/spark/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Copy requirements and install
COPY spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create checkpoints directory
RUN mkdir -p /tmp/spark-checkpoints-traffic /tmp/spark-checkpoints-gps && \
    chmod -R 777 /tmp/spark-checkpoints-traffic /tmp/spark-checkpoints-gps

USER 1001
