FROM openjdk:11-jre-slim

WORKDIR /app

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV JAVA_HOME=/usr/local/openjdk-11

# Install Python and PySpark dependencies
RUN apt-get update && \
    apt-get install -y python3 python3-pip procps && \
    pip3 install pyspark==3.2.4 delta-spark==1.2.1 && \
    rm -rf /var/lib/apt/lists/*

# Copy the PySpark script to the container
COPY ingest_data.py /app/

COPY test_ingest_data.py /app/

COPY input/ /app/input/

COPY output/ /app/output

# Run the PySpark script
ENTRYPOINT ["spark-submit", "--packages", "io.delta:delta-core_2.12:1.2.1", "ingest_data.py"]
#"/bin/bash"



