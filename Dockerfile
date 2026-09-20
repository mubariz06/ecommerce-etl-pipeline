FROM apache/airflow:2.9.3-python3.10

USER root
# Java is required by PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.10
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"


