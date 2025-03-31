FROM apache/airflow:latest

USER root

RUN ls /bin && which bash && which sh

RUN apt-get update && \
    apt-get -y install git default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir pyspark pandas google-api-python-client emoji boto3