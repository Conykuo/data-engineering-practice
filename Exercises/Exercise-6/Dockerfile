FROM ubuntu:18.04

RUN apt-get update && \
    apt-get install -y default-jdk scala wget vim software-properties-common \
    python3.8 python3.8-dev python3-pip curl unzip libpq-dev \
    build-essential libssl-dev libffi-dev && \
    apt-get clean

RUN wget https://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz && \
    tar xvf spark-3.0.1-bin-hadoop3.2.tgz && \
    mv spark-3.0.1-bin-hadoop3.2/ /usr/local/spark && \
    ln -s /usr/local/spark spark && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.890/aws-java-sdk-bundle-1.11.890.jar && \
    mv aws-java-sdk-bundle-1.11.890.jar /spark/jars && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar && \
    mv hadoop-aws-3.2.0.jar /spark/jars

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN python3.8 -m pip install --upgrade pip setuptools==57.5.0 wheel

RUN python3.8 -m pip install cython==0.29.21

RUN python3.8 -m pip install --no-build-isolation \
    markupsafe==1.1.1 cryptography==3.3.2 numpy==1.18.5

RUN python3.8 -m pip install --no-build-isolation -r /app/requirements.txt

ENV PYSPARK_PYTHON=python3.8
ENV PYSPARK_SUBMIT_ARGS='--packages io.delta:delta-core_2.12:0.8.0 pyspark-shell'
