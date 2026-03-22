FROM python:3.8

RUN apt-get update && \
    apt-get clean

WORKDIR app
COPY . /app

RUN pip3 install -r requirements.txt
