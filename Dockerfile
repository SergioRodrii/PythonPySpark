FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean

RUN pip install pyspark pandas

WORKDIR /app
