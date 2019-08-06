FROM python:3.7

ENV REDIS_HOST localhost
ENV REDIS_PORT 6379
ENV REDIS_DB 0

RUN mkdir /app
WORKDIR /app
ENV PYTHONPATH /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .
