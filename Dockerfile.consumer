FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir pika

COPY orders-consumer.py .

CMD ["python", "orders-consumer.py"]
