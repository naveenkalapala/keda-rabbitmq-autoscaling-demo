import json
import os
import time

import pika


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def main() -> None:
    credentials = pika.PlainCredentials(
        env("RABBITMQ_USER", "guest"),
        env("RABBITMQ_PASSWORD", "guest"),
    )
    parameters = pika.ConnectionParameters(
        host=env("RABBITMQ_HOST", "localhost"),
        port=int(env("RABBITMQ_PORT", "5672")),
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=30,
    )

    queue_name = env("QUEUE_NAME", "fake-orders")
    processing_time_ms = int(env("PROCESSING_TIME_MS", "750"))
    prefetch_count = int(env("PREFETCH_COUNT", "20"))

    while True:
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True)
            channel.basic_qos(prefetch_count=prefetch_count)

            def callback(ch, method, properties, body):
                order = json.loads(body.decode("utf-8"))
                order_id = order.get("order_id", "unknown")
                time.sleep(processing_time_ms / 1000)
                print(f"processed order={order_id}", flush=True)
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=queue_name, on_message_callback=callback)
            print(f"waiting for messages on {queue_name}", flush=True)
            channel.start_consuming()
        except Exception as exc:
            print(f"consumer error: {exc}; retrying in 5 seconds", flush=True)
            time.sleep(5)


if __name__ == "__main__":
    main()
