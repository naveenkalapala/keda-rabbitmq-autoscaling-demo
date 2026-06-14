import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

import pika


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish fake order messages to RabbitMQ to validate KEDA autoscaling."
    )
    parser.add_argument("--count", type=int, default=500, help="Number of orders to publish.")
    parser.add_argument(
        "--burst-delay-ms",
        type=int,
        default=0,
        help="Delay in milliseconds between published messages.",
    )
    parser.add_argument(
        "--queue",
        default=env("QUEUE_NAME", "fake-orders"),
        help="RabbitMQ queue name.",
    )
    return parser.parse_args()


def build_connection() -> pika.BlockingConnection:
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
    return pika.BlockingConnection(parameters)


def fake_order(sequence: int) -> dict:
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"customer-{random.randint(1000, 9999)}",
        "sku": f"SKU-{random.randint(100, 999)}",
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(9.99, 250.0), 2),
        "priority": random.choice(["standard", "express", "bulk"]),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "sequence": sequence,
    }


def main() -> int:
    args = parse_args()

    try:
        connection = build_connection()
    except Exception as exc:
        print(f"unable to connect to RabbitMQ: {exc}", file=sys.stderr)
        return 1

    with connection:
        channel = connection.channel()
        channel.queue_declare(queue=args.queue, durable=True)

        for sequence in range(1, args.count + 1):
            order = fake_order(sequence)
            channel.basic_publish(
                exchange="",
                routing_key=args.queue,
                body=json.dumps(order),
                properties=pika.BasicProperties(delivery_mode=2),
            )

            if sequence % 50 == 0 or sequence == args.count:
                print(f"published {sequence}/{args.count} orders", flush=True)

            if args.burst_delay_ms > 0:
                time.sleep(args.burst_delay_ms / 1000)

    print(f"completed publishing {args.count} orders to {args.queue}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
