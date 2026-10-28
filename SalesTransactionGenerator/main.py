import json
import os
import random
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import SerializingProducer
from faker import Faker

fake = Faker()
running = True


def stop(_signum, _frame):
    global running
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)


def generate_sales_transactions():
    user = fake.simple_profile()
    transaction = {
        "transactionId": fake.uuid4(),
        "productId": random.choice(["product1", "product2", "product3", "product4", "product5", "product6"]),
        "productName": random.choice(["laptop", "mobile", "tablet", "watch", "headphone", "speaker"]),
        "productCategory": random.choice(["electronic", "fashion", "grocery", "home", "beauty", "sports"]),
        "productPrice": round(random.uniform(10, 1000), 2),
        "productQuantity": random.randint(1, 10),
        "productBrand": random.choice(["apple", "samsung", "oneplus", "mi", "boat", "sony"]),
        "currency": random.choice(["USD", "GBP"]),
        "customerId": user["username"],
        "transactionDate": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "paymentMethod": random.choice(["credit_card", "debit_card", "online_transfer"]),
    }
    transaction["totalAmount"] = round(transaction["productPrice"] * transaction["productQuantity"], 2)
    return transaction


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}", flush=True)
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]", flush=True)


def main():
    topic = os.getenv("KAFKA_TOPIC", "financial_transactions")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    interval = float(os.getenv("GENERATOR_INTERVAL_SECONDS", "5"))
    duration = int(os.getenv("GENERATOR_DURATION_SECONDS", "0"))  # 0 means run until stopped

    producer = SerializingProducer({"bootstrap.servers": bootstrap_servers})
    started = time.monotonic()

    print(f"Producing to Kafka topic '{topic}' at {bootstrap_servers}", flush=True)
    while running and (duration <= 0 or time.monotonic() - started < duration):
        transaction = generate_sales_transactions()
        try:
            producer.produce(
                topic,
                key=transaction["transactionId"],
                value=json.dumps(transaction),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            print(json.dumps(transaction), flush=True)
        except BufferError:
            print("Producer queue is full; polling before retrying.", flush=True)
            producer.poll(1)
        except Exception as exc:
            print(f"Producer error: {exc}", flush=True)
            time.sleep(1)
        time.sleep(interval)

    remaining = producer.flush(10)
    if remaining:
        print(f"Warning: {remaining} message(s) were not delivered before shutdown.", flush=True)


if __name__ == "__main__":
    main()
