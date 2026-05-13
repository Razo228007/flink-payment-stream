import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    acks="all"
)

TOPIC_NAME = "payments"
currencies = ["USD", "EUR", "GBP"]

print("Producer started...")

while True:
    payment = {
        "payment_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 100)}",
        "merchant_id": f"merchant_{random.randint(1, 20)}",
        "amount": round(random.uniform(50, 1000), 2),
        "currency": random.choice(currencies),
        "payment_time": datetime.utcnow().isoformat()
    }

    producer.send(TOPIC_NAME, payment)
    producer.flush()

    print(f"Sent: {payment}")
    time.sleep(1)