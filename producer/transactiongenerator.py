"""Synthetic financial transaction event generator"""
from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_transaction():
    return {
        "transaction_id": f"TXN_{random.randint(100000, 999999)}",
        "amount": round(random.uniform(1, 50000), 2),
        "merchant": random.choice(["Amazon", "Walmart", "Unknown_Vendor"]),
        "timestamp": datetime.utcnow().isoformat(),
        "account_id": f"ACC_{random.randint(1000, 9999)}",
        "is_fraud": random.random() < 0.02  # 2% fraud rate
    }

if __name__ == "__main__":
    print("Starting transaction stream...")
    while True:
        txn = generate_transaction()
        producer.send('financial-transactions', txn)
        time.sleep(0.1)  # 10 events/second
