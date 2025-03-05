from faker import Faker
import json
import random
from datetime import datetime
import boto3

fake = Faker()

TOPICS = ["transactions", "fraud_cases", "device_info", "ip_activity"]



def generate_transaction():
    return {
        "transaction_id": fake.uuid4(),
        "user_id": random.randint(1, 1000),
        "amount": round(random.uniform(10, 5000), 2),
        "currency": "USD",
        "transaction_type": random.choice(["purchase", "withdrawal", "deposit", "transfer"]),
        "transaction_time": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        "payment_method": random.choice(["credit_card", "debit_card"]),
        "status": "completed",
        "is_fraudulent": random.choice([True, False]),
    }

kinesis_client = boto3.client("kinesis", region_name = "us-east-1")
stream_name = "fraud-detection-data-stream"

print("Streaming started to Kafka.")

for _ in range(10):
    generated_transaction = generate_transaction()

    response = kinesis_client.put_record(
        StreamName = stream_name,
        Data = json.dumps(generated_transaction),
        PartitionKey = generated_transaction.get("transaction_time"))
    print(f"Sent Record: {response}")
