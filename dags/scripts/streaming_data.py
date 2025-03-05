from faker import Faker
import json
import random
from datetime import datetime
import boto3

fake = Faker()





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

def generate_fraud_case():
    return {
        "fraud_id": fake.uuid4(),
        "transaction_id": fake.uuid4(),
        "user_id": random.randint(1, 1000),
        "fraud_reason": fake.sentence(),
        "fraud_detected_by": random.choice(["rule_engine", "machine_learning", "manual_review"]),
        "detected_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        "action_taken": random.choice(["account_freeze", "refund", "report_to_authorities"]),
    }

def generate_device_info():
    return {
        "device_id": fake.uuid4(),
        "user_id": random.randint(1, 1000),
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "os": random.choice(["iOS", "Android", "Windows", "macOS"]),
        "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
        "is_rooted": random.choice([True, False]),
        "last_used_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
    }

def generate_ip_activity():
    return {
        "ip_id": fake.uuid4(),
        "user_id": random.randint(1, 1000),
        "ip_address": fake.ipv4(),
        "login_time": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        "failed_attempts": random.randint(0, 5),
        "location": fake.city(),
        "vpn_used": random.choice([True, False]),
    }

dictionary_of_generators = {
    "transaction": generate_transaction(),
    "fraudCases": generate_fraud_case(),
    "deviceInfo": generate_device_info(),
    "ipActvity": generate_ip_activity(),
}


def send_data_to_kinesis(generator_name,record_count, stream_name):
    kinesis_client = boto3.client("kinesis", region_name = "us-east-1")
    print("Streaming started to Kinesis.")
    for _ in range(record_count):
        generated_data = dictionary_of_generators.get(generator_name)
        response = kinesis_client.put_record(
            StreamName = stream_name,
            Data = json.dumps(generated_data),
            PartitionKey = generated_data.get("transaction_time") if generator_name == "transactions" else "default_key")



for generator in dictionary_of_generators.keys():
    stream_name = f"fraud-project-{generator}-stream"
    send_data_to_kinesis(generator, 10, stream_name)
    print(f"Data sent to Kinesis for {generator}")
