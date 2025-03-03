from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import time
import uuid
import random
from faker import Faker

# AWS and S3 settings
AWS_CONN_ID = "my_aws_conn"           # The AWS connection name you created in Airflow
S3_BUCKET = "my-fraud-project-raw"    # Replace with your S3 bucket
S3_PREFIX = "synthetic_data/"         # Folder (prefix) within the bucket

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def generate_synthetic_data_callable(**context):
    """
    Generates a batch of synthetic transactions and uploads them to S3
    using the Airflow S3Hook with a specified AWS connection.
    """
    fake = Faker()
    BATCH_SIZE = 10_000
    file_index = int(time.time())  # Simple approach to get a unique index
    file_name = f"transactions_{file_index}.json"

    # Generate JSON lines
    records = []
    for _ in range(BATCH_SIZE):
        record = {
            "transaction_id": str(uuid.uuid4()),
            "user_id": f"user_{random.randint(1, 1_000_000)}",
            "merchant_id": f"merchant_{random.randint(1, 50_000)}",
            "transaction_amount": round(random.uniform(1.0, 2000.0), 2),
            "transaction_timestamp": fake.iso8601(),
            "payment_method": random.choice(["CreditCard", "DebitCard", "PayPal", "Crypto"]),
            "fraud_label": random.choice([True, False, False, False])  # ~25% chance True
        }
        records.append(json.dumps(record))

    file_contents = "\n".join(records)

    # Use S3Hook to upload
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_key = f"{S3_PREFIX}{file_name}"

    s3_hook.load_string(
        string_data=file_contents,
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=True  # Overwrite if file exists
    )

    print(f"Uploaded {BATCH_SIZE} records to s3://{S3_BUCKET}/{s3_key}")

with DAG(
    dag_id='fraud_pipeline_dag',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:

    # 1) Generate synthetic data and upload to S3
    generate_data = PythonOperator(
        task_id='generate_synthetic_data',
        python_callable=generate_synthetic_data_callable
    )

    # 2) Trigger a Databricks job (replace job_id with your actual job in Databricks)
    databricks_job_task = DatabricksRunNowOperator(
        task_id='run_databricks_job',
        job_id=1234,  # e.g., the numeric ID from your Databricks "Job" config
        databricks_conn_id='databricks_default'
    )

    # 3) Run dbt transformations (assuming dbt is installed in the same environment)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /path/to/dbt/project && dbt run --profiles-dir .'
    )

    # Define task order
    generate_data >> databricks_job_task >> dbt_run