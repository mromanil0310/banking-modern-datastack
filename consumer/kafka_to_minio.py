import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
import uuid
from dotenv import load_dotenv

# -----------------------------
# Load secrets from .env
# -----------------------------
load_dotenv()

print("🔎 Loaded Kafka Bootstrap:", os.getenv("KAFKA_BOOTSTRAP"))
print("🔎 Kafka Group:", os.getenv("KAFKA_GROUP"))
print("🔎 MinIO Endpoint:", os.getenv("MINIO_ENDPOINT"))
print("🔎 Bucket:", os.getenv("MINIO_BUCKET"))

# Kafka consumer settings
consumer = KafkaConsumer(
    'banking_server.public.customers',
    'banking_server.public.accounts',
    'banking_server.public.transactions',
    bootstrap_servers=[os.getenv("KAFKA_BOOTSTRAP")],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id=f"minio-group-{uuid.uuid4().hex[:6]}",  # 🔥 unique group each run
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# MinIO client
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
)

bucket = os.getenv("MINIO_BUCKET")

# Create bucket if not exists
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)

# Consume and write function


def write_to_minio(table_name, records):
    if not records:
        return
    df = pd.DataFrame(records)
    date_str = datetime.now().strftime('%Y-%m-%d')
    file_path = f'{table_name}_{date_str}.parquet'
    df.to_parquet(file_path, engine='fastparquet', index=False)
    s3_key = f'{table_name}/date={date_str}/{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'
    s3.upload_file(file_path, bucket, s3_key)
    os.remove(file_path)
    print(f'✅ Uploaded {len(records)} records to s3://{bucket}/{s3_key}')


# Batch consume
batch_size = 1
buffer = {
    'banking_server.public.customers': [],
    'banking_server.public.accounts': [],
    'banking_server.public.transactions': []
}

# Force assignment before seeking
consumer.poll(timeout_ms=1000)
print("Assigned partitions:", consumer.assignment())  # 👀 should NOT be empty

for tp in consumer.assignment():
    consumer.seek_to_beginning(tp)

print("🚀 Listening for Kafka CDC events...")

for message in consumer:
    topic = message.topic
    event = message.value
    payload = event.get("payload", {})
    op = payload.get("op")

    # 👀 Log each message for visibility
    print(f"\n📩 CDC Event received from {topic} | op={op}")
    print(f"Raw event: {event}")

    if op in ["c", "u", "r"]:  # include snapshot, insert, update
        record = payload.get("after")
    elif op == "d":
        record = payload.get("before")
    else:
        record = None

    print(f"🗂 Extracted record to store: {record}")

    if record:
        buffer[topic].append(record)

    if len(buffer[topic]) >= batch_size:
        print(f"⬆ Uploading {len(buffer[topic])} records to MinIO...")
        write_to_minio(topic.split('.')[-1], buffer[topic])
        buffer[topic] = []
