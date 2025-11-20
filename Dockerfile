FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir kafka-python boto3 python-dotenv pandas fastparquet

COPY consumer /app

CMD ["python", "kafka_to_minio.py"]
