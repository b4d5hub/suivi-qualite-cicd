"""Central configuration for the Lakehouse Bronze (MinIO) and Silver (Hudi) jobs."""

import os

# MinIO S3-compatible storage settings
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
LAKEHOUSE_BUCKET = os.getenv("LAKEHOUSE_BUCKET", "cicd-lakehouse")

# Kafka settings inside the shared Docker network
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
KAFKA_TOPICS = ("builds", "tests", "deployments")


def bronze_path(topic: str) -> str:
    """Return the MinIO S3A path for a raw Bronze Parquet table."""
    return f"s3a://{LAKEHOUSE_BUCKET}/bronze/{topic}/"


def silver_path(table: str) -> str:
    """Return the MinIO S3A path for a cleaned Silver Hudi table."""
    return f"s3a://{LAKEHOUSE_BUCKET}/silver/{table}"


def checkpoint_path(name: str) -> str:
    """Return the MinIO S3A path for Spark Structured Streaming checkpoints."""
    return f"s3a://{LAKEHOUSE_BUCKET}/_checkpoints/{name}/"
