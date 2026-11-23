"""Explicit PySpark StructType schemas for the 3 Kafka JSON payloads.

Matches ingestion/src/collector/transform.py:
- transform_build(run) -> topic 'builds'
- transform_job(job, run, attempt_number) -> topic 'tests'
- transform_deployment(deployment, statuses) -> topic 'deployments'

Note: Spark's from_json() always produces nullable fields regardless of the
nullable flag in StructField. Required fields are therefore enforced via
explicit null-count checks rather than relying on nullable=False.
"""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

BUILDS_SCHEMA = StructType(
    [
        StructField("run_id", LongType(), True),
        StructField("workflow", StringType(), True),
        StructField("run_attempt", IntegerType(), True),
        StructField("event", StringType(), True),
        StructField("branch", StringType(), True),
        StructField("head_sha", StringType(), True),
        StructField("status", StringType(), True),
        StructField("conclusion", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("started_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("duration_s", LongType(), True),
    ]
)

TESTS_SCHEMA = StructType(
    [
        StructField("job_id", LongType(), True),
        StructField("run_id", LongType(), True),
        StructField("run_attempt", IntegerType(), True),
        StructField("head_sha", StringType(), True),
        StructField("job_name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("conclusion", StringType(), True),
        StructField("started_at", StringType(), True),
        StructField("completed_at", StringType(), True),
        StructField("duration_s", LongType(), True),
    ]
)

DEPLOYMENT_STATUS_SCHEMA = StructType(
    [
        StructField("status_id", LongType(), True),
        StructField("state", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)

DEPLOYMENTS_SCHEMA = StructType(
    [
        StructField("deployment_id", LongType(), True),
        StructField("environment", StringType(), True),
        StructField("production_environment", BooleanType(), True),
        StructField("ref", StringType(), True),
        StructField("sha", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("latest_status", StringType(), True),
        StructField("latest_status_at", StringType(), True),
        StructField("status_history", ArrayType(DEPLOYMENT_STATUS_SCHEMA, True), True),
    ]
)

SCHEMAS = {
    "builds": BUILDS_SCHEMA,
    "tests": TESTS_SCHEMA,
    "deployments": DEPLOYMENTS_SCHEMA,
}

TIMESTAMP_FIELDS = {
    "builds": ["created_at", "started_at", "updated_at"],
    "tests": ["started_at", "completed_at"],
    "deployments": ["created_at", "updated_at", "latest_status_at"],
}
