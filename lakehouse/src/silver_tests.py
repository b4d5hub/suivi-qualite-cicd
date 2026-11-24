"""Silver batch job for silver_tests: Bronze Parquet -> Apache Hudi COPY_ON_WRITE.

Note: Each message in topic 'tests' represents a GitHub Actions JOB (not an
individual unit test). Reads s3a://cicd-lakehouse/bronze/tests/, parses raw_json
with TESTS_SCHEMA, casts UTC timestamps, keeps duration_s untouched as delivered
by the collector, computes event_ts = coalesce(completed_at, started_at),
deduplicates by job_id ordering by (event_ts DESC, source_kafka_offset DESC),
and upserts into s3a://cicd-lakehouse/silver/silver_tests.
"""

import sys

from pyspark.sql import functions as F

from config import bronze_path, silver_path
from schemas import TESTS_SCHEMA, TIMESTAMP_FIELDS
from silver_common import build_hudi_options, cast_utc_timestamps, deduplicate_batch
from spark_session import get_spark

SILVER_TESTS_COLUMNS = [
    "job_id",
    "run_id",
    "run_attempt",
    "head_sha",
    "job_name",
    "status",
    "conclusion",
    "started_at",
    "completed_at",
    "duration_s",
    "event_ts",
    "source_kafka_partition",
    "source_kafka_offset",
]


def main() -> None:
    print("=" * 60)
    print("LAKEHOUSE SILVER JOB: silver_tests (HUDI COPY_ON_WRITE UPSERT)")
    print("=" * 60)

    spark = get_spark("lakehouse-silver-tests")
    spark.sparkContext.setLogLevel("WARN")

    try:
        src_path = bronze_path("tests")
        dst_path = silver_path("silver_tests")

        parsed_df = (
            spark.read.parquet(src_path)
            .withColumn("data", F.from_json(F.col("raw_json"), TESTS_SCHEMA))
            .select(
                "data.*",
                F.col("kafka_partition").cast("int").alias("source_kafka_partition"),
                F.col("kafka_offset").cast("long").alias("source_kafka_offset"),
            )
            .cache()
        )

        rows_read = parsed_df.count()
        print(f"[INFO] Rows read from Bronze 'tests': {rows_read}")

        casted_df = cast_utc_timestamps(parsed_df, TIMESTAMP_FIELDS["tests"])

        # In the current historical snapshot completed_at is never null, but future
        # incremental polls can emit jobs that are still in_progress (completed_at is null),
        # so coalesce(completed_at, started_at) guarantees a non-null Hudi precombine key.
        enriched_df = casted_df.withColumn(
            "event_ts",
            F.coalesce(F.col("completed_at"), F.col("started_at")),
        )

        silver_df = deduplicate_batch(
            enriched_df,
            key_cols=["job_id"],
            precombine_col="event_ts",
            final_cols=SILVER_TESTS_COLUMNS,
        ).cache()

        rows_dedup = silver_df.count()
        print(f"[INFO] Rows after batch deduplication on job_id: {rows_dedup}")

        hudi_options = build_hudi_options(
            table_name="silver_tests",
            recordkey_field="job_id",
            precombine_field="event_ts",
        )

        (
            silver_df.write.format("hudi")
            .options(**hudi_options)
            .mode("append")
            .save(dst_path)
        )

        rows_written = spark.read.format("hudi").load(dst_path).count()
        print(
            f"[OK] silver_tests Hudi write complete at {dst_path}: "
            f"read={rows_read}, deduplicated={rows_dedup}, table_total={rows_written}"
        )

        parsed_df.unpersist()
        casted_df.unpersist()
        silver_df.unpersist()

    except Exception as exc:
        print(f"[ERROR] silver_tests job failed: {exc}")
        spark.stop()
        sys.exit(1)

    spark.stop()
    print("SILVER_TESTS COMPLETED SUCCESSFULLY")
    sys.exit(0)


if __name__ == "__main__":
    main()
