"""Silver batch job for silver_builds: Bronze Parquet -> Apache Hudi COPY_ON_WRITE.

Reads s3a://cicd-lakehouse/bronze/builds/, parses raw_json with BUILDS_SCHEMA,
casts UTC timestamps, computes an approximate duration_s for completed runs,
deduplicates deterministically by (run_id, run_attempt) ordering by
(updated_at DESC, source_kafka_offset DESC), and upserts into
s3a://cicd-lakehouse/silver/silver_builds.
"""

import sys

from pyspark.sql import functions as F

from config import bronze_path, silver_path
from schemas import BUILDS_SCHEMA, TIMESTAMP_FIELDS
from silver_common import build_hudi_options, cast_utc_timestamps, deduplicate_batch
from spark_session import get_spark

SILVER_BUILDS_COLUMNS = [
    "run_id",
    "run_attempt",
    "workflow",
    "event",
    "branch",
    "head_sha",
    "status",
    "conclusion",
    "created_at",
    "started_at",
    "updated_at",
    "duration_s",
    "source_kafka_partition",
    "source_kafka_offset",
]


def main() -> None:
    print("=" * 60)
    print("LAKEHOUSE SILVER JOB: silver_builds (HUDI COPY_ON_WRITE UPSERT)")
    print("=" * 60)

    spark = get_spark("lakehouse-silver-builds")
    spark.sparkContext.setLogLevel("WARN")

    try:
        src_path = bronze_path("builds")
        dst_path = silver_path("silver_builds")

        parsed_df = (
            spark.read.parquet(src_path)
            .withColumn("data", F.from_json(F.col("raw_json"), BUILDS_SCHEMA))
            .select(
                "data.*",
                F.col("kafka_partition").cast("int").alias("source_kafka_partition"),
                F.col("kafka_offset").cast("long").alias("source_kafka_offset"),
            )
            .cache()
        )

        rows_read = parsed_df.count()
        print(f"[INFO] Rows read from Bronze 'builds': {rows_read}")

        casted_df = cast_utc_timestamps(parsed_df, TIMESTAMP_FIELDS["builds"])

        # Compute duration_s (LongType):
        # NOTE: GitHub's workflow run object has no explicit completed_at field; updated_at is
        # the last update timestamp of the run, so updated_at - started_at is an approximation.
        diff_s = F.unix_timestamp(F.col("updated_at")) - F.unix_timestamp(F.col("started_at"))
        enriched_df = casted_df.withColumn(
            "duration_s",
            F.when(
                (F.col("status") == F.lit("completed"))
                & F.col("started_at").isNotNull()
                & F.col("updated_at").isNotNull()
                & (diff_s >= 0),
                diff_s.cast("long"),
            ).otherwise(F.lit(None).cast("long")),
        )

        silver_df = deduplicate_batch(
            enriched_df,
            key_cols=["run_id", "run_attempt"],
            precombine_col="updated_at",
            final_cols=SILVER_BUILDS_COLUMNS,
        ).cache()

        rows_dedup = silver_df.count()
        print(f"[INFO] Rows after batch deduplication on (run_id, run_attempt): {rows_dedup}")

        hudi_options = build_hudi_options(
            table_name="silver_builds",
            recordkey_field="run_id,run_attempt",
            precombine_field="updated_at",
        )

        (
            silver_df.write.format("hudi")
            .options(**hudi_options)
            .mode("append")
            .save(dst_path)
        )

        rows_written = spark.read.format("hudi").load(dst_path).count()
        print(
            f"[OK] silver_builds Hudi write complete at {dst_path}: "
            f"read={rows_read}, deduplicated={rows_dedup}, table_total={rows_written}"
        )

        parsed_df.unpersist()
        casted_df.unpersist()
        silver_df.unpersist()

    except Exception as exc:
        print(f"[ERROR] silver_builds job failed: {exc}")
        spark.stop()
        sys.exit(1)

    spark.stop()
    print("SILVER_BUILDS COMPLETED SUCCESSFULLY")
    sys.exit(0)


if __name__ == "__main__":
    main()
