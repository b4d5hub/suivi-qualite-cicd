"""Silver batch job for silver_deployments: Bronze Parquet -> Apache Hudi COPY_ON_WRITE.

Reads s3a://cicd-lakehouse/bronze/deployments/, parses raw_json with
DEPLOYMENTS_SCHEMA, casts top-level and nested status_history[].created_at
timestamps to UTC TimestampType, derives was_successful, first_success_at,
is_prd_environment, and event_ts = coalesce(latest_status_at, updated_at, created_at),
deduplicates by deployment_id ordering by (event_ts DESC, source_kafka_offset DESC),
and upserts into s3a://cicd-lakehouse/silver/silver_deployments.
"""

import sys

from pyspark.sql import functions as F

from config import bronze_path, silver_path
from schemas import DEPLOYMENTS_SCHEMA, TIMESTAMP_FIELDS
from silver_common import (
    ISO_FORMAT,
    build_hudi_options,
    cast_utc_timestamps,
    deduplicate_batch,
)
from spark_session import get_spark

SILVER_DEPLOYMENTS_COLUMNS = [
    "deployment_id",
    "environment",
    "production_environment",
    "ref",
    "sha",
    "created_at",
    "updated_at",
    "latest_status",
    "latest_status_at",
    "status_history",
    "was_successful",
    "first_success_at",
    "is_prd_environment",
    "event_ts",
    "source_kafka_partition",
    "source_kafka_offset",
]


def main() -> None:
    print("=" * 60)
    print("LAKEHOUSE SILVER JOB: silver_deployments (HUDI COPY_ON_WRITE UPSERT)")
    print("=" * 60)

    spark = get_spark("lakehouse-silver-deployments")
    spark.sparkContext.setLogLevel("WARN")

    try:
        src_path = bronze_path("deployments")
        dst_path = silver_path("silver_deployments")

        parsed_df = (
            spark.read.parquet(src_path)
            .withColumn("data", F.from_json(F.col("raw_json"), DEPLOYMENTS_SCHEMA))
            .select(
                "data.*",
                F.col("kafka_partition").cast("int").alias("source_kafka_partition"),
                F.col("kafka_offset").cast("long").alias("source_kafka_offset"),
            )
            .cache()
        )

        rows_read = parsed_df.count()
        print(f"[INFO] Rows read from Bronze 'deployments': {rows_read}")

        # 1. Cast top-level timestamps (created_at, updated_at, latest_status_at) with null check
        casted_df = cast_utc_timestamps(parsed_df, TIMESTAMP_FIELDS["deployments"])

        # 2. Cast nested status_history[].created_at to TimestampType using SQL higher-order transform
        # and verify that 0 elements or nested created_at values become null after the cast.
        nested_before = casted_df.select(
            F.sum(F.expr("size(filter(status_history, x -> x is null))")).alias("null_elems"),
            F.sum(F.expr("size(filter(status_history, x -> x.created_at is null))")).alias("null_ts"),
        ).collect()[0]

        casted_nested_df = casted_df.withColumn(
            "status_history",
            F.expr(
                f"transform(status_history, x -> named_struct("
                f"'status_id', x.status_id, "
                f"'state', x.state, "
                f"'created_at', to_timestamp(x.created_at, \"{ISO_FORMAT}\")"
                f"))"
            ),
        ).cache()

        nested_after = casted_nested_df.select(
            F.sum(F.expr("size(filter(status_history, x -> x is null))")).alias("null_elems"),
            F.sum(F.expr("size(filter(status_history, x -> x.created_at is null))")).alias("null_ts"),
        ).collect()[0]

        b_ne = int(nested_before["null_elems"] or 0)
        b_nt = int(nested_before["null_ts"] or 0)
        a_ne = int(nested_after["null_elems"] or 0)
        a_nt = int(nested_after["null_ts"] or 0)
        print(
            f"[INFO] Nested status_history cast check: "
            f"null_elements(before={b_ne}, after={a_ne}), "
            f"null_created_at(before={b_nt}, after={a_nt})"
        )
        if a_ne != b_ne or a_nt != b_nt:
            raise RuntimeError(
                f"Nested status_history timestamp cast introduced nulls: "
                f"before=({b_ne}, {b_nt}), after=({a_ne}, {a_nt})"
            )

        # 3. Derived columns:
        # - was_successful: True if ANY element in status_history has state == 'success'.
        # - first_success_at: Earliest created_at among status_history elements with state == 'success'
        #   (null if none). This records the moment a deployment actually succeeded; latest_status is
        #   often 'inactive' because a newer deployment replaced it, so latest_status alone under-counts
        #   successes. Kamal needs first_success_at for DORA deployment frequency and lead time.
        # - is_prd_environment: True when environment starts with 'prd-'. Note: production_environment
        #   is false for every row in this repository's data, so it cannot be used to filter production;
        #   the 'prd-' / 'stg-' naming prefix is an assumption to be confirmed by the team.
        # - event_ts: coalesce(latest_status_at, updated_at, created_at) as Hudi precombine key.
        enriched_df = (
            casted_nested_df.withColumn(
                "was_successful",
                F.coalesce(
                    F.expr("exists(status_history, x -> x.state = 'success')"),
                    F.lit(False),
                ),
            )
            .withColumn(
                "first_success_at",
                F.expr(
                    "array_min(transform(filter(status_history, x -> x.state = 'success'), x -> x.created_at))"
                ),
            )
            .withColumn(
                "is_prd_environment",
                F.coalesce(F.col("environment").startswith("prd-"), F.lit(False)),
            )
            .withColumn(
                "event_ts",
                F.coalesce(
                    F.col("latest_status_at"),
                    F.col("updated_at"),
                    F.col("created_at"),
                ),
            )
        )

        silver_df = deduplicate_batch(
            enriched_df,
            key_cols=["deployment_id"],
            precombine_col="event_ts",
            final_cols=SILVER_DEPLOYMENTS_COLUMNS,
        ).cache()

        rows_dedup = silver_df.count()
        print(f"[INFO] Rows after batch deduplication on deployment_id: {rows_dedup}")

        hudi_options = build_hudi_options(
            table_name="silver_deployments",
            recordkey_field="deployment_id",
            precombine_field="event_ts",
        )

        (
            silver_df.write.format("hudi")
            .options(**hudi_options)
            .mode("append")
            .save(dst_path)
        )

        read_back_df = spark.read.format("hudi").load(dst_path)
        rows_written = read_back_df.count()
        sh_type = read_back_df.schema["status_history"].dataType.simpleString()
        print(
            f"[OK] silver_deployments Hudi write complete at {dst_path}: "
            f"read={rows_read}, deduplicated={rows_dedup}, table_total={rows_written}, "
            f"status_history_hudi_type={sh_type}"
        )

        parsed_df.unpersist()
        casted_df.unpersist()
        casted_nested_df.unpersist()
        silver_df.unpersist()

    except Exception as exc:
        print(f"[ERROR] silver_deployments job failed: {exc}")
        spark.stop()
        sys.exit(1)

    spark.stop()
    print("SILVER_DEPLOYMENTS COMPLETED SUCCESSFULLY")
    sys.exit(0)


if __name__ == "__main__":
    main()
