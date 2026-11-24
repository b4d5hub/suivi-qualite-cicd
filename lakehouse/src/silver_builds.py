"""Silver batch job for silver_builds: Bronze Parquet -> Apache Hudi COPY_ON_WRITE.

Reads s3a://cicd-lakehouse/bronze/builds/, parses raw_json with BUILDS_SCHEMA,
casts UTC timestamps, computes an approximate duration_s for completed runs,
deduplicates deterministically by (run_id, run_attempt) ordering by
(updated_at DESC, kafka_offset DESC), and upserts into
s3a://cicd-lakehouse/silver/silver_builds.
"""

import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from config import bronze_path, silver_path
from schemas import BUILDS_SCHEMA, TIMESTAMP_FIELDS
from spark_session import get_spark

ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

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

        bronze_df = spark.read.parquet(src_path)
        parsed_df = (
            bronze_df.withColumn("data", F.from_json(F.col("raw_json"), BUILDS_SCHEMA))
            .select(
                "data.*",
                F.col("kafka_partition").cast("int").alias("source_kafka_partition"),
                F.col("kafka_offset").cast("long").alias("source_kafka_offset"),
            )
            .cache()
        )

        rows_read = parsed_df.count()
        print(f"[INFO] Rows read from Bronze 'builds': {rows_read}")

        # Compare timestamp null counts before and after casting to ensure 0 parse losses
        ts_cols = TIMESTAMP_FIELDS["builds"]
        before_exprs = [
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"{c}_before")
            for c in ts_cols
        ]

        casted_df = parsed_df
        for c in ts_cols:
            casted_df = casted_df.withColumn(c, F.to_timestamp(F.col(c), ISO_FORMAT))
        casted_df = casted_df.cache()

        after_exprs = [
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"{c}_after")
            for c in ts_cols
        ]

        before_row = parsed_df.agg(*before_exprs).collect()[0]
        after_row = casted_df.agg(*after_exprs).collect()[0]

        for c in ts_cols:
            b_nulls = int(before_row[f"{c}_before"] or 0)
            a_nulls = int(after_row[f"{c}_after"] or 0)
            print(f"[INFO] Timestamp cast check '{c}': nulls_before={b_nulls}, nulls_after={a_nulls}")
            if a_nulls != b_nulls:
                raise RuntimeError(
                    f"Timestamp cast introduced nulls for '{c}': before={b_nulls}, after={a_nulls}"
                )

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

        # Deduplicate inside the batch BEFORE writing:
        # Keep 1 row per (run_id, run_attempt) with highest updated_at, breaking ties by highest kafka_offset.
        dedup_window = Window.partitionBy("run_id", "run_attempt").orderBy(
            F.col("updated_at").desc(),
            F.col("source_kafka_offset").desc(),
        )

        silver_df = (
            enriched_df.withColumn("_rn", F.row_number().over(dedup_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .select(*SILVER_BUILDS_COLUMNS)
            .cache()
        )

        rows_dedup = silver_df.count()
        print(f"[INFO] Rows after batch deduplication on (run_id, run_attempt): {rows_dedup}")

        # In Hudi 0.15, NonpartitionedKeyGenerator supports composite record keys
        # (comma-separated in hoodie.datasource.write.recordkey.field) while keeping
        # partitionPath empty ("") so the table stays unpartitioned.
        hudi_options = {
            "hoodie.table.name": "silver_builds",
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
            "hoodie.datasource.write.operation": "upsert",
            "hoodie.datasource.write.recordkey.field": "run_id,run_attempt",
            "hoodie.datasource.write.precombine.field": "updated_at",
            "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
            "hoodie.upsert.shuffle.parallelism": "2",
            "hoodie.insert.shuffle.parallelism": "2",
            "hoodie.metadata.enable": "false",
        }

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
