"""Shared helpers for Silver Hudi batch jobs: UTC timestamp casting, batch deduplication, and Hudi options."""

from typing import Dict, List, Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def cast_utc_timestamps(df: DataFrame, ts_cols: Sequence[str]) -> DataFrame:
    """Cast ISO-8601 UTC string columns to TimestampType and assert no new nulls are introduced."""
    before_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"{c}_before")
        for c in ts_cols
    ]

    casted_df = df
    for c in ts_cols:
        casted_df = casted_df.withColumn(c, F.to_timestamp(F.col(c), ISO_FORMAT))
    casted_df = casted_df.cache()

    after_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"{c}_after")
        for c in ts_cols
    ]

    before_row = df.agg(*before_exprs).collect()[0]
    after_row = casted_df.agg(*after_exprs).collect()[0]

    for c in ts_cols:
        b_nulls = int(before_row[f"{c}_before"] or 0)
        a_nulls = int(after_row[f"{c}_after"] or 0)
        print(f"[INFO] Timestamp cast check '{c}': nulls_before={b_nulls}, nulls_after={a_nulls}")
        if a_nulls != b_nulls:
            raise RuntimeError(
                f"Timestamp cast introduced nulls for '{c}': before={b_nulls}, after={a_nulls}"
            )

    return casted_df


def deduplicate_batch(
    df: DataFrame,
    key_cols: Sequence[str],
    precombine_col: str,
    final_cols: Sequence[str],
) -> DataFrame:
    """Keep 1 row per key_cols with highest precombine_col and highest source_kafka_offset tie-breaker."""
    win = Window.partitionBy(*[F.col(k) for k in key_cols]).orderBy(
        F.col(precombine_col).desc(),
        F.col("source_kafka_offset").desc(),
    )
    return (
        df.withColumn("_rn", F.row_number().over(win))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(*final_cols)
    )


def build_hudi_options(
    table_name: str,
    recordkey_field: str,
    precombine_field: str,
) -> Dict[str, str]:
    """Return standard unpartitioned Hudi COPY_ON_WRITE upsert options for a small machine."""
    # In Hudi 0.15, NonpartitionedKeyGenerator supports single or comma-separated
    # composite record keys while keeping partitionPath empty ("").
    return {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": recordkey_field,
        "hoodie.datasource.write.precombine.field": precombine_field,
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        "hoodie.upsert.shuffle.parallelism": "2",
        "hoodie.insert.shuffle.parallelism": "2",
        "hoodie.metadata.enable": "false",
    }
