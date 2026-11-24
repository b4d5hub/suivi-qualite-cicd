"""Read-only verification of Silver Hudi tables with a per-table check registry.

Usage:
    spark-submit tools/verify_silver.py silver_builds
"""

import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from config import bronze_path, silver_path
from schemas import BUILDS_SCHEMA
from spark_session import get_spark

ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

EXPECTED_SILVER_BUILDS_SCHEMA = [
    ("run_id", "LongType"),
    ("run_attempt", "IntegerType"),
    ("workflow", "StringType"),
    ("event", "StringType"),
    ("branch", "StringType"),
    ("head_sha", "StringType"),
    ("status", "StringType"),
    ("conclusion", "StringType"),
    ("created_at", "TimestampType"),
    ("started_at", "TimestampType"),
    ("updated_at", "TimestampType"),
    ("duration_s", "LongType"),
    ("source_kafka_partition", "IntegerType"),
    ("source_kafka_offset", "LongType"),
]

EXPECTED_BUILDS_CONCLUSIONS = {
    "success": 5857,
    "failure": 808,
    "cancelled": 561,
    "action_required": 207,
    "startup_failure": 5,
}


def verify_silver_builds(spark) -> bool:
    """Run checks (a) through (h) for silver_builds."""
    s_path = silver_path("silver_builds")
    b_path = bronze_path("builds")

    print("\n" + "-" * 60)
    print(f"VERIFY SILVER TABLE: 'silver_builds' ({s_path})")
    print("-" * 60)

    raw_hudi_df = spark.read.format("hudi").load(s_path).cache()
    # Filter out Hudi's 5 prefix meta columns (_hoodie_*) when checking data columns
    data_cols = [c for c in raw_hudi_df.columns if not c.startswith("_hoodie_")]
    silver_df = raw_hudi_df.select(*data_cols).cache()

    ok = True

    try:
        # (a) Exact column names, order, and Spark data types
        actual_schema = [
            (field.name, type(field.dataType).__name__)
            for field in silver_df.schema.fields
        ]
        if actual_schema == EXPECTED_SILVER_BUILDS_SCHEMA:
            print(f"[PASS] (a) Columns and Spark types match exact specification ({len(actual_schema)} columns)")
        else:
            print(
                f"[FAIL] (a) Schema mismatch:\n"
                f"  got:      {actual_schema}\n"
                f"  expected: {EXPECTED_SILVER_BUILDS_SCHEMA}"
            )
            ok = False

        # (b) Row count = 7,438 and distinct (run_id, run_attempt) = 7,438
        total_rows = silver_df.count()
        distinct_keys = silver_df.select("run_id", "run_attempt").distinct().count()
        if total_rows == 7438 and distinct_keys == 7438:
            print(
                f"[PASS] (b) Row count == 7438 ({total_rows}) and "
                f"distinct (run_id, run_attempt) == 7438 ({distinct_keys})"
            )
        else:
            print(
                f"[FAIL] (b) Count mismatch: total_rows={total_rows}, "
                f"distinct_keys={distinct_keys} (expected 7438 for both)"
            )
            ok = False

        # Prepare deduplicated latest Bronze records for checks (c) and (g)
        bronze_parsed = (
            spark.read.parquet(b_path)
            .withColumn("data", F.from_json(F.col("raw_json"), BUILDS_SCHEMA))
            .select(
                F.col("data.run_id").alias("b_run_id"),
                F.col("data.run_attempt").alias("b_run_attempt"),
                F.col("data.status").alias("b_status"),
                F.col("data.conclusion").alias("b_conclusion"),
                F.to_timestamp(F.col("data.updated_at"), ISO_FORMAT).alias("b_updated_at"),
                F.col("kafka_offset").cast("long").alias("b_offset"),
            )
        )
        b_win = Window.partitionBy("b_run_id", "b_run_attempt").orderBy(
            F.col("b_updated_at").desc(),
            F.col("b_offset").desc(),
        )
        bronze_latest = (
            bronze_parsed.withColumn("_rn", F.row_number().over(b_win))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .cache()
        )

        # (c) Benchmark key 44069120063:1 appears once and matches latest Bronze
        s_bench = silver_df.filter(
            (F.col("run_id") == 44069120063) & (F.col("run_attempt") == 1)
        ).collect()
        b_bench = bronze_latest.filter(
            (F.col("b_run_id") == 44069120063) & (F.col("b_run_attempt") == 1)
        ).collect()

        if (
            len(s_bench) == 1
            and len(b_bench) == 1
            and s_bench[0]["status"] == b_bench[0]["b_status"]
            and s_bench[0]["conclusion"] == b_bench[0]["b_conclusion"]
        ):
            print(
                f"[PASS] (c) Key 44069120063:1 appears once in Silver "
                f"(status='{s_bench[0]['status']}', conclusion='{s_bench[0]['conclusion']}', "
                f"offset={s_bench[0]['source_kafka_offset']}) matching latest Bronze"
            )
        else:
            print(f"[FAIL] (c) Key 44069120063:1 check failed: silver={s_bench}, bronze={b_bench}")
            ok = False

        # (d) Null counts for run_id, run_attempt, status, updated_at == 0
        req_cols = ["run_id", "run_attempt", "status", "updated_at"]
        null_row = silver_df.agg(
            *[
                F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
                for c in req_cols
            ]
        ).collect()[0]
        null_counts = {c: int(null_row[c] or 0) for c in req_cols}
        if all(v == 0 for v in null_counts.values()):
            print(f"[PASS] (d) Required columns have 0 nulls: {null_counts}")
        else:
            print(f"[FAIL] (d) Unexpected nulls in required columns: {null_counts}")
            ok = False

        # (e) Conclusion distribution
        conc_rows = (
            silver_df.groupBy("conclusion")
            .count()
            .orderBy(F.desc("count"))
            .collect()
        )
        actual_conc = {r["conclusion"]: int(r["count"]) for r in conc_rows}
        if actual_conc == EXPECTED_BUILDS_CONCLUSIONS:
            print(f"[PASS] (e) Conclusion distribution matches exact expected counts: {actual_conc}")
        else:
            print(
                f"[FAIL] (e) Conclusion distribution mismatch: "
                f"got {actual_conc}, expected {EXPECTED_BUILDS_CONCLUSIONS}"
            )
            ok = False

        # (f) Duration checks + 5 sample rows
        dur_stats = silver_df.agg(
            F.sum(F.when(F.col("duration_s").isNotNull(), 1).otherwise(0)).alias("non_null"),
            F.sum(F.when(F.col("duration_s") < 0, 1).otherwise(0)).alias("negative"),
            F.sum(
                F.when(
                    (F.col("status") == "completed") & F.col("duration_s").isNull(),
                    1,
                ).otherwise(0)
            ).alias("completed_but_null"),
            F.min("duration_s").alias("min_d"),
            F.avg("duration_s").alias("avg_d"),
            F.max("duration_s").alias("max_d"),
        ).collect()[0]

        non_null_d = int(dur_stats["non_null"] or 0)
        neg_d = int(dur_stats["negative"] or 0)
        comp_null_d = int(dur_stats["completed_but_null"] or 0)
        min_d = dur_stats["min_d"]
        avg_d = float(dur_stats["avg_d"] or 0.0)
        max_d = dur_stats["max_d"]

        print(
            f"[INFO] (f) duration_s stats -> non_null={non_null_d}, negative={neg_d}, "
            f"completed_with_null_duration={comp_null_d}, "
            f"min={min_d}s, avg={avg_d:.2f}s, max={max_d}s"
        )
        if neg_d == 0 and non_null_d > 0:
            print(f"[PASS] (f) duration_s has 0 negative values and {non_null_d} computed durations")
        else:
            print(f"[FAIL] (f) duration_s check failed: non_null={non_null_d}, negative={neg_d}")
            ok = False

        sample_rows = (
            silver_df.select("run_id", "run_attempt", "started_at", "updated_at", "duration_s")
            .orderBy("run_id", "run_attempt")
            .limit(5)
            .collect()
        )
        print("[INFO] (f) 5 sample rows for manual arithmetic verification (updated_at - started_at == duration_s):")
        for sr in sample_rows:
            print(
                f"       run_id={sr['run_id']}, attempt={sr['run_attempt']}, "
                f"started_at={sr['started_at']}, updated_at={sr['updated_at']}, "
                f"duration_s={sr['duration_s']}"
            )

        # (g) Equivalence with latest Bronze record for every key
        joined = silver_df.join(
            bronze_latest,
            (F.col("run_id") == F.col("b_run_id"))
            & (F.col("run_attempt") == F.col("b_run_attempt")),
            "full_outer",
        )
        diff_count = joined.filter(
            F.col("run_id").isNull()
            | F.col("b_run_id").isNull()
            | (F.col("status") != F.col("b_status"))
            | (~F.col("conclusion").eqNullSafe(F.col("b_conclusion")))
            | (F.col("updated_at") != F.col("b_updated_at"))
        ).count()

        if diff_count == 0:
            print("[PASS] (g) Equivalence with Bronze: 0 differences across all 7,438 keys (status, conclusion, updated_at)")
        else:
            print(f"[FAIL] (g) Equivalence with Bronze failed: {diff_count} keys differ")
            ok = False

        # (h) Composite key sanity: distinct run_id vs distinct (run_id, run_attempt)
        distinct_run_ids = silver_df.select("run_id").distinct().count()
        multi_attempt_df = (
            silver_df.groupBy("run_id")
            .agg(F.count("*").alias("attempt_rows"))
            .filter(F.col("attempt_rows") > 1)
            .orderBy(F.desc("attempt_rows"), F.col("run_id"))
        )
        multi_attempt_count = multi_attempt_df.count()
        print(
            f"[INFO] (h) Composite key sanity -> distinct run_id={distinct_run_ids}, "
            f"distinct (run_id, run_attempt)={distinct_keys}, "
            f"run_ids with >1 attempt in Silver={multi_attempt_count}"
        )
        if multi_attempt_count > 0:
            sample_multi_ids = [r["run_id"] for r in multi_attempt_df.limit(5).collect()]
            multi_rows = (
                silver_df.filter(F.col("run_id").isin(sample_multi_ids))
                .select("run_id", "run_attempt", "status", "conclusion", "updated_at")
                .orderBy("run_id", "run_attempt")
                .collect()
            )
            print("[INFO] (h) Sample multi-attempt run_ids stored as separate rows in Silver:")
            for mr in multi_rows:
                print(
                    f"       run_id={mr['run_id']}, run_attempt={mr['run_attempt']}, "
                    f"status={mr['status']}, conclusion={mr['conclusion']}, updated_at={mr['updated_at']}"
                )
        if distinct_keys >= distinct_run_ids and (distinct_keys - distinct_run_ids) >= multi_attempt_count:
            print("[PASS] (h) Composite key (run_id, run_attempt) preserves multiple attempts as distinct rows")
        else:
            print("[FAIL] (h) Composite key sanity check failed")
            ok = False

        bronze_latest.unpersist()

    finally:
        silver_df.unpersist()
        raw_hudi_df.unpersist()

    return ok


TABLE_VERIFIERS = {
    "silver_builds": verify_silver_builds,
}


def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage:verify_silver.py <table_name> (available: {list(TABLE_VERIFIERS.keys())})")
        sys.exit(2)

    targets = sys.argv[1:]
    for t in targets:
        if t not in TABLE_VERIFIERS:
            print(f"[ERROR] Unknown Silver table '{t}'. Available: {list(TABLE_VERIFIERS.keys())}")
            sys.exit(2)

    print("=" * 60)
    print(f"LAKEHOUSE SILVER VERIFICATION: {targets}")
    print("=" * 60)

    spark = get_spark("lakehouse-verify-silver")
    spark.sparkContext.setLogLevel("WARN")

    all_ok = True
    try:
        for t in targets:
            if not TABLE_VERIFIERS[t](spark):
                all_ok = False
    finally:
        spark.stop()

    print("\n" + "=" * 60)
    if all_ok:
        print("ALL SILVER VERIFICATION CHECKS PASSED")
        sys.exit(0)
    else:
        print("SILVER VERIFICATION FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
