"""Read-only verification of Silver Hudi tables with a per-table check registry.

Usage:
    spark-submit tools/verify_silver.py silver_builds
    spark-submit tools/verify_silver.py silver_tests
"""

import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from config import bronze_path, silver_path
from schemas import BUILDS_SCHEMA, DEPLOYMENTS_SCHEMA, TESTS_SCHEMA
from silver_common import ISO_FORMAT
from spark_session import get_spark

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

EXPECTED_SILVER_TESTS_SCHEMA = [
    ("job_id", "LongType"),
    ("run_id", "LongType"),
    ("run_attempt", "IntegerType"),
    ("head_sha", "StringType"),
    ("job_name", "StringType"),
    ("status", "StringType"),
    ("conclusion", "StringType"),
    ("started_at", "TimestampType"),
    ("completed_at", "TimestampType"),
    ("duration_s", "LongType"),
    ("event_ts", "TimestampType"),
    ("source_kafka_partition", "IntegerType"),
    ("source_kafka_offset", "LongType"),
]

EXPECTED_TESTS_CONCLUSIONS = {
    "success": 17490,
    "skipped": 3654,
    "cancelled": 951,
    "failure": 649,
}

EXPECTED_SILVER_DEPLOYMENTS_SCHEMA = [
    ("deployment_id", "LongType"),
    ("environment", "StringType"),
    ("production_environment", "BooleanType"),
    ("ref", "StringType"),
    ("sha", "StringType"),
    ("created_at", "TimestampType"),
    ("updated_at", "TimestampType"),
    ("latest_status", "StringType"),
    ("latest_status_at", "TimestampType"),
    ("status_history", "ArrayType"),
    ("was_successful", "BooleanType"),
    ("first_success_at", "TimestampType"),
    ("is_prd_environment", "BooleanType"),
    ("event_ts", "TimestampType"),
    ("source_kafka_partition", "IntegerType"),
    ("source_kafka_offset", "LongType"),
]

EXPECTED_STATUS_HISTORY_ELEMENT_SCHEMA = [
    ("status_id", "LongType"),
    ("state", "StringType"),
    ("created_at", "TimestampType"),
]

EXPECTED_DEPLOYMENTS_LATEST_STATUS = {
    "inactive": 262,
    "success": 39,
    "failure": 2,
}

EXPECTED_DEPLOYMENTS_ENVIRONMENTS = {
    "stg-clients": 144,
    "prd-clients": 134,
    "stg-api": 13,
    "prd-api": 12,
}

EXPECTED_DEPLOYMENTS_WAS_SUCCESSFUL = {
    True: 301,
    False: 2,
}


def verify_silver_builds(spark) -> bool:
    """Run checks (a) through (i) for silver_builds."""
    s_path = silver_path("silver_builds")
    b_path = bronze_path("builds")

    print("\n" + "-" * 60)
    print(f"VERIFY SILVER TABLE: 'silver_builds' ({s_path})")
    print("-" * 60)

    raw_hudi_df = spark.read.format("hudi").load(s_path).cache()
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

        # (i) INFO duration_s percentiles (p50, p90, p99), count > 24h (86400s), and 10 longest durations
        pct_row = silver_df.select(
            F.expr("percentile_approx(duration_s, array(0.5, 0.9, 0.99), 10000)").alias("pcts"),
            F.sum(F.when(F.col("duration_s") > 86400, 1).otherwise(0)).alias("over_24h"),
        ).collect()[0]
        p50, p90, p99 = pct_row["pcts"]
        over_24h = int(pct_row["over_24h"] or 0)
        print(
            f"[INFO] (i) duration_s distribution -> p50(median)={p50}s, p90={p90}s, p99={p99}s, "
            f"count(>24h / 86400s)={over_24h}"
        )
        top10_longest = (
            silver_df.select(
                "run_id",
                "run_attempt",
                "workflow",
                "event",
                "conclusion",
                "started_at",
                "updated_at",
                "duration_s",
            )
            .orderBy(F.col("duration_s").desc(), F.col("run_id"))
            .limit(10)
            .collect()
        )
        print("[INFO] (i) Top 10 longest duration_s rows in silver_builds:")
        for r in top10_longest:
            print(
                f"       run_id={r['run_id']}, attempt={r['run_attempt']}, "
                f"workflow='{r['workflow']}', event='{r['event']}', conclusion='{r['conclusion']}', "
                f"started_at={r['started_at']}, updated_at={r['updated_at']}, duration_s={r['duration_s']}s"
            )

        bronze_latest.unpersist()

    finally:
        silver_df.unpersist()
        raw_hudi_df.unpersist()

    return ok


def verify_silver_tests(spark) -> bool:
    """Run checks (a) through (k) for silver_tests."""
    s_path = silver_path("silver_tests")
    b_path = bronze_path("tests")
    sb_path = silver_path("silver_builds")

    print("\n" + "-" * 60)
    print(f"VERIFY SILVER TABLE: 'silver_tests' ({s_path})")
    print("-" * 60)

    raw_hudi_df = spark.read.format("hudi").load(s_path).cache()
    data_cols = [c for c in raw_hudi_df.columns if not c.startswith("_hoodie_")]
    silver_df = raw_hudi_df.select(*data_cols).cache()

    ok = True

    try:
        # (a) Columns and Spark types
        actual_schema = [
            (field.name, type(field.dataType).__name__)
            for field in silver_df.schema.fields
        ]
        if actual_schema == EXPECTED_SILVER_TESTS_SCHEMA:
            print(f"[PASS] (a) Columns and Spark types match exact specification ({len(actual_schema)} columns)")
        else:
            print(
                f"[FAIL] (a) Schema mismatch:\n"
                f"  got:      {actual_schema}\n"
                f"  expected: {EXPECTED_SILVER_TESTS_SCHEMA}"
            )
            ok = False

        # (b) Row count = 22,744 and distinct job_id = 22,744
        total_rows = silver_df.count()
        distinct_keys = silver_df.select("job_id").distinct().count()
        if total_rows == 22744 and distinct_keys == 22744:
            print(f"[PASS] (b) Row count == 22744 ({total_rows}) and distinct job_id == 22744 ({distinct_keys})")
        else:
            print(
                f"[FAIL] (b) Count mismatch: total_rows={total_rows}, "
                f"distinct_job_id={distinct_keys} (expected 22744 for both)"
            )
            ok = False

        # Prepare deduplicated latest Bronze records for checks (c) and (i)
        bronze_parsed = (
            spark.read.parquet(b_path)
            .withColumn("data", F.from_json(F.col("raw_json"), TESTS_SCHEMA))
            .select(
                F.col("data.job_id").alias("b_job_id"),
                F.col("data.status").alias("b_status"),
                F.col("data.conclusion").alias("b_conclusion"),
                F.to_timestamp(F.col("data.started_at"), ISO_FORMAT).alias("b_started_at"),
                F.to_timestamp(F.col("data.completed_at"), ISO_FORMAT).alias("b_completed_at"),
                F.col("kafka_offset").cast("long").alias("b_offset"),
            )
            .withColumn("b_event_ts", F.coalesce(F.col("b_completed_at"), F.col("b_started_at")))
        )
        b_win = Window.partitionBy("b_job_id").orderBy(
            F.col("b_event_ts").desc(),
            F.col("b_offset").desc(),
        )
        bronze_latest = (
            bronze_parsed.withColumn("_rn", F.row_number().over(b_win))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .cache()
        )

        # (c) job_id 133990467008 (published 4 times in Kafka) appears once
        s_bench = silver_df.filter(F.col("job_id") == 133990467008).collect()
        if len(s_bench) == 1:
            print(
                f"[PASS] (c) job_id 133990467008 appears exactly once in Silver "
                f"(status='{s_bench[0]['status']}', conclusion='{s_bench[0]['conclusion']}', "
                f"offset={s_bench[0]['source_kafka_offset']})"
            )
        else:
            print(f"[FAIL] (c) job_id 133990467008 count in Silver: got {len(s_bench)}, expected 1")
            ok = False

        # (d) Null counts = 0 for job_id, run_id, status, event_ts
        req_cols = ["job_id", "run_id", "status", "event_ts"]
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

        # (e) Conclusion distribution: success 17,490 / skipped 3,654 / cancelled 951 / failure 649
        conc_rows = (
            silver_df.groupBy("conclusion")
            .count()
            .orderBy(F.desc("count"))
            .collect()
        )
        actual_conc = {r["conclusion"]: int(r["count"]) for r in conc_rows}
        if actual_conc == EXPECTED_TESTS_CONCLUSIONS:
            print(f"[PASS] (e) Conclusion distribution matches exact expected counts: {actual_conc}")
        else:
            print(
                f"[FAIL] (e) Conclusion distribution mismatch: "
                f"got {actual_conc}, expected {EXPECTED_TESTS_CONCLUSIONS}"
            )
            ok = False

        # (f) duration_s non-null count (~18,950), null count (3,794), negative count (0)
        dur_row = silver_df.agg(
            F.sum(F.when(F.col("duration_s").isNotNull(), 1).otherwise(0)).alias("non_null"),
            F.sum(F.when(F.col("duration_s").isNull(), 1).otherwise(0)).alias("null_cnt"),
            F.sum(F.when(F.col("duration_s") < 0, 1).otherwise(0)).alias("negative"),
        ).collect()[0]
        non_null_d = int(dur_row["non_null"] or 0)
        null_d = int(dur_row["null_cnt"] or 0)
        neg_d = int(dur_row["negative"] or 0)
        print(
            f"[INFO] (f) duration_s counts -> non_null={non_null_d} (expected ~18950), "
            f"null={null_d} (expected 3794), negative={neg_d} (expected 0)"
        )
        if neg_d == 0 and abs(non_null_d - 18950) <= 3 and null_d == 3794:
            print(f"[PASS] (f) duration_s counts match expectations: non_null={non_null_d}, null={null_d}, negative={neg_d}")
        else:
            print(f"[FAIL] (f) duration_s counts outside expected range: non_null={non_null_d}, null={null_d}, negative={neg_d}")
            ok = False

        # (g) duration consistency: for every row with non-null duration_s,
        # duration_s == unix_timestamp(completed_at) - unix_timestamp(started_at)
        expected_dur_expr = F.unix_timestamp(F.col("completed_at")) - F.unix_timestamp(F.col("started_at"))
        dur_mismatches = (
            silver_df.filter(
                F.col("duration_s").isNotNull() & (F.col("duration_s") != expected_dur_expr)
            ).count()
        )
        if dur_mismatches == 0:
            print(
                f"[PASS] (g) Duration consistency: 0 mismatches across all {non_null_d} non-null duration_s rows "
                f"(duration_s == completed_at - started_at)"
            )
        else:
            print(f"[FAIL] (g) Duration consistency mismatches: {dur_mismatches}")
            ok = False

        sample_dur = (
            silver_df.filter(F.col("duration_s").isNotNull())
            .select("job_id", "started_at", "completed_at", "duration_s")
            .orderBy("job_id")
            .limit(5)
            .collect()
        )
        print("[INFO] (g) 5 sample rows for manual duration arithmetic verification:")
        for sr in sample_dur:
            print(
                f"       job_id={sr['job_id']}, started_at={sr['started_at']}, "
                f"completed_at={sr['completed_at']}, duration_s={sr['duration_s']}"
            )

        # (h) INFO breakdown of rows with null duration_s by (status, conclusion) + inspect the non-skipped ones
        null_dur_breakdown = (
            silver_df.filter(F.col("duration_s").isNull())
            .groupBy("status", "conclusion")
            .agg(
                F.count("*").alias("count"),
                F.sum(
                    F.when(F.col("completed_at") < F.col("started_at"), 1).otherwise(0)
                ).alias("completed_before_started"),
                F.sum(
                    F.when(F.col("completed_at").isNull() | F.col("started_at").isNull(), 1).otherwise(0)
                ).alias("missing_timestamp"),
            )
            .orderBy(F.desc("count"))
            .collect()
        )
        print("[INFO] (h) Breakdown of rows with null duration_s by (status, conclusion):")
        for r in null_dur_breakdown:
            print(
                f"       status='{r['status']}', conclusion='{r['conclusion']}': count={r['count']} "
                f"(completed_before_started={r['completed_before_started']}, missing_ts={r['missing_timestamp']})"
            )
        other_samples = (
            silver_df.filter(F.col("duration_s").isNull() & (F.col("conclusion") != "skipped"))
            .select("job_id", "job_name", "status", "conclusion", "started_at", "completed_at")
            .orderBy("job_id")
            .limit(5)
            .collect()
        )
        print("[INFO] (h) 5 sample non-skipped rows with null duration_s (showing why duration_s is null):")
        for osr in other_samples:
            print(
                f"       job_id={osr['job_id']}, job_name='{osr['job_name']}', "
                f"status='{osr['status']}', conclusion='{osr['conclusion']}', "
                f"started_at={osr['started_at']}, completed_at={osr['completed_at']}"
            )

        # (i) Equivalence with Bronze: for every job_id, status, conclusion, completed_at equal latest Bronze
        joined = silver_df.join(
            bronze_latest,
            F.col("job_id") == F.col("b_job_id"),
            "full_outer",
        )
        diff_count = joined.filter(
            F.col("job_id").isNull()
            | F.col("b_job_id").isNull()
            | (F.col("status") != F.col("b_status"))
            | (~F.col("conclusion").eqNullSafe(F.col("b_conclusion")))
            | (~F.col("completed_at").eqNullSafe(F.col("b_completed_at")))
        ).count()
        if diff_count == 0:
            print("[PASS] (i) Equivalence with Bronze: 0 differences across all 22,744 job_ids (status, conclusion, completed_at)")
        else:
            print(f"[FAIL] (i) Equivalence with Bronze failed: {diff_count} job_ids differ")
            ok = False

        # (j) INFO referential integrity with silver_builds on (run_id, run_attempt)
        sb_keys = (
            spark.read.format("hudi")
            .load(sb_path)
            .select(
                F.col("run_id").alias("sb_run_id"),
                F.col("run_attempt").alias("sb_run_attempt"),
            )
            .distinct()
        )
        orphan_jobs_df = silver_df.join(
            sb_keys,
            (F.col("run_id") == F.col("sb_run_id"))
            & (F.col("run_attempt") == F.col("sb_run_attempt")),
            "left_anti",
        )
        orphan_count = orphan_jobs_df.count()
        print(
            f"[INFO] (j) Integrity with silver_builds: {orphan_count} silver_tests rows have no matching "
            f"(run_id, run_attempt) in silver_builds"
        )
        if orphan_count > 0:
            orphan_samples = (
                orphan_jobs_df.select("job_id", "run_id", "run_attempt", "job_name", "started_at")
                .orderBy("job_id")
                .limit(5)
                .collect()
            )
            for orow in orphan_samples:
                print(
                    f"       orphan job_id={orow['job_id']}, run_id={orow['run_id']}, "
                    f"run_attempt={orow['run_attempt']}, job_name='{orow['job_name']}'"
                )

        # (k) INFO top 20 job_name values and distinct job_name count
        distinct_job_names = silver_df.select("job_name").distinct().count()
        top20_names = (
            silver_df.groupBy("job_name")
            .count()
            .orderBy(F.desc("count"), F.col("job_name"))
            .limit(20)
            .collect()
        )
        print(f"[INFO] (k) Distinct job_name count in silver_tests: {distinct_job_names}")
        print("[INFO] (k) Top 20 most frequent job_name values:")
        for idx, nr in enumerate(top20_names, start=1):
            print(f"       {idx:2d}. '{nr['job_name']}': {nr['count']}")

        bronze_latest.unpersist()

    finally:
        silver_df.unpersist()
        raw_hudi_df.unpersist()

    return ok


def verify_silver_deployments(spark) -> bool:
    """Run checks (a) through (k) for silver_deployments."""
    s_path = silver_path("silver_deployments")
    b_path = bronze_path("deployments")

    print("\n" + "-" * 60)
    print(f"VERIFY SILVER TABLE: 'silver_deployments' ({s_path})")
    print("-" * 60)

    raw_hudi_df = spark.read.format("hudi").load(s_path).cache()
    data_cols = [c for c in raw_hudi_df.columns if not c.startswith("_hoodie_")]
    silver_df = raw_hudi_df.select(*data_cols).cache()

    ok = True

    try:
        # (a) Columns, Spark types, and nested status_history element struct types
        actual_schema = [
            (field.name, type(field.dataType).__name__)
            for field in silver_df.schema.fields
        ]
        sh_field = silver_df.schema["status_history"].dataType
        nested_schema = []
        if hasattr(sh_field, "elementType") and hasattr(sh_field.elementType, "fields"):
            nested_schema = [
                (nf.name, type(nf.dataType).__name__)
                for nf in sh_field.elementType.fields
            ]

        if (
            actual_schema == EXPECTED_SILVER_DEPLOYMENTS_SCHEMA
            and nested_schema == EXPECTED_STATUS_HISTORY_ELEMENT_SCHEMA
        ):
            print(
                f"[PASS] (a) Columns and Spark types match exact specification "
                f"({len(actual_schema)} columns, status_history={nested_schema})"
            )
        else:
            print(
                f"[FAIL] (a) Schema mismatch:\n"
                f"  got top-level:      {actual_schema}\n"
                f"  expected top-level: {EXPECTED_SILVER_DEPLOYMENTS_SCHEMA}\n"
                f"  got nested:         {nested_schema}\n"
                f"  expected nested:    {EXPECTED_STATUS_HISTORY_ELEMENT_SCHEMA}"
            )
            ok = False

        # (b) Row count = 303 and distinct deployment_id = 303
        total_rows = silver_df.count()
        distinct_keys = silver_df.select("deployment_id").distinct().count()
        if total_rows == 303 and distinct_keys == 303:
            print(f"[PASS] (b) Row count == 303 ({total_rows}) and distinct deployment_id == 303 ({distinct_keys})")
        else:
            print(
                f"[FAIL] (b) Count mismatch: total_rows={total_rows}, "
                f"distinct_deployment_id={distinct_keys} (expected 303 for both)"
            )
            ok = False

        # Prepare deduplicated latest Bronze records for checks (c) and (i)
        bronze_parsed = (
            spark.read.parquet(b_path)
            .withColumn("data", F.from_json(F.col("raw_json"), DEPLOYMENTS_SCHEMA))
            .select(
                F.col("data.deployment_id").alias("b_deployment_id"),
                F.col("data.latest_status").alias("b_latest_status"),
                F.to_timestamp(F.col("data.created_at"), ISO_FORMAT).alias("b_created_at"),
                F.to_timestamp(F.col("data.updated_at"), ISO_FORMAT).alias("b_updated_at"),
                F.to_timestamp(F.col("data.latest_status_at"), ISO_FORMAT).alias("b_latest_status_at"),
                F.size(F.col("data.status_history")).alias("b_sh_size"),
                F.col("kafka_offset").cast("long").alias("b_offset"),
            )
            .withColumn(
                "b_event_ts",
                F.coalesce(F.col("b_latest_status_at"), F.col("b_updated_at"), F.col("b_created_at")),
            )
        )
        b_win = Window.partitionBy("b_deployment_id").orderBy(
            F.col("b_event_ts").desc(),
            F.col("b_offset").desc(),
        )
        bronze_latest = (
            bronze_parsed.withColumn("_rn", F.row_number().over(b_win))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .cache()
        )

        # (c) deployment_id 7769301245 (published 4 times in Kafka) appears exactly once
        s_bench = silver_df.filter(F.col("deployment_id") == 7769301245).collect()
        if len(s_bench) == 1:
            print(
                f"[PASS] (c) deployment_id 7769301245 appears exactly once in Silver "
                f"(environment='{s_bench[0]['environment']}', latest_status='{s_bench[0]['latest_status']}', "
                f"was_successful={s_bench[0]['was_successful']}, offset={s_bench[0]['source_kafka_offset']})"
            )
        else:
            print(f"[FAIL] (c) deployment_id 7769301245 count in Silver: got {len(s_bench)}, expected 1")
            ok = False

        # (d) Null counts = 0 for deployment_id, environment, created_at, event_ts
        req_cols = ["deployment_id", "environment", "created_at", "event_ts"]
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

        # (e) latest_status distribution & environment distribution
        ls_rows = (
            silver_df.groupBy("latest_status")
            .count()
            .orderBy(F.desc("count"))
            .collect()
        )
        actual_ls = {r["latest_status"]: int(r["count"]) for r in ls_rows}

        env_rows = (
            silver_df.groupBy("environment")
            .count()
            .orderBy(F.desc("count"))
            .collect()
        )
        actual_env = {r["environment"]: int(r["count"]) for r in env_rows}

        if (
            actual_ls == EXPECTED_DEPLOYMENTS_LATEST_STATUS
            and actual_env == EXPECTED_DEPLOYMENTS_ENVIRONMENTS
        ):
            print(
                f"[PASS] (e) Distributions match exact expected counts: "
                f"latest_status={actual_ls}, environment={actual_env}"
            )
        else:
            print(
                f"[FAIL] (e) Distribution mismatch:\n"
                f"  latest_status: got {actual_ls}, expected {EXPECTED_DEPLOYMENTS_LATEST_STATUS}\n"
                f"  environment:   got {actual_env}, expected {EXPECTED_DEPLOYMENTS_ENVIRONMENTS}"
            )
            ok = False

        # (f) was_successful: True 301 / False 2; inspect False rows
        ws_rows = (
            silver_df.groupBy("was_successful")
            .count()
            .orderBy(F.desc("count"))
            .collect()
        )
        actual_ws = {bool(r["was_successful"]): int(r["count"]) for r in ws_rows}
        if actual_ws == EXPECTED_DEPLOYMENTS_WAS_SUCCESSFUL:
            print(f"[PASS] (f) was_successful distribution matches exact expected counts: {actual_ws}")
        else:
            print(
                f"[FAIL] (f) was_successful mismatch: "
                f"got {actual_ws}, expected {EXPECTED_DEPLOYMENTS_WAS_SUCCESSFUL}"
            )
            ok = False

        false_rows = (
            silver_df.filter(~F.col("was_successful"))
            .select(
                "deployment_id",
                "environment",
                "latest_status",
                F.expr("transform(status_history, x -> x.state)").alias("history_states"),
            )
            .orderBy("deployment_id")
            .collect()
        )
        print("[INFO] (f) Details of rows with was_successful = False:")
        for fr in false_rows:
            print(
                f"       deployment_id={fr['deployment_id']}, environment='{fr['environment']}', "
                f"latest_status='{fr['latest_status']}', status_history_states={fr['history_states']}"
            )

        # (g) Consistency: first_success_at is not null iff was_successful is true;
        # also check first_success_at < created_at
        inconsistent_fs = silver_df.filter(
            (F.col("was_successful") & F.col("first_success_at").isNull())
            | ((~F.col("was_successful")) & F.col("first_success_at").isNotNull())
        ).count()
        before_created_df = silver_df.filter(
            F.col("first_success_at").isNotNull() & (F.col("first_success_at") < F.col("created_at"))
        )
        before_created_count = before_created_df.count()
        print(
            f"[INFO] (g) Rows where first_success_at < created_at: {before_created_count}"
        )
        if before_created_count > 0:
            for bcr in before_created_df.select("deployment_id", "created_at", "first_success_at").limit(5).collect():
                print(
                    f"       deployment_id={bcr['deployment_id']}, "
                    f"created_at={bcr['created_at']}, first_success_at={bcr['first_success_at']}"
                )
        if inconsistent_fs == 0 and before_created_count == 0:
            print(
                "[PASS] (g) first_success_at consistency: non-null iff was_successful is True, "
                "and 0 rows where first_success_at < created_at"
            )
        else:
            print(
                f"[FAIL] (g) first_success_at consistency check failed: "
                f"inconsistent={inconsistent_fs}, before_created={before_created_count}"
            )
            ok = False

        # (h) status_history integrity: 0 null elements, 0 null created_at, size distribution, state counts
        sh_stats = silver_df.select(
            F.sum(F.expr("size(filter(status_history, x -> x is null))")).alias("null_elems"),
            F.sum(F.expr("size(filter(status_history, x -> x.created_at is null))")).alias("null_ts"),
            F.min(F.size("status_history")).alias("min_len"),
            F.expr("percentile_approx(size(status_history), 0.5)").alias("med_len"),
            F.max(F.size("status_history")).alias("max_len"),
        ).collect()[0]
        null_elems = int(sh_stats["null_elems"] or 0)
        null_ts = int(sh_stats["null_ts"] or 0)
        min_len = int(sh_stats["min_len"] or 0)
        med_len = int(sh_stats["med_len"] or 0)
        max_len = int(sh_stats["max_len"] or 0)

        state_counts_rows = (
            silver_df.select(F.explode("status_history").alias("sh"))
            .groupBy(F.col("sh.state").alias("state"))
            .count()
            .orderBy(F.desc("count"))
            .collect()
        )
        state_dist = {r["state"]: int(r["count"]) for r in state_counts_rows}
        print(
            f"[INFO] (h) status_history size per deployment -> min={min_len}, median={med_len}, max={max_len}"
        )
        print(f"[INFO] (h) status_history distinct state counts across all elements: {state_dist}")

        if null_elems == 0 and null_ts == 0:
            print("[PASS] (h) status_history integrity: 0 null elements and 0 null created_at timestamps")
        else:
            print(f"[FAIL] (h) status_history nulls found: null_elements={null_elems}, null_created_at={null_ts}")
            ok = False

        # (i) Equivalence with Bronze: latest_status, latest_status_at, and size(status_history)
        joined = silver_df.join(
            bronze_latest,
            F.col("deployment_id") == F.col("b_deployment_id"),
            "full_outer",
        )
        diff_count = joined.filter(
            F.col("deployment_id").isNull()
            | F.col("b_deployment_id").isNull()
            | (~F.col("latest_status").eqNullSafe(F.col("b_latest_status")))
            | (~F.col("latest_status_at").eqNullSafe(F.col("b_latest_status_at")))
            | (F.size(F.col("status_history")) != F.col("b_sh_size"))
        ).count()
        if diff_count == 0:
            print(
                "[PASS] (i) Equivalence with Bronze: 0 differences across all 303 deployment_ids "
                "(latest_status, latest_status_at, status_history size)"
            )
        else:
            print(f"[FAIL] (i) Equivalence with Bronze failed: {diff_count} deployment_ids differ")
            ok = False

        # (j) INFO for Kamal: is_prd_environment == True breakdown & monthly successful production deployments
        prd_df = silver_df.filter(F.col("is_prd_environment"))
        prd_total = prd_df.count()
        prd_succ_df = prd_df.filter(F.col("was_successful"))
        prd_succ_count = prd_succ_df.count()
        prd_range = prd_succ_df.agg(
            F.min("first_success_at").alias("min_fs"),
            F.max("first_success_at").alias("max_fs"),
        ).collect()[0]
        monthly_prd = (
            prd_succ_df.withColumn("month", F.date_format(F.col("first_success_at"), "yyyy-MM"))
            .groupBy("month")
            .count()
            .orderBy("month")
            .collect()
        )
        monthly_str = ", ".join(f"{r['month']}={r['count']}" for r in monthly_prd)
        print(
            f"[INFO] (j) Production deployments (is_prd_environment=True): total={prd_total}, "
            f"was_successful=True: {prd_succ_count}, "
            f"first_success_at range: [{prd_range['min_fs']} .. {prd_range['max_fs']}]"
        )
        print(f"[INFO] (j) Successful production deployments per calendar month (by first_success_at): {monthly_str}")

        # (k) sha validation: nulls == 0, non-matching ^[0-9a-f]{40}$ == 0, distinct sha count
        sha_row = silver_df.agg(
            F.sum(F.when(F.col("sha").isNull(), 1).otherwise(0)).alias("null_sha"),
            F.sum(
                F.when(F.col("sha").isNotNull() & (~F.col("sha").rlike(r"^[0-9a-f]{40}$")), 1).otherwise(0)
            ).alias("bad_sha"),
            F.countDistinct("sha").alias("distinct_sha"),
        ).collect()[0]
        null_sha = int(sha_row["null_sha"] or 0)
        bad_sha = int(sha_row["bad_sha"] or 0)
        distinct_sha = int(sha_row["distinct_sha"] or 0)
        print(f"[INFO] (k) sha stats -> nulls={null_sha}, invalid_hex40={bad_sha}, distinct_sha={distinct_sha}")
        if null_sha == 0 and bad_sha == 0:
            print(f"[PASS] (k) All 303 sha values are valid 40-char lowercase hex strings ({distinct_sha} distinct commits)")
        else:
            print(f"[FAIL] (k) Invalid sha values detected: nulls={null_sha}, bad_format={bad_sha}")
            ok = False

        bronze_latest.unpersist()

    finally:
        silver_df.unpersist()
        raw_hudi_df.unpersist()

    return ok


TABLE_VERIFIERS = {
    "silver_builds": verify_silver_builds,
    "silver_tests": verify_silver_tests,
    "silver_deployments": verify_silver_deployments,
}


def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: verify_silver.py <table_name> (available: {list(TABLE_VERIFIERS.keys())})")
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
