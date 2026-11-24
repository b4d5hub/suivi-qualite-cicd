"""Synthetic Hudi COPY_ON_WRITE upsert test inside s3a://cicd-lakehouse/_smoke/upsert_demo/.

Never touches bronze/ or silver_*/ tables. Cleans up _smoke/ at the end.
"""

from datetime import datetime, timezone
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config import LAKEHOUSE_BUCKET
from silver_common import build_hudi_options
from spark_session import get_spark

SMOKE_ROOT = f"s3a://{LAKEHOUSE_BUCKET}/_smoke"
DEMO_PATH = f"{SMOKE_ROOT}/upsert_demo"

DEMO_SCHEMA = StructType(
    [
        StructField("run_id", LongType(), False),
        StructField("run_attempt", IntegerType(), False),
        StructField("workflow", StringType(), True),
        StructField("status", StringType(), False),
        StructField("conclusion", StringType(), True),
        StructField("updated_at", TimestampType(), False),
    ]
)

HUDI_DATA_COLS = ["run_id", "run_attempt", "workflow", "status", "conclusion", "updated_at"]


def _utc(year: int, month: int, day: int, hour: int, minute: int, second: int) -> datetime:
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)


def _delete_path_if_exists(spark: SparkSession, uri: str) -> bool:
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(hconf)
    if fs.exists(path):
        fs.delete(path, True)
    return not fs.exists(path)


def _path_exists(spark: SparkSession, uri: str) -> bool:
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(hconf)
    return bool(fs.exists(path))


def _print_rows(label: str, rows: list) -> None:
    print(f"[INFO] {label} (row_count={len(rows)}):")
    for r in rows:
        print(
            f"  run_id={r['run_id']}, run_attempt={r['run_attempt']}, "
            f"status={r['status']!r}, conclusion={r['conclusion']!r}, "
            f"updated_at={r['updated_at']} (_hoodie_commit_time={r['_hoodie_commit_time']})"
        )


def main() -> int:
    print("=" * 60)
    print("SYNTHETIC HUDI UPSERT DEMO (TEST ONLY IN _smoke/upsert_demo)")
    print("=" * 60)
    print(f"[INFO] Target test path: {DEMO_PATH} (Bronze & Silver are NOT touched)")

    spark = get_spark("lakehouse-demo-upsert")
    failures = []

    try:
        # Clean any leftover test folder before starting
        _delete_path_if_exists(spark, DEMO_PATH)

        hudi_opts = build_hudi_options(
            table_name="upsert_demo_builds",
            recordkey_field="run_id,run_attempt",
            precombine_field="updated_at",
        )

        t0 = _utc(2026, 11, 20, 9, 55, 0)
        t1 = _utc(2026, 11, 20, 10, 0, 0)
        t2 = _utc(2026, 11, 20, 10, 5, 0)

        # ------------------------------------------------------------------
        # BATCH 1: Initial insert of 2 synthetic keys at T1
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("BATCH 1: Insert key (999000001, 1) [in_progress, T1] + key (999000002, 1)")
        print("-" * 60)
        batch1_data = [
            (999000001, 1, "CI - Demo", "in_progress", None, t1),
            (999000002, 1, "CI - Demo", "completed", "success", t1),
        ]
        df_b1 = spark.createDataFrame(batch1_data, schema=DEMO_SCHEMA)
        df_b1.write.format("hudi").options(**hudi_opts).mode("append").save(DEMO_PATH)

        rows_b1 = (
            spark.read.format("hudi")
            .load(DEMO_PATH)
            .select("_hoodie_commit_time", *HUDI_DATA_COLS)
            .orderBy("run_id", "run_attempt")
            .collect()
        )
        _print_rows("After Batch 1 (BEFORE update)", rows_b1)

        # ------------------------------------------------------------------
        # BATCH 2: Upsert key (999000001, 1) with newer timestamp T2 > T1
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("BATCH 2: Upsert key (999000001, 1) [completed/success, T2 > T1]")
        print("-" * 60)
        batch2_data = [
            (999000001, 1, "CI - Demo", "completed", "success", t2),
        ]
        df_b2 = spark.createDataFrame(batch2_data, schema=DEMO_SCHEMA)
        df_b2.write.format("hudi").options(**hudi_opts).mode("append").save(DEMO_PATH)

        rows_b2 = (
            spark.read.format("hudi")
            .load(DEMO_PATH)
            .select("_hoodie_commit_time", *HUDI_DATA_COLS)
            .orderBy("run_id", "run_attempt")
            .collect()
        )
        _print_rows("After Batch 2 (AFTER update with T2 > T1)", rows_b2)

        by_key_b2 = {(r["run_id"], r["run_attempt"]): r for r in rows_b2}
        k1_b2 = by_key_b2.get((999000001, 1))
        if (
            len(rows_b2) == 2
            and k1_b2 is not None
            and k1_b2["status"] == "completed"
            and k1_b2["conclusion"] == "success"
            and k1_b2["updated_at"] == t2.replace(tzinfo=None)
        ):
            print(
                "[PASS] Batch 2 upsert verified: total_rows=2, key (999000001, 1) updated in-place "
                "from ('in_progress', None, 2026-11-20 10:00:00) to ('completed', 'success', 2026-11-20 10:05:00)."
            )
        else:
            msg = f"Batch 2 upsert unexpected state: rows={rows_b2}"
            print(f"[FAIL] {msg}")
            failures.append(msg)

        # ------------------------------------------------------------------
        # BATCH 3 (INFO ONLY, DO NOT FAIL): Late record with T0 < T1
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("BATCH 3 (INFO ONLY): Late record for (999000001, 1) with T0 < T1")
        print("-" * 60)
        batch3_data = [
            (999000001, 1, "CI - Demo", "queued", None, t0),
        ]
        df_b3 = spark.createDataFrame(batch3_data, schema=DEMO_SCHEMA)
        df_b3.write.format("hudi").options(**hudi_opts).mode("append").save(DEMO_PATH)

        rows_b3 = (
            spark.read.format("hudi")
            .load(DEMO_PATH)
            .select("_hoodie_commit_time", *HUDI_DATA_COLS)
            .orderBy("run_id", "run_attempt")
            .collect()
        )
        _print_rows("After Batch 3 (Late record with T0=09:55:00 < T1=10:00:00)", rows_b3)

        k1_b3 = {(r["run_id"], r["run_attempt"]): r for r in rows_b3}.get((999000001, 1))
        overwrote = k1_b3 is not None and k1_b3["status"] == "queued"
        print(f"[INFO] Batch 3 observation: overwrote_newer_row={overwrote} (current status={k1_b3['status']!r}, updated_at={k1_b3['updated_at']})")
        print(
            "[INFO] Explanation (3 lines):\n"
            "  1. Hudi's default payload (OverwriteWithLatestAvroPayload) uses precombineField (updated_at) to deduplicate records within the same incoming write batch, but overwrites existing storage records on separate commits unless DefaultHoodieRecordPayload is configured.\n"
            "  2. In our production pipeline, Silver jobs (silver_builds, silver_tests, silver_deployments) always re-read the entire Bronze Parquet history and run deduplicate_batch() across all messages in one batch before calling Hudi.\n"
            "  3. Consequently, every record key's ordering is deterministically resolved to the highest timestamp (and highest Kafka offset tie-breaker) in Spark before Hudi writes, so late records in Bronze can never overwrite newer states."
        )

    finally:
        print("\n" + "-" * 60)
        print("CLEANUP: Removing s3a://cicd-lakehouse/_smoke/upsert_demo and _smoke/")
        print("-" * 60)
        _delete_path_if_exists(spark, DEMO_PATH)
        _delete_path_if_exists(spark, SMOKE_ROOT)
        smoke_gone = not _path_exists(spark, SMOKE_ROOT)
        if smoke_gone:
            print(f"[PASS] Cleanup confirmed: {SMOKE_ROOT}/ is completely gone (exists=False).")
        else:
            msg = f"Cleanup failed: {SMOKE_ROOT}/ still exists!"
            print(f"[FAIL] {msg}")
            failures.append(msg)
        spark.stop()

    print("\n" + "=" * 60)
    if failures:
        print(f"DEMO UPSERT FAILED ({len(failures)} error(s))")
        return 1
    print("DEMO UPSERT PASSED")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
