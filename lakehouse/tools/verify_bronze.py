"""Read-only verification of the MinIO Bronze Parquet tables against Kafka."""

import sys

from pyspark.sql import functions as F

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS, bronze_path
from schemas import SCHEMAS
from spark_session import get_spark

EXPECTED_COLUMNS = [
    "kafka_key",
    "raw_json",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
]

EXPECTED_TOTAL_ROWS = {
    "builds": 7441,
    "tests": 22747,
    "deployments": 306,
}

EXPECTED_DISTINCT_KEYS = {
    "builds": 7438,
    "tests": 22744,
    "deployments": 303,
}


def get_folder_size_bytes(spark, s3a_uri: str) -> int:
    """Return total size in bytes of an S3A folder using Hadoop FileSystem ContentSummary."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    uri = jvm.java.net.URI(s3a_uri)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
    path = jvm.org.apache.hadoop.fs.Path(s3a_uri)
    if not fs.exists(path):
        return 0
    return int(fs.getContentSummary(path).getLength())


def verify_topic(spark, topic: str) -> bool:
    """Run Bronze checks (a) through (h) for a single topic."""
    path = bronze_path(topic)
    print("\n" + "-" * 60)
    print(f"VERIFY BRONZE TOPIC: '{topic}' ({path})")
    print("-" * 60)

    size_bytes = get_folder_size_bytes(spark, path)
    print(
        f"[INFO] Bronze folder size for '{topic}': "
        f"{size_bytes:,} bytes ({size_bytes / (1024 * 1024):.2f} MiB)"
    )

    bronze_df = spark.read.parquet(path).cache()
    topic_ok = True

    try:
        # (a) Exact column names and order
        actual_cols = bronze_df.columns
        if actual_cols == EXPECTED_COLUMNS:
            print(f"[PASS] (a) Columns match exact order: {actual_cols}")
        else:
            print(f"[FAIL] (a) Columns mismatch: got {actual_cols}, expected {EXPECTED_COLUMNS}")
            topic_ok = False

        # (b) Total row count
        total_rows = bronze_df.count()
        exp_rows = EXPECTED_TOTAL_ROWS[topic]
        if total_rows == exp_rows:
            print(f"[PASS] (b) Row count for '{topic}': {total_rows} == {exp_rows}")
        else:
            print(f"[FAIL] (b) Row count for '{topic}': got {total_rows}, expected {exp_rows}")
            topic_ok = False

        # (c) Distinct kafka_key count
        distinct_keys = bronze_df.select("kafka_key").distinct().count()
        exp_keys = EXPECTED_DISTINCT_KEYS[topic]
        if distinct_keys == exp_keys:
            print(f"[PASS] (c) Distinct kafka_key in '{topic}': {distinct_keys} == {exp_keys}")
        else:
            print(
                f"[FAIL] (c) Distinct kafka_key in '{topic}': "
                f"got {distinct_keys}, expected {exp_keys}"
            )
            topic_ok = False

        # (d) Benchmark key 44069120063:1 count == 4 in builds
        if topic == "builds":
            benchmark_key = "44069120063:1"
            b_count = bronze_df.filter(F.col("kafka_key") == benchmark_key).count()
            if b_count == 4:
                print(
                    f"[PASS] (d) Benchmark key '{benchmark_key}' appears "
                    f"exactly {b_count} times in bronze builds"
                )
            else:
                print(
                    f"[FAIL] (d) Benchmark key '{benchmark_key}' count in bronze builds: "
                    f"got {b_count}, expected 4"
                )
                topic_ok = False

        # (e) No null in kafka_key, raw_json, kafka_offset
        null_row = bronze_df.agg(
            F.sum(F.when(F.col("kafka_key").isNull(), 1).otherwise(0)).alias("null_key"),
            F.sum(F.when(F.col("raw_json").isNull(), 1).otherwise(0)).alias("null_json"),
            F.sum(F.when(F.col("kafka_offset").isNull(), 1).otherwise(0)).alias("null_offset"),
        ).collect()[0]
        null_counts = {
            "kafka_key": int(null_row["null_key"] or 0),
            "raw_json": int(null_row["null_json"] or 0),
            "kafka_offset": int(null_row["null_offset"] or 0),
        }
        if all(v == 0 for v in null_counts.values()):
            print(f"[PASS] (e) Zero nulls in kafka_key, raw_json, kafka_offset: {null_counts}")
        else:
            print(f"[FAIL] (e) Unexpected nulls in '{topic}': {null_counts}")
            topic_ok = False

        # (f) Offsets per partition: distinct == count, min == 0, max == count - 1
        part_rows = (
            bronze_df.groupBy("kafka_partition")
            .agg(
                F.count("*").alias("cnt"),
                F.countDistinct("kafka_offset").alias("dist_cnt"),
                F.min("kafka_offset").alias("min_off"),
                F.max("kafka_offset").alias("max_off"),
            )
            .orderBy("kafka_partition")
            .collect()
        )
        offsets_ok = len(part_rows) > 0
        part_summaries = []
        for pr in part_rows:
            p_id = pr["kafka_partition"]
            cnt = pr["cnt"]
            dist_cnt = pr["dist_cnt"]
            min_off = pr["min_off"]
            max_off = pr["max_off"]
            part_summaries.append(
                f"p{p_id}(count={cnt}, distinct={dist_cnt}, min={min_off}, max={max_off})"
            )
            if not (dist_cnt == cnt and min_off == 0 and max_off == cnt - 1):
                offsets_ok = False
        if offsets_ok:
            print(f"[PASS] (f) Contiguous offsets with no gaps or duplicates: {', '.join(part_summaries)}")
        else:
            print(f"[FAIL] (f) Offset gap or duplicate in '{topic}': {', '.join(part_summaries)}")
            topic_ok = False

        # (g) RAW PROOF: batch-read Kafka topic and compare on (kafka_partition, kafka_offset)
        kafka_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
            .select(
                F.col("partition").cast("int").alias("k_partition"),
                F.col("offset").cast("long").alias("k_offset"),
                F.col("key").cast("string").alias("k_key"),
                F.col("value").cast("string").alias("k_raw_json"),
            )
        )

        joined = bronze_df.select(
            "kafka_partition", "kafka_offset", "kafka_key", "raw_json"
        ).join(
            kafka_df,
            (F.col("kafka_partition") == F.col("k_partition"))
            & (F.col("kafka_offset") == F.col("k_offset")),
            "full_outer",
        )

        diff_row = joined.agg(
            F.sum(F.when(F.col("k_offset").isNull(), 1).otherwise(0)).alias("missing_in_kafka"),
            F.sum(F.when(F.col("kafka_offset").isNull(), 1).otherwise(0)).alias("missing_in_bronze"),
            F.sum(
                F.when(
                    F.col("kafka_offset").isNotNull()
                    & F.col("k_offset").isNotNull()
                    & (
                        (F.col("kafka_key") != F.col("k_key"))
                        | (F.col("raw_json") != F.col("k_raw_json"))
                    ),
                    1,
                ).otherwise(0)
            ).alias("payload_diff"),
        ).collect()[0]

        missing_in_kafka = int(diff_row["missing_in_kafka"] or 0)
        missing_in_bronze = int(diff_row["missing_in_bronze"] or 0)
        payload_diff = int(diff_row["payload_diff"] or 0)

        if missing_in_kafka == 0 and missing_in_bronze == 0 and payload_diff == 0:
            print(
                f"[PASS] (g) RAW PROOF: 100% byte-identical match with Kafka "
                f"(missing_in_bronze=0, missing_in_kafka=0, key_or_json_diff=0)"
            )
        else:
            print(
                f"[FAIL] (g) RAW PROOF mismatch in '{topic}': "
                f"missing_in_bronze={missing_in_bronze}, "
                f"missing_in_kafka={missing_in_kafka}, "
                f"key_or_json_diff={payload_diff}"
            )
            topic_ok = False

        # (h) raw_json parses with schemas.py with 0 null structs
        schema = SCHEMAS[topic]
        null_structs = (
            bronze_df.select(F.from_json(F.col("raw_json"), schema).alias("parsed"))
            .filter(F.col("parsed").isNull())
            .count()
        )
        if null_structs == 0:
            print(f"[PASS] (h) All {total_rows} Bronze raw_json records parse with SCHEMAS['{topic}'] (0 null structs)")
        else:
            print(f"[FAIL] (h) {null_structs} Bronze rows failed schema parsing in '{topic}'")
            topic_ok = False

    finally:
        bronze_df.unpersist()

    return topic_ok


def main() -> None:
    print("=" * 60)
    print("LAKEHOUSE BRONZE VERIFICATION (READ-ONLY)")
    print("=" * 60)
    spark = get_spark("lakehouse-verify-bronze")
    spark.sparkContext.setLogLevel("WARN")

    all_passed = True
    try:
        for topic in KAFKA_TOPICS:
            if not verify_topic(spark, topic):
                all_passed = False
    finally:
        spark.stop()

    print("\n" + "=" * 60)
    if all_passed:
        print("ALL BRONZE VERIFICATION CHECKS PASSED")
        sys.exit(0)
    else:
        print("BRONZE VERIFICATION FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
