"""Read-only data-contract verification against real Kafka messages."""

import sys

from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS
from schemas import SCHEMAS, TIMESTAMP_FIELDS
from spark_session import get_spark

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

REQUIRED_FIELDS = {
    "builds": ["run_id", "run_attempt", "status"],
    "tests": ["job_id", "run_id", "status"],
    "deployments": ["deployment_id"],
}

ISO_UTC_REGEX = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"


def load_topic_cached(spark, topic: str):
    """Read a Kafka topic once (earliest -> latest), parse JSON, and cache in memory."""
    schema = SCHEMAS[topic]
    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_json"),
        )
        .withColumn("data", F.from_json(F.col("raw_json"), schema))
        .cache()
    )
    return df


def format_distribution(rows, col_name: str) -> str:
    """Format a list of (value, count) rows into a compact single-line string."""
    parts = [f"{row[col_name]}={row['count']}" for row in rows]
    return ", ".join(parts)


def check_topic(spark, topic: str) -> bool:
    """Run checks (a) through (i) for a single topic and return True if all PASS."""
    print("\n" + "-" * 60)
    print(f"TOPIC: {topic}")
    print("-" * 60)

    df = load_topic_cached(spark, topic)
    topic_ok = True

    try:
        # (a) Total row count
        total_rows = df.count()
        exp_rows = EXPECTED_TOTAL_ROWS[topic]
        if total_rows == exp_rows:
            print(f"[PASS] (a) Row count for '{topic}': {total_rows} == {exp_rows}")
        else:
            print(f"[FAIL] (a) Row count for '{topic}': got {total_rows}, expected {exp_rows}")
            topic_ok = False

        # (b) Rows whose JSON fails to parse (null struct)
        parse_failures = df.filter(F.col("data").isNull()).count()
        if parse_failures == 0:
            print(f"[PASS] (b) Unparseable JSON rows (null struct) in '{topic}': 0")
        else:
            print(f"[FAIL] (b) Unparseable JSON rows in '{topic}': {parse_failures}")
            topic_ok = False

        # (c) Distinct Kafka keys (and 44069120063:1 count == 4 in builds)
        distinct_keys = df.select("kafka_key").distinct().count()
        exp_keys = EXPECTED_DISTINCT_KEYS[topic]
        if distinct_keys == exp_keys:
            print(f"[PASS] (c) Distinct Kafka keys in '{topic}': {distinct_keys} == {exp_keys}")
        else:
            print(
                f"[FAIL] (c) Distinct Kafka keys in '{topic}': "
                f"got {distinct_keys}, expected {exp_keys}"
            )
            topic_ok = False

        if topic == "builds":
            sample_key = "44069120063:1"
            sample_count = df.filter(F.col("kafka_key") == sample_key).count()
            if sample_count == 4:
                print(f"[PASS] (c) Benchmark key '{sample_key}' appears {sample_count} times in 'builds'")
            else:
                print(
                    f"[FAIL] (c) Benchmark key '{sample_key}' count in 'builds': "
                    f"got {sample_count}, expected 4"
                )
                topic_ok = False

        # (d) Kafka key consistency with payload IDs
        if topic == "builds":
            expected_key_expr = F.concat_ws(":", F.col("data.run_id"), F.col("data.run_attempt"))
        elif topic == "tests":
            expected_key_expr = F.col("data.job_id").cast("string")
        else:
            expected_key_expr = F.col("data.deployment_id").cast("string")

        key_mismatches = df.filter(
            F.col("kafka_key").isNull()
            | expected_key_expr.isNull()
            | (F.col("kafka_key") != expected_key_expr)
        ).count()
        if key_mismatches == 0:
            print(f"[PASS] (d) Kafka key consistency in '{topic}': 0 mismatches")
        else:
            print(f"[FAIL] (d) Kafka key consistency in '{topic}': {key_mismatches} mismatches")
            topic_ok = False

        # (e) Unexpected or missing JSON fields across ALL rows
        schema_fields = set(SCHEMAS[topic].fieldNames())
        observed_keys_rows = (
            df.select(
                F.explode(
                    F.map_keys(F.from_json(F.col("raw_json"), MapType(StringType(), StringType())))
                ).alias("json_key")
            )
            .distinct()
            .collect()
        )
        observed_fields = {row["json_key"] for row in observed_keys_rows}
        extra_in_data = sorted(observed_fields - schema_fields)
        missing_in_data = sorted(schema_fields - observed_fields)
        print(
            f"[INFO] (e) '{topic}' field comparison -> "
            f"extra_in_data={extra_in_data}, never_present_in_json_keys={missing_in_data}"
        )
        if not extra_in_data and not missing_in_data:
            print(f"[PASS] (e) JSON keys in '{topic}' match schema fields 100% ({len(schema_fields)} fields)")
        else:
            print(
                f"[FAIL] (e) Schema field mismatch in '{topic}': "
                f"extra={extra_in_data}, missing={missing_in_data}"
            )
            topic_ok = False

        # Validate nested status_history keys for deployments as well
        if topic == "deployments":
            nested_schema_fields = {"status_id", "state", "created_at"}
            nested_raw_schema = "struct<status_history:array<map<string,string>>>"
            nested_keys_rows = (
                df.select(
                    F.explode(
                        F.from_json(F.col("raw_json"), nested_raw_schema).getField("status_history")
                    ).alias("status_map")
                )
                .select(F.explode(F.map_keys(F.col("status_map"))).alias("nkey"))
                .distinct()
                .collect()
            )
            observed_nested = {row["nkey"] for row in nested_keys_rows}
            extra_nested = sorted(observed_nested - nested_schema_fields)
            missing_nested = sorted(nested_schema_fields - observed_nested)
            print(
                f"[INFO] (e) 'deployments.status_history' nested field comparison -> "
                f"extra={extra_nested}, missing={missing_nested}"
            )
            if extra_nested or missing_nested:
                topic_ok = False

        # (f) Timestamp format validation (^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$)
        ts_fields = TIMESTAMP_FIELDS[topic]
        ts_agg_exprs = []
        for ts_col in ts_fields:
            col_ref = F.col(f"data.{ts_col}")
            ts_agg_exprs.append(
                F.sum(
                    F.when(
                        col_ref.isNotNull() & (~col_ref.rlike(ISO_UTC_REGEX)),
                        1,
                    ).otherwise(0)
                ).alias(f"{ts_col}_violations")
            )
            ts_agg_exprs.append(
                F.sum(F.when(col_ref.isNull(), 1).otherwise(0)).alias(f"{ts_col}_nulls")
            )

        ts_row = df.agg(*ts_agg_exprs).collect()[0]
        total_ts_violations = 0
        ts_details = []
        for ts_col in ts_fields:
            v_count = int(ts_row[f"{ts_col}_violations"] or 0)
            n_count = int(ts_row[f"{ts_col}_nulls"] or 0)
            total_ts_violations += v_count
            ts_details.append(f"{ts_col}(violations={v_count}, nulls={n_count})")

        print(f"[INFO] (f) '{topic}' timestamp check details: {', '.join(ts_details)}")
        if total_ts_violations == 0:
            print(f"[PASS] (f) All non-null TIMESTAMP_FIELDS in '{topic}' match ISO-8601 UTC format")
        else:
            print(f"[FAIL] (f) Timestamp format violations in '{topic}': {total_ts_violations}")
            topic_ok = False

        # (g) Required-field null counts
        req_fields = REQUIRED_FIELDS[topic]
        req_exprs = [
            F.sum(F.when(F.col(f"data.{rf}").isNull(), 1).otherwise(0)).alias(rf)
            for rf in req_fields
        ]
        req_row = df.agg(*req_exprs).collect()[0]
        req_counts = {rf: int(req_row[rf] or 0) for rf in req_fields}
        print(f"[INFO] (g) '{topic}' required-field null counts: {req_counts}")
        if all(c == 0 for c in req_counts.values()):
            print(f"[PASS] (g) Required fields in '{topic}' have 0 nulls")
        else:
            print(f"[FAIL] (g) Required fields in '{topic}' contain nulls: {req_counts}")
            topic_ok = False

        # (h) INFO value distributions
        if topic == "builds":
            dist_all = (
                df.groupBy(F.col("data.conclusion").alias("conclusion"))
                .count()
                .orderBy(F.desc("count"))
                .collect()
            )
            dist_dedup = (
                df.dropDuplicates(["kafka_key"])
                .groupBy(F.col("data.conclusion").alias("conclusion"))
                .count()
                .orderBy(F.desc("count"))
                .collect()
            )
            print(
                f"[INFO] (h) 'builds' conclusion (all {total_rows} Kafka records): "
                f"{format_distribution(dist_all, 'conclusion')}"
            )
            print(
                f"[INFO] (h) 'builds' conclusion (distinct {distinct_keys} keys): "
                f"{format_distribution(dist_dedup, 'conclusion')}"
            )
            status_dist = (
                df.groupBy(F.col("data.status").alias("status"))
                .count()
                .orderBy(F.desc("count"))
                .collect()
            )
            print(
                f"[INFO] (h) 'builds' status (all records): "
                f"{format_distribution(status_dist, 'status')}"
            )

        elif topic == "tests":
            dist_all = (
                df.groupBy(F.col("data.conclusion").alias("conclusion"))
                .count()
                .orderBy(F.desc("count"))
                .collect()
            )
            dist_dedup = (
                df.dropDuplicates(["kafka_key"])
                .groupBy(F.col("data.conclusion").alias("conclusion"))
                .count()
                .orderBy(F.desc("count"))
                .collect()
            )
            print(
                f"[INFO] (h) 'tests' conclusion (all {total_rows} Kafka records): "
                f"{format_distribution(dist_all, 'conclusion')}"
            )
            print(
                f"[INFO] (h) 'tests' conclusion (distinct {distinct_keys} keys): "
                f"{format_distribution(dist_dedup, 'conclusion')}"
            )

        elif topic == "deployments":
            status_all = (
                df.groupBy(F.col("data.latest_status").alias("latest_status"))
                .count()
                .orderBy(F.desc("count"))
                .collect()
            )
            status_dedup = (
                df.dropDuplicates(["kafka_key"])
                .groupBy(F.col("data.latest_status").alias("latest_status"))
                .count()
                .orderBy(F.desc("count"))
                .collect()
            )
            env_all = (
                df.groupBy(F.col("data.environment").alias("environment"))
                .count()
                .orderBy(F.desc("count"))
                .collect()
            )
            env_dedup = (
                df.dropDuplicates(["kafka_key"])
                .groupBy(F.col("data.environment").alias("environment"))
                .count()
                .orderBy(F.desc("count"))
                .collect()
            )
            prod_dist = (
                df.groupBy(F.col("data.production_environment").alias("production_environment"))
                .count()
                .collect()
            )
            was_succ = (
                df.dropDuplicates(["kafka_key"])
                .withColumn(
                    "was_successful",
                    F.expr("array_contains(transform(data.status_history, x -> x.state), 'success')"),
                )
                .groupBy("was_successful")
                .count()
                .orderBy(F.desc("count"))
                .collect()
            )
            print(
                f"[INFO] (h) 'deployments' latest_status (all {total_rows} records): "
                f"{format_distribution(status_all, 'latest_status')}"
            )
            print(
                f"[INFO] (h) 'deployments' latest_status (distinct {distinct_keys} keys): "
                f"{format_distribution(status_dedup, 'latest_status')}"
            )
            print(
                f"[INFO] (h) 'deployments' environment (all {total_rows} records): "
                f"{format_distribution(env_all, 'environment')}"
            )
            print(
                f"[INFO] (h) 'deployments' environment (distinct {distinct_keys} keys): "
                f"{format_distribution(env_dedup, 'environment')}"
            )
            print(
                f"[INFO] (h) 'deployments' production_environment (all records): "
                f"{format_distribution(prod_dist, 'production_environment')}"
            )
            print(
                f"[INFO] (h) 'deployments' derived was_successful (distinct {distinct_keys} keys): "
                f"{format_distribution(was_succ, 'was_successful')}"
            )

        # (i) INFO null counts of duration_s in builds and tests
        if topic in ("builds", "tests"):
            dur_row = df.agg(
                F.sum(F.when(F.col("data.duration_s").isNull(), 1).otherwise(0)).alias("null_count"),
                F.sum(F.when(F.col("data.duration_s").isNotNull(), 1).otherwise(0)).alias("non_null_count"),
            ).collect()[0]
            print(
                f"[INFO] (i) '{topic}' duration_s -> "
                f"nulls={dur_row['null_count']}/{total_rows}, "
                f"non_nulls={dur_row['non_null_count']}/{total_rows}"
            )

    finally:
        df.unpersist()

    return topic_ok


def main() -> None:
    print("=" * 60)
    print("LAKEHOUSE DATA-CONTRACT VERIFICATION (READ-ONLY)")
    print("=" * 60)
    spark = get_spark("lakehouse-check-contract")
    spark.sparkContext.setLogLevel("WARN")

    all_passed = True
    try:
        for topic in KAFKA_TOPICS:
            if not check_topic(spark, topic):
                all_passed = False
    finally:
        spark.stop()

    print("\n" + "=" * 60)
    if all_passed:
        print("ALL DATA-CONTRACT CHECKS PASSED")
        sys.exit(0)
    else:
        print("DATA-CONTRACT CHECKS FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
