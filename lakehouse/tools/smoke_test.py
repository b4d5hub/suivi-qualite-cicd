"""End-to-end smoke test for S3A Parquet, Kafka 4.4 batch reads, and Hudi 0.15 upserts."""

import sys

from config import KAFKA_BOOTSTRAP_SERVERS, LAKEHOUSE_BUCKET
from spark_session import get_spark

EXPECTED_KAFKA_COUNTS = {
    "builds": 7441,
    "tests": 22747,
    "deployments": 306,
}


def delete_s3a_path(spark, s3a_uri: str) -> None:
    """Delete a path in MinIO using Hadoop FileSystem API so no temporary test data remains."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    uri = jvm.java.net.URI(s3a_uri)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
    path = jvm.org.apache.hadoop.fs.Path(s3a_uri)
    if fs.exists(path):
        fs.delete(path, True)


def check_s3a_parquet(spark) -> bool:
    """(a) S3A round trip: write a tiny DataFrame as Parquet, read it back, compare, delete it."""
    target = f"s3a://{LAKEHOUSE_BUCKET}/_smoke/parquet/"
    try:
        data = [(1, "alpha"), (2, "beta")]
        df = spark.createDataFrame(data, ["id", "label"])
        df.write.mode("overwrite").parquet(target)

        read_back = [
            (row["id"], row["label"])
            for row in spark.read.parquet(target).orderBy("id").collect()
        ]
        delete_s3a_path(spark, target)

        if read_back == data:
            print(f"[PASS] (a) S3A Parquet round trip succeeded at {target}: {read_back}")
            return True
        print(f"[FAIL] (a) S3A Parquet mismatch: expected {data}, got {read_back}")
        return False
    except Exception as exc:
        print(f"[FAIL] (a) S3A Parquet error: {exc}")
        return False


def check_kafka_counts(spark) -> bool:
    """(b) Kafka batch read of builds, tests, deployments from earliest to latest offsets."""
    all_ok = True
    for topic, expected in EXPECTED_KAFKA_COUNTS.items():
        try:
            df = (
                spark.read.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load()
            )
            count = df.count()
            if count == expected:
                print(
                    f"[PASS] (b) Kafka topic '{topic}' count matches expected: "
                    f"{count} == {expected}"
                )
            else:
                print(
                    f"[FAIL] (b) Kafka topic '{topic}' count mismatch: "
                    f"got {count}, expected {expected}"
                )
                all_ok = False
        except Exception as exc:
            print(f"[FAIL] (b) Kafka topic '{topic}' batch read error: {exc}")
            all_ok = False
    return all_ok


def check_hudi_upsert(spark) -> bool:
    """(c) Hudi COPY_ON_WRITE upsert round trip: insert 3 rows, update 1 key, verify 3 rows."""
    target = f"s3a://{LAKEHOUSE_BUCKET}/_smoke/hudi_test/"
    smoke_root = f"s3a://{LAKEHOUSE_BUCKET}/_smoke/"
    hudi_options = {
        "hoodie.table.name": "hudi_smoke_test",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "id",
        "hoodie.datasource.write.precombine.field": "ts",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        "hoodie.upsert.shuffle.parallelism": "2",
        "hoodie.insert.shuffle.parallelism": "2",
    }
    try:
        delete_s3a_path(spark, smoke_root)

        initial_rows = [
            ("k1", "running", 100),
            ("k2", "success", 100),
            ("k3", "failure", 100),
        ]
        df_init = spark.createDataFrame(initial_rows, ["id", "state", "ts"])
        df_init.write.format("hudi").options(**hudi_options).mode("overwrite").save(target)

        # Upsert 1 existing key (k1) with a newer precombine value (ts=200)
        update_rows = [("k1", "completed", 200)]
        df_update = spark.createDataFrame(update_rows, ["id", "state", "ts"])
        df_update.write.format("hudi").options(**hudi_options).mode("append").save(target)

        result_df = spark.read.format("hudi").load(target)
        rows = {
            row["id"]: (row["state"], row["ts"])
            for row in result_df.select("id", "state", "ts").collect()
        }
        total_rows = len(rows)

        delete_s3a_path(spark, smoke_root)

        if total_rows == 3 and rows.get("k1") == ("completed", 200):
            print(
                f"[PASS] (c) Hudi COPY_ON_WRITE upsert succeeded: "
                f"total_rows={total_rows}, k1={rows.get('k1')}, all={rows}"
            )
            return True
        print(
            f"[FAIL] (c) Hudi upsert unexpected state: "
            f"total_rows={total_rows}, rows={rows}"
        )
        return False
    except Exception as exc:
        print(f"[FAIL] (c) Hudi upsert error: {exc}")
        return False


def main() -> None:
    print("=" * 60)
    print("LAKEHOUSE SMOKE TEST (S3A + KAFKA + HUDI)")
    print("=" * 60)
    spark = get_spark("lakehouse-smoke-test")
    spark.sparkContext.setLogLevel("WARN")

    try:
        ok_s3a = check_s3a_parquet(spark)
        ok_kafka = check_kafka_counts(spark)
        ok_hudi = check_hudi_upsert(spark)
    finally:
        spark.stop()

    print("=" * 60)
    if ok_s3a and ok_kafka and ok_hudi:
        print("ALL SMOKE TESTS PASSED")
        sys.exit(0)
    else:
        print("SMOKE TEST FAILED")
        sys.exit(1)


if __name__ == "__main__":
    main()
