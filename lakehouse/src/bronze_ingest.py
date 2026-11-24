"""Bronze Structured Streaming ingestion: Kafka -> MinIO raw Parquet.

Reads each topic in KAFKA_TOPICS from Kafka (startingOffsets=earliest) using
Spark Structured Streaming with trigger(availableNow=True) and writes unmodified
raw messages to s3a://cicd-lakehouse/bronze/<topic>/ with a per-topic checkpoint
at s3a://cicd-lakehouse/_checkpoints/bronze/<topic>/.
"""

import sys

from pyspark.sql import functions as F

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS, bronze_path, checkpoint_path
from spark_session import get_spark


def ingest_topic(spark, topic: str) -> int:
    """Stream one topic from Kafka to MinIO Bronze Parquet and return total numInputRows."""
    target_path = bronze_path(topic)
    ckpt_path = checkpoint_path(f"bronze/{topic}")

    print("\n" + "-" * 60)
    print(f"BRONZE INGESTION: topic='{topic}'")
    print(f"  Target:     {target_path}")
    print(f"  Checkpoint: {ckpt_path}")
    print("-" * 60)

    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        # Select EXACTLY the 7 required Bronze columns in order, keeping raw_json untouched
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_json"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").cast("int").alias("kafka_partition"),
            F.col("offset").cast("long").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingested_at"),
        )
    )

    # Process topics sequentially (start -> awaitTermination -> next) rather than
    # running 3 concurrent streams so memory usage stays well below the 3.7 GiB host limit.
    query = (
        stream_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", target_path)
        .option("checkpointLocation", ckpt_path)
        .queryName(f"bronze_{topic}")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

    if query.exception() is not None:
        raise RuntimeError(f"Streaming query for '{topic}' failed: {query.exception()}")

    recent = query.recentProgress
    total_input_rows = sum(int(p.get("numInputRows", 0)) for p in recent)
    last_input_rows = int(recent[-1].get("numInputRows", 0)) if recent else 0

    print(
        f"[OK] Topic '{topic}' finished: batches={len(recent)}, "
        f"lastProgress.numInputRows={last_input_rows}, totalInputRows={total_input_rows}"
    )
    return total_input_rows


def main() -> None:
    print("=" * 60)
    print("LAKEHOUSE BRONZE INGESTION (KAFKA -> MINIO PARQUET)")
    print("=" * 60)
    spark = get_spark("lakehouse-bronze-ingest")
    spark.sparkContext.setLogLevel("WARN")

    summary = {}
    failed = False

    try:
        for topic in KAFKA_TOPICS:
            try:
                rows = ingest_topic(spark, topic)
                summary[topic] = rows
            except Exception as exc:
                print(f"[ERROR] Bronze ingestion failed for topic '{topic}': {exc}")
                summary[topic] = f"FAILED ({exc})"
                failed = True
                break
    finally:
        spark.stop()

    print("\n" + "=" * 60)
    print("BRONZE INGESTION SUMMARY")
    print("=" * 60)
    for topic in KAFKA_TOPICS:
        print(f"  {topic}: numInputRows={summary.get(topic, 'NOT_RUN')}")

    if failed:
        print("BRONZE INGESTION FAILED")
        sys.exit(1)

    print("BRONZE INGESTION COMPLETED SUCCESSFULLY")
    sys.exit(0)


if __name__ == "__main__":
    main()
