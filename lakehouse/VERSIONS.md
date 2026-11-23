# Lakehouse Pinned Versions & Compatibility Matrix

All components in `lakehouse/` use pinned versions (never `latest`) so that Bronze, Silver, and downstream Gold (Kamal) jobs behave deterministically across team machines.

## Version Table

| Component | Pinned Version / Artifact / Tag | Why This Version |
| :--- | :--- | :--- |
| **Apache Spark** | `3.5.x` (`apache/spark` exact tag: `TO VERIFY`) | Official Apache Spark 3.5 series required by Hudi 0.15.0; runs in lightweight `local[*]` mode without custom image builds. |
| **Scala** | `2.12` | Binary compatibility requirement for Spark 3.5 and `hudi-spark3.5-bundle_2.12`. |
| **Java (JRE/JDK)** | `11` or `17` (bundled in official Spark 3.5 image: `TO VERIFY`) | Required runtime for Spark 3.5 and Hudi 0.15.0. |
| **Apache Hudi Bundle** | `org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0` | Provides Hudi `COPY_ON_WRITE` tables and `upsert` support compiled specifically for Spark 3.5 + Scala 2.12. |
| **Spark Kafka Connector** | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.x` (exact patch matching Spark image: `TO VERIFY`) | Required for Spark Structured Streaming to consume from Kafka topics (`builds`, `tests`, `deployments`). |
| **Hadoop AWS (S3A)** | `org.apache.hadoop:hadoop-aws:3.3.4` | Matches Hadoop 3.3.4 bundled inside Spark 3.5 so Spark can read/write `s3a://cicd-lakehouse/...` in MinIO without `NoSuchMethodError`. |
| **AWS Java SDK Bundle** | `com.amazonaws:aws-java-sdk-bundle:1.12.262` | Exact S3 client dependency paired with `hadoop-aws:3.3.4`. |
| **MinIO Server Image** | `minio/minio:RELEASE.2024-01-16T16-07-38Z` (Verified local image; host ports `127.0.0.1:9100->9000` API and `127.0.0.1:9101->9001` Console to avoid conflict with unrelated `minio_prod` on `9000/9001`) | S3-compatible object storage hosting the `cicd-lakehouse` bucket for Bronze Parquet, Silver Hudi, and Spark checkpoints. |
| **MinIO Client (`mc`) Image** | Not needed (`sh -c "mkdir -p /data/cicd-lakehouse && exec minio server ..."` creates the bucket directly inside the MinIO container) | Saves RAM and disk by avoiding an extra init image/container; `/usr/bin/sh`, `/usr/bin/mkdir`, and `/usr/bin/mc` (`mc ready local` healthcheck) are verified inside the MinIO image. |
| **Python Image (for tools)** | `python` (exact `3.11-slim` / `3.12-slim` tag: `TO VERIFY`) | Host Windows machine uses Python 3.14; containerized Python ensures compatible wheels for `confluent-kafka` and tooling if needed. |

## Known Risk

- **Kafka Broker `4.4.0` vs. Spark `3.5` Kafka Client**: The upstream ingestion stack (`ingestion/compose.kafka.yml`) runs `apache/kafka:4.4.0`, whereas Spark 3.5's `spark-sql-kafka-0-10_2.12` embeds an older `kafka-clients` 3.4/3.5 library. Compatibility relies on Kafka broker API version negotiation; this combination is **NOT yet tested** in this repository and will be validated in the Bronze ingestion task.
