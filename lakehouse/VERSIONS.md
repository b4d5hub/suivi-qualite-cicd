# Lakehouse Pinned Versions & Compatibility Matrix

All components in `lakehouse/` use pinned versions (never `latest`) so that Bronze, Silver, and downstream Gold (Kamal) jobs behave deterministically across team machines.

## Version Table

| Component | Pinned Version / Artifact / Tag | Why This Version |
| :--- | :--- | :--- |
| **Apache Spark** | `3.5.1` (`apache/spark:3.5.1-scala2.12-java17-python3-ubuntu`, verified public image: `558 MB` compressed / `1.61 GB` unpacked) | Official Apache Spark 3.5.1 image with Python 3, Scala 2.12, and Java 17; runs in lightweight `local[2]` mode (`--driver-memory 1g`) without custom image builds. |
| **Scala** | `2.12` (Verified in `apache/spark:3.5.1-scala2.12-java17-python3-ubuntu`) | Binary compatibility requirement for Spark 3.5.1 and `hudi-spark3.5-bundle_2.12`. |
| **Java (JRE/JDK)** | `17` (Verified in `apache/spark:3.5.1-scala2.12-java17-python3-ubuntu`) | Required runtime for Spark 3.5.1 and Hudi 0.15.0. |
| **Apache Hudi Bundle** | `org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0` (Verified via `smoke_test.py`) | Provides Hudi `COPY_ON_WRITE` tables and `upsert` support compiled specifically for Spark 3.5 + Scala 2.12. |
| **Spark Kafka Connector** | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` (Verified against `apache/kafka:4.4.0` via `smoke_test.py`) | Required for Spark batch/streaming reads from Kafka topics (`builds`, `tests`, `deployments`). |
| **Hadoop AWS (S3A)** | `org.apache.hadoop:hadoop-aws:3.3.4` (Verified via `smoke_test.py`) | Matches Hadoop 3.3.4 bundled inside Spark 3.5.1 so Spark can read/write `s3a://cicd-lakehouse/...` in MinIO without `NoSuchMethodError`. |
| **AWS Java SDK Bundle** | `com.amazonaws:aws-java-sdk-bundle:1.12.262` (Verified via `smoke_test.py`) | Exact S3 client dependency paired with `hadoop-aws:3.3.4`. |
| **MinIO Server Image** | `minio/minio:RELEASE.2024-01-16T16-07-38Z` (Verified local image; host ports `127.0.0.1:9100->9000` API and `127.0.0.1:9101->9001` Console to avoid conflict with unrelated `minio_prod` on `9000/9001`) | S3-compatible object storage hosting the `cicd-lakehouse` bucket for Bronze Parquet, Silver Hudi, and Spark checkpoints. |
| **MinIO Client (`mc`) Image** | Not needed (`sh -c "mkdir -p /data/cicd-lakehouse && exec minio server ..."` creates the bucket directly inside the MinIO container) | Saves RAM and disk by avoiding an extra init image/container; `/usr/bin/sh`, `/usr/bin/mkdir`, and `/usr/bin/mc` (`mc ready local` healthcheck) are verified inside the MinIO image. |
| **Python Runtime** | Python 3 bundled in `apache/spark:3.5.1-scala2.12-java17-python3-ubuntu` | All PySpark jobs and verification tools run inside the Spark container via `submit.sh`, avoiding the Windows host's Python 3.14 environment. |

## Known Risk (Resolved in Smoke Test)

- **Kafka Broker `4.4.0` vs. Spark `3.5.1` Kafka Client**: The upstream ingestion stack (`ingestion/compose.kafka.yml`) runs `apache/kafka:4.4.0`, whereas `spark-sql-kafka-0-10_2.12:3.5.1` embeds an older `kafka-clients` library. Compatibility was **verified in `tools/smoke_test.py`**, where Spark 3.5.1 successfully read all `7,441` (`builds`), `22,747` (`tests`), and `306` (`deployments`) records from `kafka:19092`.
