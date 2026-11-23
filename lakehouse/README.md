# Lakehouse Module — Bronze (MinIO Parquet) & Silver (Apache Hudi)

## Purpose

This folder (`lakehouse/`) implements the **Data Lake (Bronze)** and **Lakehouse (Silver)** layers of the CI/CD Quality Monitoring platform for `freeCodeCamp/freeCodeCamp` (owned by Mohamed):

1. **MinIO S3 Storage**: Hosts the `cicd-lakehouse` bucket for raw Bronze data, Silver Hudi tables, and Spark Structured Streaming checkpoints.
2. **Bronze Layer**: Uses Spark Structured Streaming (`startingOffsets=earliest`, `trigger(availableNow=True)`) to consume raw JSON messages from the 3 Kafka topics (`builds`, `tests`, `deployments`) and write append-only Parquet files with Kafka metadata.
3. **Silver Layer**: Uses PySpark batch jobs to parse Bronze JSON payloads, cast types, compute derived fields (`builds.duration_s` and `deployments.was_successful`), and **upsert** deduplicated records into 3 Apache Hudi (`COPY_ON_WRITE`, unpartitioned) tables.

---

## Planned Directory Layout

```text
lakehouse/
├── .env.example              # Safe development defaults for MinIO, Kafka, and bucket name
├── README.md                 # Architecture, data contracts, checklist, and run guide
├── VERSIONS.md               # Pinned versions for Spark, Hudi, Hadoop-AWS, MinIO, and known risks
├── compose.lakehouse.yml     # (Planned) Docker Compose for MinIO, bucket init, and Spark container
├── run_all.sh                # (Planned) Helper script to run Bronze + Silver + verification
├── src/                      # (Planned) PySpark jobs and shared modules
│   ├── config.py             # Environment settings and S3A/Hudi options
│   ├── spark_session.py      # SparkSession builder with Kryo, Hudi, and S3A config
│   ├── schemas.py            # Explicit PySpark StructType schemas for Kafka JSON payloads
│   ├── bronze_ingest.py      # Kafka -> MinIO Bronze Structured Streaming job
│   ├── silver_builds.py      # Bronze -> Hudi silver_builds upsert job
│   ├── silver_tests.py       # Bronze -> Hudi silver_tests upsert job
│   └── silver_deployments.py # Bronze -> Hudi silver_deployments upsert job
└── tools/                    # (Planned) Verification and diagnostic scripts
    └── verify_lakehouse.py   # Validates row counts and proves zero duplicates after re-runs
```

---

## Data Contract Summary

> **Note on Bronze Naming**: Project specifications (`cdc_v1__2_.docx`) refer to the logical Bronze tables as `bronze_builds`, `bronze_tests`, and `bronze_deployments`, while the physical object storage paths in MinIO are `s3a://cicd-lakehouse/bronze/builds/`, `s3a://cicd-lakehouse/bronze/tests/`, and `s3a://cicd-lakehouse/bronze/deployments/`.

### 1. Bronze Layer (Raw Append-Only Parquet)

- **Checkpoints**: `s3a://cicd-lakehouse/_checkpoints/bronze/<topic>/`
- **Columns (all 3 topics)**:
  `kafka_key` (`string`), `raw_json` (`string`), `kafka_topic` (`string`), `kafka_partition` (`int`), `kafka_offset` (`bigint`), `kafka_timestamp` (`timestamp`), `ingested_at` (`timestamp`).

| Logical Name | Kafka Topic | MinIO Path | Expected Rows (Snapshot) |
| :--- | :--- | :--- | ---: |
| `bronze_builds` | `builds` | `s3a://cicd-lakehouse/bronze/builds/` | `7,441` |
| `bronze_tests` | `tests` | `s3a://cicd-lakehouse/bronze/tests/` | `22,747` |
| `bronze_deployments` | `deployments` | `s3a://cicd-lakehouse/bronze/deployments/` | `306` |

### 2. Silver Layer (Apache Hudi `COPY_ON_WRITE`, Unpartitioned, `upsert`)

| Hudi Table | MinIO Path | Record Key | Precombine Field | Special Transformations | Expected Rows (Snapshot) |
| :--- | :--- | :--- | :--- | :--- | ---: |
| `silver_builds` | `s3a://cicd-lakehouse/silver/silver_builds` | `run_id,run_attempt` | `updated_at` | Cast ISO timestamps; compute `duration_s = updated_at - started_at` (seconds) when `status == 'completed'`. | `7,438` |
| `silver_tests` | `s3a://cicd-lakehouse/silver/silver_tests` | `job_id` | `event_ts` | Cast ISO timestamps; compute `event_ts = coalesce(completed_at, started_at)`. | `22,744` |
| `silver_deployments` | `s3a://cicd-lakehouse/silver/silver_deployments` | `deployment_id` | `event_ts` | Cast ISO timestamps; compute `event_ts = coalesce(latest_status_at, updated_at, created_at)` and boolean `was_successful` (`true` if any entry in `status_history` has `state == 'success'`). | `303` |

- **Deduplication Benchmark**: Key `44069120063:1` (`run_id=44069120063, run_attempt=1`) appears **4 times** in `bronze/builds/` and must appear **exactly once** in `silver_builds`.

---

## Task Checklist

- [x] Foundation of `lakehouse/` (`.gitignore`, folders, `VERSIONS.md`, `.env.example`, `README.md`)
- [x] MinIO service (`lakehouse-minio`) with automatic `cicd-lakehouse` bucket creation on `ingestion_default`
- [ ] Spark container in `compose.lakehouse.yml`
- [ ] Verify Kafka network and historical snapshot data (`30,494` records)
- [ ] Shared configuration and `SparkSession` builder (`config.py`, `spark_session.py`)
- [ ] Explicit PySpark JSON schemas (`schemas.py`)
- [ ] Kafka -> MinIO Bronze Structured Streaming job (`bronze_ingest.py`)
- [ ] Bronze -> Hudi `silver_builds` upsert job (`silver_builds.py`)
- [ ] Bronze -> Hudi `silver_tests` and `silver_deployments` upsert jobs (`silver_tests.py`, `silver_deployments.py`)
- [ ] End-to-end verification script (`verify_lakehouse.py`), idempotency proof, and final run documentation

---

## Start MinIO

With the Kafka stack's network (`ingestion_default`) already present, start the Lakehouse MinIO service from the repository root:

```powershell
docker compose -f lakehouse/compose.lakehouse.yml up -d minio
docker ps --filter "name=lakehouse-minio"
docker exec lakehouse-minio ls /data
Invoke-WebRequest -UseBasicParsing http://localhost:9100/minio/health/live
```

- **Internal Docker S3 Endpoint (for Spark)**: `http://minio:9000`
- **Host S3 API**: `http://localhost:9100` (mapped to `9100` to avoid conflicts with other projects using `9000`)
- **Host Web Console**: `http://localhost:9101`

---

## How to Run

*Placeholder — exact Docker Compose and `spark-submit` commands will be documented here in the final task once verified.*
