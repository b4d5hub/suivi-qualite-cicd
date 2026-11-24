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
├── compose.lakehouse.yml     # Docker Compose for MinIO (cicd-lakehouse) and Spark 3.5.1 container
├── submit.sh                 # Central spark-submit wrapper with Maven packages and local[2] settings
├── run_all.sh                # (Planned) Helper script to run Bronze + Silver + verification
├── src/                      # PySpark jobs and shared modules
│   ├── config.py             # Environment settings and S3A/checkpoint/table path helpers
│   ├── spark_session.py      # SparkSession builder with Kryo, Hudi 0.15, and MinIO S3A config
│   ├── schemas.py            # Explicit PySpark StructType schemas for Kafka JSON payloads
│   ├── bronze_ingest.py      # Kafka -> MinIO Bronze Structured Streaming job
│   ├── silver_common.py      # Shared UTC timestamp cast, batch deduplication, and Hudi options
│   ├── silver_builds.py      # Bronze -> Hudi silver_builds upsert job
│   ├── silver_tests.py       # Bronze -> Hudi silver_tests upsert job
│   └── silver_deployments.py # (Planned) Bronze -> Hudi silver_deployments upsert job
└── tools/                    # Verification and diagnostic scripts
    ├── smoke_test.py         # Verifies S3A Parquet round trip, Kafka counts, and Hudi COW upsert
    ├── check_contract.py     # Verifies Kafka data contract (schemas, keys, nulls, timestamps, distributions)
    ├── verify_bronze.py      # Verifies Bronze Parquet tables, contiguous offsets, and byte-identical Kafka raw proof
    ├── verify_silver.py      # Verifies Silver Hudi tables (schemas, counts, deduplication, equivalence with Bronze)
    └── verify_lakehouse.py   # (Planned) Validates row counts and proves zero duplicates after re-runs
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
- [x] Spark container (`apache/spark:3.5.1-scala2.12-java17-python3-ubuntu`) and `submit.sh` in `compose.lakehouse.yml`
- [x] Verify Kafka network and historical snapshot data (`7,441` / `22,747` / `306` records via `tools/smoke_test.py`)
- [x] Shared configuration and `SparkSession` builder (`config.py`, `spark_session.py`)
- [x] Explicit PySpark JSON schemas and data-contract check (`schemas.py`, `tools/check_contract.py`)
- [x] Kafka -> MinIO Bronze Structured Streaming job and verification (`src/bronze_ingest.py`, `tools/verify_bronze.py`)
- [x] Bronze -> Hudi `silver_builds` upsert job and verification (`src/silver_builds.py`, `tools/verify_silver.py silver_builds`)
- [x] Bronze -> Hudi `silver_tests` upsert job and verification (`src/silver_tests.py`, `tools/verify_silver.py silver_tests`)
- [ ] Bronze -> Hudi `silver_deployments` upsert job (`silver_deployments.py`)
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

## Run Smoke Test (S3A + Kafka + Hudi)

Run the one-shot Spark container against MinIO (`http://minio:9000`) and Kafka (`kafka:19092`):

```powershell
docker compose -f lakehouse/compose.lakehouse.yml run --rm spark tools/smoke_test.py
```

---

## Run Data-Contract Verification (Read-Only on Kafka)

Validate all 30,494 Kafka messages against `src/schemas.py`:

```powershell
docker compose -f lakehouse/compose.lakehouse.yml run --rm spark tools/check_contract.py
```

### Data Contract Findings (Verified on Snapshot)

- **100% Schema & Key Match**: Every message across `builds` (`7,441`), `tests` (`22,747`), and `deployments` (`306`) parses cleanly with `0` null structs, `0` key mismatches, `0` extra/missing JSON fields, and `0` timestamp format violations (`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$`).
- **Duplicate Keys in Snapshot**: There are exactly 3 extra records per topic (`7,441 -> 7,438`, `22,747 -> 22,744`, `306 -> 303`). In `builds`, key `44069120063:1` appears `4` times (accounting for the 3 duplicates).
- **`builds` Conclusions & `duration_s`**:
  - Across all `7,441` records: `success=5,860`, `failure=808`, `cancelled=561`, `action_required=207`, `startup_failure=5` (after deduplicating to `7,438` keys: `success=5,857`). All `7,441` records have `status='completed'`.
  - `builds.duration_s` is `null` in all `7,441 / 7,441` rows (`100%` null) and must be computed in Silver from `updated_at - started_at`.
- **`tests` Conclusions & `duration_s`**:
  - Across all `22,747` records: `success=17,493`, `skipped=3,654`, `cancelled=951`, `failure=649` (after deduplicating to `22,744` keys: `success=17,490`).
  - `tests.duration_s` is `null` in `3,794 / 22,747` rows (`3,654` skipped jobs + `140` cancelled jobs that never started or had a zero/missing duration) and populated in `18,953` rows.
- **`deployments` Environments & `was_successful`**:
  - `production_environment` is `False` for all `306 / 306` records; environment filtering must use `environment` (`stg-clients=144`, `prd-clients=134`, `stg-api=13`, `prd-api=12` distinct keys; `prd-api=15` in raw records).
  - `latest_status` is mostly `inactive=262` (`success=42` raw / `39` distinct, `failure=2`), whereas deriving `was_successful` from `status_history` recovers `True=301` successful deployments and `False=2` failed deployments out of `303` distinct deployments.

---

## Run Bronze Ingestion & Verification (Kafka -> MinIO Parquet)

Stream all 3 topics (`builds`, `tests`, `deployments`) sequentially from Kafka (`startingOffsets=earliest`, `trigger(availableNow=True)`) into `s3a://cicd-lakehouse/bronze/<topic>/` with per-topic checkpoints at `s3a://cicd-lakehouse/_checkpoints/bronze/<topic>/`:

```powershell
# 1. Run Bronze Structured Streaming ingestion
docker compose -f lakehouse/compose.lakehouse.yml run --rm spark src/bronze_ingest.py

# 2. Run Bronze verification (columns, counts, offsets, byte-identical Kafka raw proof, schema parse)
docker compose -f lakehouse/compose.lakehouse.yml run --rm spark tools/verify_bronze.py

# 3. Inspect Bronze directories and size in MinIO
docker exec lakehouse-minio ls /data/cicd-lakehouse/bronze
docker exec lakehouse-minio du -sh /data/cicd-lakehouse
```

### Bronze Reset Procedure (Documentation Only — Do Not Run Unnecessarily)

Because the Bronze layer is **append-only Parquet** and Spark Structured Streaming tracks consumed Kafka offsets in its checkpoint folder (`s3a://cicd-lakehouse/_checkpoints/bronze/<topic>/`), **a Bronze data folder and its checkpoint folder are a linked pair**:

- If you delete only `_checkpoints/bronze/<topic>/` while leaving `bronze/<topic>/` in place, the next run of `src/bronze_ingest.py` will start from `earliest` again and **append duplicate rows** into Bronze (which Silver Hudi upserts would later deduplicate, but Bronze row counts would double).
- If you delete only `bronze/<topic>/` while leaving `_checkpoints/bronze/<topic>/` in place, Spark will think all offsets were already ingested and write `0` rows.
- **Correct way to reset a Bronze topic (if ever needed)**: Always remove **both** the data directory and the checkpoint directory together before re-running `src/bronze_ingest.py`:
  ```powershell
  # Example only (do NOT run during normal operation):
  # docker exec lakehouse-minio rm -rf /data/cicd-lakehouse/bronze/<topic> /data/cicd-lakehouse/_checkpoints/bronze/<topic>
  ```

---

## Run Silver `silver_builds` Job & Verification (Bronze -> Hudi COPY_ON_WRITE)

Read `s3a://cicd-lakehouse/bronze/builds/`, cast UTC timestamps, compute approximate `duration_s`, deduplicate by `(run_id, run_attempt)` ordering by `(updated_at DESC, source_kafka_offset DESC)`, and upsert into `s3a://cicd-lakehouse/silver/silver_builds`:

```powershell
# 1. Run silver_builds Hudi upsert job
docker compose -f lakehouse/compose.lakehouse.yml run --rm spark src/silver_builds.py

# 2. Verify silver_builds (schema, 7,438 rows, key 44069120063:1, conclusion distribution, Bronze equivalence)
docker compose -f lakehouse/compose.lakehouse.yml run --rm spark tools/verify_silver.py silver_builds

# 3. Inspect Hudi table properties in MinIO (note: MinIO xl.meta stores objects inside a folder)
docker exec lakehouse-minio ls /data/cicd-lakehouse/silver
docker exec lakehouse-minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null && mc cat local/cicd-lakehouse/silver/silver_builds/.hoodie/hoodie.properties"
```

### Silver Contract: `silver_builds` (Handover Reference for Kamal — Gold / MLlib)

- **MinIO S3A Path**: `s3a://cicd-lakehouse/silver/silver_builds`
- **Hudi Table Configuration**:
  - `hoodie.table.name`: `silver_builds`
  - `hoodie.datasource.write.table.type`: `COPY_ON_WRITE` (pure Parquet base files; read with `spark.read.format("hudi").load("s3a://cicd-lakehouse/silver/silver_builds")`)
  - `hoodie.datasource.write.operation`: `upsert`
  - `hoodie.datasource.write.recordkey.field`: `run_id,run_attempt` (composite key using `org.apache.hudi.keygen.NonpartitionedKeyGenerator` so the table is unpartitioned while supporting a 2-column primary key)
  - `hoodie.datasource.write.precombine.field`: `updated_at`
  - `hoodie.metadata.enable`: `false` (disabled to keep memory and small-file overhead low on 3.7 GiB RAM machines)
  - `spark.sql.session.timeZone`: `UTC` (enforced in `spark_session.py`)

| # | Column | Spark Type | Nullable | Description / Contract Notes |
| :- | :--- | :--- | :--- | :--- |
| 1 | `run_id` | `LongType` | No | GitHub Actions workflow run ID (part 1 of composite record key). |
| 2 | `run_attempt` | `IntegerType` | No | Attempt number (`1, 2, 3, ...`) for this workflow run (part 2 of composite record key; `195` runs in the snapshot have `>1` attempt stored as distinct rows). |
| 3 | `workflow` | `StringType` | Yes | Workflow name (e.g., `"CI - Node.js"`). |
| 4 | `event` | `StringType` | Yes | GitHub trigger event (`"push"`, `"pull_request"`, `"pull_request_target"`, `"schedule"`, etc.). |
| 5 | `branch` | `StringType` | Yes | Target/head git branch name (`head_branch`). |
| 6 | `head_sha` | `StringType` | Yes | Commit SHA tested by the run. |
| 7 | `status` | `StringType` | No | Execution state (`"completed"`, `"in_progress"`, `"queued"`). |
| 8 | `conclusion` | `StringType` | Yes | Final outcome when `status == 'completed'`: `"success"` (`5,857`), `"failure"` (`808`), `"cancelled"` (`561`), `"action_required"` (`207`), `"startup_failure"` (`5`). |
| 9 | `created_at` | `TimestampType` | Yes | UTC timestamp when the workflow run was created. |
| 10 | `started_at` | `TimestampType` | Yes | UTC timestamp when the run attempt started (`run_started_at`). |
| 11 | `updated_at` | `TimestampType` | No | UTC timestamp of the last update to the run (Hudi `precombine` field). |
| 12 | `duration_s` | `LongType` | Yes | **Approximate** run duration in seconds: `unix_timestamp(updated_at) - unix_timestamp(started_at)` when `status == 'completed'` and difference $\ge 0$ (`null` otherwise). Since GitHub's workflow run object does not provide an explicit `completed_at` timestamp, `updated_at` is used as an approximation (a re-run or late status update can yield a large span). |
| 13 | `source_kafka_partition` | `IntegerType` | Yes | Traceability: Kafka partition (`0`) of the winning Bronze message. |
| 14 | `source_kafka_offset` | `LongType` | Yes | Traceability: Kafka offset of the winning Bronze message. |

> [!WARNING]
> **Important Notes for Gold Success-Rate, Duration & MLlib Queries (Kamal)**:
> 1. **Conclusions**: All `7,438` rows in `silver_builds` have `status = 'completed'`, **but not all completed runs are successes or failures**. Specifically, `conclusion` can be `'cancelled'` (`561` rows), `'action_required'` (`207` rows), or `'startup_failure'` (`5` rows). Per the Cahier des Charges (Section 8: `Taux de réussite = Runs réussis / (runs réussis + runs en échec)`), **do NOT count `'cancelled'`, `'action_required'`, or `'startup_failure'` as either `success` or `failure`** when computing build success rates or training binary classification labels (`conclusion IN ('success', 'failure')`).
> 2. **Extreme Outliers in `duration_s` (Use the Median / Filter Outliers)**: Because GitHub's workflow run object does not provide an explicit `completed_at` timestamp, `duration_s = updated_at - started_at` reflects the last time the run object was touched on GitHub (for example, when old artifacts expire or a run is re-evaluated weeks later). In the snapshot, **p50 (median) is `61s` (~1 min)** and **p90 is `2,139s` (~35.6 min)**, while **p99 is `83,540s`** (`75` runs exceed 24 hours, up to ~30 days / `2,595,846s`, yielding a skewed mean of `12,587.64s`). Use the **median (`percentile_approx(duration_s, 0.5)`)** or cap/filter extreme outliers (`duration_s <= 86400`), or aggregate job durations from `silver_tests`.

---

## Run Silver `silver_tests` Job & Verification (Bronze -> Hudi COPY_ON_WRITE)

Read `s3a://cicd-lakehouse/bronze/tests/`, cast UTC timestamps, keep `duration_s` untouched as computed by the collector, compute `event_ts = coalesce(completed_at, started_at)`, deduplicate by `job_id` ordering by `(event_ts DESC, source_kafka_offset DESC)`, and upsert into `s3a://cicd-lakehouse/silver/silver_tests`:

```powershell
# 1. Run silver_tests Hudi upsert job
docker compose -f lakehouse/compose.lakehouse.yml run --rm spark src/silver_tests.py

# 2. Verify silver_tests (schema, 22,744 rows, job_id 133990467008, duration consistency, Bronze equivalence, silver_builds integrity)
docker compose -f lakehouse/compose.lakehouse.yml run --rm spark tools/verify_silver.py silver_tests

# 3. Inspect Hudi table properties in MinIO
docker exec lakehouse-minio ls /data/cicd-lakehouse/silver
docker exec lakehouse-minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null && mc cat local/cicd-lakehouse/silver/silver_tests/.hoodie/hoodie.properties"
```

### Silver Contract: `silver_tests` (Handover Reference for Kamal — Flaky Jobs & Gold)

- **What a Row Represents**: Each row in `silver_tests` is a **GitHub Actions Job** (e.g., `'Build E2E API (Container)'`, `'Test - i18n'`, `'Lint (24)'`, `'triage'`), **not an individual unit test case**. There are `73` distinct `job_name` values across `22,744` jobs, and `100%` of `(run_id, run_attempt)` pairs (`0` orphans) join cleanly to `silver_builds`.
- **MinIO S3A Path**: `s3a://cicd-lakehouse/silver/silver_tests`
- **Hudi Table Configuration**:
  - `hoodie.table.name`: `silver_tests`
  - `hoodie.datasource.write.table.type`: `COPY_ON_WRITE`
  - `hoodie.datasource.write.operation`: `upsert`
  - `hoodie.datasource.write.recordkey.field`: `job_id` (using `org.apache.hudi.keygen.NonpartitionedKeyGenerator`)
  - `hoodie.datasource.write.precombine.field`: `event_ts` (`coalesce(completed_at, started_at)`)
  - `hoodie.metadata.enable`: `false`

| # | Column | Spark Type | Nullable | Description / Contract Notes |
| :- | :--- | :--- | :--- | :--- |
| 1 | `job_id` | `LongType` | No | GitHub Actions job ID (Hudi `recordkey`; `22,744` distinct rows). |
| 2 | `run_id` | `LongType` | No | Foreign key to `silver_builds.run_id`. |
| 3 | `run_attempt` | `IntegerType` | No | Foreign key to `silver_builds.run_attempt` (`0` orphan rows when joining on `run_id, run_attempt`). |
| 4 | `head_sha` | `StringType` | Yes | Commit SHA tested by this job (used for flaky-job detection across runs on the same commit). |
| 5 | `job_name` | `StringType` | Yes | Name of the GitHub Actions job (`73` distinct names). |
| 6 | `status` | `StringType` | No | Execution state (`"completed"`, `"in_progress"`). |
| 7 | `conclusion` | `StringType` | Yes | Final outcome: `"success"` (`17,490`), `"skipped"` (`3,654`), `"cancelled"` (`951`), `"failure"` (`649`). |
| 8 | `started_at` | `TimestampType` | Yes | UTC timestamp when the job started. |
| 9 | `completed_at` | `TimestampType` | Yes | UTC timestamp when the job completed. |
| 10 | `duration_s` | `LongType` | Yes | Exact job duration in seconds (`completed_at - started_at`) delivered by the collector (`18,950` non-null rows, `3,794` null rows, `0` negative values). |
| 11 | `event_ts` | `TimestampType` | No | `coalesce(completed_at, started_at)` — non-null ordering timestamp used as Hudi `precombine` field. |
| 12 | `source_kafka_partition` | `IntegerType` | Yes | Traceability: Kafka partition (`0`) of the winning Bronze message. |
| 13 | `source_kafka_offset` | `LongType` | Yes | Traceability: Kafka offset of the winning Bronze message. |

> [!NOTE]
> **How to Handle `NULL` `duration_s` in `silver_tests` (`3,794` rows)**:
> `duration_s` is kept exactly as computed by Badr's collector (`transform.py`) and is `NULL` in three exact cases verified on the snapshot:
> 1. `status = 'completed'` and `conclusion = 'skipped'` (**`3,654` rows**): skipped jobs never ran (`transform.py` intentionally sets `duration_s = None`).
> 2. `status = 'completed'` and `conclusion = 'cancelled'` (**`138` rows**): aborted jobs where GitHub recorded `completed_at` 1 second before `started_at` (`completed_at < started_at`, clock skew on instant cancellation), which `transform.py` rejected (`duration < 0 -> None`).
> 3. `status = 'in_progress'` and `conclusion = 'success'` (**`2` rows**): jobs captured while `status` had not yet flipped to `'completed'`.
> When computing average/total job execution time in Gold, filter with `WHERE duration_s IS NOT NULL` (or `WHERE status = 'completed' AND conclusion IN ('success', 'failure')`).

---

## How to Run

*Placeholder — exact Docker Compose and `spark-submit` commands will be documented here in the final task once verified.*
