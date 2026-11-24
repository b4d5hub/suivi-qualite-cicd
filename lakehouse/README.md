# Lakehouse Module — Bronze (MinIO Parquet) & Silver (Apache Hudi)

## 1. Overview & Architecture

This folder (`lakehouse/`, owned by **Mohamed**) implements the **Data Lake (Bronze)** and **Lakehouse (Silver)** layers of the CI/CD Quality Monitoring platform for `freeCodeCamp/freeCodeCamp`:

1. **MinIO S3 Storage (`lakehouse-minio`)**: Hosts the `cicd-lakehouse` S3 bucket for raw Bronze Parquet data, Silver Apache Hudi tables, and Spark Structured Streaming checkpoints.
2. **Bronze Layer (`s3a://cicd-lakehouse/bronze/<topic>/`)**: Uses Spark Structured Streaming (`startingOffsets=earliest`, `trigger(availableNow=True)`) to consume raw JSON messages from the 3 Kafka topics (`builds`, `tests`, `deployments`) and write append-only Parquet files preserving the untouched `raw_json` payload and Kafka metadata.
3. **Silver Layer (`s3a://cicd-lakehouse/silver/<table>`)**: Uses PySpark batch jobs to parse Bronze JSON payloads against explicit `StructType` schemas, cast ISO-8601 UTC timestamps, compute derived fields (`builds.duration_s`, `tests.event_ts`, `deployments.was_successful`, `deployments.first_success_at`, `deployments.is_prd_environment`, `deployments.event_ts`), deterministically deduplicate by record key, and **upsert** into 3 Apache Hudi (`COPY_ON_WRITE`, unpartitioned) tables.

```text
+-----------------------------------------------------------------------------------+
|                        Kafka Broker (ingestion-kafka-1)                           |
|               kafka:19092 on Docker network: ingestion_default                    |
|     Topics:  builds (7,441 msgs)  |  tests (22,747 msgs)  |  deployments (306)    |
+-----------------------------------+-----------------------------------------------+
                                    |
                                    | Spark Structured Streaming (availableNow=True)
                                    | src/bronze_ingest.py
                                    v
+-----------------------------------------------------------------------------------+
|             MinIO Bucket: s3a://cicd-lakehouse (lakehouse-minio:9000)             |
|                                                                                   |
|  1. Checkpoints (offset tracking paired with Bronze folders):                     |
|     - s3a://cicd-lakehouse/_checkpoints/bronze/{builds,tests,deployments}/        |
|                                                                                   |
|  2. Bronze Layer (Append-Only Raw Parquet, 7 columns, byte-identical raw_json):   |
|     - s3a://cicd-lakehouse/bronze/builds/             (7,441 rows, 7,438 keys)    |
|     - s3a://cicd-lakehouse/bronze/tests/              (22,747 rows, 22,744 keys)  |
|     - s3a://cicd-lakehouse/bronze/deployments/        (306 rows, 303 keys)        |
|                                   |                                               |
|                                   | PySpark Batch + Hudi COPY_ON_WRITE Upsert     |
|                                   | src/silver_{builds,tests,deployments}.py      |
|                                   v                                               |
|  3. Silver Layer (Apache Hudi 0.15.0, COPY_ON_WRITE, unpartitioned, deduplicated):|
|     - s3a://cicd-lakehouse/silver/silver_builds       (7,438 rows, 14 columns)    |
|         key: (run_id, run_attempt) | precombine: updated_at                       |
|     - s3a://cicd-lakehouse/silver/silver_tests        (22,744 rows, 13 columns)   |
|         key: job_id                | precombine: event_ts                         |
|     - s3a://cicd-lakehouse/silver/silver_deployments  (303 rows, 16 columns)      |
|         key: deployment_id         | precombine: event_ts                         |
+-----------------------------------+-----------------------------------------------+
                                    |
                                    v
             Downstream Consumers: Gold Analytics & MLlib (Kamal),
                 Airflow Orchestration (Youssef), Dashboard
```

### Directory Layout

```text
lakehouse/
├── .env.example              # Safe development defaults for MinIO, Kafka, and bucket name
├── .gitattributes            # Enforces LF line endings on *.sh files for Linux containers
├── README.md                 # Architecture, quick start, command reference, data contracts, handover
├── VERSIONS.md               # Pinned versions for Spark, Hudi, Hadoop-AWS, MinIO, and known risks
├── compose.lakehouse.yml     # Docker Compose for MinIO (cicd-lakehouse) and Spark 3.5.1 container
├── submit.sh                 # Central spark-submit wrapper with Maven packages and local[2] settings
├── run_all.sh                # One-command pipeline runner (Bronze -> Silver -> Verification)
├── src/                      # PySpark jobs and shared modules
│   ├── config.py             # Environment settings and S3A/checkpoint/table path helpers
│   ├── spark_session.py      # SparkSession builder with UTC, Kryo, Hudi 0.15, and MinIO S3A config
│   ├── schemas.py            # Explicit PySpark StructType schemas for Kafka JSON payloads
│   ├── bronze_ingest.py      # Kafka -> MinIO Bronze Structured Streaming job
│   ├── silver_common.py      # Shared UTC timestamp cast, batch deduplication, and Hudi options
│   ├── silver_builds.py      # Bronze -> Hudi silver_builds upsert job
│   ├── silver_tests.py       # Bronze -> Hudi silver_tests upsert job
│   └── silver_deployments.py # Bronze -> Hudi silver_deployments upsert job
└── tools/                    # Verification and diagnostic scripts
    ├── smoke_test.py         # Verifies S3A Parquet round trip, Kafka counts, and Hudi COW upsert
    ├── check_contract.py     # Verifies Kafka data contract (schemas, keys, nulls, timestamps, distributions)
    ├── verify_bronze.py      # Verifies Bronze Parquet tables, contiguous offsets, and byte-identical Kafka raw proof
    ├── verify_silver.py      # Verifies Silver Hudi tables (schemas, counts, deduplication, equivalence with Bronze)
    └── demo_upsert.py        # Self-contained synthetic Hudi upsert test inside _smoke/upsert_demo/
```

### Task Checklist

- [x] Foundation of `lakehouse/` (`.gitignore`, `.gitattributes`, folders, `VERSIONS.md`, `.env.example`, `README.md`)
- [x] MinIO service (`lakehouse-minio`) with automatic `cicd-lakehouse` bucket creation on `ingestion_default`
- [x] Spark container (`apache/spark:3.5.1-scala2.12-java17-python3-ubuntu`) and `submit.sh` in `compose.lakehouse.yml`
- [x] Verify Kafka network and historical snapshot data (`7,441` / `22,747` / `306` records via `tools/smoke_test.py`)
- [x] Shared configuration and `SparkSession` builder (`config.py`, `spark_session.py`)
- [x] Explicit PySpark JSON schemas and data-contract check (`schemas.py`, `tools/check_contract.py`)
- [x] Kafka -> MinIO Bronze Structured Streaming job and verification (`src/bronze_ingest.py`, `tools/verify_bronze.py`)
- [x] Bronze -> Hudi `silver_builds` upsert job and verification (`src/silver_builds.py`, `tools/verify_silver.py silver_builds`)
- [x] Bronze -> Hudi `silver_tests` upsert job and verification (`src/silver_tests.py`, `tools/verify_silver.py silver_tests`)
- [x] Bronze -> Hudi `silver_deployments` upsert job and verification (`src/silver_deployments.py`, `tools/verify_silver.py silver_deployments`)
- [x] End-to-end pipeline script (`run_all.sh`), synthetic upsert proof (`tools/demo_upsert.py`), idempotency verification, and final handover documentation

---

## 2. Prerequisites & Tested Machine Resources

- **OS & Shell**: Windows 10/11 with PowerShell (or Linux/macOS bash) and Docker Desktop (WSL2 backend).
- **Resource Envelope (Verified on a Constrained Laptop)**:
  - **Available Docker RAM**: `~3.7 GiB` total (`lakehouse-minio` capped at `512m`, uses `~184 MiB`; `lakehouse-spark` capped at `2g` with `1g` driver heap; `ingestion-kafka-1` uses `~410 MiB`).
  - **Disk Footprint**:
    - Docker image `apache/spark:3.5.1-scala2.12-java17-python3-ubuntu`: `~1.05 GB`.
    - Docker image `minio/minio:RELEASE.2024-01-16T16-07-38Z`: `~151 MB`.
    - Cached Maven JARs in Docker volume `lakehouse_ivy_cache`: `~130 MB` (downloaded once on first run).
    - Complete MinIO bucket `/data/cicd-lakehouse` (all 3 Bronze tables + checkpoints + all 3 Silver Hudi tables): **`~11 MB`**.
- **Port Mappings**:
  - `lakehouse-minio` binds to **`127.0.0.1:9100` (S3 API)** and **`127.0.0.1:9101` (Web Console)** on the host so it never collides with other local services using `9000/9001`. Inside the Docker network (`ingestion_default`), Spark connects to **`http://minio:9000`**.

---

## 3. Quick Start from Zero (Copy-Paste Commands)

Run all commands from the **repository root** (`suivi-qualite-cicd/`):

### Step 1 — Start Kafka and Replay the Historical Snapshot (if not already seeded)

If starting from a fresh clone where the `ingestion_kafka_data` Docker volume is empty or has root-owned permissions, initialize the volume ownership for `appuser`, start Kafka, wait for topic leaders, and replay the JSONL snapshot:

```powershell
# 1. Start the Kafka container (creates the ingestion_default network and volume)
docker compose -f ingestion/compose.kafka.yml up -d kafka

# 2. If starting from a brand-new volume on Windows/WSL2, ensure /var/lib/kafka/data is owned by appuser
docker compose -f ingestion/compose.kafka.yml exec -T -u root kafka chown -R appuser:appuser /var/lib/kafka/data

# 3. Replay the historical snapshot (builds=7,441, tests=22,747, deployments=306)
python ingestion/tools/kafka_history.py replay

# 4. Wait a few seconds and confirm all 3 topic offsets are ready (7441 / 22747 / 306)
docker compose -f ingestion/compose.kafka.yml exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:19092 --topic builds
docker compose -f ingestion/compose.kafka.yml exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:19092 --topic tests
docker compose -f ingestion/compose.kafka.yml exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:19092 --topic deployments
```

### Step 2 — Start Lakehouse MinIO

```powershell
docker compose -f lakehouse/compose.lakehouse.yml up -d minio
docker ps --filter "name=lakehouse-minio"
Invoke-WebRequest -UseBasicParsing http://localhost:9100/minio/health/live
```

### Step 3 — Run the Entire Lakehouse Pipeline & Verifications in One Command

```powershell
docker compose -f lakehouse/compose.lakehouse.yml run --rm --entrypoint bash spark /opt/lakehouse/run_all.sh
```

---

## 4. Command Reference

All Spark jobs and verification tools run inside the one-shot `spark` service (`profiles: ["tools"]`, `--rm` removes the container immediately after exit):

| Action | Host Command (from Repository Root) | Expected Result |
| :--- | :--- | :--- |
| **Start MinIO** | `docker compose -f lakehouse/compose.lakehouse.yml up -d minio` | `lakehouse-minio` Up (`healthy`), `/data/cicd-lakehouse` created |
| **Full Pipeline + Verify** | `docker compose -f lakehouse/compose.lakehouse.yml run --rm --entrypoint bash spark /opt/lakehouse/run_all.sh` | Runs `bronze_ingest`, `silver_builds`, `silver_tests`, `silver_deployments`, `verify_bronze`, `verify_silver` (`PASS`) |
| **Pipeline (Skip Verify)** | `docker compose -f lakehouse/compose.lakehouse.yml run --rm --entrypoint bash spark /opt/lakehouse/run_all.sh --skip-verify` | Runs only the 4 ingestion/upsert steps |
| **Run Single Step via `run_all.sh`** | `docker compose -f lakehouse/compose.lakehouse.yml run --rm --entrypoint bash spark /opt/lakehouse/run_all.sh --only <step>` | Runs one step (`bronze_ingest`, `silver_builds`, `silver_tests`, `silver_deployments`, `verify_bronze`, `verify_silver`) |
| **Bronze Ingestion Only** | `docker compose -f lakehouse/compose.lakehouse.yml run --rm spark src/bronze_ingest.py` | Streams `builds`, `tests`, `deployments` from Kafka to Bronze Parquet |
| **Silver Builds Only** | `docker compose -f lakehouse/compose.lakehouse.yml run --rm spark src/silver_builds.py` | Upserts `7,438` rows into `s3a://cicd-lakehouse/silver/silver_builds` |
| **Silver Tests Only** | `docker compose -f lakehouse/compose.lakehouse.yml run --rm spark src/silver_tests.py` | Upserts `22,744` rows into `s3a://cicd-lakehouse/silver/silver_tests` |
| **Silver Deployments Only** | `docker compose -f lakehouse/compose.lakehouse.yml run --rm spark src/silver_deployments.py` | Upserts `303` rows into `s3a://cicd-lakehouse/silver/silver_deployments` |
| **Verify Bronze** | `docker compose -f lakehouse/compose.lakehouse.yml run --rm spark tools/verify_bronze.py` | Verifies columns, row counts, contiguous offsets, byte-identical Kafka match |
| **Verify All Silver Tables** | `docker compose -f lakehouse/compose.lakehouse.yml run --rm spark tools/verify_silver.py silver_builds silver_tests silver_deployments` | Verifies schemas, row counts, zero duplicates, distributions, Bronze equivalence |
| **Verify One Silver Table** | `docker compose -f lakehouse/compose.lakehouse.yml run --rm spark tools/verify_silver.py <table_name>` | Verifies `silver_builds`, `silver_tests`, or `silver_deployments` |
| **Check Kafka Data Contract** | `docker compose -f lakehouse/compose.lakehouse.yml run --rm spark tools/check_contract.py` | Read-only check of all `30,494` Kafka messages against `src/schemas.py` |
| **Synthetic Upsert Demo** | `docker compose -f lakehouse/compose.lakehouse.yml run --rm spark tools/demo_upsert.py` | Tests Hudi in-place update & late-record behavior in `_smoke/upsert_demo/` |
| **Environment Smoke Test** | `docker compose -f lakehouse/compose.lakehouse.yml run --rm spark tools/smoke_test.py` | Tests S3A Parquet round trip, Kafka connectivity, and Hudi COW upsert |

---

## 5. Data Layers & Contracts

> **Note on Bronze Naming**: Project specifications (`cdc_v1__2_.docx`) refer to the logical Bronze tables as `bronze_builds`, `bronze_tests`, and `bronze_deployments`, while the physical object storage paths in MinIO are `s3a://cicd-lakehouse/bronze/builds/`, `s3a://cicd-lakehouse/bronze/tests/`, and `s3a://cicd-lakehouse/bronze/deployments/`.

### 5.1 Bronze Layer (Raw Append-Only Parquet)

- **Checkpoints**: `s3a://cicd-lakehouse/_checkpoints/bronze/<topic>/`
- **Columns (exact order across all 3 topics)**:
  1. `kafka_key` (`StringType`) — message key (`"{run_id}:{run_attempt}"`, `"{job_id}"`, or `"{deployment_id}"`)
  2. `raw_json` (`StringType`) — untouched UTF-8 JSON string from Kafka (`value` cast to string without parsing or re-serializing)
  3. `kafka_topic` (`StringType`) — `"builds"`, `"tests"`, or `"deployments"`
  4. `kafka_partition` (`IntegerType`) — Kafka partition (`0`)
  5. `kafka_offset` (`LongType`) — Kafka offset (`0 .. N-1`)
  6. `kafka_timestamp` (`TimestampType`) — Kafka message timestamp (UTC)
  7. `ingested_at` (`TimestampType`) — Spark ingestion UTC timestamp (`current_timestamp()`)

| Logical Name | Kafka Topic | MinIO Path | Total Rows | Distinct Keys | Benchmark Duplicate Key (4x in Bronze -> 1x in Silver) |
| :--- | :--- | :--- | ---: | ---: | :--- |
| `bronze_builds` | `builds` | `s3a://cicd-lakehouse/bronze/builds/` | `7,441` | `7,438` | `44069120063:1` (offsets `3253, 3254, 3255, 3256`) |
| `bronze_tests` | `tests` | `s3a://cicd-lakehouse/bronze/tests/` | `22,747` | `22,744` | `133990467008` (offsets `9951, 9952, 9953, 9954`) |
| `bronze_deployments` | `deployments` | `s3a://cicd-lakehouse/bronze/deployments/` | `306` | `303` | `7769301245` (offsets `0, 1, 2, 3`) |

#### Bronze Reset Procedure (Documentation Only — Do Not Run Unnecessarily)

Because the Bronze layer is **append-only Parquet** and Spark Structured Streaming tracks consumed Kafka offsets in its checkpoint folder (`s3a://cicd-lakehouse/_checkpoints/bronze/<topic>/`), **a Bronze data folder and its checkpoint folder are a linked pair**:

- If you delete only `_checkpoints/bronze/<topic>/` while leaving `bronze/<topic>/` in place, the next run of `src/bronze_ingest.py` will start from `earliest` again and **append duplicate rows** into Bronze (which Silver Hudi upserts would later deduplicate, but Bronze row counts would double).
- If you delete only `bronze/<topic>/` while leaving `_checkpoints/bronze/<topic>/` in place, Spark will think all offsets were already ingested and write `0` rows.
- **Correct way to reset a Bronze topic (if ever needed)**: Always remove **both** the data directory and the checkpoint directory together before re-running `src/bronze_ingest.py`:
  ```powershell
  # Example only (do NOT run during normal operation):
  # docker exec lakehouse-minio rm -rf /data/cicd-lakehouse/bronze/<topic> /data/cicd-lakehouse/_checkpoints/bronze/<topic>
  ```

---

### 5.2 Silver Table 1: `silver_builds` (Hudi `COPY_ON_WRITE`, `7,438` rows)

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

### 5.3 Silver Table 2: `silver_tests` (Hudi `COPY_ON_WRITE`, `22,744` rows)

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

### 5.4 Silver Table 3: `silver_deployments` (Hudi `COPY_ON_WRITE`, `303` rows)

- **MinIO S3A Path**: `s3a://cicd-lakehouse/silver/silver_deployments`
- **Hudi Table Configuration**:
  - `hoodie.table.name`: `silver_deployments`
  - `hoodie.datasource.write.table.type`: `COPY_ON_WRITE`
  - `hoodie.datasource.write.operation`: `upsert`
  - `hoodie.datasource.write.recordkey.field`: `deployment_id` (using `org.apache.hudi.keygen.NonpartitionedKeyGenerator`)
  - `hoodie.datasource.write.precombine.field`: `event_ts` (`coalesce(latest_status_at, updated_at, created_at)`)
  - `hoodie.metadata.enable`: `false`

| # | Column | Spark Type | Nullable | Description / Contract Notes |
| :- | :--- | :--- | :--- | :--- |
| 1 | `deployment_id` | `LongType` | No | GitHub deployment ID (Hudi `recordkey`; `303` distinct rows). |
| 2 | `environment` | `StringType` | No | Target deployment environment: `'stg-clients'` (`144`), `'prd-clients'` (`134`), `'stg-api'` (`13`), `'prd-api'` (`12`). |
| 3 | `production_environment` | `BooleanType` | Yes | Raw GitHub flag preserved as delivered (`False` for all `303` rows — see warning below). |
| 4 | `ref` | `StringType` | Yes | Git ref deployed (e.g., branch or tag). |
| 5 | `sha` | `StringType` | Yes | 40-char lowercase hex commit SHA deployed (`0` nulls, `13` distinct commits across the `303` deployments). |
| 6 | `created_at` | `TimestampType` | No | UTC timestamp when the deployment record was created. |
| 7 | `updated_at` | `TimestampType` | Yes | UTC timestamp when the deployment record was last updated. |
| 8 | `latest_status` | `StringType` | Yes | State of the most recent status entry: `'inactive'` (`262`), `'success'` (`39`), `'failure'` (`2`). |
| 9 | `latest_status_at` | `TimestampType` | Yes | UTC timestamp of the most recent status entry. |
| 10 | `status_history` | `ArrayType(StructType(status_id: LongType, state: StringType, created_at: TimestampType))` | Yes | Full chronological status history array (4 to 8 elements per deployment, `created_at` cast to UTC `TimestampType`). |
| 11 | `was_successful` | `BooleanType` | No | `True` if **any** entry in `status_history` has `state = 'success'` (`301` `True`, `2` `False`). |
| 12 | `first_success_at` | `TimestampType` | Yes | Earliest `created_at` among `status_history` entries with `state = 'success'` (`null` when `was_successful = False`). |
| 13 | `is_prd_environment` | `BooleanType` | No | `True` when `environment` starts with `'prd-'` (`146` production deployments: `134` `prd-clients` + `12` `prd-api`, of which `144` have `was_successful = True`). |
| 14 | `event_ts` | `TimestampType` | No | `coalesce(latest_status_at, updated_at, created_at)` — non-null ordering timestamp used as Hudi `precombine` field. |
| 15 | `source_kafka_partition` | `IntegerType` | Yes | Traceability: Kafka partition (`0`) of the winning Bronze message. |
| 16 | `source_kafka_offset` | `LongType` | Yes | Traceability: Kafka offset of the winning Bronze message. |

> [!WARNING]
> **Critical DORA Warnings for Gold Analytics (Kamal)**:
> 1. **`latest_status = 'inactive'` does NOT mean a failed deployment**: GitHub automatically marks a previous deployment as `'inactive'` (`262 / 303` rows) as soon as a newer deployment supersedes it. Relying on `latest_status = 'success'` would only count `39` deployments instead of `301`. Always filter successful deployments with **`was_successful = True`** and use **`first_success_at`** as the actual timestamp when the deployment succeeded.
> 2. **`production_environment` is always `False`**: In `freeCodeCamp/freeCodeCamp`, the GitHub API field `production_environment` is `False` on all `303` rows. Never filter on `production_environment = True`; instead use **`is_prd_environment = True`** (`environment IN ('prd-clients', 'prd-api')`, an assumption based on the `prd-` / `stg-` naming prefix to be confirmed by the team). For `is_prd_environment = True AND was_successful = True`, there are **`144` successful production deployments** between `2026-09-21 06:44:26` and `2026-11-13 17:20:30` (`2026-09`: `27`, `2026-10`: `65`, `2026-11`: `52`).
> 3. **DORA Metrics Scope & Limitations in Current Data**:
>    - **Deployment Frequency**: Fully computable from `silver_deployments` using `is_prd_environment = True AND was_successful = True` grouped by `first_success_at`.
>    - **Lead Time for Changes**: Requires the commit authorship/merge timestamp (`GET /repos/{owner}/{repo}/commits/{sha}`), which is **not** present in the Kafka messages today (unless approximated by joining `sha` to `silver_builds.head_sha`).
>    - **Change Failure Rate & Time to Restore Service (MTTR)**: Cannot be reliably computed from this dataset (only `2` failed deployments exist across the entire snapshot, and incidents/rollbacks are not tracked via GitHub deployment statuses) and must be reported as **"not available"** per the team guide.

---

## 6. What Each Verification Tool Proves

1. **`tools/smoke_test.py`**:
   - Proves that the Spark container can read/write Parquet via S3A (`s3a://cicd-lakehouse/_smoke/`), connect to Kafka (`kafka:19092`) and read all 3 topic counts (`7,441 / 22,747 / 306`), and perform a Hudi `COPY_ON_WRITE` upsert before cleaning up `_smoke/`.
2. **`tools/check_contract.py`**:
   - Read-only check against live Kafka messages proving `100%` JSON schema compliance (`0` null structs, `0` unexpected/missing JSON keys, `0` key mismatches, `0` ISO-8601 UTC format violations) and reporting exact raw vs. distinct key counts (`7,441 -> 7,438`, `22,747 -> 22,744`, `306 -> 303`).
3. **`tools/verify_bronze.py`**:
   - Proves that `s3a://cicd-lakehouse/bronze/{builds,tests,deployments}/` have the exact 7 required columns, exact row and distinct key counts, contiguous Kafka offsets `0 .. N-1` with zero gaps or duplicates, and **100% byte-identical `(kafka_partition, kafka_offset, kafka_key, raw_json)` equivalence** against a fresh read from Kafka.
4. **`tools/verify_silver.py`**:
   - Proves that `silver_builds` (`7,438`), `silver_tests` (`22,744`), and `silver_deployments` (`303`) contain all 5 Hudi metadata columns (`_hoodie_commit_time`, `_hoodie_record_key`, etc.), exact data schemas, zero duplicate primary keys, exact benchmark key deduplication (`44069120063:1`, `133990467008`, `7769301245` reduced from 4 Bronze rows to 1 Silver row at the highest offset), exact status/conclusion/environment distributions, `0` orphan foreign keys between `silver_tests` and `silver_builds`, and **100% row-by-row equivalence** with an independent deduplication of Bronze.
5. **`tools/demo_upsert.py`**:
   - Self-contained synthetic test in `s3a://cicd-lakehouse/_smoke/upsert_demo/` proving that Hudi updates an existing record key `(999000001, 1)` in place from `status='in_progress'` (`T1`) to `status='completed', conclusion='success'` (`T2 > T1`) while keeping the total row count at `2`, documents late-record (`T0 < T1`) behavior across commits, and verifies complete cleanup of `_smoke/`.

---

## 7. Design Decisions & Rationale

1. **`COPY_ON_WRITE` and Unpartitioned Hudi Tables (`NonpartitionedKeyGenerator`)**:
   - Chosen because the dataset has ~30.5k rows across ~2 months (~11 MB total in MinIO). Unpartitioned `COPY_ON_WRITE` avoids small-file proliferation and Compaction overhead while writing pure Parquet base files that downstream Spark jobs can query rapidly. `NonpartitionedKeyGenerator` in Hudi 0.15 natively supports both single keys (`job_id`, `deployment_id`) and composite keys (`run_id,run_attempt`).
2. **Composite Primary Key `(run_id, run_attempt)` for `silver_builds`**:
   - GitHub reuses the same `run_id` when a workflow run is re-executed, incrementing `run_attempt` (`1, 2, 3, ...`). In the snapshot, `7,438` build records share `7,237` distinct `run_id` values (`195` runs have multiple attempts). Keying on `run_id,run_attempt` preserves every distinct attempt.
3. **Deterministic Batch Deduplication Before Hudi Write (`deduplicate_batch`)**:
   - All three Silver jobs re-read the full Bronze table and deduplicate in Spark using `Window.partitionBy(key).orderBy(precombine.desc(), source_kafka_offset.desc())` before calling Hudi. This guarantees deterministic tie-breaking when two Kafka messages have identical timestamps (such as the 4 benchmark duplicates) and ensures that out-of-order/late records in Bronze are resolved before Hudi writes.
   - **Why pre-deduplicating full Bronze in Spark matters (from `tools/demo_upsert.py` Batch 3)**: In `tools/demo_upsert.py` Batch 3, a late record (older `updated_at`) written in a **separate Hudi commit** overwrote the newer row (observed: `status` went back to `'queued'`). The Hudi `precombine` field only protects within one write batch. Our pipeline is safe today because every Silver job re-reads **all** Bronze rows and keeps the latest row per key in Spark before writing. (Warning: switching Silver to an incremental read of new Bronze rows only would require configuring the Hudi payload/ordering behavior and re-testing with `tools/demo_upsert.py` Batch 3; this was **not** tested.)
4. **Sequential Streaming & `local[2]` Memory Profile**:
   - Running one topic/table at a time with `--master local[2]`, `--driver-memory 1g`, `spark.sql.shuffle.partitions=4`, and `hoodie.metadata.enable=false` keeps the entire Spark container comfortably inside a `2 GiB` memory limit on laptops with `~3.7 GiB` available RAM.
5. **Enforced UTC Session Timezone (`spark.sql.session.timeZone=UTC`)**:
   - All GitHub timestamps end with `'Z'` (UTC). Pinning Spark's session timezone to `UTC` ensures `to_timestamp` and `unix_timestamp` produce identical values regardless of the host machine's local timezone.

---

## 8. Known Limitations

1. **Kafka Broker 4.4.0 vs. `kafka-clients` 3.4.1**:
   - Spark 3.5.1's `spark-sql-kafka-0-10_2.12:3.5.1` bundles `kafka-clients:3.4.1`, while `ingestion/compose.kafka.yml` runs `apache/kafka:4.4.0`. The Kafka protocol is backward-compatible for `subscribe` and offset reads (verified end-to-end across all 30,494 records), with only benign `AdminClientConfig` unused-config warnings in the logs.
2. **Approximate `silver_builds.duration_s`**:
   - Because GitHub's workflow run payload has no `completed_at` field, `duration_s = updated_at - started_at` can be inflated (`75` runs > 24h) when GitHub updates a run object weeks later (e.g., artifact expiration). Downstream analytics should use the median (`61s`), filter outliers (`duration_s <= 86400`), or sum job durations from `silver_tests`.
3. **Missing Commit Timestamp for DORA Lead Time & Missing Incident Data for CFR/MTTR**:
   - As documented in Section 5.4, exact commit timestamps (`commits/{sha}`) and production incident/recovery events are not part of the 3 Kafka topics.
4. **`is_prd_environment` Prefix Assumption**:
   - Because GitHub returns `production_environment = False` on every deployment in `freeCodeCamp/freeCodeCamp`, `is_prd_environment` is derived from `environment.startswith('prd-')` (`prd-clients`, `prd-api`).
5. **Late Records Across Separate Hudi Commits (`tools/demo_upsert.py` Batch 3)**:
   - A late record (older `updated_at`) written in a **separate Hudi commit** overwrote the newer row (observed in `tools/demo_upsert.py` Batch 3: `status` went back from `'completed'` to `'queued'`).
   - The Hudi `precombine` field only protects within one write batch.
   - Our pipeline is safe today because every Silver job re-reads **all** Bronze and keeps the latest row per key in Spark (`deduplicate_batch`) before writing.
   - **Warning**: Switching Silver to an incremental read (new Bronze rows only) would require configuring the Hudi payload/ordering behavior and re-testing with `tools/demo_upsert.py` Batch 3. This was **not** tested.

---

## 9. Troubleshooting

| Symptom / Error | Cause | Fix |
| :--- | :--- | :--- |
| `network ingestion_default declared as external, but could not be found` | The Kafka stack (`ingestion/compose.kafka.yml`) has not been started yet. | Start Kafka first: `docker compose -f ingestion/compose.kafka.yml up -d kafka` (or set `KAFKA_NETWORK=<actual_network_name>`). |
| Kafka container exits with `AccessDeniedException: /var/lib/kafka/data/...` | Fresh Docker volume `ingestion_kafka_data` owned by `root` instead of `appuser`. | Run `docker compose -f ingestion/compose.kafka.yml exec -T -u root kafka chown -R appuser:appuser /var/lib/kafka/data` and restart `kafka`. |
| `UnknownTopicOrPartitionException` right after starting Kafka | Kafka broker is still electing partition leaders or topics have not been replayed yet. | Wait 5–10 seconds and run `python ingestion/tools/kafka_history.py replay`, then verify with `kafka-get-offsets.sh`. |
| `$'\r': command not found` when running `submit.sh` or `run_all.sh` | Windows Git converted `.sh` files to `CRLF` line endings. | `lakehouse/.gitattributes` enforces `*.sh text eol=lf`. Run `git checkout -- lakehouse/*.sh` or strip `\r` bytes. |
| First Spark run takes ~60–90 seconds before printing `[INFO]` | Ivy is downloading Hudi, Spark-Kafka, and Hadoop-AWS JARs into `lakehouse_ivy_cache`. | Wait for download to complete; all subsequent runs reuse `/tmp/ivy` (`0 downloaded`). |
| `cat /data/cicd-lakehouse/.../hoodie.properties: Is a directory` inside `lakehouse-minio` | MinIO single-drive erasure mode stores every S3 object as a directory containing `xl.meta`. | Use `mc` inside the container: `docker exec lakehouse-minio sh -c "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null && mc cat local/cicd-lakehouse/silver/silver_builds/.hoodie/hoodie.properties"`. |

---

## 10. Handover Guide for Teammates

### For Kamal (Member 3 — Gold Layer, DORA Metrics, & MLlib)

- **How to Read the Silver Hudi Tables in PySpark**:
  Reuse `get_spark("<app_name>")` from `lakehouse/src/spark_session.py` (or configure the same Hudi + S3A options) and load the tables with `format("hudi")`:
  ```python
  from config import silver_path
  from spark_session import get_spark

  spark = get_spark("gold-analytics")

  builds_df = spark.read.format("hudi").load(silver_path("silver_builds"))           # 7,438 rows
  tests_df = spark.read.format("hudi").load(silver_path("silver_tests"))             # 22,744 rows
  deployments_df = spark.read.format("hudi").load(silver_path("silver_deployments")) # 303 rows
  ```
- **Gold Path Convention**: Write Gold tables to **`s3a://cicd-lakehouse/gold/<table_name>`** inside the same `cicd-lakehouse` bucket.
- **Key Filters & Traps to Remember**:
  1. **`silver_builds` Success Rate**: Filter `WHERE conclusion IN ('success', 'failure')` (`5,857` successes vs. `808` failures -> `87.88%` success rate). Exclude `'cancelled'` (`561`), `'action_required'` (`207`), and `'startup_failure'` (`5`).
  2. **`silver_builds` Duration**: Use median (`percentile_approx(duration_s, 0.5)` = `61s`) or filter `duration_s <= 86400` because `75` runs have `updated_at` touched ~30 days after `started_at`.
  3. **`silver_tests` Granularity & Null Durations**: Each row is a **GitHub Actions Job** (`73` distinct `job_name` values), joining cleanly to `silver_builds` on `(run_id, run_attempt)` with `0` orphans. Filter `WHERE duration_s IS NOT NULL` when aggregating job durations (`3,654` skipped and `138` cancelled jobs have `duration_s IS NULL`).
  4. **`silver_deployments` DORA Queries**: Never filter on `latest_status = 'success'` (most rows are `'inactive'`) or `production_environment = True` (always `False`). Filter successful production deployments with **`WHERE is_prd_environment = True AND was_successful = True`** (`144` rows) and use **`first_success_at`** as the deployment completion timestamp.

### For Youssef (Member 4 — Airflow Orchestration)

- **Single Command to Trigger the Entire Bronze + Silver + Verification Pipeline**:
  ```bash
  docker compose -f lakehouse/compose.lakehouse.yml run --rm --entrypoint bash spark /opt/lakehouse/run_all.sh
  ```
- **Granular Step Execution for Separate Airflow Tasks (`BashOperator`)**:
  Use `--only <step_name>` so each Airflow task runs independently and receives a non-zero exit code on failure:
  ```bash
  docker compose -f lakehouse/compose.lakehouse.yml run --rm --entrypoint bash spark /opt/lakehouse/run_all.sh --only bronze_ingest
  docker compose -f lakehouse/compose.lakehouse.yml run --rm --entrypoint bash spark /opt/lakehouse/run_all.sh --only silver_builds
  docker compose -f lakehouse/compose.lakehouse.yml run --rm --entrypoint bash spark /opt/lakehouse/run_all.sh --only silver_tests
  docker compose -f lakehouse/compose.lakehouse.yml run --rm --entrypoint bash spark /opt/lakehouse/run_all.sh --only silver_deployments
  docker compose -f lakehouse/compose.lakehouse.yml run --rm --entrypoint bash spark /opt/lakehouse/run_all.sh --only verify_bronze
  docker compose -f lakehouse/compose.lakehouse.yml run --rm --entrypoint bash spark /opt/lakehouse/run_all.sh --only verify_silver
  ```
- **Idempotency & Exit Codes**: Every step is idempotent (`bronze_ingest` resumes from its S3A checkpoint and ingests `0` rows when no new Kafka offsets exist; Silver jobs deterministically deduplicate and upsert into Hudi) and propagates a non-zero exit code (`$? != 0`) to the host if any ingestion or assertion fails.

### For Badr (Member 1 — Ingestion & Collector Alignment)

- **Confirmed Data Contract (`tools/check_contract.py`)**:
  - `src/schemas.py` matches `ingestion/src/collector/transform.py` across all `30,494` messages (`builds=7,441`, `tests=22,747`, `deployments=306`) with `0` unparsed rows, `0` key mismatches, and `0` unexpected/missing fields.
  - Note that `transform.py` always sets `builds.duration_s = None` (computed in `silver_builds.py` as `updated_at - started_at`) and rejects negative `tests.duration_s` values (`138` cancelled jobs where `completed_at < started_at` by 1s are stored as `None`).
