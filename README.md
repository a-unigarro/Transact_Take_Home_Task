# UK Companies House → harmonized company table

AWS Glue (PySpark) → PostgreSQL, built for the transact.digital data engineer take-home task.

## 1. At a glance

```text
CSV (one split file) → raw Parquet (S3 or local) → Glue PySpark job → PostgreSQL
                                                        ├─→ data quality report (JSON)
                                                        └─→ registry.company_rejected
```

- **Snapshot:** `BasicCompanyData-2026-10-01-part1_7` (849,999 rows, one of seven split files)
- **Target:** `registry.company`, keyed by `(country_code, registry_id)`, ready for more countries

## Where each requirement is covered

| # | Requirement | Where |
|---|---|---|
| i | Glue job (PySpark) | `src/glue/transform_load.py` |
| ii | CSV → Parquet first | `src/ingestion/ingest_data.py` |
| iii | Idempotent load | `transform_load.py`, `sql/001_schema.sql` |
| iv | Spark JDBC → PostgreSQL | `jdbc_write()`, `run_sql()` |
| v | Data quality checks | `output/dq/.../report.json` |
| vi | |[Section 7](#7-monthly-refresh-on-aws) - [Section 10](#10-use-of-ai-tools) |

## Evidence of idempotency

Same snapshot, run twice, fresh database:

| Run | Rows inserted/updated |
|---|---|
| 1 | 849,999 |
| 2 | **0** |

Count chain: CSV 849,999 = Parquet 849,999 = rows read 849,999 = company + rejected

# 2. Repository tour

| Path | What it does | Point out |
|---|---|---|
| `src/download_source.py`, `src/ingestion/ingest_data.py` | Download, then CSV → Parquet (S3 or local) | Row counts checked three times (CSV = Spark read = Parquet). Manifest written only after the check passes |
| `src/glue/transform_load.py` | **The Glue job (main deliverable)** | Read in this order: `transform()` → `split_rejects()` → `dq_report()` → `jdbc_write()` → `upsert_sql()`. Key points: `country_code` vs `CountryOfOrigin`, legal form lookup, deterministic duplicate removal, row reconciliation |
| `sql/001_schema.sql` | `company`, `company_staging`, `company_rejected` | [Idempotency design](#4-idempotent-load). Live demo: `python run_pipeline.py --skip-download --skip-ingest --runs 2`, second run prints `0` |
| `output/dq/snapshot_date=2026-10-01/report.json` | DQ report | Plus the rejected table ([demo queries](#10-demo-queries)) |

Supporting files:

| Path | Purpose |
|---|---|
| `run_pipeline.py` | Runs all steps in order (driver, DB, download, Parquet, job) |
| `settings.py` | Configuration from `.env` (paths, AWS, snapshot date) |
| `src/ingestion/storage.py`, `session.py` | Same calls for local or `s3://`. Local Spark session for ingestion |
| `docker-compose.yml` | PostgreSQL, the Glue 5.0 image, optional pgAdmin |
| `scripts/get_jdbc.py` | Downloads the PostgreSQL JDBC driver into `./jars` |
| `pgadmin/servers.example.json` | pgAdmin connection template (optional tool) |
| `output/dq/`, `output/logs/` | DQ reports (folder versioned) and full Spark logs (content git-ignored) |


## 3. How to run

**Requirements:**

- Docker
- Python 3.11+
- Java 17+ (only for the ingestion step on the host, which runs PySpark). Install it with your package manager, for example `sudo apt install openjdk-17-jre`, `brew install openjdk@17`, or `conda install -c conda-forge openjdk=17` if you use conda. Check with `java -version`.

```bash
cp .env.example .env            # fill DB_*, SOURCE_NAME, and AWS keys if using S3
python -m venv env && source env/bin/activate
pip install -r requirements.txt
```
Run everything with one command:
```bash
python run_pipeline.py --runs 2
```

### Fully local run

Leave `LANDING_ROOT` unset. The Parquet goes to `data/COUNTRY=UK/landing/snapshot_date=<date>/` and no AWS keys are needed. If you set a local `LANDING_ROOT` or `DATA_DIR`, it must be inside the project folder, because the container only sees the mounted project.

### Run the Glue job by hand

```bash
docker compose run --rm glue spark-submit \
  --jars /home/hadoop/workspace/jars/postgresql-42.7.4.jar \
  src/glue/transform_load.py --SOURCE_PATH <landing path>
```

`<landing path>` is one of:

- `s3://<bucket>/COUNTRY=UK/landing/snapshot_date=2026-10-01`
- `/home/hadoop/workspace/data/COUNTRY=UK/landing/snapshot_date=2026-10-01` (local)

`run_pipeline.py` creates `output/dq` and makes it writable for the container user. Only if you run the job by hand without ever running the script, do it yourself first: `mkdir -p output/dq && chmod -R 777 output`.

### Optional: pgAdmin

Only for inspecting tables. Open http://localhost:8080 (login `admin@admin.com` / `admin`. It asks for `DB_PASSWORD` the first time).

```bash
cp pgadmin/servers.example.json pgadmin/servers.json   # user/db must match .env
docker compose --profile manual up -d pgadmin
```

### What the script does

1. Downloads the JDBC driver into `./jars`.
2. Starts PostgreSQL (waits until healthy) and makes `output/` writable for the container user.
3. Downloads and extracts the CSV, and writes a manifest (row count, sha256).
4. Converts the CSV to Parquet: local by default, or S3 if `LANDING_ROOT=s3://bucket/prefix` is set in `.env`.
5. Runs the Glue job in Docker, N times (`--runs`).

### Options

| Option | Effect |
|---|---|
| `--skip-download` | Reuse the existing CSV and manifest |
| `--skip-ingest` | Reuse the existing Parquet |
| `--runs N` | Run the Glue job N times |

The screen shows only the key log lines. The full Spark log is in `output/logs/`, and the DQ report in `output/dq/snapshot_date=<date>/report.json`.

### Notes

- `sql/` runs only when the database volume is created. After changing the DDL:
  `docker compose down -v && docker compose up -d --wait db`
- **AWS credentials:** set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `.env`. They are used by the ingestion step (host) and passed to the Glue container. Use a dedicated IAM user limited to the bucket (`GetObject`, `PutObject`, `DeleteObject`, `ListBucket`).
- If the DQ report cannot be written (permissions), the job still loads the data, prints the report in the log and shows a `WARNING`.

## 4. Decisions

### 4.1 Glue runtime

**Choice:** the official AWS Glue 5.0 Docker image, run locally, instead of deploying the job to the Glue service in my own AWS account. Storage is a separate choice: the Parquet lives in my S3 bucket, and the job reads it directly.

- **Why local:** no Glue billing, fast iteration, and the same Spark 3.5 / `awsglue` libraries as the service.
- **Database:** A Glue job in AWS could not reach it without a VPC connection and an RDS instance, so running Glue in AWS would have added cost and setup. S3 access is real (my bucket, IAM keys). Only compute and database are local.
- **Credentials:** keys in `.env` (git-ignored) are injected into the container as environment variables.
- **Versions:** ingestion runs PySpark on the host. The job runs on Spark 3.5.4 (Glue 5.0 image).

### 4.2 Harmonized schema 

| Column | Decision |
|---|---|
| `country_code` |Constant for this source (UK), part of the primary key. **Not** `CountryOfOrigin`. |
| `country_of_origin` | Raw `CountryOfOrigin`, kept as is |
| `legal_form_code`, `legal_form_label` | Companies House gives only a text category. The job maps it to a code. Unmapped categories keep their raw text as label, get a `NULL` code and are listed in the DQ report |
| `industry_code` | UK SIC digits only |
| `city` | Upper case, dots removed, whitespace collapsed (`St. Albans` = `ST ALBANS`) |
| `source_last_updated` | Snapshot date in which the row last changed |
| `ingested_at` | Timestamp of the run that last changed the row (see [section 5](#5-idempotent-load)) |

### 4.3 SIC → NACE

UK SIC 2007 is aligned with NACE Rev. 2 at four digits. The fifth digit is a UK extension.

- **Mapping:** keep the first four digits and format `NN.NN` (SIC `62012` → NACE `62.01`).

## 5. Idempotent load

**Approach:** staging table + a single `INSERT ... ON CONFLICT DO UPDATE`.

1. Spark overwrites `registry.company_staging` (`truncate`, so the DDL types are kept).
2. One SQL statement, in one transaction (atomic), upserts staging into `registry.company`:

   ```sql
   INSERT ... ON CONFLICT (country_code, registry_id) DO UPDATE SET ...
   WHERE (business columns) IS DISTINCT FROM (new business columns)
   ```

3. The `WHERE` clause prevents unmodified rows from being updated.

Duplicates inside the snapshot are removed in Spark before staging.

**Why this approach:** it is atomic, needs no extra infrastructure and works with plain Spark JDBC. Glue job bookmarks only track new files, not changed rows. Iceberg requires the integration of a catalog alongside an engine capable of handling MERGE operations.

### Where it breaks

- **Concurrent runs:** they share the staging table and can overwrite each other. Needs an advisory lock or a per-run staging table.
- **Disappearing companies:** the upsert never deletes, so companies missing from a snapshot stay in the table.
- **Stricter rules later:** tightening a validation rule does not remove rows loaded earlier under a looser rule (same reason: no deletes). Recreate the table or add a cleanup step.
- **Volume:** JDBC batch inserts and a single-statement upsert are fine for ~850k rows, not for millions.

## 6. Data quality

Computed in the job on the transformed data, before filtering. Output: `output/dq/snapshot_date=<date>/report.json`, also printed in the log.

**Checks**

- Rows read, rows valid and unique, rows rejected by reason
- Null rate per harmonized column
- Top 20 unmapped legal form categories
- Reconciliation: rows read = valid + rejected, otherwise the job fails

**Validity rule:** `registry_id` must be 8 digits, or a 2-letter prefix + 6 digits.

**Rejected rows** go to `registry.company_rejected` (snapshot, reason, id, name, form, status):

| Reason | Meaning |
|---|---|
| `INVALID_ID` | Missing or invalid `registry_id` |
| `DUPLICATE_KEY` | Repeated key within the snapshot |

They are replaced per snapshot, so re-runs do not duplicate them.

## 7. Monthly refresh on AWS

Companies House publishes a new snapshot at the start of each month (about 5-6 million companies in total). 

### S3 layout

One bucket, one prefix per zone, partitioned by country and `snapshot_date`:

```text
s3://<bucket>/COUNTRY=UK/zip/snapshot_date=YYYY-MM-DD/       original downloads
s3://<bucket>/COUNTRY=UK/landing/snapshot_date=YYYY-MM-DD/   raw Parquet + manifest
```

- In this repo only the Parquet goes to S3. zip and CSV stay on local disk. In AWS the download step would write the zip files to S3.
- The monthly job reads exactly one partition, and old snapshots are kept for reprocessing.
- Lifecycle rule: expire after N months, Parquet kept.

### Orchestration

EventBridge Scheduler (monthly, a few days after the 1st) → Step Functions:

1. **Download + ingest:** a Python shell job (or Lambda, for the download only)
2. **`transform_load`:** an AWS Glue Spark job that accepts `SOURCE_PATH` / `SNAPSHOT_DATE` as parameters.
3. **DQ gate:** a Choice state that evaluates the report. It halts execution and sends a notification if critical failures are detected.
4. **Notification:** data Quality summary sent via Amazon SNS

Retries with backoff on steps 1 and 2. Both are idempotent, so a retry or a manual re-run for the same snapshot is safe. `run_pipeline.py` is the local equivalent of this state machine.

### Data Catalog

Not needed by the job, which reads directly from S3 paths. It would be better to create a Glue Data Catalog table over the landing Parquet (enabling partition projection on `snapshot_date`) only so analysts can query raw snapshots in Athena and view the schema.

### Cost

Rough estimates for eu-central-1 :

| Item | Estimate |
|---|---|
| Glue Spark job | ~2-5 DPU for ~10-15 min per month, below ~1 EUR per run |
| Step Functions, EventBridge, SNS | Cents |
| S3 | A few GB per snapshot, cents per month |
| Managed PostgreSQL (small instance + ~20 GB storage) | ~25 EUR/month, the dominant cost. It is the only component that runs all month|


## 8. What changes for France (SIRENE, INSEE)

### Data model differences

- **Identifier:** The ID validity rule becomes per-country configuration.
- **Legal form:** INSEE "catégorie juridique", is a numeric, hierarchical code (up to 4 digits). To process it, we need a dedicated lookup table that maps these values to the standard `legal_form_code`. Rather than keeping these lookup dicts inside the job itself, they will now be stored in versioned, per-country reference tables (CSV in the repo or a PostgreSQL table). Unmapped codes still feed the DQ report.
- **Activity:** NAF rév. 2 (e.g. `62.01Z`), which is NACE Rev. 2 plus a letter. Conversion to NACE is dropping the letter. Same idea as UK SIC (drop the fifth digit). 
- **Status:** state codes (A active, C ceased) mapped to a harmonized status.
- **`source_last_updated`:** each row has its own last-processing date, so it comes from the data and not from the snapshot date.
- **Format:** ISO dates and a different delimiter / encoding, so read options become per-country.

### Scaling (30M rows vs ~850k)

- **Ingestion:** count with Spark, not with Python's `csv` module (as `download_source.py` does ). Write several Parquet files instead of `coalesce(1)`.
- **Transform:** Spark handles 30M rows on a few DPUs. The DQ step should use one aggregation pass instead of several `count()` calls.
- **Load:** this is the step that breaks first. Today Spark inserts every row into the staging table over JDBC, and one SQL statement then upserts the whole table in a single transaction. At 30M rows that means slow batched inserts, one very long transaction, and re-sending every row each month even though most rows did not change. The load would need to move the data in bulk and touch only new or changed rows. 
- **Code structure:** one module per country (read, rename, clean) producing the common schema. Shared code for harmonization, loading and DQ.

## 9. What I cut 

- The legal form lookup covers the main categories, the rest are reported by the DQ step instead of being guessed
- No mapping of `CountryOfOrigin` to ISO codes (kept raw), and no status harmonization
- No deletion handling or history. The table holds the latest state
- No automated tests or CI
- The job runs on the Glue Docker image, not on the Glue service


## 10. Use of AI tools

Claude was used as a coding assistant for:

- Docker Compose and pgAdmin setup
- Structure of the Glue job 
- Debugging (AWS credentials and file permissions in the container)
- Code review and drafting this README



