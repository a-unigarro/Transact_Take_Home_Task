# UK Companies House → harmonized company table

AWS Glue (PySpark) → PostgreSQL, built for the transact.digital data engineer take-home task.

## At a glance

```text
CSV (one split file) → raw Parquet (S3 or local) → Glue PySpark job → PostgreSQL
                                                        ├─→ data quality report (JSON)
                                                        └─→ registry.company_rejected
```

Run everything with one command:

```bash
python run_pipeline.py --runs 2
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
| vi-a | Monthly refresh on AWS | [Section 7](#7-monthly-refresh-on-aws) |
| vi-b | France (SIRENE) | [Section 8](#8-what-changes-for-france-sirene-insee) |
| | SIC → NACE note | [4.4](#44-sic--nace) | 
| | What I cut | [Section 9](#9-what-i-cut-and-why) |
| | AI tools used | [Section 10](#10-use-of-ai-tools) |

## Evidence of idempotency

Same snapshot, run twice, fresh database:

| Run | Rows inserted/updated |
|---|---|
| 1 | [N] |
| 2 | **0** |

Count chain: CSV 849,999 = Parquet 849,999 = rows read 849,999 = company [N] + rejected [N]

# 1. Repository tour

Core files in the order I suggest reading them (about 20 minutes).

| Path | What it does | Point out |
|---|---|---|
| This README | Flow and requirement map | The table above |
| `src/download_source.py`, `src/ingestion/ingest_data.py` | Download, then CSV → Parquet (S3 or local) | Row counts checked three times (CSV = Spark read = Parquet); manifest written only after the check passes |
| `src/glue/transform_load.py` | **The Glue job (main deliverable)** | Read in this order: `transform()` → `split_rejects()` → `dq_report()` → `jdbc_write()` → `upsert_sql()`. Key points: `country_code` vs `CountryOfOrigin`, legal form lookup, deterministic duplicate removal, row reconciliation |
| `sql/001_schema.sql` | `company`, `company_staging`, `company_rejected` | [Idempotency design](#4-idempotent-load). Live demo: `python run_pipeline.py --skip-download --skip-ingest --runs 2`, second run prints `0` |
| `output/dq/snapshot_date=2026-10-01/report.json` | DQ report | Plus the rejected table ([demo queries](#10-demo-queries)) |

Supporting files:

| Path | Purpose |
|---|---|
| `run_pipeline.py` | Runs all steps in order (driver, DB, download, Parquet, job) |
| `settings.py` | Configuration from `.env` (paths, AWS, snapshot date) |
| `src/ingestion/storage.py`, `session.py` | Same calls for local or `s3://`; local Spark session for ingestion |
| `docker-compose.yml` | PostgreSQL, the Glue 5.0 image, optional pgAdmin |
| `scripts/get_jdbc.py` | Downloads the PostgreSQL JDBC driver into `./jars` |
| `pgadmin/servers.example.json` | pgAdmin connection template (optional tool) |
| `output/dq/`, `output/logs/` | DQ reports (folder versioned) and full Spark logs (content git-ignored) |

Not included: the Terraform used to create the S3 bucket (any private bucket works).

## 2. How to run

**Requirements:** Docker, Python 3.11+, Java 17+ (only for the ingestion step on the host).

```bash
cp .env.example .env            # fill DB_*, SOURCE_NAME, and AWS keys if using S3
python -m venv env && source env/bin/activate
pip install -r requirements.txt

python run_pipeline.py --runs 2
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

Only for inspecting tables. Open http://localhost:8080 (login `admin@admin.com` / `admin`; it asks for `DB_PASSWORD` the first time).

```bash
cp pgadmin/servers.example.json pgadmin/servers.json   # user/db must match .env
docker compose --profile manual up -d pgadmin
```

### Notes

- `sql/` runs only when the database volume is created. After changing the DDL:
  `docker compose down -v && docker compose up -d --wait db`
- **AWS credentials:** set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `.env`. They are used by the ingestion step (host) and passed to the Glue container. Use a dedicated IAM user limited to the bucket (`GetObject`, `PutObject`, `DeleteObject`, `ListBucket`).
- If the DQ report cannot be written (permissions), the job still loads the data, prints the report in the log and shows a `WARNING`.

## 4. Decisions

### 4.1 Glue runtime

Official AWS Glue 5.0 Docker image (local), not my AWS account.

- **Why:** no cost, fast iteration, and the same Spark 3.5 / `awsglue` libraries as the service.
- **What is real:** the job reads Parquet directly from S3, so S3 access is real. Only compute and database are local.
- **Same script everywhere:** no environment-specific code (no dotenv, no profile handling). Parameters come from the command line or Glue job parameters, and AWS credentials from the default provider chain (environment variables locally, the IAM role in Glue).
- **Credentials:** keys in `.env` (git-ignored) are injected into the container as environment variables. Mounting `~/.aws` did not work: the container user (UID 10000) cannot read 600-mode files owned by another UID, and the SDK reports that as an empty profile file.
- **Versions:** ingestion runs PySpark on the host; the job runs on Spark 3.5.4 (Glue 5.0 image).

### 4.2 Landing format

Raw CSV is converted to Parquet first, as required. Row counts are checked three times:

1. CSV records (Python `csv` module, written to a manifest at download time)
2. Rows read by Spark
3. Rows in the written Parquet

The landing manifest (row count, columns, files) is written only after that check passes.

### 4.3 Harmonized schema (changes to the draft)

| Column | Decision |
|---|---|
| `country_code` | Registry jurisdiction (ISO `GB`), constant for this source, part of the primary key. **Not** `CountryOfOrigin`: that value is the company's origin, free text, and can change between snapshots (which would create duplicates) |
| `country_of_origin` | New. Raw `CountryOfOrigin`, kept as is |
| `legal_form_code`, `legal_form_label` | Companies House gives only a text category. A small lookup in the job maps it to a code. Unmapped categories keep their raw text as label, get a `NULL` code and are listed in the DQ report |
| `industry_code` | UK SIC 2007, first SIC field only, digits only |
| `city` | Upper case, dots removed, whitespace collapsed (`St. Albans` = `ST ALBANS`) |
| `source_last_updated` | Snapshot date in which the row last changed |
| `ingested_at` | Timestamp of the run that last changed the row (see [section 5](#5-idempotent-load)) |

### 4.4 SIC → NACE

UK SIC 2007 is aligned with NACE Rev. 2 at four digits; the fifth digit is a UK extension.

- **Mapping:** keep the first four digits and format `NN.NN` (SIC `62012` → NACE `62.01`).
- **Not materialized:** SIC is kept, as required.
- **With more countries:** add `industry_code_system` (`SIC2007`, `NAF2`, ...) and a derived `nace_code` column.

## 5. Idempotent load

**Approach:** staging table + a single `INSERT ... ON CONFLICT DO UPDATE`.

1. Spark overwrites `registry.company_staging` (`truncate`, so the DDL types are kept).
2. One SQL statement, in one transaction, upserts staging into `registry.company`:

   ```sql
   INSERT ... ON CONFLICT (country_code, registry_id) DO UPDATE SET ...
   WHERE (business columns) IS DISTINCT FROM (new business columns)
   ```

3. The `WHERE` clause makes unchanged rows a no-op.

Duplicates inside the snapshot are removed in Spark before staging, with a total ordering on all business columns, so a re-run always keeps the same row.

**Why this approach:** it is atomic (one transaction), needs no extra infrastructure and works with plain Spark JDBC. Glue job bookmarks only track new files, not changed rows. Iceberg would add a catalog and a MERGE engine that a PostgreSQL target does not need.

### Where it breaks

- **Concurrent runs:** they share the staging table and can overwrite each other. Needs an advisory lock or a per-run staging table.
- **Disappearing companies:** the upsert never deletes, so companies missing from a snapshot stay in the table. Needs a "last seen snapshot" column and a soft delete.
- **`ingested_at` semantics:** `ingested_at` and `source_last_updated` are excluded from the change comparison on purpose (otherwise each monthly run would rewrite every row), so `ingested_at` is not "time of the last run". A `load_audit` table (run id, snapshot, counts) would hold that.
- **Stricter rules later:** tightening a validation rule does not remove rows loaded earlier under a looser rule (same reason: no deletes). Recreate the table or add a cleanup step.
- **Rejected rows:** replaced with delete + append in two steps; if the job fails in between, a re-run fixes it.
- **Volume:** JDBC batch inserts and a single-statement upsert are fine for ~850k rows, not for tens of millions (see [section 8](#8-what-changes-for-france-sirene-insee)).
- **Failure between staging and upsert:** the target is untouched.

## 6. Data quality

Computed in the job on the transformed data, before filtering. Output: `output/dq/snapshot_date=<date>/report.json`, also printed in the log.

**Checks**

- Rows read, rows valid and unique, rows rejected by reason
- Null rate per harmonized column
- `founding_date` in the future
- Top 20 unmapped legal form categories
- Reconciliation: rows read = valid + rejected, otherwise the job fails

**Validity rule:** `registry_id` must be 8 digits, or a 2-letter prefix + 6 digits.

**Rejected rows** go to `registry.company_rejected` (snapshot, reason, id, name, form, status):

| Reason | Meaning |
|---|---|
| `INVALID_ID` | Missing or invalid `registry_id` |
| `DUPLICATE_KEY` | Repeated key within the snapshot |

They are replaced per snapshot, so re-runs do not duplicate them.

**Results for this snapshot**

| Metric | Value |
|---|---|
| Rows read | 849,999 |
| Valid unique | [N] |
| Rejected | [N] (`INVALID_ID` [n], `DUPLICATE_KEY` [n]) |
| Highest null rates | [industry_code x%, ...] |
| Unmapped legal forms | [list or "none"] |

**Not done:** failing the job when a threshold is exceeded (e.g. more than 1% invalid ids).

## 7. Monthly refresh on AWS

Companies House publishes a new snapshot at the start of each month, as seven split files (about 5-6 million companies in total). The refresh reads all parts of one snapshot.

### S3 layout

One bucket, one prefix per zone, partitioned by country and `snapshot_date`:

```text
s3://<bucket>/COUNTRY=UK/zip/snapshot_date=YYYY-MM-DD/       original downloads
s3://<bucket>/COUNTRY=UK/landing/snapshot_date=YYYY-MM-DD/   raw Parquet + manifest
```

- In this repo only the Parquet goes to S3; zip and CSV stay on local disk. In AWS the download step would write the zip files to S3.
- The monthly job reads exactly one partition, and old snapshots are kept for reprocessing.
- Lifecycle rule: zip to Glacier or expire after N months; Parquet kept.

### Orchestration

EventBridge Scheduler (monthly, a few days after the 1st) → Step Functions:

1. **Download + ingest:** Glue Python shell job (Lambda is too small for the full set)
2. **`transform_load`:** Glue Spark job, parameters `SOURCE_PATH` / `SNAPSHOT_DATE`
3. **DQ gate:** Choice state on the report; stop and notify on critical failures
4. **Notification:** SNS with the DQ summary

Retries with backoff on steps 1 and 2. Both are idempotent, so a retry or a manual re-run for the same snapshot is safe. `run_pipeline.py` is the local equivalent of this state machine.

### Data Catalog

Not needed by the job, which reads by S3 path. I would add a Glue Data Catalog table over the landing Parquet (partition projection on `snapshot_date`, no crawler) only so analysts can query raw snapshots in Athena and see the schema. A crawler is unnecessary cost for a stable schema.

### Database and secrets

PostgreSQL on RDS in a private subnet. The Glue job uses a Glue Connection (VPC) and reads credentials from Secrets Manager. Locally the credentials are job parameters, which is not acceptable in production (job arguments are visible in the console). An S3 gateway endpoint avoids NAT costs.

### Cost

Rough estimates for eu-central-1, to be checked against the AWS pricing page:

| Item | Estimate |
|---|---|
| Glue Spark job | ~2-5 DPU for ~10-15 min per month, below ~1 EUR per run |
| Step Functions, EventBridge, SNS | Cents |
| S3 | A few GB per snapshot, cents per month |
| RDS `db.t4g.micro/small` | ~15-25 EUR/month, the dominant cost; could be stopped between runs if only used for the monthly load |
| Local development | 0 EUR for compute (Docker); S3 only for the landing data |

## 8. What changes for France (SIRENE, INSEE)

**Source:** `StockUniteLegale` (~30M rows) + `StockEtablissement` (addresses, ~40M rows).

### Data model differences

- **Identifier:** SIREN, 9 digits, Luhn check digit (SIRET = 14 digits identifies establishments). The ID validity rule becomes per-country configuration.
- **Legal form:** INSEE "catégorie juridique", a numeric hierarchical code (up to 4 digits). It needs its own lookup, mapped to the common `legal_form_code`. Lookups move from a dict in the job to versioned per-country reference tables (CSV in the repo or a PostgreSQL table). Unmapped codes still feed the DQ report.
- **Activity:** NAF rév. 2 (e.g. `62.01Z`), which is NACE Rev. 2 plus a French letter; conversion to NACE is dropping the letter. Same idea as UK SIC (drop the fifth digit). Add `industry_code_system` and `nace_code` so both countries share one scale.
- **Address:** it belongs to the headquarters establishment (`etablissementSiege = true`), so a join with `StockEtablissement` is needed before loading.
- **Name:** `denominationUniteLegale`; sole traders use name / first-name fields.
- **Status:** state codes (A active, C ceased) mapped to a harmonized status.
- **Non-diffusible units** (`statutDiffusionUniteLegale`): data is masked; needs a documented privacy rule.
- **`source_last_updated`:** each row has its own last-processing date, so it comes from the data and not from the snapshot date.
- **Format:** ISO dates and a different delimiter / encoding, so read options become per-country.

### Scaling (30M rows vs ~850k)

- **Ingestion:** count with Spark, not with Python's `csv` module (as `download_source.py` does today); write several Parquet files instead of `coalesce(1)`.
- **Transform:** Spark handles 30M rows on a few DPUs. The DQ step should use one aggregation pass instead of several `count()` calls.
- **Load:** JDBC batch inserts become the bottleneck. Options:
  - (a) stage only new or changed rows by comparing a row hash with the target
  - (b) export to S3 and use `COPY` (RDS `aws_s3` extension)
  - (c) list-partition the target by `country_code` and upsert in key ranges
  - At this size, Iceberg on S3 with MERGE (queried via Athena) is a better target than PostgreSQL.
- **Code structure:** one module per country (read, rename, clean) producing the common schema; shared code for harmonization, loading and DQ.

## 9. What I cut and why

Planned time was 8-10 hours; left out on purpose:

- Only one split file of the snapshot (allowed by the task)
- Only the first SIC code (`SicText_1`), not `SicText_2..4`
- The legal form lookup covers the main categories; the rest are reported by the DQ step instead of being guessed
- No mapping of `CountryOfOrigin` to ISO codes (kept raw), and no status harmonization
- Basic city normalization (case, dots, spaces); no fuzzy matching or gazetteer
- No deletion handling or history (SCD); the table holds the latest state
- No DQ thresholds that fail the job (only the row reconciliation); no automated tests or CI
- No DQ summary table or view in PostgreSQL: the JSON report covers it
- The job runs on the Glue Docker image, not on the Glue service; Step Functions, RDS and Secrets Manager are described, not deployed
- pgAdmin is an optional inspection tool, not part of the pipeline

## 10. Use of AI tools

Claude (Anthropic) was used as a coding assistant for:

- Docker Compose and pgAdmin setup
- Structure of the Glue job and the upsert SQL
- The data quality functions and `run_pipeline.py`
- Debugging (AWS credentials and file permissions in the container)
- Code review and drafting this README

I ran and verified the pipeline myself (including the double run showing 0 updates) and made the design decisions described above.

## 11. Demo queries

Run them with:

```bash
docker compose exec db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "<SQL>"'
```

Or paste them into pgAdmin's Query Tool.

```sql
-- count chain: target + rejected = rows read
select (select count(*) from registry.company)          as target,
       (select count(*) from registry.company_rejected) as rejected;

-- no duplicates
select count(*), count(distinct (country_code, registry_id)) from registry.company;

-- why rows were rejected
select reject_reason, count(*) from registry.company_rejected group by 1;

-- unmapped legal forms
select legal_form_label, count(*) from registry.company
where legal_form_code is null group by 1 order by 2 desc limit 10;

-- sample of harmonized rows
select * from registry.company limit 5;
```

## 12. Questions I expect

**Why staging + upsert and not job bookmarks or Iceberg?**
Atomic, no extra infrastructure, and bookmarks do not detect changed rows ([section 5](#5-idempotent-load)).

**Why is `country_code` not `CountryOfOrigin`?**
It must be a stable part of the key; origin is free text and can change ([4.3](#43-harmonized-schema-changes-to-the-draft)).

**What if the same snapshot is loaded twice, or a job dies halfway?**
Run 2 updates 0 rows. The upsert is one transaction, so the target stays consistent.

**What about companies that vanish from the registry?**
Not handled; needs last-seen tracking ([section 5](#where-it-breaks)).

**How would this scale to 30M rows?**
Spark is fine; the JDBC load is the bottleneck (`COPY`, row hashes, Iceberg). See [section 8](#8-what-changes-for-france-sirene-insee).

**Why run Glue locally?**
Same libraries, zero cost; the script is identical on the service ([4.1](#41-glue-runtime)).
