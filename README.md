UK Companies House -> harmonized company table (AWS Glue / PySpark / PostgreSQL)
=================================================================================

0. AT A GLANCE
--------------
  CSV (one split file) -> raw Parquet (S3 or local) -> Glue PySpark job -> PostgreSQL
                                                             |
                                                             +-> data quality report (JSON)
                                                             +-> registry.company_rejected

One command runs everything:   python run_pipeline.py --runs 2

Snapshot: BasicCompanyData-2026-10-01-part1_7 (849,999 rows, one of seven split files).
Target:   registry.company, keyed by (country_code, registry_id), ready for more countries.

Where each requirement of the task is covered
  (i)   Glue job (PySpark)          src/glue/transform_load.py, run on the official
                                    AWS Glue 5.0 Docker image (section 4.1)
  (ii)  CSV -> Parquet first        src/ingestion/ingest_data.py; the job reads the Parquet
  (iii) Idempotent load             staging + INSERT ... ON CONFLICT (section 5)
  (iv)  Spark JDBC -> PostgreSQL    jdbc_write() and run_sql() in transform_load.py
  (v)   Data quality checks         report.json + registry.company_rejected (section 6)
  (vi)  Monthly refresh / France    sections 7 and 8
  Also: SIC -> NACE note (4.4), what I cut (9), AI tools used (10)

Evidence for the idempotency claim (same snapshot, run twice, fresh database):
  run 1: rows inserted/updated = [N]
  run 2: rows inserted/updated = 0
  count chain: CSV 849,999 = Parquet 849,999 = rows_read 849,999 = company [N] + rejected [N]


1. SUGGESTED WALKTHROUGH (about 20 minutes)
-------------------------------------------
  1. (2 min)  This section and section 0: the flow and the requirement map.
  2. (3 min)  Ingestion: src/download_source.py, src/ingestion/ingest_data.py.
              Row counts are checked three times (CSV = Spark read = Parquet) and the
              landing manifest is written only after the check passes.
  3. (6 min)  The Glue job, src/glue/transform_load.py, in this order:
              transform() -> split_rejects() -> dq_report() -> jdbc_write() -> upsert_sql().
              Key points: country_code vs CountryOfOrigin, the legal form lookup,
              deterministic duplicate removal, row reconciliation.
  4. (3 min)  sql/001_schema.sql and the idempotency design (section 5). Live demo:
              python run_pipeline.py --skip-download --skip-ingest --runs 2
              and show "rows inserted/updated ... 0" on the second run.
  5. (2 min)  Data quality: output/dq/snapshot_date=2026-10-01/report.json and the
              rejected table (queries in section 11).
  6. (3 min)  Sections 7 and 8: monthly refresh on AWS and what changes for France.
  7. (1 min)  Section 9: what I cut and why.


2. REPOSITORY LAYOUT
--------------------
run_pipeline.py                runs all steps in order (driver, DB, download, Parquet, job)
settings.py                    configuration from .env (paths, AWS, snapshot date)
src/download_source.py         download the zip, extract the CSV, write a local manifest
src/ingestion/ingest_data.py   raw CSV -> Parquet (S3 or local), row counts verified
src/ingestion/storage.py       same calls for a local folder or an s3:// location
src/ingestion/session.py       local Spark session for the ingestion step
src/glue/transform_load.py     the Glue job (main deliverable)
sql/001_schema.sql             company, company_staging, company_rejected
docker-compose.yml             PostgreSQL, the Glue 5.0 image, optional pgAdmin
scripts/get_jdbc.py            downloads the PostgreSQL JDBC driver into ./jars
pgadmin/servers.example.json   pgAdmin connection template (optional tool)
output/dq/                     DQ reports (folder versioned, content git-ignored)
output/logs/                   full Spark logs per run (git-ignored)

Not included: the Terraform used to create the S3 bucket (any private bucket works).


3. HOW TO RUN
-------------
Requirements: Docker, Python 3.11+, Java 17+ (only for the ingestion step on the host).

  cp .env.example .env            # fill DB_*, SOURCE_NAME, and AWS keys if using S3
  python -m venv env && source env/bin/activate
  pip install -r requirements.txt

  python run_pipeline.py --runs 2

What it does:
  1. downloads the JDBC driver into ./jars
  2. starts PostgreSQL (waits until healthy) and makes output/ writable for the container
  3. downloads and extracts the CSV, writes a manifest (row count, sha256)
  4. converts the CSV to Parquet (local, or S3 if LANDING_ROOT=s3://bucket/prefix)
  5. runs the Glue job in Docker, N times (--runs)

Options: --skip-download, --skip-ingest (reuse existing files), --runs N.
The screen shows only the key log lines; the full Spark log is in output/logs/.
The DQ report is written to output/dq/snapshot_date=<date>/report.json.

Run the Glue job by hand (landing path: s3://<bucket>/COUNTRY=UK/landing/snapshot_date=...
or /home/hadoop/workspace/data/COUNTRY=UK/landing/snapshot_date=... for local):
  docker compose run --rm glue spark-submit \
    --jars /home/hadoop/workspace/jars/postgresql-42.7.4.jar \
    src/glue/transform_load.py --SOURCE_PATH <landing path>
  (if you do this before run_pipeline.py has ever run: chmod -R 777 output)

Optional, pgAdmin UI to inspect tables (http://localhost:8080, login admin@admin.com / admin;
it asks for DB_PASSWORD the first time):
  cp pgadmin/servers.example.json pgadmin/servers.json   # user/db must match .env
  docker compose --profile manual up -d pgadmin


Local vs S3: If LANDING_ROOT is not defined, by default everything runs locally (CSV and Parquet under ./data) 
and no AWS credentials are needed. Set LANDING_ROOT=s3://<bucket>/COUNTRY=UK/landing in .env
to write the Parquet to S3 and have the Glue job read it from there.  

Notes
  - sql/ runs only when the database volume is created. After changing the DDL:
      docker compose down -v && docker compose up -d --wait db
  - AWS credentials: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env. They are
    used by the ingestion step (host) and passed to the Glue container. Use a
    dedicated IAM user limited to the bucket (GetObject, PutObject, DeleteObject,
    ListBucket). Not needed for a fully local run.
  - If the DQ report cannot be written (permissions), the job still loads the data,
    prints the report in the log and shows a WARNING.


4. DECISIONS
------------
4.1 Glue runtime: official AWS Glue 5.0 Docker image (local), not my AWS account
  Reason: no cost, fast iteration, and the same Spark 3.5 / awsglue libraries as the
  service. The job reads Parquet directly from S3, so S3 access is real; only compute
  and database are local. The script has no environment-specific code (no dotenv, no
  profile handling): parameters come from the command line / Glue job parameters and
  AWS credentials from the default provider chain (environment variables in the
  container locally, the IAM role in Glue). The same script runs in both places.
  Credentials: keys in .env (git-ignored) are injected into the container as
  environment variables. Mounting ~/.aws did not work: the container user (UID 10000)
  cannot read 600-mode files owned by another UID, and the SDK reports that as an
  empty profile file. Environment variables avoid the problem and are portable.
  Versions: the ingestion step runs PySpark on the host; the job runs on Spark 3.5.4
  (Glue 5.0 image).

4.2 Landing format
  Raw CSV is converted to Parquet first, as required. Row counts are checked three
  times: CSV records (Python csv module, written to a manifest at download time) =
  rows read by Spark = rows in the written Parquet. The landing manifest (row count,
  columns, files) is written only after that check passes.

4.3 Harmonized schema (changes to the draft)
  - country_code is the registry jurisdiction (ISO code "GB"), constant for this
    source and part of the primary key. It is NOT the CountryOfOrigin column of the
    CSV: that value is the company's origin, free text, and can change between
    snapshots (which would create duplicates).
  - country_of_origin (new): raw CountryOfOrigin, kept as is.
  - legal_form_code / legal_form_label: Companies House provides only a text
    category. A small lookup in the job maps it to a code. Unmapped categories keep
    their raw text as label, get a NULL code and are listed in the DQ report.
  - industry_code: UK SIC 2007, first SIC field only, digits only.
  - city: upper case, dots removed, whitespace collapsed ("St. Albans" = "ST ALBANS").
  - source_last_updated: snapshot date in which the row last changed.
  - ingested_at: timestamp of the run that last changed the row (see section 5).

4.4 SIC -> NACE
  UK SIC 2007 is aligned with NACE Rev. 2 at four digits; the fifth digit is a UK
  extension. Mapping: keep the first four digits and format NN.NN (SIC 62012 ->
  NACE 62.01). Not materialized: SIC is kept as required. With more countries I would
  add industry_code_system ('SIC2007', 'NAF2', ...) and a derived nace_code column.


5. IDEMPOTENT LOAD
------------------
Approach: staging table + a single INSERT ... ON CONFLICT DO UPDATE.

  1. Spark overwrites registry.company_staging (truncate, so the DDL types are kept).
  2. One SQL statement, in one transaction, upserts staging into registry.company:
       INSERT ... ON CONFLICT (country_code, registry_id) DO UPDATE SET ...
       WHERE (business columns) IS DISTINCT FROM (new business columns)
  3. The WHERE clause makes unchanged rows a no-op.
  Duplicates inside the snapshot are removed in Spark before staging, with a total
  ordering on all business columns, so a re-run always keeps the same row.

Why this approach: it is atomic (one transaction), needs no extra infrastructure and
works with plain Spark JDBC. Glue job bookmarks only track new files, not changed
rows. Iceberg would add a catalog and a MERGE engine that a PostgreSQL target does
not need.

Where it breaks:
  - Concurrent runs share the staging table and can overwrite each other. Needs an
    advisory lock or a per-run staging table.
  - Companies that disappear from a snapshot stay in the table (the upsert never
    deletes). Needs a "last seen snapshot" column and a soft delete.
  - ingested_at and source_last_updated are excluded from the change comparison on
    purpose (otherwise each monthly run would rewrite every row), so ingested_at is
    not "time of the last run". A load_audit table (run id, snapshot, counts) would
    hold that.
  - Tightening a validation rule does not remove rows loaded earlier under a looser
    rule (same reason: no deletes). Recreate the table or add a cleanup step.
  - The rejected-rows table is replaced with delete + append in two steps; if the job
    fails in between, a re-run fixes it.
  - Volume: JDBC batch inserts and a single-statement upsert are fine for ~850k rows,
    not for tens of millions (see section 8).
  - If the job fails between staging and upsert, the target is untouched.


6. DATA QUALITY
---------------
Computed in the job on the transformed data, before filtering. Output:
output/dq/snapshot_date=<date>/report.json, also printed in the log.

  - rows read, rows valid and unique, rows rejected by reason
  - null rate per harmonized column
  - founding_date in the future
  - top 20 unmapped legal form categories
  - reconciliation: rows read = valid + rejected, otherwise the job fails

registry_id is valid if it is 8 digits or a 2-letter prefix + 6 digits. Missing or
invalid ids (INVALID_ID) and repeated keys (DUPLICATE_KEY) are written to
registry.company_rejected (snapshot, reason, id, name, form, status). They are
replaced per snapshot, so re-runs do not duplicate them.

Results for this snapshot:
  rows read: 849,999   valid unique: [N]   rejected: [N] ([INVALID_ID n], [DUPLICATE_KEY n])
  highest null rates: [industry_code x%, ...]
  unmapped legal forms: [list or "none"]

Not done: failing the job when a threshold is exceeded (e.g. > 1% invalid ids).


7. MONTHLY REFRESH ON AWS
-------------------------
Companies House publishes a new snapshot at the start of each month, as seven split
files (about 5-6 million companies in total). The refresh reads all parts of one
snapshot.

S3 layout (one bucket, prefix per zone), partitioned by country and snapshot_date:
  s3://<bucket>/COUNTRY=UK/zip/snapshot_date=YYYY-MM-DD/       original downloads
  s3://<bucket>/COUNTRY=UK/landing/snapshot_date=YYYY-MM-DD/   raw Parquet + manifest
  (In this repo only the Parquet goes to S3; zip and CSV stay on local disk. In AWS
  the download step would write the zip files to S3.) The monthly job reads exactly
  one partition and old snapshots are kept for reprocessing. Lifecycle rule: zip to
  Glacier / expire after N months; Parquet kept.

Orchestration: EventBridge Scheduler (monthly, a few days after the 1st) -> Step
Functions:
  1. download + ingest (Glue Python shell job; Lambda is too small for the full set)
  2. transform_load (Glue Spark job, parameters SOURCE_PATH / SNAPSHOT_DATE)
  3. DQ gate (Choice state on the report: stop and notify on critical failures)
  4. SNS notification with the DQ summary
  Retries with backoff on steps 1 and 2. Both are idempotent, so a retry or a manual
  re-run for the same snapshot is safe. run_pipeline.py is the local equivalent of
  this state machine.

Data Catalog: not needed by the job, which reads by S3 path. I would add a Glue
Data Catalog table over the landing Parquet (partition projection on snapshot_date,
no crawler) only so analysts can query raw snapshots in Athena and see the schema.
A crawler is unnecessary cost for a stable schema.

Database and secrets: PostgreSQL on RDS in a private subnet; the Glue job uses a
Glue Connection (VPC) and reads credentials from Secrets Manager. Locally the
credentials are job parameters, which is not acceptable in production (job
arguments are visible in the console). An S3 gateway endpoint avoids NAT costs.

Cost (rough estimates, eu-central-1, to be checked against the pricing page):
  - Glue Spark job: ~2-5 DPU for ~10-15 minutes per month -> below ~1 EUR per run
  - Step Functions, EventBridge, SNS: cents
  - S3: a few GB per snapshot -> cents per month
  - RDS db.t4g.micro/small: ~15-25 EUR/month, the dominant cost; could be stopped
    between runs if it is only used for the monthly load
  Local development cost: 0 EUR for compute (Docker); S3 only for the landing data.


8. WHAT CHANGES FOR FRANCE (SIRENE, INSEE)
-------------------------------------------
Source: StockUniteLegale (~30M rows) + StockEtablissement (addresses, ~40M rows).

  Identifier: SIREN, 9 digits, Luhn check digit (SIRET = 14 digits identifies
  establishments). The ID validity rule becomes per-country configuration.

  Legal form: INSEE "catégorie juridique", a numeric hierarchical code (up to 4
  digits). It needs its own lookup, mapped to the common legal_form_code. Lookups
  move from a dict in the job to versioned per-country reference tables (CSV in the
  repo or a PostgreSQL table). Unmapped codes still feed the DQ report.

  Activity: NAF rév. 2 (e.g. 62.01Z), which is NACE Rev. 2 plus a French letter;
  conversion to NACE is dropping the letter. Same idea as UK SIC (drop the fifth
  digit). Add industry_code_system and nace_code so both countries share one scale.

  Other differences:
  - The address belongs to the headquarters establishment (etablissementSiege =
    true), so a join with StockEtablissement is needed before loading.
  - Name: denominationUniteLegale; sole traders use name / first-name fields.
  - Status: state codes (A active, C ceased) mapped to a harmonized status.
  - Non-diffusible units (statutDiffusionUniteLegale) have masked data; they need a
    documented privacy rule.
  - Each row has its own last-processing date, so source_last_updated comes from the
    data and not from the snapshot date.
  - ISO dates and different delimiter / encoding: separate read options per country.

  Scaling (30M rows vs ~850k):
  - Ingestion: count with Spark, not with Python's csv module (as download_source.py
    does today); write several Parquet files instead of coalesce(1).
  - Transform: Spark handles 30M rows on a few DPUs; the DQ step should use one
    aggregation pass instead of several count() calls.
  - Load: JDBC batch inserts become the bottleneck. Options: (a) stage only new or
    changed rows by comparing a row hash with the target; (b) export to S3 and use
    COPY (RDS aws_s3 extension); (c) list-partition the target by country_code and
    upsert in key ranges. At this size, Iceberg on S3 with MERGE (queried via Athena)
    is a better target than PostgreSQL.
  - Code structure: one module per country (read, rename, clean) producing the common
    schema; shared code for harmonization, loading and DQ.


9. WHAT I CUT AND WHY
---------------------
Planned time was 8-10 hours; left out on purpose:
  - Only one split file of the snapshot (allowed by the task).
  - Only the first SIC code (SicText_1), not SicText_2..4.
  - The legal form lookup covers the main categories; the rest are reported by the
    DQ step instead of being guessed.
  - No mapping of CountryOfOrigin to ISO codes; kept raw. No status harmonization.
  - Basic city normalization (case, dots, spaces); no fuzzy matching or gazetteer.
  - No deletion handling or history (SCD); the table holds the latest state.
  - No DQ thresholds that fail the job (only the row reconciliation); no automated
    tests or CI.
  - No DQ summary table or view in PostgreSQL: the JSON report covers it.
  - The job runs on the Glue Docker image, not on the Glue service; Step Functions,
    RDS and Secrets Manager are described, not deployed.
  - pgAdmin is an optional inspection tool, not part of the pipeline.


10. USE OF AI TOOLS
-------------------
Claude (Anthropic) was used as a coding assistant for: the Docker Compose and pgAdmin
setup, the structure of the Glue job, the upsert SQL, the data quality functions,
run_pipeline.py, debugging (AWS credentials and file permissions in the container),
code review and drafting this README. I ran and verified the pipeline myself
(including the double run showing 0 updates) and made the design decisions described
above.


11. DEMO QUERIES
----------------
  docker compose exec db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "<SQL>"'

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


12. QUESTIONS I EXPECT (short answers)
--------------------------------------
  Why staging + upsert and not job bookmarks or Iceberg?
    Section 5: atomic, no extra infrastructure, bookmarks do not detect changed rows.
  Why is country_code not CountryOfOrigin?
    Section 4.3: it must be a stable key part; origin is free text and can change.
  What if the same snapshot is loaded twice, or a job dies halfway?
    Run 2 updates 0 rows; the upsert is one transaction, so the target stays consistent.
  What about companies that vanish from the registry?
    Not handled (section 5, "where it breaks"); needs last-seen tracking.
  How would this scale to 30M rows?
    Section 8: Spark is fine; the JDBC load is the bottleneck (COPY, row hashes, Iceberg).
  Why run Glue locally?
    Section 4.1: same libraries, zero cost; the script is identical on the service.