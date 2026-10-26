CREATE SCHEMA IF NOT EXISTS registry;

CREATE TABLE IF NOT EXISTS registry.company (
  country_code        char(2)     NOT NULL,   -- jurisdiction of the registry (UK), not the company's origin
  registry_id         text        NOT NULL,   -- company number as issued by the registry
  legal_name          text,
  legal_form_code     text,                   -- harmonized code, NULL if the category is unmapped
  legal_form_label    text,                   -- raw category text when unmapped
  status              text,
  industry_code       text,                   -- UK SIC 2007; mapping to NACE documented in README
  founding_date       date,
  postal_code         text,
  city                text,                   -- normalized: upper case, no dots, collapsed spaces
  country_of_origin   text,                   -- raw CountryOfOrigin value (company origin, not registry jurisdiction)
  source_last_updated date,                   -- snapshot in which the row last changed
  ingested_at         timestamptz NOT NULL,
  PRIMARY KEY (country_code, registry_id)
);

-- No PK on staging: deduplication happens in Spark before the write.
CREATE TABLE IF NOT EXISTS registry.company_staging (LIKE registry.company);



-- Rejected rows, one batch per snapshot. Replaced per snapshot so re-runs stay idempotent.
CREATE TABLE IF NOT EXISTS registry.company_rejected (
  snapshot_date date NOT NULL,
  reject_reason text NOT NULL,      -- INVALID_ID | DUPLICATE_KEY
  registry_id   text,
  legal_name    text,
  legal_form_label text,
  status        text,
  raw_company_number text
);
CREATE INDEX IF NOT EXISTS ix_rejected_snapshot ON registry.company_rejected (snapshot_date, reject_reason);

