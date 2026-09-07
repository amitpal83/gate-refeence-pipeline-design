-- setup/create_staging_external_table.sql
--
-- *** NOT VERIFIED against a real Trino/Iceberg deployment *** — no network
-- access to a running Trino cluster in the environment this was written in.
--
-- Bridges a real gap: sources.yml (models/sources.yml) DECLARES a table
-- named staging.project_monitoring_raw so dbt models can reference it via
-- {{ source('staging', 'project_monitoring_raw') }} — but nothing in this
-- repo actually CREATES that table. Our real staging output is plain CSV
-- files sitting in MinIO at
--   /staging/{agency}/project_monitoring/{date}/raw_{source_id}.csv
-- not a queryable table at all.
--
-- This is a ONE-TIME setup statement (run once per environment when the
-- staging path convention is first established, or whenever a new
-- dataset's staging path is added) — NOT something the pipeline runs on
-- every DAG execution. Trino/Iceberg's Hive connector can define an
-- external table directly over a path pattern in object storage, so no
-- separate "load" step is needed afterward — Trino reads the underlying
-- files fresh on every query.
--
-- Run this via the Trino CLI or any SQL client connected to the `staging`
-- catalog/schema, once:

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.project_monitoring_raw (
    project_id             VARCHAR,
    agency                 VARCHAR,
    region                 VARCHAR,
    program_area           VARCHAR,
    project_title          VARCHAR,
    principal_investigator VARCHAR,
    start_date             VARCHAR,   -- kept as text here; Bronze's own
    end_date               VARCHAR,   -- CAST(... AS date) handles typing,
                                       -- matching what ingestion actually
                                       -- writes (loosely-typed CSV output)
    budget_allocated_php   VARCHAR,
    budget_utilized_php    VARCHAR,
    status                 VARCHAR,
    funding_source         VARCHAR,
    last_updated_at        VARCHAR,
    _gx_validation_status  VARCHAR   -- 'pass' / 'fail' — referenced by
                                       -- bronze_project_monitoring.sql's
                                       -- WHERE clause; nothing in this repo
                                       -- currently WRITES this column onto
                                       -- staged files either (see note below)
)
WITH (
    format = 'CSV',
    external_location = 's3a://gates-staging/PCHRD/project_monitoring/',
    skip_header_line_count = 1
);

-- ---------------------------------------------------------------------------
-- A second, related gap surfaced by writing this DDL, worth flagging
-- explicitly rather than silently working around: bronze_project_monitoring
-- .sql filters on `_gx_validation_status = 'pass'`, but validation/
-- run_validation.py's report is a SEPARATE JSON object (rule_results, a
-- dataset-level DQI) — it does NOT currently write a per-row
-- _gx_validation_status column back onto the staged CSV files themselves.
-- Closing this gap for real needs one of:
--   (a) validation writes an augmented CSV (staged data + a new
--       _gx_validation_status column per row, based on the row-level hard
--       rules) back to staging before Bronze
--       runs, or
--   (b) Bronze's WHERE clause changes to reference a separate validation-
--       results table/file instead of assuming a column that was never
--       actually added.
-- Neither is implemented here — this DDL only fixes "the table dbt
-- references now exists and is queryable," not "the column it filters on
-- is actually populated correctly."
-- ---------------------------------------------------------------------------

-- Per-dataset repetition: a second dataset registered in
-- orchestration/dataset_registry.yaml needs its own CREATE TABLE here too,
-- matching that dataset's own canonical schema fields and staging path.
