# GATES Project Monitoring Pipeline — Reference Implementation

This is a working implementation of the LLD we designed: ingestion (Airbyte +
NAVI-GATES) → staging → Kafka landing event → Great Expectations validation →
DataHub metadata → dbt Bronze → Silver → Gold, all orchestrated by Airflow.

The project is a config-driven pipeline proof of concept and reference implementation. The main architectural work is complete; the most important production work remaining is aligning validation output, staging schema, and dbt’s expected columns, then verifying the external service integrations.

## Project walkthrough

This repository implements a DOST/GATES Project Monitoring data pipeline. It
supports two ingestion channels — an API and a regional file submission — and
converts both into one canonical dataset before validation and transformation.

The end-to-end flow is:

```text
API + regional file
  |
  v
Canonical staging CSVs + manifests
  |
  v
Kafka landing event
  |
  v
Great Expectations validation
  |
  v
DataHub metadata registration
  |
  v
Bronze -> Silver -> Gold
```

The reference implementation uses Airflow, Airbyte, Kafka, Great
Expectations, DataHub, dbt, and Trino/Iceberg.

### Repository structure

```text
config/                  Dataset contracts, aliases, and quality rules
common/                  Shared ingestion utilities
ingestion/               API, file, and Airbyte connector code
orchestration/           Airflow DAG factory and Kafka publishing
validation/              Great Expectations validation suites
transformation/          dbt project
metadata/                DataHub metadata emission
sample_data/             Deliberately invalid test input
staging/                 Canonicalized ingestion output
```

### Configuration and data contracts

[config/schema_project_monitoring.yaml](config/schema_project_monitoring.yaml)
is the canonical contract for the `project_monitoring` dataset. It defines
the twelve canonical fields, their types, required status, permissible values,
and `project_id` as the unique key. It also describes the API and regional file
sources, including the agency and region metadata for the file channel.

[config/column_aliasing.yaml](config/column_aliasing.yaml) maps source-specific
file columns such as `Proj_ID` and `Budget Allocated (Php)` to canonical names.
It declares the date and currency transformations applied during ingestion.
The API source uses an empty alias list because its fields already match the
canonical schema.

[config/common_quality_rules.yaml](config/common_quality_rules.yaml) defines
the reusable validation categories: null checks, duplicate checks, schema
validation, and permissible-value checks. Hard rules quarantine invalid data;
soft rules flag data but allow it to continue.

[orchestration/dataset_registry.yaml](orchestration/dataset_registry.yaml)
defines the datasets that the Airflow DAG factory creates. The active dataset
is `project_monitoring`, scheduled daily, with Bronze, Silver, and Gold dbt
layers. A commented second-dataset template shows the intended extension
pattern.

### Ingestion layer

[ingestion/ingest_api_airbyte.py](ingestion/ingest_api_airbyte.py) reads the
simplified Airbyte manifest, performs an authenticated HTTP request, extracts
records from the configured response path, applies the canonical mapping, and
writes the result to staging.

[ingestion/ingest_file_navi_gates.py](ingestion/ingest_file_navi_gates.py)
reads CSV or Excel files, applies the aliases and transformations from the
contract, and fills agency and region from the source metadata. This allows a
file submission to carry batch-level metadata without repeating it in every
raw row.

[common/ingestion_framework.py](common/ingestion_framework.py) contains the
shared implementation used by both channels:

- YAML contract and alias loading
- Source alias and metadata lookup
- Canonical field and unique-key lookup
- Date and currency transformation dispatch
- Identity pass-through for already-canonical API records
- Canonical CSV staging
- SHA-256 manifest generation

Staging output follows this layout:

```text
staging/{agency}/{dataset}/{date}/
├── raw_{source_id}.csv
└── manifest_{source_id}.json
```

The real low-code Airbyte connector is under
[ingestion/airbyte_cdk_connector](ingestion/airbyte_cdk_connector). Its
`manifest.yaml` defines the requester, authenticator, paginator, and record
extractor, while `main.py` exposes Airbyte `spec`, `check`, `discover`, and
`read` commands. This connector has not been verified against a live
`airbyte-cdk` installation in this environment.

### Validation layer

[validation/run_validation.py](validation/run_validation.py) is the common
validation entry point. It combines the staged CSVs and runs the configured
Great Expectations common and custom suites.

The validation rules are:

- Required-field null checks
- Duplicate `project_id` checks
- Exact canonical-schema checks
- Permissible-value checks
- `budget_utilized_php <= budget_allocated_php`
- `end_date > start_date`
- `start_date` within the configured validity window

[validation/gx_common_suite.py](validation/gx_common_suite.py) builds the
contract-driven Great Expectations suite.
[validation/gx_custom_suite_project_monitoring.py](validation/gx_custom_suite_project_monitoring.py)
adds project-monitoring-specific budget and date rules.

The result contains the validation engine, row count, hard-rule status, Data
Quality Index, and individual rule results. Airflow stops the run when hard
validation rules fail so that invalid data does not reach dbt Bronze.

### Orchestration layer

[orchestration/airflow_dag.py](orchestration/airflow_dag.py) is a DAG factory,
not a hardcoded single-dataset DAG. For each registry entry it creates this
task chain:

```text
ingest_api + ingest_file
  |
publish_to_kafka
  |
trigger_gx_validation
  |
register_metadata
  |
dbt_run_bronze -> dbt_run_silver -> dbt_run_gold
```

The dbt chain is generated from the registry's `dbt_layers` list. Models are
selected with both the layer tag and dataset tag, for example:

```text
tag:bronze,tag:project_monitoring
```

[orchestration/kafka_publish.py](orchestration/kafka_publish.py) publishes a
landing event through `confluent_kafka`.

### Metadata layer

[metadata/datahub_emit.py](metadata/datahub_emit.py) registers dataset
properties, ownership, classification tags, upstream lineage, and the
validation summary in DataHub.

The DataHub URN construction, owner mapping, and lineage platform names are
implementation assumptions and must be checked against the target DataHub
deployment before production use.

### Transformation layer

The dbt project is under
[transformation/dbt_project](transformation/dbt_project):

- `models/bronze/project_monitoring/bronze_project_monitoring.sql` casts and
  promotes validated staging rows.
- `models/silver/project_monitoring/stg_project_monitoring.sql` deduplicates,
  joins region names, and calculates utilization.
- `models/gold/project_monitoring/mart_rd_portfolio_performance.sql` creates
  quarterly summaries by agency, program, region, and status.
- `profiles.yml` targets Trino with an Iceberg catalog.
- `setup/create_staging_external_table.sql` contains the one-time staging-table
  DDL.

### Runtime outputs

The deployed pipeline writes canonicalized source data and manifests beneath
`staging/{agency}/{dataset}/{date}/`. Bronze, Silver, and Gold outputs are
managed by dbt in the configured Trino/Iceberg catalog.

## Reference implementation

**1. Production code** — accurate to the real tool APIs, meant to be deployed
against actual infrastructure:
- `orchestration/airflow_dag.py` — a **DAG factory**, not a single hardcoded
  DAG. It reads `orchestration/dataset_registry.yaml` (one entry per
  dataset: name, schedule, start_date, source_ids, agency) and generates one
  DAG per entry. Adding a dataset to this pipeline means adding a block to
  the registry, never editing this file. Deploy both to Airflow's `dags/`
  folder.
- `validation/gx_common_suite.py`, `validation/gx_custom_suite_project_monitoring.py`
  — real Great Expectations API (`pip install great-expectations`)
- `transformation/dbt_project/` — a real dbt project; `profiles.yml` targets
  Trino-on-Iceberg (swap for `dbt-spark` if Spark is the execution engine)
- `config/airbyte_manifest.yaml` — a *simplified* Airbyte-style manifest read
  by our own hand-rolled parser (`ingestion/ingest_api_airbyte.py`) — not the
  real airbyte-cdk. For the real thing, see
  `ingestion/airbyte_cdk_connector/` (a genuine low-code CDK manifest +
  connector entrypoint, plus `ingestion/ingest_api_airbyte_cdk.py` as the
  adapter back into this pipeline) — **unverified in this environment; see
  its own README for the disclaimer and exact commands.**
- `orchestration/kafka_publish.py` — real `confluent_kafka` producer
- `metadata/datahub_emit.py` — real `datahub` REST emitter

