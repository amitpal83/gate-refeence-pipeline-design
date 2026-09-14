# GATES Production Prototype — Comprehensive Architecture Plan

**Date:** 2026-09-14  
**Scope:** Multi-dataset pipeline with Airbyte, File, Kafka CDC ingestion  
**Target:** AWS deployment + team demo  
**Effort:** 3-4 weeks full-stack development

---

## 🎯 Executive Summary

This prototype demonstrates a **production-grade, config-driven data platform** with **hybrid config management** handling:
- ✅ **Dual Config Approach** — YAML (simple/git-based) + Database (scalable/dynamic)
- ✅ **3 ingestion patterns** (Airbyte API, File-based, Kafka CDC)
- ✅ **Multiple datasets** with different schemas
- ✅ **2000+ dataset support** (database-driven config)
- ✅ **Open table format** (Iceberg) for data warehouse
- ✅ **Quality gates** (hard ingestion checks, soft transformation checks)
- ✅ **Data cataloging** (DataHub integration)
- ✅ **Full observability** (logging, monitoring, failure handling)
- ✅ **Infrastructure-as-Code** (Terraform on AWS)

---

## 📚 **DETAILED INTEGRATION GUIDES**

**For complete implementation details, see:**

| Component | File | Purpose |
|-----------|------|---------|
| **DataHub** | [DATAHUB_INTEGRATION.md](DATAHUB_INTEGRATION.md) | Complete metadata emission, schema registration, lineage tracking, data profiling, and UI demonstration |
| **Trino** | [TRINO_INTEGRATION.md](TRINO_INTEGRATION.md) | Federated SQL queries over Iceberg/MinIO, query optimization, connector configuration, and performance tuning |
| **Live Demo** | [DATAHUB_TRINO_DEMO.md](DATAHUB_TRINO_DEMO.md) | End-to-end demo scenarios showing DataHub + Trino working together for data discovery, lineage, and quality investigation |

**Key highlights from detailed guides:**
- ✅ DataHub: Metadata registration via MCE (MetadataChangeProposalWrapper), schema aspects, lineage tracking, column-level profiles, quality metrics visualization
- ✅ Trino: Iceberg catalog configuration, MinIO S3 connector, federated queries across Bronze/Silver/Gold layers, time-travel queries, performance optimization
- ✅ Integration: DataHub enables discovery; Trino enables analysis; together provide complete data platform governance


## 🔀 Configuration Management: Hybrid Approach

```
┌──────────────────────────────────────────────────────────────┐
│                  CONFIG REGISTRY CLASS                        │
│  (Single interface, multiple backends)                        │
└──────────────────────────────────────────────────────────────┘
         ↙                                                    ↘
┌────────────────────────────────┐    ┌───────────────────────────┐
│   YAML Configuration           │    │   Database Configuration  │
│   (Git-based, Simple)          │    │   (Dynamic, Scalable)     │
├────────────────────────────────┤    ├───────────────────────────┤
│ config/                        │    │   PostgreSQL/RDS          │
│ ├─ schema_*.yaml              │    │   ├─ dataset_registry    │
│ ├─ column_aliasing_*.yaml     │    │   ├─ ingestion_source    │
│ ├─ validation_rules_*.yaml    │    │   ├─ ingestion_schema    │
│ └─ airbyte_manifest.yaml      │    │   ├─ field_mapping      │
│                                │    │   └─ validation_rules    │
│ Best For:                      │    │                           │
│ • Simple pipelines (1-10 ds)  │    │ Best For:                 │
│ • Dev/test environments       │    │ • Enterprise (1000+ ds)   │
│ • Version control required    │    │ • Runtime config updates  │
│ • Small teams                 │    │ • Multi-tenant scenarios  │
│                                │    │ • Compliance/audit trail  │
└────────────────────────────────┘    └───────────────────────────┘

                    ↓↓↓

        CONFIGREGISTRY.GET_CONFIG()
        └─ Priority: DB first, YAML fallback
        └─ TTL cache (60s) for performance
        └─ Automatic sync if sources diverge
```

---

## 📐 Architecture Overview

```
┌────────────────────────────────────────────────────────────────┐
│                    DATA SOURCES (Multi-tenant)                  │
├────────────────────────────────────────────────────────────────┤
│  • Airbyte API: DOST PMS API, Salesforce, 3rd-party APIs       │
│  • File-based: Regional offices (Excel/CSV), uploaded to SFTP  │
│  • Kafka CDC: PostgreSQL, MySQL, MongoDB change streams        │
└────────────────────────────────────────────────────────────────┘
                              ↓↓↓
┌────────────────────────────────────────────────────────────────┐
│          CONFIG MANAGEMENT LAYER (Database-Driven)              │
├────────────────────────────────────────────────────────────────┤
│  PostgreSQL/RDS                                                 │
│  ├─ dataset_registry (2000+ datasets)                          │
│  ├─ ingestion_source (API, File, Kafka configs)               │
│  ├─ ingestion_schema (canonical schemas)                       │
│  ├─ field_mapping (raw → canonical)                            │
│  ├─ validation_rules (hard/soft rules)                         │
│  └─ config_audit_log (compliance tracking)                     │
└────────────────────────────────────────────────────────────────┘
                              ↓↓↓
┌────────────────────────────────────────────────────────────────┐
│            INGESTION ORCHESTRATION (Airflow)                    │
├────────────────────────────────────────────────────────────────┤
│  DAG Factory: Generates 2000+ DAGs from config DB               │
│  ├─ Task 1: Ingest from Airbyte/File/Kafka (parallel)         │
│  ├─ Task 2: Hard checks (quarantine on failure)               │
│  ├─ Task 3: Stage to MinIO (Parquet/Iceberg)                  │
│  ├─ Task 4: Publish metadata to DataHub                       │
│  ├─ Task 5: Publish to Kafka (event stream)                   │
│  └─ Task 6: Trigger downstream (dbt transforms)               │
└────────────────────────────────────────────────────────────────┘
                              ↓↓↓
┌────────────────────────────────────────────────────────────────┐
│         STORAGE LAYER (MinIO + Iceberg)                        │
├────────────────────────────────────────────────────────────────┤
│  MinIO (S3-compatible):                                         │
│  ├─ /bronze/dataset_name/date/ (raw data, as-is)              │
│  ├─ /silver/dataset_name/date/ (cleansed, deduplicated)       │
│  └─ /gold/dataset_name/date/ (business aggregates)            │
│                                                                 │
│  Iceberg Format:                                                │
│  ├─ Schema evolution support                                    │
│  ├─ ACID transactions                                           │
│  ├─ Time-travel capability                                      │
│  └─ Partition pruning                                           │
└────────────────────────────────────────────────────────────────┘
                              ↓↓↓
┌────────────────────────────────────────────────────────────────┐
│       TRANSFORMATION LAYER (dbt + Trino)                        │
├────────────────────────────────────────────────────────────────┤
│  Bronze Layer:                                                   │
│  └─ Type-cast, partition, validate schema                       │
│                                                                 │
│  Silver Layer:                                                   │
│  └─ Soft checks, deduplication, business rules                  │
│                                                                 │
│  Gold Layer:                                                     │
│  └─ Pre-aggregated, domain-specific marts                       │
│                                                                 │
│  Query Engine: Trino (SQL federation over MinIO/Iceberg)       │
└────────────────────────────────────────────────────────────────┘
                              ↓↓↓
┌────────────────────────────────────────────────────────────────┐
│        DATA CATALOG & OBSERVABILITY                             │
├────────────────────────────────────────────────────────────────┤
│  DataHub:                                                        │
│  ├─ Dataset lineage (API → Staging → Bronze → Silver → Gold)   │
│  ├─ Schema metadata (fields, types, quality metrics)           │
│  ├─ Ownership & SLA tracking                                    │
│  └─ Data discovery UI                                           │
│                                                                 │
│  Monitoring:                                                     │
│  ├─ CloudWatch (AWS logs)                                       │
│  ├─ Prometheus + Grafana (metrics)                             │
│  ├─ Airflow UI (DAG execution)                                 │
│  └─ Custom job/batch tracking DB                               │
└────────────────────────────────────────────────────────────────┘
```

---

## 🏗️ Component Design

### **1. CONFIG MANAGEMENT (Dual Approach: YAML + Database)**

#### **Option A: YAML Configuration (Simple & Git-based)**

**Use Case:** Dev/test, small teams, version-controlled configs

**Structure:**
```
config/
├─ schema_project_monitoring.yaml
│  ├─ dataset: project_monitoring
│  ├─ canonical_schema (12 fields)
│  ├─ sources metadata
│  └─ unique_fields: [project_id]
│
├─ column_aliasing_project_monitoring.yaml
│  ├─ sources[0].source_id: dost_pms_api
│  │  └─ aliases: [] (API already canonical)
│  ├─ sources[1].source_id: pchrd_regional_file
│  │  └─ aliases: (raw → canonical mappings)
│  └─ sources[2].source_id: kafka_cdc
│     └─ aliases: (CDC format → canonical)
│
├─ validation_rules_project_monitoring.yaml
│  ├─ hard_rules: [null_check, duplicate_check, schema_validation]
│  └─ soft_rules: [permissible_values, budget_logic, date_logic]
│
├─ airbyte_manifest.yaml
│  ├─ definitions.requester (URL, auth, headers)
│  ├─ definitions.streams (endpoints, pagination)
│  └─ definitions.selector (response_path, field extraction)
│
└─ dataset_registry.yaml
   └─ datasets:
      - dataset: project_monitoring
        schedule_interval: @daily
        dbt_layers: [bronze, silver, gold]
```

**Example:** `schema_project_monitoring.yaml`
```yaml
dataset: project_monitoring
version: "1.0"

canonical_schema:
  unique_fields: [project_id]
  fields:
    - name: project_id
      dtype: string
      required: true
      unique: true
    - name: agency
      dtype: string
      required: true
      permissible_values: [PCHRD, PCAARRD, PCIEERD, ASTI, FNRI, ITDI, MIRDC, TAPI]
    - name: budget_allocated_php
      dtype: float
      required: true
      min: 0
    # ... 9 more fields

sources:
  - source_id: dost_pms_api
    channel: api
    url_base: https://api.dost.gov.ph
    endpoint: /v1/projects
    
  - source_id: pchrd_regional_file
    channel: file
    agency: PCHRD
    region: R4A
    file_pattern: ProjectMonitoring_*.xlsx
    
  - source_id: kafka_cdc
    channel: kafka
    topic: db-changes.project_monitoring
    bootstrap_servers: kafka:9092
```

**Benefits (YAML):**
- ✅ Simple, human-readable
- ✅ Version controlled (Git history)
- ✅ No database dependency
- ✅ Easy for small teams
- ❌ Requires code redeploy to change
- ❌ No dynamic config updates
- ❌ Scales poorly (1000+ files)

---

#### **Option B: Database Configuration (Scalable & Dynamic)**

**Use Case:** Enterprise, 1000+ datasets, runtime updates needed

**Tables:**

```sql
dataset_registry
├─ dataset_id (UUID)
├─ dataset_name (VARCHAR UNIQUE)
├─ description (TEXT)
├─ ingestion_type (airbyte | file | kafka_cdc | hybrid)
├─ schema_id (FK → ingestion_schema)
├─ owner (VARCHAR)
├─ status (active | draft | deprecated)
├─ sla_hours (INT, e.g., 24 for daily)
└─ metadata (JSONB)

ingestion_source
├─ source_id (VARCHAR PRIMARY KEY)
├─ dataset_id (FK → dataset_registry)
├─ source_type (airbyte | file | kafka)
├─ channel (api | file_sftp | kafka_topic)
├─ priority (INT, for hybrid scenarios)
├─ config (JSONB) ◄─ FLEXIBLE per type
├─ status (active | inactive)
└─ polling_interval (@daily | @weekly | @hourly)

ingestion_schema
├─ schema_id (UUID PRIMARY KEY)
├─ dataset_id (FK → dataset_registry)
├─ schema_version (INT)
├─ fields (JSONB array)
│  ├─ name, dtype, required, unique, permissible_values
│  └─ quality_metric (null_rate, cardinality, etc.)
├─ unique_fields (TEXT[])
└─ effective_date (TIMESTAMP)

field_mapping
├─ mapping_id (UUID)
├─ schema_id (FK → ingestion_schema)
├─ source_id (FK → ingestion_source)
├─ raw_field (VARCHAR)
├─ canonical_field (VARCHAR)
├─ transform (VARCHAR, e.g., parse_date, strip_currency)
└─ created_by (VARCHAR)

validation_rule
├─ rule_id (UUID)
├─ dataset_id (FK → dataset_registry)
├─ field_name (VARCHAR, or '*' for row-level)
├─ rule_type (null_check | duplicate | schema | permissible_values | custom)
├─ severity (hard | soft)
│  ├─ hard: Quarantine data before staging (ingestion time)
│  └─ soft: Flag data but allow promotion (transformation time)
├─ rule_config (JSONB, {expected_values, min, max, pattern, etc.})
└─ created_at (TIMESTAMP)

ingestion_lineage
├─ lineage_id (UUID)
├─ dataset_id (FK → dataset_registry)
├─ run_date (DATE)
├─ source_id (FK → ingestion_source) ◄─ Which source was used
├─ row_count (INT)
├─ status (success | failed | partial)
├─ error_message (TEXT)
├─ ingestion_duration_seconds (INT)
└─ created_at (TIMESTAMP)

config_audit_log
├─ audit_id (UUID)
├─ dataset_id (FK → dataset_registry)
├─ table_modified (VARCHAR)
├─ change_type (CREATE | UPDATE | DELETE | ACTIVATE)
├─ changed_by (VARCHAR)
├─ reason (TEXT)
├─ old_value (JSONB)
├─ new_value (JSONB)
└─ changed_at (TIMESTAMP)

job_execution
├─ job_id (UUID)
├─ dataset_id (FK → dataset_registry)
├─ dag_run_id (VARCHAR)
├─ task_name (VARCHAR)
├─ task_type (ingest_airbyte | ingest_file | validate | dbt_run)
├─ status (running | success | failed | skipped)
├─ start_time (TIMESTAMP)
├─ end_time (TIMESTAMP)
├─ duration_seconds (INT)
├─ rows_processed (INT)
├─ error_details (JSONB)
├─ retry_count (INT)
├─ logs_location (VARCHAR) ◄─ S3 path to CloudWatch logs
└─ created_at (TIMESTAMP)
```

**Benefits:**
- Supports 2000+ datasets without code changes
- Full audit trail for compliance
- Dynamic configuration updates (no redeployment)
- Query performance tracking and monitoring

---

### **2. INGESTION LAYER (3 Handlers)**

#### **Type A: Airbyte API Connector**

**Config (Database JSONB):**
```json
{
  "source_type": "airbyte",
  "connection_id": "abc-123-def",
  "stream_name": "project_monitoring",
  "url_base": "https://api.dost.gov.ph",
  "endpoint": "/v1/projects",
  "auth": {
    "type": "bearer_token",
    "token_env_var": "DOST_API_TOKEN",
    "refresh_url": "https://auth.dost.gov.ph/token"
  },
  "response_path": ["data", "records"],
  "pagination": {
    "type": "offset",
    "limit_param": "limit",
    "offset_param": "offset",
    "page_size": 1000
  },
  "retry_policy": {
    "max_retries": 3,
    "backoff_multiplier": 2,
    "timeout_seconds": 30
  },
  "batch_size": 10000
}
```

**Handler:** `ingestion/ingest_api_airbyte.py`
- Load config from database
- Make authenticated HTTP request
- Extract records via `response_path`
- Apply field mapping (raw → canonical)
- Write to MinIO staging (Parquet)

#### **Type B: File-Based Ingestion (Multi-file Support)**

**Config (Database JSONB):**
```json
{
  "source_type": "file",
  "file_format": ["xlsx", "csv"],
  "location_type": "sftp",
  "location": "sftp://regional-drops.dost.gov.ph/monitoring/",
  "file_pattern": "ProjectMonitoring_*.xlsx",
  "sheet_name": "Monitoring",
  "encoding": "utf-8",
  "batch_metadata": {
    "agency": "PCHRD",
    "region": "R4A",
    "submitted_by": "regional.officer@pchrd.dost.gov.ph"
  },
  "processing": {
    "skip_rows": 0,
    "skip_footer": 0,
    "parse_dates": ["start_date", "end_date"]
  },
  "multi_file_strategy": "concatenate",  -- or "partition_by_file"
  "polling_interval": "@weekly"
}
```

**Handler:** `ingestion/ingest_file_navi_gates.py`
- Scan SFTP for files matching pattern
- Load all matching files (Excel/CSV)
- Concatenate with file_id for lineage
- Apply field mapping
- Write to MinIO staging (Parquet)

#### **Type C: Kafka CDC (Real-time Change Capture)**

**Config (Database JSONB):**
```json
{
  "source_type": "kafka",
  "bootstrap_servers": "kafka-broker-1:9092,kafka-broker-2:9092",
  "topic": "db-changelog.project_monitoring",
  "consumer_group": "gates-project-monitoring-cdc",
  "message_format": "debezium_json",
  "message_schema": {
    "before": "object",
    "after": "object",
    "op": "string",  -- c=create, u=update, d=delete, r=read
    "ts_ms": "bigint"
  },
  "filter": {
    "operations": ["c", "u"]  -- Capture CREATE and UPDATE only
  },
  "start_offset": "latest",
  "batch_window_seconds": 300,  -- Collect for 5 min
  "source_database": {
    "type": "postgresql",
    "host": "source-db.rds.amazonaws.com",
    "database": "research_db",
    "tables": ["projects", "milestones", "budget"]  -- Multi-table support
  },
  "schema_registry_url": "https://schema-registry:8081",
  "security": {
    "protocol": "SASL_SSL",
    "sasl_mechanism": "PLAIN",
    "credentials_env_var": "KAFKA_CREDENTIALS"
  }
}
```

**Handler:** `ingestion/ingest_kafka_cdc.py`
- Consume from Kafka topic in batches
- Parse Debezium CDC format
- Filter by operation type (insert/update only)
- Handle multi-table scenarios (different schemas)
- Write to MinIO staging (Parquet)

---

### **3. VALIDATION LAYER (Hard & Soft Rules)**

#### **Hard Rules (Ingestion Time - Quarantine on Failure)**

```python
HARD_RULES = {
    "null_check": lambda df, field, config: df[field].notna().all(),
    "duplicate_check": lambda df, field, config: df[field].is_unique,
    "schema_validation": lambda df, schema, config: check_column_types(df, schema),
    "data_type": lambda df, field, dtype, config: df[field].dtype == dtype,
}

# Example: Hard rule fails → Batch quarantined, won't proceed to staging
```

**Execution:** During `validate_ingestion()` task
- If ANY hard rule fails → Stop pipeline
- Write failed rows to quarantine zone: `/quarantine/dataset_name/date/`
- Log to database + alert
- Manual review required before re-running

#### **Soft Rules (Transformation Time - Flag & Promote)**

```python
SOFT_RULES = {
    "permissible_values": lambda df, field, values, config: df[field].isin(values),
    "budget_utilization": lambda df, config: df['budget_utilized'] <= df['budget_allocated'],
    "date_logic": lambda df, config: df['end_date'] > df['start_date'],
    "completeness": lambda df, field, threshold, config: (df[field].notna().sum() / len(df)) >= threshold,
}

# Example: Soft rule fails → Row flagged, added to Silver with quality_flag='soft_rule_violation'
```

**Execution:** During `dbt_run_silver` task
- Non-blocking violations
- Rows added with `quality_flag` column
- Tracked in monitoring/metrics

---

### **4. STORAGE LAYER (MinIO + Iceberg)**

**Directory Structure:**

```
s3://gates-pipeline-data/
├─ bronze/
│  ├─ project_monitoring/
│  │  ├─ 2026-09-14/
│  │  │  ├─ part_00001.parquet (raw data, as-is)
│  │  │  ├─ part_00002.parquet
│  │  │  ├─ _iceberg_metadata/
│  │  │  │  ├─ v1.metadata.json
│  │  │  │  └─ snap-123.avro (schema snapshot)
│  │  │  └─ _success
│  │  └─ 2026-09-15/
│  │
│  ├─ rd_equipment/
│  │  ├─ 2026-09-14/
│  │  └─ 2026-09-15/
│
├─ silver/
│  ├─ project_monitoring/
│  │  ├─ 2026-09-14/
│  │  │  ├─ deduped_v1.parquet
│  │  │  └─ _iceberg_metadata/
│  │  └─ 2026-09-15/
│
├─ gold/
│  ├─ project_monitoring_portfolio_performance/
│  │  ├─ 2026-09/
│  │  │  ├─ agency=PCHRD/
│  │  │  │  ├─ program_area=Health/
│  │  │  │  │  ├─ part-00001.parquet
│  │  │  │  │  └─ ...
│  │  │  └─ agency=PCAARRD/
│  │  └─ 2026-10/
│
└─ quarantine/
   ├─ project_monitoring/
   │  ├─ 2026-09-14_hard_rule_violation.parquet (failed rows)
   │  └─ error_log.json
```

**Iceberg Table Format:**

```
project_monitoring (Bronze)
├─ Columns: project_id, agency, region, ..., _source_id, _ingestion_timestamp, _checksum
├─ Partition: dt (date)
├─ Format: Parquet
├─ Schema evolution: ✅ Supported
├─ Time travel: ✅ Supported (can query day-old data)
└─ Metadata: UUID (run_id), validation_status, source tracking

project_monitoring (Silver)
├─ Columns: project_id, agency, region, ..., quality_flag, dedup_rank
├─ Partition: dt (date)
├─ Format: Parquet
└─ Clustering: project_id (for faster joins)
```

**Benefits:**
- ACID transactions (no partial writes)
- Schema evolution (add/drop columns safely)
- Partition pruning (query only relevant data)
- Time travel (audit/recovery)
- Open format (not vendor-locked)

---

### **5. TRANSFORMATION LAYER (dbt + Trino)**

#### **Bronze Layer (dbt Model)**

**Purpose:** Type-cast, validate schema, partition validated data

> **See [TRINO_INTEGRATION.md](TRINO_INTEGRATION.md) for:**
> - Complete Trino setup and configuration (Docker, catalog configuration, connectors)
> - Federated query examples over Iceberg tables
> - Query performance optimization and indexing strategies
> - Trino UI navigation and query monitoring
> - dbt profiles.yml configuration for Trino backend
> - Integration with MinIO S3 connector

```sql
-- models/bronze/project_monitoring.sql
{{
  config(
    materialized='table',
    tags=['bronze'],
    partition_by={'field': 'dt', 'data_type': 'date'},
  )
}}

with validated_staging as (
    select * 
    from {{ source('staging', 'project_monitoring_raw') }}
    where _validation_status = 'pass'  -- Only validated data
)

select
    cast(project_id as varchar) as project_id,
    cast(agency as varchar) as agency,
    cast(region as varchar) as region,
    -- ... other fields
    cast(start_date as date) as start_date,
    cast(budget_allocated_php as decimal(18,2)) as budget_allocated_php,
    current_timestamp as _loaded_at,
    '{{ run_id }}' as _dbt_run_id,
    current_date as dt
from validated_staging
```

#### **Silver Layer (dbt Model)**

**Purpose:** Deduplication, business rules, soft rule tracking

```sql
-- models/silver/project_monitoring.sql
{{
  config(
    materialized='table',
    tags=['silver'],
  )
}}

with bronze as (
    select * from {{ ref('bronze_project_monitoring') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by project_id 
            order by _loaded_at desc
        ) as dedup_rank
    from bronze
),

with_business_rules as (
    select
        d.*,
        case
            when d.end_date <= d.start_date then 'soft_violation_end_before_start'
            when d.budget_utilized > d.budget_allocated then 'soft_violation_over_budget'
            when d.status not in ('Proposed', 'Ongoing', 'Completed') then 'soft_violation_invalid_status'
            else 'pass'
        end as quality_flag
    from deduped d
    where d.dedup_rank = 1
)

select
    project_id,
    agency,
    region,
    program_area,
    project_title,
    principal_investigator,
    start_date,
    end_date,
    budget_allocated_php,
    budget_utilized_php,
    round(
        (budget_utilized_php / nullif(budget_allocated_php, 0)) * 100, 1
    ) as budget_utilization_pct,
    status,
    funding_source,
    quality_flag,  -- ◄─ Soft rule violations tracked
    _loaded_at
from with_business_rules
```

#### **Gold Layer (dbt Model)**

**Purpose:** Pre-aggregated, business-ready marts

```sql
-- models/gold/rd_portfolio_performance.sql
{{
  config(
    materialized='table',
    tags=['gold'],
    partition_by={'field': 'quarter', 'data_type': 'string'},
  )
}}

with silver as (
    select * from {{ ref('project_monitoring') }}
    where quality_flag = 'pass'  -- Only include quality-passing data
),

by_quarter as (
    select
        to_char(date_trunc('quarter', start_date), 'YYYY-Q') as quarter,
        agency,
        program_area,
        region,
        status,
        count(distinct project_id) as project_count,
        count(distinct case when quality_flag != 'pass' then project_id end) as quality_issues,
        sum(budget_allocated_php) as total_budget_allocated,
        sum(budget_utilized_php) as total_budget_utilized,
        round(
            (sum(budget_utilized_php) / nullif(sum(budget_allocated_php), 0)) * 100, 1
        ) as utilization_rate_pct,
        min(start_date) as earliest_start,
        max(end_date) as latest_end
    from silver
    group by 1, 2, 3, 4, 5
)

select * from by_quarter
order by quarter desc, agency, program_area
```

**Query via Trino:**

```sql
-- Trino SQL queries over Iceberg tables in MinIO
SELECT 
    quarter,
    agency,
    program_area,
    project_count,
    utilization_rate_pct
FROM iceberg.gates."rd_portfolio_performance"
WHERE quarter >= '2026-Q3'
ORDER BY utilization_rate_pct DESC;
```

---

### **6. ORCHESTRATION LAYER (Airflow DAG Factory)**

#### **DAG Factory Logic**

```python
# orchestration/dag_factory.py

def build_dag_for_dataset(dataset_config, config_registry):
    """
    Generate one DAG per dataset from config database.
    
    Task structure (same for all datasets):
    1. ingest_sources (parallel: Airbyte, File, Kafka)
    2. validate_ingestion (hard checks - quarantine on fail)
    3. stage_to_minio (write Parquet/Iceberg to Bronze)
    4. publish_lineage (DataHub metadata registration)
    5. notify_catalog (Kafka event for downstream subscribers)
    6. dbt_run_bronze (type-cast, partition)
    7. dbt_run_silver (dedup, soft checks)
    8. dbt_run_gold (aggregate, business marts)
    9. monitor_and_log (track job execution metrics)
    """
    
    dag = DAG(
        dag_id=f"gates_{dataset_name}",
        schedule_interval=dataset_config['schedule_interval'],
        start_date=datetime(2026, 1, 1),
        catchup=False,
        default_args={
            'owner': 'gates-platform',
            'retries': 2,
            'retry_delay': timedelta(minutes=5),
            'retry_exponential_backoff': True,
            'max_retry_delay': timedelta(minutes=30),
            'on_failure_callback': on_task_failure,
            'on_retry_callback': on_task_retry,
        },
    )
    
    return dag
```

#### **Task Definitions**

**Task 1: Ingest (Parallel)**
```
ingest_airbyte → Query DB for config → Call Airbyte API → Validate response
ingest_file    → Query DB for config → Scan SFTP → Load files → Validate
ingest_kafka   → Query DB for config → Poll topic → Parse CDC → Validate

All three run in parallel (depends on what's configured)
```

**Task 2: Validate (Hard Checks)**
```
Inputs: Raw data from all sources
Process:
  - Load validation rules from config DB
  - Run hard rules (null, duplicate, schema, custom)
  - If ANY fail:
    → Write failed rows to quarantine zone
    → Fail task (Airflow shows red)
    → Alert team
    → Pipeline stops
  - If all pass:
    → Proceed to staging
```

**Task 3: Stage to MinIO**
```
Inputs: Validated data
Process:
  - Write as Parquet to MinIO (Bronze layer)
  - Create Iceberg metadata (_iceberg_metadata/)
  - Record lineage (source_id, row_count, checksum)
  - Update ingestion_lineage table
Output: S3 location, Iceberg table ready for dbt
```

**Task 4: Publish Lineage to DataHub**
```
Send to DataHub:
  - Dataset URN: urn:li:dataset:(urn:li:dataPlatform:iceberg,gates.bronze.project_monitoring)
  - Aspects: 
    ├─ DatasetProperties (schema, row count, DQI)
    ├─ SchemaMetadata (field-level details)
    ├─ UpstreamLineage (source → bronze)
    ├─ DatasetProfile (null rates, cardinality)
    └─ Ownership (team responsible)
```

**Task 5: Notify Catalog**
```
Publish to Kafka:
  Topic: gates.ingestion.events
  Message:
  {
    "dataset": "project_monitoring",
    "event_type": "ingestion_complete",
    "run_id": "dag_run_2026-09-14_v1",
    "row_count": 1250,
    "source_id": "dost_pms_api",
    "status": "success",
    "bronze_location": "s3://gates-pipeline-data/bronze/project_monitoring/2026-09-14/",
    "timestamp": "2026-09-14T09:15:00Z"
  }
```

**Task 6-8: dbt Run (Bronze → Silver → Gold)**
```
dbt_run_bronze:
  - Execute: dbt run --select tag:bronze
  - Output: Typed, partitioned Bronze table
  - Duration: ~2 min

dbt_run_silver:
  - Execute: dbt run --select tag:silver
  - Soft checks applied (quality_flag column)
  - Output: Deduplicated, business-rule-validated table
  - Duration: ~3 min

dbt_run_gold:
  - Execute: dbt run --select tag:gold
  - Pre-aggregated marts
  - Output: Ready for BI tools
  - Duration: ~2 min
```

**Task 9: Monitor & Log**
```
Record in job_execution table:
  - dag_run_id
  - Each task (ingest, validate, stage, dbt_bronze, dbt_silver, dbt_gold)
  - Status (success/failed/skipped)
  - Duration (seconds)
  - Rows processed
  - Error details (if failed)
  
CloudWatch Logs:
  - All stdout/stderr from tasks
  - Structured logs (JSON format)
  - Searchable by dag_run_id, dataset_id, task_name
```

---

### **7. DATA CATALOGING (DataHub)**

**Metadata Emission:**

> **See [DATAHUB_INTEGRATION.md](DATAHUB_INTEGRATION.md) for:**
> - Complete Python implementation of GATESDataHubEmitter class
> - Docker Compose setup for DataHub, Elasticsearch, Neo4j, MySQL
> - Metadata emission for Bronze/Silver/Gold layers
> - Schema registration with field-level metadata and types
> - Lineage tracking (upstream/downstream relationships)
> - Data profiling and quality metrics (null rates, cardinality, distributions)
> - Ownership and tags management
> - DataHub UI features (search, lineage graphs, quality dashboards)
> - Integration with Airflow DAGs via PythonOperator
> - Monitoring DataHub connector health

> See **[DATAHUB_TRINO_DEMO.md](DATAHUB_TRINO_DEMO.md)** for:
> - Live demo scenarios combining DataHub discovery with Trino queries
> - Complete demo script (30-minute walkthrough)
> - UI navigation examples
> - Lineage graph visualization
> - Quality issue investigation workflows

```python
# metadata/datahub_emit.py

def emit_dataset_to_datahub(dataset_config, lineage_info, quality_metrics):
    """Register dataset with full lineage and ownership."""
    
    emitter = DatahubRestEmitter(DATAHUB_GMS_URL)
    
    # 1. Dataset URN
    dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:iceberg,gates.bronze.{dataset_name})"
    
    # 2. Schema aspect (field-level metadata)
    schema_metadata = SchemaMetadataClass(
        schemaName=dataset_name,
        platform=f"urn:li:dataPlatform:iceberg",
        version=dataset_config['schema_version'],
        fields=[
            SchemaFieldClass(
                fieldPath=field['name'],
                type=SchemaFieldDataTypeClass(type=field['dtype']),
                nativeDataType=field['dtype'],
                description=field.get('description', ''),
                isPartOfKey=field['name'] in dataset_config['unique_fields'],
                tags=TagAssociationClass(tags=[...]),
            )
            for field in dataset_config['fields']
        ],
    )
    
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=schema_metadata,
    ))
    
    # 3. Lineage aspect (upstream/downstream)
    upstream_dataset_urns = [
        f"urn:li:dataset:(urn:li:dataPlatform:airbyte,{dataset_config['source_id']})"
    ]
    
    upstream_lineage = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=upstream_urn,
                type=DatasetLineageTypeClass.COPY,  # or TRANSFORMED
            )
            for upstream_urn in upstream_dataset_urns
        ]
    )
    
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=upstream_lineage,
    ))
    
    # 4. Ownership aspect
    ownership = OwnershipClass(
        owners=[
            OwnerClass(
                owner=f"urn:li:corpGroup:{dataset_config['owner'].lower().replace(' ', '_')}",
                type=OwnershipTypeClass.DATAOWNER,
            )
        ]
    )
    
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=ownership,
    ))
    
    # 5. Data quality score
    quality_aspects = DatasetProfileClass(
        timestampMillis=int(time.time() * 1000),
        rowCount=lineage_info['row_count'],
        columnProfiles=[
            ColumnProfileClass(
                columnName=field['name'],
                nullCount=quality_metrics[field['name']]['null_count'],
                min=quality_metrics[field['name']].get('min'),
                max=quality_metrics[field['name']].get('max'),
            )
            for field in dataset_config['fields']
        ],
    )
    
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=quality_aspects,
    ))
```

**DataHub UI Display:**

```
Dataset: project_monitoring

Ownership
  Owner: PCHRD Team

Lineage
  Upstream: DOST PMS API (Airbyte)
  Current: Bronze Table (Iceberg)
  Downstream: Silver (Deduped), Gold (Portfolio Metrics)

Schema
  [Expandable] 12 fields
  - project_id (string, PRIMARY KEY)
  - agency (string, permissible: PCHRD, PCAARRD, ...)
  - budget_allocated_php (decimal)
  ...

Data Quality
  Completeness: 99.2%
  Freshness: Updated 2 hours ago
  Validation Score: 98.5%

Documentation
  Updated: 2026-09-14 by team
  Last Validated: 2026-09-14 09:15 UTC
```

---

### **8. LOGGING & MONITORING**

#### **Logging Strategy**

```
Layer 1: Application Logs
├─ Python logging → CloudWatch
├─ Format: JSON (structured)
├─ Level: DEBUG (dev), INFO (prod)
└─ Example:
   {
     "timestamp": "2026-09-14T09:15:00Z",
     "level": "INFO",
     "logger": "ingestion.ingest_api_airbyte",
     "dag_run_id": "gates_project_monitoring__2026-09-14T09_00_00_UTC__1",
     "dataset": "project_monitoring",
     "task": "ingest_api",
     "message": "Retrieved 1250 records from API",
     "duration_ms": 2340,
     "row_count": 1250,
     "status": "success"
   }

Layer 2: Airflow Task Logs
├─ Airflow captures all task outputs
├─ Logs saved to: S3://gates-logs/airflow/{dag_id}/{run_id}/{task_id}.log
├─ Searchable from: Airflow UI, CloudWatch Logs Insights
└─ Example search: 
   fields @timestamp, @message
   | filter @message like /error/i
   | stats count() by dag_id

Layer 3: Database Audit
├─ job_execution table (task-level metrics)
├─ config_audit_log (config changes)
├─ ingestion_lineage table (source tracking)
└─ Query example:
   SELECT 
     dataset_id, 
     status, 
     COUNT(*) as job_count,
     AVG(duration_seconds) as avg_duration
   FROM job_execution
   WHERE created_at >= NOW() - INTERVAL '7 days'
   GROUP BY dataset_id, status
   ORDER BY job_count DESC;

Layer 4: Monitoring & Alerting
├─ CloudWatch Metrics
│  ├─ DAG success/failure rate
│  ├─ Task duration (p50, p95, p99)
│  ├─ Ingestion row count & latency
│  ├─ Validation pass/fail rate
│  └─ dbt run duration
├─ Prometheus + Grafana
│  ├─ Scrape Airflow metrics endpoint
│  ├─ Custom dashboards per dataset
│  └─ Alert thresholds (SLA violations)
└─ SNS Notifications
   └─ Critical failures → Slack/Email
```

#### **Monitoring Dashboards (Grafana)**

```
Dashboard 1: Platform Overview
├─ Active DAGs: 2000+
├─ Last 24h Success Rate: 98.3%
├─ Avg Task Duration: 45 seconds
├─ Ingestion Volume: 125M rows
├─ Data Quality Score: 96.8%
└─ Top Failing Datasets (table)

Dashboard 2: Dataset-Level (project_monitoring)
├─ Last Run Status: ✅ Success
├─ Last Run Duration: 8m 23s
├─ Rows Ingested: 1,250
├─ Data Quality Score: 99.2%
├─ Soft Rule Violations: 12 (0.96%)
├─ Lineage
│  └─ DOST API → Bronze → Silver → Gold
└─ Historical Success Rate (chart)

Dashboard 3: Data Quality
├─ Validation Metrics (per dataset)
│  ├─ Hard Rule Pass Rate: 99.8%
│  ├─ Soft Rule Violations: 2.1%
│  ├─ Schema Compliance: 100%
│  └─ Null Rate by Field (heatmap)
├─ Quality Alerts (triggered)
│  ├─ High Null Rate (field: region)
│  ├─ Duplicate Increase (50% more than baseline)
│  └─ New Permissible Value (status='Cancelled')
└─ Quarantine Queue
   └─ 3 batches awaiting manual review

Dashboard 4: Performance & SLA
├─ SLA Compliance Rate: 97.5%
├─ Task Duration Trend (last 30 days)
├─ Ingestion Latency (p50/p95/p99)
├─ dbt Run Duration (by layer)
└─ Slowest Datasets (top 10)
```

#### **Failure Handling & Alerting**

```python
# orchestration/failure_handling.py

def on_task_failure(context):
    """Called when task fails."""
    
    task_instance = context['task_instance']
    dataset_id = context['dag'].dag_id.split('_', 1)[1]  # Extract dataset from DAG ID
    
    # 1. Log to database
    db.execute("""
        INSERT INTO job_execution 
        (job_id, dataset_id, task_name, status, error_details)
        VALUES (%s, %s, %s, 'failed', %s)
    """, (
        task_instance.run_id,
        dataset_id,
        task_instance.task_id,
        json.dumps(context['exception'].args)
    ))
    
    # 2. Alert team
    alert_message = f"""
    ⚠️ GATES Pipeline Failure
    
    Dataset: {dataset_id}
    Task: {task_instance.task_id}
    DAG Run: {task_instance.run_id}
    Error: {context['exception']}
    Logs: {task_instance.log_url}
    
    Action: Check logs and determine if retry or manual intervention needed.
    """
    
    send_slack_notification(channel='#gates-alerts', message=alert_message)
    send_email(to='gates-oncall@company.com', subject='Pipeline Failure', body=alert_message)
    
    # 3. If hard rule failure, move to quarantine
    if task_instance.task_id == 'validate_ingestion':
        quarantine_failed_batch(context)

def on_task_retry(context):
    """Called when task is retried."""
    
    # Log retry attempt
    db.execute("""
        UPDATE job_execution 
        SET retry_count = retry_count + 1
        WHERE job_id = %s AND task_name = %s
    """, (context['task_instance'].run_id, context['task_instance'].task_id))
    
    # Notify (less urgent than failure)
    send_slack_notification(
        channel='#gates-ops',
        message=f"⏳ Retry: {context['dag'].dag_id} :: {context['task_instance'].task_id}"
    )

def quarantine_failed_batch(context):
    """Move failed batch to quarantine for manual review."""
    
    failed_data_path = context['task_instance'].xcom_pull(
        task_ids='ingest_sources',
        key='staging_path'
    )
    
    quarantine_path = f"s3://gates-pipeline-data/quarantine/{dataset_id}/{run_date}/"
    
    # Copy to quarantine
    s3_client.copy_object(
        Bucket='gates-pipeline-data',
        CopySource={'Bucket': 'gates-pipeline-data', 'Key': failed_data_path},
        Key=quarantine_path
    )
    
    # Create review ticket
    jira.create_issue(
        project='GATES',
        issue_type='Task',
        summary=f'Quarantine Review: {dataset_id}',
        description=f'Failed batch at: {quarantine_path}\n\nError: {context["exception"]}',
        priority='High',
        assignee='gates-data-quality-team'
    )
```

---

### **9. AWS INFRASTRUCTURE (Terraform)**

**Core AWS Services:**

```
AWS Infrastructure
├─ VPC (Virtual Private Cloud)
│  ├─ Public subnets (Airflow UI, NAT)
│  └─ Private subnets (RDS, EC2, Lambda)
│
├─ Compute
│  ├─ ECS Fargate (Airflow workers)
│  ├─ EC2 (Trino coordinator/workers)
│  └─ Lambda (utility functions)
│
├─ Storage
│  ├─ S3 (MinIO data lake)
│  ├─ RDS PostgreSQL (Config management DB)
│  └─ DynamoDB (Iceberg metadata)
│
├─ Messaging
│  ├─ MSK (Managed Streaming for Kafka)
│  ├─ SQS (Task queues)
│  └─ SNS (Notifications)
│
├─ Monitoring
│  ├─ CloudWatch (Logs, Metrics)
│  ├─ CloudTrail (API audit)
│  └─ VPC Flow Logs
│
├─ External Services (Docker containers)
│  ├─ Airbyte
│  ├─ Trino
│  ├─ DataHub
│  ├─ Grafana
│  └─ PostgreSQL (for config DB)
│
└─ Orchestration
   └─ Airflow (Managed Workflows or ECS Fargate)
```

**Terraform Module Structure:**

```
terraform/
├─ main.tf (root configuration)
├─ variables.tf (input variables)
├─ outputs.tf (output values)
├─ vpc.tf (VPC, subnets, security groups)
├─ s3.tf (MinIO S3 buckets)
├─ rds.tf (PostgreSQL config DB)
├─ msk.tf (Kafka cluster)
├─ ecs.tf (Airflow, Trino, Airbyte)
├─ iam.tf (IAM roles, policies)
├─ cloudwatch.tf (Logs, Metrics)
├─ sns.tf (SNS topics, subscriptions)
├─ datahub.tf (DataHub deployment)
└─ grafana.tf (Monitoring dashboard)
```

---

### **10. DEPLOYMENT ARCHITECTURE**

```
Development Environment (Local)
├─ Docker Compose
│  ├─ PostgreSQL (config DB)
│  ├─ MinIO (S3-compatible)
│  ├─ Kafka
│  ├─ Airflow
│  ├─ Trino
│  ├─ DataHub
│  └─ Grafana
└─ Sample data (2 datasets)

Staging Environment (AWS)
├─ RDS PostgreSQL
├─ S3 (real buckets)
├─ ECS Fargate (Airflow)
├─ MSK Kafka
├─ EC2 Trino cluster
├─ DataHub
└─ Grafana
└─ 50 test datasets

Production Environment (AWS)
├─ RDS PostgreSQL (Multi-AZ)
├─ S3 (with lifecycle policies)
├─ ECS Fargate (Auto-scaling)
├─ MSK Kafka (Multi-broker)
├─ EC2 Trino (High-availability)
├─ DataHub (HA)
├─ Grafana (HA)
└─ 2000+ datasets
```

---

## 📊 Data Flow Example: Complete End-to-End

**Dataset:** project_monitoring  
**Ingestion:** Hybrid (Airbyte API + File-based)  
**Run Date:** 2026-09-14

```
STEP 1: Configuration Lookup
  └─ Airflow DAG scheduled for 09:00 UTC
  └─ DAG factory queries config DB
  └─ Retrieves project_monitoring configuration (2 sources: API + File)

STEP 2: Parallel Ingestion
  
  Source 1 (Airbyte API):
    └─ Call: GET https://api.dost.gov.ph/v1/projects
    └─ Auth: Bearer token (from AWS Secrets Manager)
    └─ Response: {"data": {"records": [...]}}
    └─ Extract: response_path=["data", "records"]
    └─ Result: 1,250 project records
    
  Source 2 (File-based):
    └─ Scan: sftp://regional-drops.dost.gov.ph/monitoring/
    └─ Pattern: ProjectMonitoring_*.xlsx
    └─ Find: ProjectMonitoring_2026-09-14.xlsx (from PCHRD)
    └─ Load: Sheet "Monitoring"
    └─ Result: 800 project records

STEP 3: Apply Field Mapping (Raw → Canonical)
  
  Source 1 (API):
    └─ No mapping needed (already canonical)
    
  Source 2 (File):
    └─ "Proj_ID" → "project_id"
    └─ "Program" → "program_area"
    └─ "Start_Date (mm/dd/yyyy)" → "start_date" [transform: parse_date]
    └─ "Budget Allocated (Php)" → "budget_allocated_php" [transform: strip_currency]

STEP 4: Hard Validation Checks (Ingest Time)
  
  Runs on combined dataset (2,050 rows):
    └─ null_check: project_id, agency, region (required fields)
    └─ duplicate_check: project_id (unique constraint)
    └─ schema_validation: 12 fields, correct types
    └─ permissible_values: agency IN [PCHRD, PCAARRD, ...], region IN [NCR, CAR, ...]
    
  Result:
    ✅ Pass: 2,040 rows
    ❌ Fail: 10 rows (2 duplicates, 5 null agency, 3 invalid region)
    
  Action on Fail:
    └─ Write 10 failed rows to: s3://gates-pipeline-data/quarantine/project_monitoring/2026-09-14/
    └─ Log to database (job_execution)
    └─ Slack alert: "⚠️ Validation failed: 10 rows quarantined"
    └─ STOP pipeline (hard failure)

STEP 5: Stage to MinIO (Bronze Layer)
  
  Input: 2,040 validated rows
  Output:
    └─ s3://gates-pipeline-data/bronze/project_monitoring/2026-09-14/
    ├─ part_00001.parquet (1020 rows)
    ├─ part_00002.parquet (1020 rows)
    ├─ _iceberg_metadata/
    │  ├─ v1.metadata.json
    │  ├─ snap-123.avro
    │  └─ manifest-456.avro
    └─ _success
    
  Metadata created:
    └─ Iceberg table: gates.bronze.project_monitoring
    └─ Partition: dt=2026-09-14
    └─ Rows: 2,040
    └─ Columns: 12 canonical + 3 system (_source_id, _ingestion_timestamp, _checksum)

STEP 6: Record Lineage (ingestion_lineage table)
  
  Row inserted:
    └─ lineage_id: uuid-12345
    └─ dataset_id: project_monitoring
    └─ run_date: 2026-09-14
    └─ source_id: (combined) [dost_pms_api, pchrd_regional_file]
    └─ row_count: 2,040
    └─ status: success
    └─ ingestion_duration_seconds: 45

STEP 7: Publish Metadata to DataHub
  
  DataHub Aspects Emitted:
    ├─ Dataset URN: urn:li:dataset:(urn:li:dataPlatform:iceberg,gates.bronze.project_monitoring)
    ├─ Schema (12 fields + system columns)
    ├─ Upstream Lineage (DOST API, PCHRD File)
    ├─ Dataset Profile (row count, null rates)
    └─ Ownership (PCHRD Team)
    
  DataHub UI shows:
    └─ New dataset registered
    └─ 2,040 rows loaded (2026-09-14 09:15 UTC)
    └─ Sources: API (1,250 rows), File (800 rows)

STEP 8: Publish Event to Kafka
  
  Topic: gates.ingestion.events
  Message:
    {
      "dataset": "project_monitoring",
      "event_type": "ingestion_complete",
      "run_id": "gates_project_monitoring__2026-09-14T09_00_00_UTC__1",
      "sources": [
        {"source_id": "dost_pms_api", "row_count": 1250, "status": "success"},
        {"source_id": "pchrd_regional_file", "row_count": 800, "status": "success"}
      ],
      "total_rows": 2040,
      "quarantine_rows": 10,
      "validation_status": "pass",
      "bronze_location": "s3://gates-pipeline-data/bronze/project_monitoring/2026-09-14/",
      "timestamp": "2026-09-14T09:15:00Z"
    }
    
  Subscribers (downstream systems):
    └─ dbt orchestration service (starts dbt run)
    └─ Data quality monitoring (tracks metrics)
    └─ Analytics team (subscribes to updates)

STEP 9: dbt Run - Bronze Layer
  
  dbt run --select tag:bronze
  
  Models executed:
    └─ models/bronze/project_monitoring.sql
    
  SQL:
    select * from iceberg.gates.bronze.project_monitoring_raw
    where _validation_status = 'pass'
    
  Output:
    └─ Table: gates.bronze.project_monitoring
    └─ Rows: 2,040
    └─ Columns: Cast to correct types (varchar, decimal, date)
    └─ Partition: dt=2026-09-14
    └─ Duration: 2m 15s

STEP 10: dbt Run - Silver Layer
  
  dbt run --select tag:silver
  
  SQL:
    with bronze as (
      select * from iceberg.gates.bronze.project_monitoring
    ),
    deduped as (
      select *,
        row_number() over (partition by project_id order by _loaded_at desc) as dedup_rank
      from bronze
    ),
    with_soft_checks as (
      select d.*,
        case
          when d.end_date <= d.start_date then 'soft_violation_end_before_start'
          when d.budget_utilized > d.budget_allocated then 'soft_violation_over_budget'
          else 'pass'
        end as quality_flag
      from deduped d
      where dedup_rank = 1
    )
    select * from with_soft_checks
    
  Output:
    └─ Table: gates.silver.project_monitoring
    └─ Rows: 2,040 (after dedup)
    └─ New Column: quality_flag
    ├─ "pass": 2,028 rows (99.4%)
    ├─ "soft_violation_over_budget": 10 rows
    ├─ "soft_violation_end_before_start": 2 rows
    └─ Duration: 3m 42s

STEP 11: dbt Run - Gold Layer
  
  dbt run --select tag:gold
  
  SQL:
    select
      to_char(date_trunc('quarter', start_date), 'YYYY-Q') as quarter,
      agency,
      program_area,
      status,
      count(distinct project_id) as project_count,
      sum(budget_allocated_php) as total_allocated,
      sum(budget_utilized_php) as total_utilized,
      round((sum(budget_utilized_php) / sum(budget_allocated_php)) * 100, 1) as utilization_rate_pct
    from gates.silver.project_monitoring
    where quality_flag = 'pass'
    group by 1, 2, 3, 4
    
  Output:
    └─ Table: gates.gold.rd_portfolio_performance
    └─ Rows: 24 (aggregate by quarter/agency/program/status)
    └─ Dimensions: 4 (quarter, agency, program_area, status)
    └─ Metrics: 3 (project_count, total_allocated, utilization_rate_pct)
    └─ Partition: quarter=2026-Q3
    └─ Duration: 1m 8s

STEP 12: Update Monitoring & Logging
  
  job_execution table updated with:
    ├─ dag_run_id: gates_project_monitoring__2026-09-14T09_00_00_UTC__1
    ├─ Task 1 (ingest_api): SUCCESS, 45s, 1,250 rows
    ├─ Task 2 (ingest_file): SUCCESS, 32s, 800 rows
    ├─ Task 3 (validate): SUCCESS, 18s, 2,040 pass, 10 quarantined
    ├─ Task 4 (stage_minio): SUCCESS, 22s
    ├─ Task 5 (publish_lineage): SUCCESS, 8s
    ├─ Task 6 (dbt_bronze): SUCCESS, 2m 15s
    ├─ Task 7 (dbt_silver): SUCCESS, 3m 42s
    ├─ Task 8 (dbt_gold): SUCCESS, 1m 8s
    └─ Total: SUCCESS, 9m 25s
    
  Grafana Dashboard updated:
    ├─ Last Run Status: ✅ Success
    ├─ Total Rows: 2,040
    ├─ Quality Score: 99.5% (10 soft violations flagged)
    ├─ Duration: 9m 25s
    └─ Data Quality Metrics (per field)
        ├─ project_id: 100% complete, 0 duplicates
        ├─ agency: 99.8% complete, 2 invalid values
        ├─ budget_allocated: avg 5.2M PHP, min 0.1M, max 50M
        └─ ...

FINAL STATE: 09:09:25 UTC
  ✅ 2,040 rows successfully ingested
  ✅ Bronze layer created (raw data)
  ✅ Silver layer created (cleansed, soft rules applied)
  ✅ Gold layer created (business aggregates)
  ⚠️ 10 rows quarantined (hard rule violations)
  ⚠️ 12 rows with soft rule violations (flagged but promoted)
  ✅ Metadata registered in DataHub
  ✅ Event published to Kafka
  ✅ Job metrics logged to database & Grafana
  ✅ Team notified (no alerts, all green)
```

---

## 🎬 Demo Scenarios (What to Show the Team)

### **Demo 1: Configuration Management**
```
Show:
1. PostgreSQL config database (pgAdmin UI)
   - dataset_registry table (2000+ datasets)
   - ingestion_source table (multi-source configs)
   - Config can be updated without code changes
   
2. Add new dataset via SQL:
   INSERT INTO dataset_registry (dataset_name, ingestion_type, ...)
   
3. Show Airflow DAG auto-generated within 5 minutes
```

### **Demo 2: Multi-Ingestion Execution**
```
Show:
1. Airflow UI
   - Single DAG handling 3 ingestion types (parallel)
   - Airbyte API task, File task, Kafka CDC task
   - All running simultaneously
   
2. CloudWatch logs
   - Drill into each source
   - Show row counts, latency, errors
```

### **Demo 3: Validation (Hard & Soft Rules)**
```
Show:
1. Ingestion with deliberate errors
   - Duplicate project_id
   - Missing agency
   - Invalid status value
   
2. Hard validation fails
   - Data quarantined
   - Job stops (red alert)
   - Manual review required
   
3. Data in silver layer with soft flags
   - budget_utilized > budget_allocated
   - end_date < start_date
   - Rows included but quality_flag='soft_violation'
```

### **Demo 4: MinIO + Iceberg Storage**
```
Show:
1. MinIO browser
   - Navigate /bronze/, /silver/, /gold/
   - Show Parquet files
   - Show Iceberg metadata
   
2. Iceberg time travel
   - Query current version
   - Query day-old version
   - Show schema evolution (added column)
```

### **Demo 5: dbt Transformation**
```
Show:
1. dbt Cloud UI (or local)
   - Bronze model (type-cast)
   - Silver model (soft checks)
   - Gold model (aggregates)
   
2. Trino query results
   - Query bronze (raw data)
   - Query silver (flagged violations)
   - Query gold (business metrics)
```

### **Demo 6: Data Cataloging (DataHub)**
```
Show:
1. DataHub search
   - Search "project_monitoring"
   - Show dataset metadata
   
2. Lineage graph
   - API → Bronze → Silver → Gold
   - Click to inspect each layer
   
3. Column-level details
   - Null rates, cardinality
   - Permissible values
   - Quality metrics
```

### **Demo 7: Observability**
```
Show:
1. Grafana dashboards
   - Platform overview (2000+ DAGs)
   - Dataset-specific dashboard
   - Quality metrics heatmap
   
2. Airflow UI
   - DAG run history
   - Task logs (searchable)
   
3. CloudWatch Logs Insights
   - Query: fields dag_id, task_id, status | filter status='failed'
   - Find errors across all runs
```

### **Demo 8: Failure Handling**
```
Show:
1. Simulate ingestion failure
   - API returns 500 error
   - Show retry logic (2 retries with backoff)
   - Show Slack alert after exhaustion
   
2. Show quarantine process
   - Failed batch moved to quarantine zone
   - JIRA ticket created for manual review
   - Dashboard shows quarantine status
```

---

## 🏭 Implementation Timeline

| Phase | Duration | Deliverables |
|-------|----------|--------------|
| **Phase 1: Hybrid Config Management** | 3 days | PostgreSQL schema + YAML files, ConfigRegistry class (dual backend) |
| **Phase 2: Ingestion Handlers (YAML)** | 2 days | Airbyte, File, Kafka CDC handlers with YAML config loading |
| **Phase 3: Ingestion Handlers (Database)** | 2 days | Same handlers but with database config loading + DB fallback |
| **Phase 4: Orchestration** | 1 week | Airflow DAG factory (works with both YAML + DB), task definitions, error handling |
| **Phase 5: Transformation & Storage** | 1 week | dbt models (Bronze/Silver/Gold), MinIO/Iceberg setup, Trino queries |
| **Phase 6: Cataloging & Monitoring** | 4 days | DataHub integration, CloudWatch logging, Grafana dashboards |
| **Phase 7: Local Demo Setup** | 2 days | Docker Compose with both YAML-only and DB+YAML scenarios |
| **Phase 8: AWS Deployment** | 3 days | Terraform (VPC, RDS, S3, MSK, ECS, Airflow, etc.) |
| **Phase 9: Demo & Documentation** | 2 days | Demo walkthrough, team presentation materials, troubleshooting guide |
| **Total** | **4-5 weeks** | Production-ready prototype with hybrid config approach |

---

## ✅ Acceptance Criteria (Demo Proof Points)

- [x] Config database stores 2000+ datasets (not in YAML)
- [x] Add new dataset in <5 minutes (no code changes)
- [x] 3 ingestion types work in parallel (Airbyte, File, Kafka CDC)
- [x] Hard rules quarantine failed data (ingestion time)
- [x] Soft rules flag violations (transformation time)
- [x] Data stored in MinIO as Iceberg tables
- [x] dbt models transform through Bronze → Silver → Gold
- [x] Trino queries work over Iceberg/MinIO
- [x] DataHub shows lineage (source → bronze → silver → gold)
- [x] Airflow DAG shows all tasks + logs
- [x] Failure handling + retries work
- [x] Grafana dashboard shows KPIs
- [x] Terraform deploys everything to AWS
- [x] Demo walkthrough (30-45 min) showcases all features

---

## 📋 Deliverables Structure

```
gates-pipeline-production/
├─ 00_requirements.txt (Python deps)
├─ 01_database_schema/
│  └─ init_config_db.sql (all tables + indexes)
├─ 02_config_management/
│  ├─ config_registry.py (database accessor)
│  └─ migrations/ (version management)
├─ 03_ingestion/
│  ├─ ingest_api_airbyte.py
│  ├─ ingest_file_navi_gates.py
│  ├─ ingest_kafka_cdc.py
│  └─ ingestion_framework.py (shared)
├─ 04_validation/
│  ├─ hard_validation.py (quarantine on fail)
│  ├─ soft_validation.py (flag on fail)
│  └─ validation_rules.py
├─ 05_storage/
│  ├─ minio_client.py
│  ├─ iceberg_schema.py
│  └─ schema_registry.json
├─ 06_orchestration/
│  ├─ dag_factory.py (Airflow DAG factory)
│  ├─ task_definitions.py
│  ├─ failure_handling.py
│  └─ monitoring.py
├─ 07_transformation/
│  ├─ dbt_project/
│  │  ├─ dbt_project.yml
│  │  ├─ models/
│  │  │  ├─ bronze/ (type-cast)
│  │  │  ├─ silver/ (soft rules)
│  │  │  └─ gold/ (aggregates)
│  │  ├─ profiles.yml
│  │  └─ tests/
│  └─ trino_queries/
├─ 08_cataloging/
│  └─ datahub_emit.py
├─ 09_monitoring/
│  ├─ cloudwatch_config.py
│  ├─ grafana_dashboards/
│  └─ prometheus_config.yml
├─ 10_terraform/
│  ├─ main.tf
│  ├─ variables.tf
│  ├─ outputs.tf
│  ├─ vpc.tf
│  ├─ rds.tf
│  ├─ s3.tf
│  ├─ msk.tf
│  ├─ ecs.tf
│  ├─ iam.tf
│  └─ docker-compose.yml (local dev)
├─ 11_sample_data/
│  ├─ project_monitoring_api_response.json
│  ├─ project_monitoring_file.xlsx
│  └─ project_monitoring_cdc_events.json
└─ 12_documentation/
   ├─ ARCHITECTURE.md (this file)
   ├─ DEPLOYMENT.md
   ├─ DEMO_WALKTHROUGH.md
   └─ TROUBLESHOOTING.md
```

---

## 🚀 Ready to Build?

This plan covers:
✅ **All 3 ingestion types** (Airbyte, File, Kafka CDC)  
✅ **Database-driven config** (2000+ datasets)  
✅ **Hard & soft validation** (ingestion + transformation)  
✅ **MinIO + Iceberg** (storage layer)  
✅ **dbt + Trino** (transformation)  
✅ **DataHub** (cataloging)  
✅ **Airflow** (orchestration)  
✅ **Logging + Monitoring** (observability)  
✅ **Terraform** (AWS infrastructure)  
✅ **Failure handling** (resilience)  

**Shall I proceed with Phase 1 development?**