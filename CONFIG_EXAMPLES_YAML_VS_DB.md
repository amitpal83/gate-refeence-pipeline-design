# Config Examples: YAML vs Database Side-by-Side

## Dataset: project_monitoring

---

## 📄 YAML Configuration (Option A)

### File 1: `config/schema_project_monitoring.yaml`

```yaml
dataset: project_monitoring
version: "1.0"
description: >
  R&D Project Monitoring data pipeline.
  Ingests from API, regional file, and Kafka CDC.

canonical_schema:
  unique_fields:
    - project_id
  
  fields:
    - name: project_id
      dtype: string
      required: true
      unique: true
      description: "Unique project identifier (e.g., PCHRD-2026-0142)"
    
    - name: agency
      dtype: string
      required: true
      permissible_values: [PCHRD, PCAARRD, PCIEERD, ASTI, FNRI, ITDI, MIRDC, TAPI]
    
    - name: region
      dtype: string
      required: true
      permissible_values: [NCR, CAR, R1, R2, R3, R4A, R4B, R5, R6, R7, R8, R9, R10, R11, R12, R13, NIR]
    
    - name: program_area
      dtype: string
      required: true
      permissible_values: [Health, Agriculture, Industry, Basic Research, ICT, Disaster Resilience]
    
    - name: project_title
      dtype: string
      required: true
    
    - name: principal_investigator
      dtype: string
      required: true
    
    - name: start_date
      dtype: date
      required: true
      format: "%Y-%m-%d"
    
    - name: end_date
      dtype: date
      required: false
      format: "%Y-%m-%d"
    
    - name: budget_allocated_php
      dtype: float
      required: true
      min: 0
    
    - name: budget_utilized_php
      dtype: float
      required: true
      min: 0
    
    - name: status
      dtype: string
      required: true
      permissible_values: [Proposed, Ongoing, Completed, Terminated, On-Hold]
    
    - name: funding_source
      dtype: string
      required: true
      permissible_values: [GAA, Special Trust Fund, Foreign-Assisted, Counterpart]

sources:
  - source_id: dost_pms_api
    channel: api
    description: "DOST PMS API - Direct API calls"
    
  - source_id: pchrd_regional_file
    channel: file
    file_format: xlsx
    agency: PCHRD
    region: R4A
    description: "Regional PCHRD Excel submissions"
    
  - source_id: kafka_cdc_updates
    channel: kafka
    topic: db-changes.project_monitoring
    description: "Real-time CDC from source database"
```

### File 2: `config/column_aliasing_project_monitoring.yaml`

```yaml
dataset: project_monitoring

sources:
  
  # Source 1: Airbyte API (already canonical)
  - source_id: dost_pms_api
    channel: api
    aliases: []  # Empty array = no transformation needed
    note: "API field names match canonical schema 1:1"
  
  # Source 2: Regional file (extensive aliasing + transforms)
  - source_id: pchrd_regional_file
    channel: file
    aliases:
      - source_field: "Proj_ID"
        canonical_field: "project_id"
      
      - source_field: "Program"
        canonical_field: "program_area"
      
      - source_field: "Project Title"
        canonical_field: "project_title"
      
      - source_field: "PI Name"
        canonical_field: "principal_investigator"
      
      - source_field: "Start_Date (mm/dd/yyyy)"
        canonical_field: "start_date"
        transform: "parse_date(mm/dd/yyyy -> ISO 8601)"
      
      - source_field: "End_Date (mm/dd/yyyy)"
        canonical_field: "end_date"
        transform: "parse_date(mm/dd/yyyy -> ISO 8601)"
      
      - source_field: "Budget Allocated (Php)"
        canonical_field: "budget_allocated_php"
        transform: "strip_currency_formatting -> float"
      
      - source_field: "Budget Utilized (Php)"
        canonical_field: "budget_utilized_php"
        transform: "strip_currency_formatting -> float"
      
      - source_field: "Status"
        canonical_field: "status"
      
      - source_field: "Fund Source"
        canonical_field: "funding_source"
  
  # Source 3: Kafka CDC (CDC format → canonical)
  - source_id: kafka_cdc_updates
    channel: kafka
    aliases:
      # Assume CDC payload has same field names (after Debezium transforms)
      - source_field: "project_id"
        canonical_field: "project_id"
      
      - source_field: "agency_code"
        canonical_field: "agency"
      
      - source_field: "region_code"
        canonical_field: "region"
      
      # ... etc for all fields
```

### File 3: `config/common_quality_rules.yaml`

```yaml
rule_categories:
  
  # HARD RULES (Quarantine on failure)
  - name: null_check
    applies_to: "Required fields (required: true)"
    severity: hard
    ge_expectation: expect_column_values_to_not_be_null
  
  - name: duplicate_check
    applies_to: "Unique fields (project_id)"
    severity: hard
    ge_expectation: expect_column_values_to_be_unique
  
  - name: schema_validation
    applies_to: "All columns"
    severity: hard
    ge_expectation: [expect_table_columns_to_match_set, expect_column_values_to_be_of_type]
  
  # SOFT RULES (Flag but allow)
  - name: permissible_values
    applies_to: "Fields with permissible_values list"
    severity: soft
    ge_expectation: expect_column_values_to_be_in_set

on_failure:
  hard_rules: [null_check, duplicate_check, schema_validation]
  hard_rule_action: quarantine  # Stop pipeline
  soft_rules: [permissible_values]
  soft_rule_action: flag_and_promote  # Continue with flag
```

### File 4: `config/airbyte_manifest.yaml` (API details)

```yaml
definitions:
  requester:
    url_base: "https://api.dost.gov.ph"
    http_method: GET
    authenticator:
      type: BearerTokenAuthenticator
      api_token: "{{ env('DOST_API_TOKEN') }}"
    request_options:
      timeout_seconds: 30
      max_retries: 3
      backoff_strategy: exponential

  project_monitoring_stream:
    type: DeclarativeStream
    name: project_monitoring
    $parameters:
      path: "/v1/projects"
    retriever:
      type: SimpleRetriever
      requester: "#/definitions/requester"
      record_selector:
        type: RecordSelector
        extractor:
          type: DPath
          field_path: ["data", "records"]
      paginator:
        type: DefaultPaginator
        pagination_strategy:
          type: OffsetIncrement
          page_size: 1000
        page_size_option:
          field_name: limit
          inject_into: request_parameter
        page_token_option:
          field_name: offset
          inject_into: request_parameter
```

---

## 💾 Database Configuration (Option B)

### SQL Initialization Script: `init_config_db.sql`

```sql
-- 1. dataset_registry table
INSERT INTO dataset_registry 
(dataset_id, dataset_name, version, description, ingestion_type, status, owner, created_at)
VALUES (
  '12345678-1234-1234-1234-123456789012',
  'project_monitoring',
  '1.0',
  'R&D Project Monitoring data pipeline. Ingests from API, regional file, and Kafka CDC.',
  'hybrid',
  'active',
  'PCHRD Team',
  NOW()
);

-- 2. ingestion_schema table
INSERT INTO ingestion_schema
(schema_id, dataset_id, schema_version, fields, unique_fields, status, created_at, created_by)
VALUES (
  '87654321-4321-4321-4321-210987654321',
  '12345678-1234-1234-1234-123456789012',
  1,
  '[
    {
      "name": "project_id",
      "dtype": "string",
      "required": true,
      "unique": true,
      "description": "Unique project identifier"
    },
    {
      "name": "agency",
      "dtype": "string",
      "required": true,
      "permissible_values": ["PCHRD", "PCAARRD", "PCIEERD", "ASTI", "FNRI", "ITDI", "MIRDC", "TAPI"]
    },
    {
      "name": "region",
      "dtype": "string",
      "required": true,
      "permissible_values": ["NCR", "CAR", "R1", "R2", "R3", "R4A", "R4B", "R5", "R6", "R7", "R8", "R9", "R10", "R11", "R12", "R13", "NIR"]
    },
    {
      "name": "program_area",
      "dtype": "string",
      "required": true,
      "permissible_values": ["Health", "Agriculture", "Industry", "Basic Research", "ICT", "Disaster Resilience"]
    },
    {
      "name": "project_title",
      "dtype": "string",
      "required": true
    },
    {
      "name": "principal_investigator",
      "dtype": "string",
      "required": true
    },
    {
      "name": "start_date",
      "dtype": "date",
      "required": true,
      "format": "%Y-%m-%d"
    },
    {
      "name": "end_date",
      "dtype": "date",
      "required": false,
      "format": "%Y-%m-%d"
    },
    {
      "name": "budget_allocated_php",
      "dtype": "float",
      "required": true,
      "min": 0
    },
    {
      "name": "budget_utilized_php",
      "dtype": "float",
      "required": true,
      "min": 0
    },
    {
      "name": "status",
      "dtype": "string",
      "required": true,
      "permissible_values": ["Proposed", "Ongoing", "Completed", "Terminated", "On-Hold"]
    },
    {
      "name": "funding_source",
      "dtype": "string",
      "required": true,
      "permissible_values": ["GAA", "Special Trust Fund", "Foreign-Assisted", "Counterpart"]
    }
  ]'::jsonb,
  '["project_id"]'::text[],
  'active',
  NOW(),
  'admin'
);

-- 3. ingestion_source table (3 sources)

-- Source 1: Airbyte API
INSERT INTO ingestion_source
(source_id, dataset_id, source_type, channel, priority, config, status, created_at)
VALUES (
  'dost_pms_api',
  '12345678-1234-1234-1234-123456789012',
  'airbyte',
  'api',
  1,
  '{
    "url_base": "https://api.dost.gov.ph",
    "endpoint": "/v1/projects",
    "auth": {
      "type": "bearer_token",
      "token_env_var": "DOST_API_TOKEN"
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
  }'::jsonb,
  'active',
  NOW()
);

-- Source 2: File-based (Regional)
INSERT INTO ingestion_source
(source_id, dataset_id, source_type, channel, priority, config, status, created_at)
VALUES (
  'pchrd_regional_file',
  '12345678-1234-1234-1234-123456789012',
  'file',
  'file_sftp',
  2,
  '{
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
    "multi_file_strategy": "concatenate",
    "polling_interval": "weekly"
  }'::jsonb,
  'active',
  NOW()
);

-- Source 3: Kafka CDC
INSERT INTO ingestion_source
(source_id, dataset_id, source_type, channel, priority, config, status, created_at)
VALUES (
  'kafka_cdc_updates',
  '12345678-1234-1234-1234-123456789012',
  'kafka',
  'kafka_topic',
  3,
  '{
    "bootstrap_servers": "kafka-broker-1:9092,kafka-broker-2:9092",
    "topic": "db-changelog.project_monitoring",
    "consumer_group": "gates-project-monitoring-cdc",
    "message_format": "debezium_json",
    "filter": {
      "operations": ["c", "u"]
    },
    "start_offset": "latest",
    "batch_window_seconds": 300,
    "source_database": {
      "type": "postgresql",
      "host": "source-db.rds.amazonaws.com",
      "database": "research_db",
      "tables": ["projects", "milestones", "budget"]
    },
    "schema_registry_url": "https://schema-registry:8081",
    "security": {
      "protocol": "SASL_SSL",
      "sasl_mechanism": "PLAIN",
      "credentials_env_var": "KAFKA_CREDENTIALS"
    }
  }'::jsonb,
  'active',
  NOW()
);

-- 4. field_mapping table (for non-API sources)

-- File-based source mappings
INSERT INTO field_mapping (mapping_id, schema_id, source_id, raw_field, canonical_field, transform)
VALUES
  ('f1-map-001', '87654321-4321-4321-4321-210987654321', 'pchrd_regional_file', 'Proj_ID', 'project_id', NULL),
  ('f1-map-002', '87654321-4321-4321-4321-210987654321', 'pchrd_regional_file', 'Program', 'program_area', NULL),
  ('f1-map-003', '87654321-4321-4321-4321-210987654321', 'pchrd_regional_file', 'Project Title', 'project_title', NULL),
  ('f1-map-004', '87654321-4321-4321-4321-210987654321', 'pchrd_regional_file', 'PI Name', 'principal_investigator', NULL),
  ('f1-map-005', '87654321-4321-4321-4321-210987654321', 'pchrd_regional_file', 'Start_Date (mm/dd/yyyy)', 'start_date', 'parse_date(mm/dd/yyyy -> ISO 8601)'),
  ('f1-map-006', '87654321-4321-4321-4321-210987654321', 'pchrd_regional_file', 'End_Date (mm/dd/yyyy)', 'end_date', 'parse_date(mm/dd/yyyy -> ISO 8601)'),
  ('f1-map-007', '87654321-4321-4321-4321-210987654321', 'pchrd_regional_file', 'Budget Allocated (Php)', 'budget_allocated_php', 'strip_currency_formatting -> float'),
  ('f1-map-008', '87654321-4321-4321-4321-210987654321', 'pchrd_regional_file', 'Budget Utilized (Php)', 'budget_utilized_php', 'strip_currency_formatting -> float'),
  ('f1-map-009', '87654321-4321-4321-4321-210987654321', 'pchrd_regional_file', 'Status', 'status', NULL),
  ('f1-map-010', '87654321-4321-4321-4321-210987654321', 'pchrd_regional_file', 'Fund Source', 'funding_source', NULL);

-- Kafka CDC mappings
INSERT INTO field_mapping (mapping_id, schema_id, source_id, raw_field, canonical_field, transform)
VALUES
  ('f1-map-k01', '87654321-4321-4321-4321-210987654321', 'kafka_cdc_updates', 'project_id', 'project_id', NULL),
  ('f1-map-k02', '87654321-4321-4321-4321-210987654321', 'kafka_cdc_updates', 'agency_code', 'agency', NULL),
  ('f1-map-k03', '87654321-4321-4321-4321-210987654321', 'kafka_cdc_updates', 'region_code', 'region', NULL),
  -- ... etc for all fields

-- 5. validation_rule table

-- Hard rule: null_check
INSERT INTO validation_rule
(rule_id, dataset_id, field_name, rule_type, severity, rule_config)
SELECT 
  gen_random_uuid(), 
  '12345678-1234-1234-1234-123456789012',
  field->>'name',
  'null_check',
  'hard',
  '{"required": true}'::jsonb
FROM ingestion_schema, jsonb_array_elements(fields) AS field
WHERE dataset_id = '12345678-1234-1234-1234-123456789012'
  AND field->>'required' = 'true';

-- Hard rule: duplicate_check
INSERT INTO validation_rule
(rule_id, dataset_id, field_name, rule_type, severity, rule_config)
VALUES ('rule-dup-01', '12345678-1234-1234-1234-123456789012', 'project_id', 'duplicate_check', 'hard', '{}'::jsonb);

-- Soft rule: permissible_values
INSERT INTO validation_rule
(rule_id, dataset_id, field_name, rule_type, severity, rule_config)
VALUES 
  ('rule-perm-01', '12345678-1234-1234-1234-123456789012', 'agency', 'permissible_values', 'soft', 
   '{"expected_values": ["PCHRD", "PCAARRD", "PCIEERD", "ASTI", "FNRI", "ITDI", "MIRDC", "TAPI"]}'::jsonb),
  ('rule-perm-02', '12345678-1234-1234-1234-123456789012', 'status', 'permissible_values', 'soft',
   '{"expected_values": ["Proposed", "Ongoing", "Completed", "Terminated", "On-Hold"]}'::jsonb);

-- Soft rule: budget_utilization
INSERT INTO validation_rule
(rule_id, dataset_id, field_name, rule_type, severity, rule_config)
VALUES ('rule-budget-01', '12345678-1234-1234-1234-123456789012', '*', 'custom', 'soft',
  '{
    "rule_name": "budget_utilization",
    "logic": "budget_utilized <= budget_allocated",
    "description": "Budget utilized cannot exceed budget allocated"
  }'::jsonb);
```

---

## 🔄 How ConfigRegistry Handles Both

### **Query Execution Pattern**

```python
# The same code works with both backends:

config_registry = ConfigRegistry(
    yaml_dir="config/",
    db_url="postgresql://...",  # Optional
    use_db=True  # Prefer DB, fallback to YAML
)

# Application doesn't care which backend is used:
config = config_registry.get_dataset("project_monitoring")

# Automatic selection:
# - If DB is available → Returns config from database (Option B)
# - If DB is unavailable → Falls back to YAML (Option A)
# - Caches for 60 seconds for performance

print(f"Config source: {config['_config_source']}")
# Output: "database" or "yaml" (transparent to caller)
```

---

## 📊 Comparison: Same Data, Different Format

| Aspect | YAML | Database |
|--------|------|----------|
| **Schema Definition** | JSONB in `fields` file | JSONB column in table |
| **Aliasing Rules** | YAML aliases array | field_mapping table rows |
| **Validation Rules** | YAML rule_categories | validation_rule table rows |
| **Update Process** | Git commit → merge | SQL UPDATE → audit log |
| **Query Performance** | File I/O (slower) | DB index (faster) |
| **Audit Trail** | Git history | config_audit_log table |
| **Scalability** | ~100 datasets | ~1000+ datasets |

---

## ✅ Demo: Show Both Side-by-Side

```
Demo Timeline: 15 minutes

PART 1 (5 min): YAML Configuration
  - Open config/schema_project_monitoring.yaml
  - Show field definitions
  - Show how Git tracks changes
  - Run: git log --oneline -- config/

PART 2 (5 min): Database Configuration
  - Connect to PostgreSQL (pgAdmin)
  - Query: SELECT * FROM dataset_registry;
  - Query: SELECT * FROM ingestion_source;
  - Show audit_log: who changed what when

PART 3 (5 min): ConfigRegistry Bridge
  - Show ConfigRegistry class code
  - Run: config_registry.get_dataset("project_monitoring")
  - Show output: "_config_source": "database"
  - Simulate DB down → show fallback to YAML
  - Explain: Best of both worlds!
```
