CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS dataset_registry (
    dataset_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_name text NOT NULL UNIQUE,
    version text NOT NULL,
    description text,
    ingestion_type text NOT NULL,
    status text NOT NULL DEFAULT 'active',
    owner text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ingestion_schema (
    schema_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id uuid NOT NULL REFERENCES dataset_registry(dataset_id),
    schema_version integer NOT NULL,
    fields jsonb NOT NULL,
    unique_fields jsonb NOT NULL DEFAULT '[]'::jsonb,
    status text NOT NULL DEFAULT 'active',
    created_at timestamptz NOT NULL DEFAULT now(),
    created_by text NOT NULL DEFAULT 'system',
    UNIQUE (dataset_id, schema_version)
);

CREATE TABLE IF NOT EXISTS ingestion_source (
    source_id text PRIMARY KEY,
    dataset_id uuid NOT NULL REFERENCES dataset_registry(dataset_id),
    source_type text NOT NULL,
    config jsonb NOT NULL DEFAULT '{}'::jsonb,
    priority integer NOT NULL DEFAULT 100,
    status text NOT NULL DEFAULT 'active',
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS field_mapping (
    mapping_id bigserial PRIMARY KEY,
    schema_id uuid NOT NULL REFERENCES ingestion_schema(schema_id),
    source_id text NOT NULL REFERENCES ingestion_source(source_id),
    raw_field text NOT NULL,
    canonical_field text NOT NULL,
    transform text,
    UNIQUE (schema_id, source_id, raw_field)
);

CREATE TABLE IF NOT EXISTS validation_rule (
    rule_id text PRIMARY KEY,
    dataset_id uuid NOT NULL REFERENCES dataset_registry(dataset_id),
    field_name text,
    rule_type text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('hard', 'soft')),
    rule_config jsonb NOT NULL DEFAULT '{}'::jsonb,
    active boolean NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS config_audit_log (
    audit_id bigserial PRIMARY KEY,
    dataset_id uuid REFERENCES dataset_registry(dataset_id),
    table_modified text NOT NULL,
    change_type text NOT NULL,
    changed_by text NOT NULL,
    reason text,
    new_value jsonb,
    changed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS job_execution (
    execution_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_name text NOT NULL,
    dag_run_id text NOT NULL,
    batch_id text,
    status text NOT NULL,
    rows_received bigint DEFAULT 0,
    rows_bronze bigint DEFAULT 0,
    rows_silver bigint DEFAULT 0,
    rows_gold bigint DEFAULT 0,
    hard_failures integer DEFAULT 0,
    soft_warnings integer DEFAULT 0,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    error_message text
);

CREATE INDEX IF NOT EXISTS ingestion_source_dataset_idx ON ingestion_source(dataset_id);
CREATE INDEX IF NOT EXISTS validation_rule_dataset_idx ON validation_rule(dataset_id);
CREATE INDEX IF NOT EXISTS job_execution_dataset_idx ON job_execution(dataset_name, started_at DESC);

INSERT INTO dataset_registry (dataset_name, version, description, ingestion_type, owner)
VALUES ('project_monitoring', '1.0', 'R&D project monitoring demo dataset', 'hybrid', 'PCHRD M&E unit')
ON CONFLICT (dataset_name) DO NOTHING;

WITH dataset AS (
    SELECT dataset_id FROM dataset_registry WHERE dataset_name = 'project_monitoring'
), schema_row AS (
    INSERT INTO ingestion_schema (dataset_id, schema_version, fields, unique_fields)
    SELECT dataset_id, 1, $$[
      {"name":"project_id","dtype":"string","required":true},
      {"name":"agency","dtype":"string","required":true},
      {"name":"region","dtype":"string","required":true},
      {"name":"program_area","dtype":"string","required":true},
      {"name":"project_title","dtype":"string","required":true},
      {"name":"principal_investigator","dtype":"string","required":true},
      {"name":"start_date","dtype":"date","required":true},
      {"name":"end_date","dtype":"date","required":false},
      {"name":"budget_allocated_php","dtype":"float","required":true},
      {"name":"budget_utilized_php","dtype":"float","required":true},
      {"name":"status","dtype":"string","required":true},
      {"name":"funding_source","dtype":"string","required":true}
    ]$$::jsonb, '["project_id"]'::jsonb
    FROM dataset
    ON CONFLICT (dataset_id, schema_version) DO NOTHING
    RETURNING schema_id, dataset_id
)
SELECT 1;

INSERT INTO ingestion_source (source_id, dataset_id, source_type, config, priority)
SELECT source_id, dataset_id, source_type, config::jsonb, priority
FROM (
    SELECT 'dost_pms_api' source_id, dataset_id, 'airbyte_api' source_type,
           '{"connection_id":"project_monitoring_airbyte","stream":"projects"}' config, 10 priority FROM dataset_registry WHERE dataset_name = 'project_monitoring'
    UNION ALL
    SELECT 'pchrd_regional_file_dropbox', dataset_id, 'file',
           '{"folder":"/data/incoming/project_monitoring","pattern":"*.csv,*.xlsx","archive_after_success":true}', 20 FROM dataset_registry WHERE dataset_name = 'project_monitoring'
    UNION ALL
    SELECT 'project_monitoring_cdc', dataset_id, 'kafka_cdc',
           '{"bootstrap_servers":"kafka:29092","schema":"project_schema","tables":["projects"],"topic_pattern":"cdc.project_schema.*"}', 30 FROM dataset_registry WHERE dataset_name = 'project_monitoring'
) sources
ON CONFLICT (source_id) DO NOTHING;

INSERT INTO field_mapping (schema_id, source_id, raw_field, canonical_field, transform)
SELECT s.schema_id, 'pchrd_regional_file_dropbox', raw_field, canonical_field, transform
FROM ingestion_schema s
JOIN dataset_registry d ON d.dataset_id = s.dataset_id AND d.dataset_name = 'project_monitoring'
JOIN (VALUES
    ('Proj_ID', 'project_id', NULL),
    ('Program', 'program_area', NULL),
    ('Project Title', 'project_title', NULL),
    ('PI Name', 'principal_investigator', NULL),
    ('Start_Date (mm/dd/yyyy)', 'start_date', 'parse_date(mm/dd/yyyy -> ISO 8601)'),
    ('End_Date (mm/dd/yyyy)', 'end_date', 'parse_date(mm/dd/yyyy -> ISO 8601)'),
    ('Budget Allocated (Php)', 'budget_allocated_php', 'strip_currency_formatting -> float'),
    ('Budget Utilized (Php)', 'budget_utilized_php', 'strip_currency_formatting -> float'),
    ('Status', 'status', NULL),
    ('Fund Source', 'funding_source', NULL)
) mappings(raw_field, canonical_field, transform) ON true
WHERE s.schema_version = 1
ON CONFLICT (schema_id, source_id, raw_field) DO NOTHING;

INSERT INTO validation_rule (rule_id, dataset_id, field_name, rule_type, severity, rule_config)
SELECT rule_id, dataset_id, field_name, rule_type, severity, config::jsonb
FROM (
    SELECT 'project_id_not_null' rule_id, dataset_id, 'project_id' field_name, 'null_check' rule_type, 'hard' severity, '{}' config FROM dataset_registry WHERE dataset_name = 'project_monitoring'
    UNION ALL SELECT 'project_id_unique', dataset_id, 'project_id', 'duplicate_check', 'hard', '{}' FROM dataset_registry WHERE dataset_name = 'project_monitoring'
    UNION ALL SELECT 'schema_matches_contract', dataset_id, NULL, 'schema_validation', 'hard', '{}' FROM dataset_registry WHERE dataset_name = 'project_monitoring'
    UNION ALL SELECT 'budget_within_allocation', dataset_id, 'budget_utilized_php', 'budget_utilized_le_allocated', 'hard', '{}' FROM dataset_registry WHERE dataset_name = 'project_monitoring'
    UNION ALL SELECT 'dates_are_ordered', dataset_id, 'end_date', 'end_date_after_start_date', 'soft', '{}' FROM dataset_registry WHERE dataset_name = 'project_monitoring'
) rules
ON CONFLICT (rule_id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- rd_equipment_inventory — the DB-ONLY reference dataset. Unlike
-- project_monitoring (which has config/schema_rd_equipment_inventory.yaml
-- equivalents in YAML too), this dataset has NO yaml files at all: every
-- ingestion, validation, and Airflow task for it resolves its configuration
-- exclusively through ConfigRegistry._load_from_db(), proving out the
-- database-backed half of the hybrid config story end to end.
--
-- Source shape: a genuinely multi-table database source (not a single
-- table, not a folder of files) — two tables in the operational
-- `equipment_schema` schema (see database/seed_equipment_source.sql for the
-- DDL + sample rows in the source database), joined on equip_id by
-- ingestion.batch_ingestion.BatchIngestor.ingest_db_tables().
-- ---------------------------------------------------------------------------

INSERT INTO dataset_registry (dataset_name, version, description, ingestion_type, owner)
VALUES ('rd_equipment_inventory', '1.0', 'PCAARRD R&D equipment inventory demo dataset (DB-only config)', 'database', 'PCAARRD Equipment Management Unit')
ON CONFLICT (dataset_name) DO NOTHING;

WITH dataset AS (
    SELECT dataset_id FROM dataset_registry WHERE dataset_name = 'rd_equipment_inventory'
)
INSERT INTO ingestion_schema (dataset_id, schema_version, fields, unique_fields)
SELECT dataset_id, 1, $$[
  {"name":"equipment_id","dtype":"string","required":true},
  {"name":"agency","dtype":"string","required":true,"permissible_values":["PCAARRD"]},
  {"name":"equipment_name","dtype":"string","required":true},
  {"name":"equipment_category","dtype":"string","required":true,"permissible_values":["Laboratory","ICT","Field Survey","Processing"]},
  {"name":"acquisition_date","dtype":"date","required":true},
  {"name":"acquisition_cost_php","dtype":"float","required":true},
  {"name":"condition_status","dtype":"string","required":true,"permissible_values":["Serviceable","Needs Repair","Condemned","Under Repair"]},
  {"name":"custodian_name","dtype":"string","required":false},
  {"name":"assigned_office","dtype":"string","required":false},
  {"name":"last_assignment_date","dtype":"date","required":false}
]$$::jsonb, '["equipment_id"]'::jsonb
FROM dataset
ON CONFLICT (dataset_id, schema_version) DO NOTHING;

INSERT INTO ingestion_source (source_id, dataset_id, source_type, config, priority)
SELECT 'pcaarrd_equipment_registry_db', dataset_id, 'db_multitable',
       '{"connection_env":"GATES_SOURCE_DB_URL","schema":"equipment_schema","tables":["equipment","equipment_assignment"],"join_key":"equip_id","agency":"PCAARRD"}', 10
FROM dataset_registry WHERE dataset_name = 'rd_equipment_inventory'
ON CONFLICT (source_id) DO NOTHING;

INSERT INTO field_mapping (schema_id, source_id, raw_field, canonical_field, transform)
SELECT s.schema_id, 'pcaarrd_equipment_registry_db', raw_field, canonical_field, transform
FROM ingestion_schema s
JOIN dataset_registry d ON d.dataset_id = s.dataset_id AND d.dataset_name = 'rd_equipment_inventory'
JOIN (VALUES
    ('equip_id', 'equipment_id', NULL),
    ('equip_name', 'equipment_name', NULL),
    ('category', 'equipment_category', NULL),
    ('acq_date', 'acquisition_date', NULL),
    ('acq_cost_php', 'acquisition_cost_php', 'strip_currency_formatting -> float'),
    ('status', 'condition_status', NULL),
    ('custodian', 'custodian_name', NULL),
    ('office', 'assigned_office', NULL),
    ('assign_date', 'last_assignment_date', NULL)
) mappings(raw_field, canonical_field, transform) ON true
WHERE s.schema_version = 1
ON CONFLICT (schema_id, source_id, raw_field) DO NOTHING;

INSERT INTO validation_rule (rule_id, dataset_id, field_name, rule_type, severity, rule_config)
SELECT rule_id, dataset_id, field_name, rule_type, severity, config::jsonb
FROM (
    SELECT 'equipment_id_not_null' rule_id, dataset_id, 'equipment_id' field_name, 'null_check' rule_type, 'hard' severity, '{}' config FROM dataset_registry WHERE dataset_name = 'rd_equipment_inventory'
    UNION ALL SELECT 'equipment_id_unique', dataset_id, 'equipment_id', 'duplicate_check', 'hard', '{}' FROM dataset_registry WHERE dataset_name = 'rd_equipment_inventory'
    UNION ALL SELECT 'equipment_schema_matches_contract', dataset_id, NULL, 'schema_validation', 'hard', '{}' FROM dataset_registry WHERE dataset_name = 'rd_equipment_inventory'
) rules
ON CONFLICT (rule_id) DO NOTHING;
