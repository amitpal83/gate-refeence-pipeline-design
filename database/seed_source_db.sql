-- database/seed_source_db.sql
--
-- Seeds the SEPARATE "source-db" Postgres instance (see
-- docker-compose.demo.yml's `source-db` service) — the operational system
-- being read from, distinct from the `postgres` service that backs Airflow
-- and the GATES config database (database/init_config_db.sql). A real
-- platform would never CDC its own pipeline-metadata database.
--
-- Two independent schemas live here:
--   1. project_schema.projects   — CDC-tracked via Debezium (see
--      deployment/kafka-connect/debezium-project-schema.json). A single
--      denormalized table so each change event already carries every
--      canonical project_monitoring field — see the comment in
--      config/schema_project_monitoring.yaml's project_monitoring_cdc
--      source block for why this isn't split into normalized sub-tables.
--   2. equipment_schema.equipment / equipment_schema.equipment_assignment
--      — read directly (not via CDC) by
--      ingestion.batch_ingestion.BatchIngestor.ingest_db_tables() for the
--      rd_equipment_inventory dataset. Two tables joined on equip_id is the
--      "database schema with multiple tables" ingestion source example.

CREATE SCHEMA IF NOT EXISTS project_schema;

CREATE TABLE IF NOT EXISTS project_schema.projects (
    project_id             VARCHAR PRIMARY KEY,
    agency                 VARCHAR NOT NULL,
    region                 VARCHAR NOT NULL,
    program_area           VARCHAR NOT NULL,
    project_title          VARCHAR NOT NULL,
    principal_investigator VARCHAR NOT NULL,
    start_date             DATE NOT NULL,
    end_date               DATE,
    budget_allocated_php   NUMERIC(18,2) NOT NULL,
    budget_utilized_php    NUMERIC(18,2) NOT NULL,
    status                 VARCHAR NOT NULL,
    funding_source         VARCHAR NOT NULL
);

INSERT INTO project_schema.projects
    (project_id, agency, region, program_area, project_title, principal_investigator,
     start_date, end_date, budget_allocated_php, budget_utilized_php, status, funding_source)
VALUES
    -- 0401/0402, not 0201/0202: sample_data/project_monitoring_api.json's
    -- dost_pms_api fixture already uses PCHRD-2026-0201/0202 — reusing
    -- those here made this channel's rows collide with the API channel's
    -- on project_id (the dataset's hard PK) the moment both get combined
    -- for validation, failing expect_column_values_to_be_unique every
    -- single run regardless of any other fix.
    ('PCHRD-2026-0401', 'PCHRD', 'R4A', 'Health', 'Point-of-care diagnostics for dengue', 'Dr. R. Santos',
     '2026-02-01', '2026-12-15', 4200000.00, 1150000.00, 'Ongoing', 'GAA'),
    ('PCHRD-2026-0402', 'PCHRD', 'NCR', 'Health', 'AI-assisted TB screening', 'Dr. M. Cruz',
     '2026-03-15', NULL, 6800000.00, 2040000.00, 'Ongoing', 'Foreign-Assisted')
ON CONFLICT (project_id) DO NOTHING;

CREATE SCHEMA IF NOT EXISTS equipment_schema;

CREATE TABLE IF NOT EXISTS equipment_schema.equipment (
    equip_id      VARCHAR PRIMARY KEY,
    equip_name    VARCHAR NOT NULL,
    category      VARCHAR NOT NULL,
    acq_date      DATE NOT NULL,
    acq_cost_php  VARCHAR NOT NULL, -- delivered as text with thousands separators, e.g. "1,250,000.00"
    status        VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS equipment_schema.equipment_assignment (
    equip_id     VARCHAR PRIMARY KEY REFERENCES equipment_schema.equipment(equip_id),
    custodian    VARCHAR,
    office       VARCHAR,
    assign_date  DATE
);

INSERT INTO equipment_schema.equipment (equip_id, equip_name, category, acq_date, acq_cost_php, status)
VALUES
    ('PCAARRD-EQ-0001', 'Real-time PCR System', 'Laboratory', '2024-06-10', '1,250,000.00', 'Serviceable'),
    ('PCAARRD-EQ-0002', 'Drone Survey Kit', 'Field Survey', '2025-01-22', '480,000.00', 'Needs Repair'),
    ('PCAARRD-EQ-0003', 'GIS Workstation', 'ICT', '2023-11-05', '150,000.00', 'Serviceable')
ON CONFLICT (equip_id) DO NOTHING;

INSERT INTO equipment_schema.equipment_assignment (equip_id, custodian, office, assign_date)
VALUES
    ('PCAARRD-EQ-0001', 'Engr. J. Dela Cruz', 'Central Luzon Regional Office', '2024-06-15'),
    ('PCAARRD-EQ-0002', 'Ms. A. Reyes', 'Bicol Regional Office', '2025-01-25'),
    ('PCAARRD-EQ-0003', 'Mr. P. Villanueva', 'Central Office', '2023-11-10')
ON CONFLICT (equip_id) DO NOTHING;
