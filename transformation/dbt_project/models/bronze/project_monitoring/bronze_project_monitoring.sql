{{
  config(
    materialized='table',
    alias='project_monitoring',
    tags=['bronze', 'project_monitoring'],
    partition_by={'field': 'dt', 'data_type': 'date'}
  )
}}

-- Bronze: typed, partitioned promotion of GX-validated staging data.
-- Only rows that passed the GX common + custom suites reach this model —
-- Airflow's trigger_gx_validation task fails the run before dbt_run_bronze
-- is ever called if the batch has a hard-rule violation.
with validated_staging as (

    select *
    from {{ source('staging', 'project_monitoring_raw') }}
    where _gx_validation_status = 'pass'

)

select
    cast(project_id as varchar) as project_id,
    cast(agency as varchar) as agency,
    cast(region as varchar) as region,
    cast(program_area as varchar) as program_area,
    cast(project_title as varchar) as project_title,
    cast(principal_investigator as varchar) as principal_investigator,
    cast(start_date as date) as start_date,
    cast(end_date as date) as end_date,
    cast(budget_allocated_php as decimal(18,2)) as budget_allocated_php,
    cast(budget_utilized_php as decimal(18,2)) as budget_utilized_php,
    cast(status as varchar) as status,
    cast(funding_source as varchar) as funding_source,
    -- last_updated_at isn't a canonical field ingestion ever writes; the
    -- ingestion-time technical column common/trino_loader.py stamps onto
    -- every staged row is the real, always-populated source for it —
    -- used downstream by Silver's "latest wins" dedup ordering.
    -- _ingested_at is Python's datetime.isoformat() — a "T" separator and
    -- a "+00:00" offset — which plain cast(... as timestamp) rejects
    -- (Trino's TIMESTAMP cast only accepts "YYYY-MM-DD HH:MM:SS[.ffffff]",
    -- no "T", no offset). from_iso8601_timestamp() parses that format
    -- correctly, returning TIMESTAMP WITH TIME ZONE; cast drops the zone.
    cast(from_iso8601_timestamp(_ingested_at) as timestamp) as last_updated_at,
    current_date as dt,
    '{{ invocation_id }}' as dbt_run_id
from validated_staging
