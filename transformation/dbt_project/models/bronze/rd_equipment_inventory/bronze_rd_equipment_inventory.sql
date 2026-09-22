{{
  config(
    materialized='table',
    alias='rd_equipment_inventory',
    tags=['bronze', 'rd_equipment_inventory'],
    partition_by={'field': 'dt', 'data_type': 'date'}
  )
}}

-- Bronze: typed, partitioned promotion of GX-validated staging data.
-- Same shape as bronze_project_monitoring.sql — proves the pattern
-- generalizes to a dataset whose config lives only in the database (see
-- database/init_config_db.sql's rd_equipment_inventory rows), not YAML.
with validated_staging as (

    select *
    from {{ source('staging', 'rd_equipment_inventory_raw') }}
    where _gx_validation_status = 'pass'

)

select
    cast(equipment_id as varchar) as equipment_id,
    cast(agency as varchar) as agency,
    cast(equipment_name as varchar) as equipment_name,
    cast(equipment_category as varchar) as equipment_category,
    cast(acquisition_date as date) as acquisition_date,
    cast(acquisition_cost_php as decimal(18,2)) as acquisition_cost_php,
    cast(condition_status as varchar) as condition_status,
    cast(custodian_name as varchar) as custodian_name,
    cast(assigned_office as varchar) as assigned_office,
    cast(last_assignment_date as date) as last_assignment_date,
    cast(from_iso8601_timestamp(_ingested_at) as timestamp) as last_updated_at,
    current_date as dt,
    '{{ invocation_id }}' as dbt_run_id
from validated_staging
