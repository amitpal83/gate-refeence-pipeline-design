{{
  config(
    materialized='table',
    alias='rd_equipment_inventory',
    tags=['silver', 'rd_equipment_inventory']
  )
}}

-- Silver: dedupe + soft-rule quality_flag (flag-and-promote, not drop —
-- same policy as silver_project_monitoring.sql's quality_flag).
with bronze as (
    select * from {{ ref('bronze_rd_equipment_inventory') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by equipment_id
            order by last_updated_at desc
        ) as rn
    from bronze
),

flagged as (
    select
        equipment_id,
        agency,
        equipment_name,
        equipment_category,
        acquisition_date,
        acquisition_cost_php,
        condition_status,
        custodian_name,
        assigned_office,
        last_assignment_date,
        case
            when acquisition_date > current_date then 'soft_violation_future_acquisition'
            when last_assignment_date is not null and last_assignment_date < acquisition_date
                then 'soft_violation_assignment_before_acquisition'
            else 'pass'
        end as quality_flag
    from deduped
    where rn = 1
)

select * from flagged
