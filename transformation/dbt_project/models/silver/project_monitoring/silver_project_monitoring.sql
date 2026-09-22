{{
  config(
    materialized='table',
    alias='project_monitoring',
    tags=['silver', 'project_monitoring']
  )
}}

-- Silver: denormalize, dedupe, drop internal columns, compute derived fields.
with bronze as (
    select * from {{ ref('bronze_project_monitoring') }}
),

deduped as (
    -- Keep the latest submission per project_id when both the API and file
    -- channels report the same project in the same load window.
    select
        *,
        row_number() over (
            partition by project_id
            order by last_updated_at desc
        ) as rn
    from bronze
),

denormalized as (
    select
        d.project_id,
        d.agency,
        d.region as region_code,
        r.region_name,
        d.program_area,
        d.project_title,
        d.principal_investigator,
        d.start_date,
        d.end_date,
        d.budget_allocated_php,
        d.budget_utilized_php,
        round(
            d.budget_utilized_php / nullif(d.budget_allocated_php, 0) * 100, 1
        ) as budget_utilization_pct,
        case
            when d.end_date <= d.start_date then 'soft_violation_invalid_dates'
            when d.budget_utilized_php > d.budget_allocated_php then 'soft_violation_over_budget'
            else 'pass'
        end as quality_flag,
        d.status,
        d.funding_source
        -- dropped: raw_row_id, source_batch_id (internal-only, Bronze-scoped)
    from deduped d
    left join {{ ref('dim_region_lookup') }} r
        on d.region = r.region_code
    where d.rn = 1
)

select * from denormalized
