{{
  config(
    materialized='table',
    tags=['gold', 'project_monitoring']
  )
}}

-- Gold: use-case-specific curation for the R&D Portfolio Performance Dashboard.
with silver as (
    select * from {{ ref('stg_project_monitoring') }}
),

by_quarter as (
    select
        agency,
        program_area,
        region_code,
        region_name,
        date_trunc('quarter', start_date) as quarter,
        status,
        count(distinct project_id) as project_count,
        sum(budget_allocated_php) as total_budget_allocated_php,
        sum(budget_utilized_php) as total_budget_utilized_php,
        round(
            sum(budget_utilized_php) / nullif(sum(budget_allocated_php), 0) * 100, 1
        ) as utilization_rate_pct
    from silver
    group by 1, 2, 3, 4, 5, 6
)

select
    agency,
    program_area,
    region_code,
    region_name,
    quarter,
    status,
    project_count,
    total_budget_allocated_php,
    total_budget_utilized_php,
    utilization_rate_pct
from by_quarter
order by quarter, agency, program_area
