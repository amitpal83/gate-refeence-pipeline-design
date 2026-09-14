{{
  config(
    materialized='table',
    tags=['gold', 'rd_equipment_inventory']
  )
}}

-- Gold: equipment condition/value rollup by agency, category, and status —
-- the equipment-dataset counterpart to mart_rd_portfolio_performance.sql.
with silver as (
    select * from {{ ref('stg_rd_equipment_inventory') }}
)

select
    agency,
    equipment_category,
    condition_status,
    count(distinct equipment_id) as equipment_count,
    sum(acquisition_cost_php) as total_acquisition_cost_php,
    round(avg(acquisition_cost_php), 2) as avg_acquisition_cost_php
from silver
group by 1, 2, 3
order by agency, equipment_category, condition_status
