# DataHub + Trino Integration: Complete Demo Workflow

**Purpose:** Show how DataHub (cataloging) and Trino (querying) work together in GATES

---

## 🎯 The Complete GATES Data Platform

```
┌─────────────────────────────────────────────────────────────────┐
│                    GATES Data Platform                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Data Sources                                                     │
│  ├─ DOST PMS API (Airbyte)                                      │
│  ├─ PCHRD Regional Drops (File)                                 │
│  └─ Database Changes (Kafka CDC)                                │
│           │                                                      │
│           ↓                                                      │
│  ┌──────────────────────────────────┐                           │
│  │ Ingestion Layer (Airflow)        │                           │
│  │ ├─ Validate data (hard rules)    │                           │
│  │ └─ Land in Bronze (MinIO)        │ ─────┐                    │
│  └──────────────────────────────────┘      │                    │
│           │                                 │ DataHub:           │
│           ↓                                 │ ├─ Register Bronze │
│  ┌──────────────────────────────────┐      │ ├─ Track lineage   │
│  │ Silver Layer (dbt via Trino)     │ ────┤ └─ Quality metrics │
│  │ ├─ Deduplicate                   │      │                    │
│  │ ├─ Soft validation flags         │ ─────┤ Trino:             │
│  │ └─ Type casting                  │      │ ├─ Query Bronze    │
│  └──────────────────────────────────┘      │ └─ Read from       │
│           │                                 │    Iceberg         │
│           ↓                                 │                    │
│  ┌──────────────────────────────────┐ ─────┤ DataHub:           │
│  │ Gold Layer (dbt via Trino)       │      │ ├─ Register Silver │
│  │ ├─ Aggregations                  │      │ └─ Show soft flags │
│  │ ├─ Business metrics              │      │                    │
│  │ └─ Portfolio performance data    │ ─────┤ Trino:             │
│  └──────────────────────────────────┘      │ ├─ Query Silver    │
│           │                                 │ ├─ Filter by flag  │
│           ↓                                 │ └─ JOIN with config
│  ┌──────────────────────────────────┐ ─────┤ DataHub:           │
│  │ Analytics Layer                  │      │ ├─ Register Gold   │
│  │ ├─ BI Dashboard (Metabase/Power) │      │ ├─ Show metrics    │
│  │ ├─ Reporting                     │      │ └─ Document usage  │
│  │ └─ Ad-hoc Analysis              │ ─────┤                    │
│  └──────────────────────────────────┘      │ Trino:             │
│                                             │ ├─ Query Gold      │
│  Storage: MinIO + Iceberg                  │ └─ Performance     │
│  Config: PostgreSQL Database               │                    │
│  Orchestration: Airflow                    │                    │
│  Monitoring: CloudWatch + Prometheus       │                    │
│                                             │ DataHub:           │
└─────────────────────────────────────────────┤ └─ Enable discovery│
                                              └────────────────────┘
```

---

## 📊 Demo Scenario: "Where Did This Data Come From?"

**User Question:** "I see this metric in our BI dashboard. Where did it come from?"

### **Step 1: Discover in DataHub UI**

**URL:** `http://localhost:9002`

```
Search: "portfolio_performance"
Results:
├─ gates.gold.rd_portfolio_performance (Table)
   ├─ 4 Owners: PCHRD RD Team
   ├─ Last Updated: 2026-09-14 09:15 UTC
   ├─ Row Count: 2,248
   └─ Quality: 98.2% Complete
```

**Click → Expand**

```
Details Tab:
├─ Description: "Gold: Business-ready aggregated metrics"
├─ Created: 2026-08-01
├─ Last Modified: 2026-09-14
└─ Owners: [pchrd-rd-team]

Schema Tab:
├─ quarter (string) - Filter dimension
├─ agency (string) - Filter dimension
├─ program_area (string) - Filter dimension
├─ region_name (string) - Geographic dimension
├─ status (string) - Project status
├─ project_count (int) - # of projects
├─ total_budget_allocated_php (decimal) - Financial metric
├─ total_budget_utilized_php (decimal) - Financial metric
└─ utilization_rate_pct (decimal) - Key metric!

Lineage Tab:  [CLICK HERE]
```

### **Step 2: View Lineage**

**Click "Lineage" tab**

```
DataHub shows complete lineage graph:

  DOST PMS API          PCHRD File           Kafka CDC
  (Airbyte)         (Regional Drops)     (db-changelog)
       │                  │                    │
       └──────────────────┼────────────────────┘
                          │
                          ↓
            ┌─────────────────────────────┐
            │ gates.bronze.               │
            │   project_monitoring        │
            │ (2,040 rows, 2h ago)        │
            │ Status: ✅ VALIDATED        │
            └─────────────────────────────┘
                          │
              dbt: stg_project_monitoring.sql
                    ↓
            ┌─────────────────────────────┐
            │ gates.silver.               │
            │   project_monitoring        │
            │ (2,040 rows)                │
            │ 12 soft violations flagged  │
            │ Status: ⚠️ CHECK QUALITY    │
            └─────────────────────────────┘
                          │
       dbt: mart_rd_portfolio_performance.sql
                    ↓
            ┌─────────────────────────────┐
            │ gates.gold.                 │
            │   rd_portfolio_performance  │
            │ (2,248 rows, aggregated)    │
            │ Status: ✅ READY FOR USE    │
            └─────────────────────────────┘
                          │
                          ↓
            Metabase/Power BI Dashboard
            "Portfolio Performance by Region"
```

**User can now see:** 
- ✅ "This metric comes from bronze data"
- ✅ "It's aggregated through dbt"
- ✅ "There are soft rule violations flagged in silver"
- ✅ "The data is 2 hours old"

### **Step 3: Query the Data**

**Switch to Trino UI: `http://localhost:8081`**

```sql
-- Get the actual data
SELECT 
    quarter,
    region_name,
    project_count,
    utilization_rate_pct
FROM gates.gold.rd_portfolio_performance
WHERE quarter = '2026-Q3'
ORDER BY utilization_rate_pct DESC;
```

**Results:**
```
quarter | region_name     | project_count | utilization_pct
2026-Q3 | Region 4A       | 45           | 81.2%
2026-Q3 | Region 5        | 32           | 78.9%
2026-Q3 | Metro Manila    | 58           | 76.5%
2026-Q3 | Visayas         | 28           | 75.2%
```

### **Step 4: Investigate Quality Issues**

**Question:** "Why is utilization only 76.5%? Are there data quality issues?"

**Query in Trino:**

```sql
-- Check the silver layer for soft violations
SELECT 
    project_id,
    quality_flag,
    COUNT(*) as violation_count
FROM gates.silver.project_monitoring
WHERE region_name = 'Metro Manila'
  AND quality_flag != 'pass'
GROUP BY quality_flag
ORDER BY violation_count DESC;
```

**Results:**
```
project_id      | quality_flag                      | violation_count
(various)       | soft_violation_end_before_start   | 12
(various)       | soft_violation_budget_negative    | 8
(various)       | soft_violation_over_budget        | 3
```

**Back to DataHub UI:**

**Click on gates.silver.project_monitoring → Quality Tab**

```
Data Profile:
├─ Total Rows: 2,040
├─ Profile Timestamp: 2026-09-14 09:15:00 UTC
├─ Column Profiles:
│  ├─ project_id
│  │  ├─ Null Count: 0
│  │  ├─ Null %: 0.0%
│  │  └─ Distinct: 1,950
│  │
│  ├─ budget_allocated_php
│  │  ├─ Null Count: 0
│  │  ├─ Min: 100,000
│  │  ├─ Max: 50,000,000
│  │  └─ Mean: 5,200,000
│  │
│  ├─ budget_utilized_php
│  │  ├─ Null Count: 8  ⚠️ Quality issue!
│  │  ├─ Null %: 0.39%
│  │  └─ Mean: 3,500,000
│  │
│  └─ end_date
│     ├─ Null Count: 12  ⚠️ Quality issue!
│     └─ Null %: 0.59%
│
└─ Validation Results:
   ├─ Hard Rules: ✅ ALL PASS (checked at ingestion)
   └─ Soft Rules: ⚠️ 23 VIOLATIONS (flagged in silver)
```

---

## 🔄 Demo Scenario 2: "Track Lineage Across Layers"

**User Question:** "How does project_id flow from API to gold layer?"

### **Step 1: DataHub Lineage**

**In DataHub UI, click lineage graph → show column-level lineage**

```
DOST PMS API (Airbyte)
├─ Field: project_id
│  ├─ Type: string
│  ├─ Extraction: Native field from API response
│  └─ Sample: "PCHRD-2026-0142"
│       │
│       ↓ (cast to string, partition by project_id)
│
Bronze: project_monitoring
├─ Field: project_id
│  ├─ Type: string (cast)
│  ├─ Nullable: false
│  ├─ Part of Key: true
│  ├─ Partition: dt, project_id
│  └─ Null Rate: 0%
│       │
│       ↓ (deduplicate on project_id + dt, keep latest)
│
Silver: project_monitoring
├─ Field: project_id
│  ├─ Type: string (unchanged)
│  ├─ Nullable: false
│  ├─ Part of Key: true
│  ├─ Quality Flag: soft violations checked per project
│  └─ Null Rate: 0%
│       │
│       ↓ (group by project_id, agency, region, quarter)
│
Gold: rd_portfolio_performance
├─ Field: N/A (aggregated at project level)
│  ├─ Type: Aggregated data (COUNT of projects)
│  ├─ Metrics: project_count, budget sums, rates
│  └─ Dimensions: agency, region_name, program_area, status
│       │
│       ↓
Portfolio Performance Dashboard (BI Tool)
```

### **Step 2: Trino Query to Trace Data**

```sql
-- Trace a single project through all layers
WITH bronze_data AS (
    SELECT 
        project_id,
        agency,
        budget_allocated_php,
        _loaded_at
    FROM gates.bronze.project_monitoring
    WHERE project_id = 'PCHRD-2026-0142'
),

silver_data AS (
    SELECT 
        project_id,
        agency,
        budget_allocated_php,
        quality_flag
    FROM gates.silver.project_monitoring
    WHERE project_id = 'PCHRD-2026-0142'
),

gold_data AS (
    SELECT 
        agency,
        region_name,
        project_count,
        total_budget_allocated_php
    FROM gates.gold.rd_portfolio_performance
    WHERE agency = (SELECT DISTINCT agency FROM silver_data LIMIT 1)
)

SELECT 
    'Bronze' as layer,
    COUNT(*) as row_count,
    MAX(_loaded_at) as last_seen
FROM bronze_data
UNION ALL
SELECT 
    'Silver' as layer,
    COUNT(*) as row_count,
    NULL as last_seen
FROM silver_data
UNION ALL
SELECT 
    'Gold' as layer,
    COUNT(*) as row_count,
    NULL as last_seen
FROM gold_data;
```

**Results:**
```
layer  | row_count | last_seen
Bronze | 1         | 2026-09-14 09:15:00
Silver | 1         | (NULL)
Gold   | 1         | (NULL)
```

---

## 📋 Demo Scenario 3: "Monitor Data Quality"

**User Question:** "Are there data quality issues I should know about?"

### **Step 1: Check DataHub for Quality Metrics**

**In DataHub UI:**

```
Browse → gates.silver.project_monitoring → Quality Tab
```

Shows:
- ✅ Hard rules: ALL PASS (hard violations were quarantined in bronze)
- ⚠️ Soft rules: 23 violations (flagged but included in silver)
- Row count: 2,040
- Column null rates

### **Step 2: Investigate with Trino Queries**

```sql
-- Count violations by type
SELECT 
    quality_flag,
    COUNT(*) as violation_count,
    ROUND(100.0 * COUNT(*) / 2040, 2) as pct_of_total
FROM gates.silver.project_monitoring
WHERE quality_flag != 'pass'
GROUP BY quality_flag
ORDER BY violation_count DESC;
```

**Results:**
```
quality_flag                      | violation_count | pct_of_total
soft_violation_end_before_start   | 12              | 0.59%
soft_violation_budget_negative    | 8               | 0.39%
soft_violation_over_budget        | 3               | 0.15%
```

### **Step 3: Get More Details**

```sql
-- Show the problematic records
SELECT 
    project_id,
    agency,
    start_date,
    end_date,
    CASE 
        WHEN end_date < start_date THEN 'End before start!'
        WHEN budget_utilized_php > budget_allocated_php THEN 'Over budget!'
        WHEN budget_allocated_php < 0 THEN 'Negative budget!'
        ELSE 'Unknown'
    END as issue
FROM gates.silver.project_monitoring
WHERE quality_flag != 'pass'
LIMIT 20;
```

**Results:**
```
project_id    | agency | start_date | end_date   | issue
PCHRD-2026-15 | PCHRD  | 2026-01-15 | 2025-12-20 | End before start!
PCHRD-2026-22 | PCHRD  | 2026-03-01 | 2026-03-05 | Over budget!
...
```

---

## 🎬 Live Demo Script (30 minutes)

**Setup (5 min):**
```bash
docker-compose up -d
# Wait for all services
curl http://localhost:9002  # DataHub
curl http://localhost:8081  # Trino
```

**Demo Flow:**

| Time | Action | Tool | Output |
|------|--------|------|--------|
| 0-2 | Explain architecture diagram | Screen share | Users understand components |
| 2-5 | Search for dataset in DataHub | DataHub UI | Show discovery capability |
| 5-8 | Show lineage graph | DataHub UI | Show end-to-end flow (API → Gold) |
| 8-12 | Query Bronze layer | Trino UI | Show raw data (2,040 rows) |
| 12-15 | Query Silver layer | Trino UI | Show soft violations flagged |
| 15-18 | Query Gold layer | Trino UI | Show aggregated metrics |
| 18-22 | Show data quality metrics | DataHub UI | Show null rates, profiles |
| 22-25 | Show failing record details | Trino UI | Show specific violations |
| 25-28 | Demonstrate time-travel | Trino UI | Show data as of yesterday |
| 28-30 | Q&A | Both tools | Answer team questions |

---

## 📊 Key Metrics to Show

```
Dashboard: "GATES Pipeline Health"

Overall Status:
├─ Ingestion Status: ✅ 2,040 rows loaded 2h ago
├─ Data Quality: 98.2% pass rate (23 soft violations)
├─ Gold Layer: 2,248 aggregated metrics ready
└─ Query Performance: Average query time 0.8s

Bronze Layer:
├─ Row Count: 2,040
├─ Ingestion Time: 45 seconds
├─ Hard Violations Quarantined: 0 rows ✅
└─ Last Updated: 2026-09-14 09:15 UTC

Silver Layer:
├─ Row Count: 2,040 (same, no duplicates)
├─ Soft Violations Flagged: 23 rows ⚠️
│  ├─ End before start: 12
│  ├─ Budget negative: 8
│  └─ Over budget: 3
└─ Ready for Analysis: 2,017 rows ✅

Gold Layer:
├─ Aggregations: 2,248 metrics
├─ Coverage: 4 agencies × 8 regions × 5 statuses = 160 combinations
├─ Last Updated: 2026-09-14 09:45 UTC (30 min after bronze)
└─ Quality: 100% (filtered from silver quality_flag='pass')
```

---

## ✅ Key Demo Takeaways

1. **DataHub = Discovery + Lineage**
   - Users can search and find datasets
   - Users can see where data came from
   - Users can see quality metrics
   - Users understand the data pedigree

2. **Trino = Query + Analysis**
   - Users can write SQL queries
   - Users can see actual data values
   - Users can investigate quality issues
   - Users can perform ad-hoc analysis

3. **Together = Data Platform**
   - Complete visibility (DataHub)
   - Complete querying capability (Trino)
   - End-to-end governance
   - Trust in data quality

4. **Production-Ready**
   - Scales to 2,000+ datasets
   - Automated quality checks
   - Orchestrated by Airflow
   - Monitored in real-time

---

## 🚀 Next Steps

After demo:
1. **Week 1:** Deploy to AWS using Terraform
2. **Week 2:** Ingest 100 sample datasets via Airbyte
3. **Week 3:** Run dbt models to populate Silver/Gold
4. **Week 4:** Demo live to stakeholders with real data

At that point, users can:
- ✅ Discover any of 2,000+ datasets
- ✅ View complete lineage
- ✅ Run ad-hoc SQL queries
- ✅ Monitor quality metrics
- ✅ Build BI dashboards

**That's what makes it "enterprise-ready"!**
