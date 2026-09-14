# Trino + Iceberg Integration Guide

**Purpose:** Query data warehouse (MinIO/Iceberg) using federated SQL queries

---

## 🎯 What Trino Does in GATES Pipeline

```
GATES Pipeline
├─ Source (Airbyte, File, Kafka CDC)
├─ Storage (MinIO + Iceberg)
│  ├─ Bronze (raw data)
│  ├─ Silver (deduplicated)
│  └─ Gold (aggregated)
└─ Query (Trino + dbt)

Trino Role:
├─ SQL Query Engine: Execute SQL queries over Iceberg tables
├─ Federation: Query across multiple data sources
├─ Data Catalog: Table discovery via Metastore
├─ Connectors: Connect to Iceberg, S3, PostgreSQL, etc.
└─ Performance: Distributed query execution with caching
```

---

## 🏗️ Trino Architecture in GATES

```
┌──────────────────────────────────────────────────────┐
│         User Query (SQL)                              │
│    "SELECT * FROM project_monitoring WHERE ..."      │
└──────────────────┬───────────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────────┐
│    Trino Coordinator                                  │
│  ├─ Query Parser & Planner                           │
│  ├─ Optimizer                                         │
│  └─ Scheduler                                         │
└──────────────────┬───────────────────────────────────┘
                   │
     ┌─────────────┼─────────────┐
     │             │             │
┌────▼─┐    ┌─────▼────┐    ┌───▼────┐
│Worker│    │  Worker  │    │ Worker │
│  1   │    │    2     │    │   3    │
└────┬─┘    └─────┬────┘    └───┬────┘
     │            │             │
     └────────────┼─────────────┘
                  │
     ┌────────────┴────────────┐
     │                         │
┌────▼──────────┐     ┌────────▼───────┐
│   Iceberg     │     │   PostgreSQL   │
│  (MinIO/S3)   │     │   (Metadata)   │
│               │     │                │
│ ├─ Bronze     │     │ ├─ Table defs  │
│ ├─ Silver     │     │ ├─ Partitions  │
│ └─ Gold       │     │ └─ Snapshots   │
└───────────────┘     └────────────────┘
```

---

## 🔧 Trino Setup

### **1. Docker Compose Configuration**

```yaml
# Part of docker-compose.yml

trino:
  image: trinodb/trino:latest
  container_name: trino-coordinator
  depends_on:
    - minio
    - postgres  # For metadata
  ports:
    - "8081:8080"  # Trino UI
  environment:
    - JAVA_TOOL_OPTIONS=-Xmx4G -Xms1G
  volumes:
    - ./trino/etc:/etc/trino
    - ./trino/catalog:/etc/trino/catalog
    - trino-data:/var/trino
  networks:
    - gates-network

# Trino worker nodes (optional, for scaling)
trino-worker-1:
  image: trinodb/trino:latest
  container_name: trino-worker-1
  depends_on:
    - trino
  environment:
    - JAVA_TOOL_OPTIONS=-Xmx4G -Xms1G
    - TRINO_COORDINATOR=trino
  volumes:
    - ./trino/etc:/etc/trino
    - ./trino/catalog:/etc/trino/catalog
  networks:
    - gates-network
```

### **2. Trino Configuration Files**

```
trino/
├─ etc/
│  ├─ config.properties (coordinator settings)
│  ├─ jvm.config (JVM settings)
│  ├─ log.properties (logging)
│  └─ node.properties (node identity)
└─ catalog/
   ├─ iceberg.properties (Iceberg connector)
   ├─ postgresql.properties (PostgreSQL connector)
   └─ hive.properties (optional, for Hive compatibility)
```

### **File 1: trino/etc/config.properties**

```properties
# Trino Coordinator Configuration

# Node environment
environment=production

# Coordinator settings
coordinator=true
node-scheduler.include-coordinator=true

# Network
http-server.http.port=8080

# Query settings
query.max-run-time=1h
query.max-memory=10GB
query.max-memory-per-node=2GB

# Memory
memory.heap-headroom-per-node=1GB
memory.heap-headroom=4GB

# Spill settings (when memory exceeded)
spill-enabled=true
spill-order-by-enabled=true
spill-window-operator-enabled=true

# Discovery
discovery-server.enabled=true
discovery.uri=http://trino:8080

# Catalog discovery
catalog.config-dir=/etc/trino/catalog
```

### **File 2: trino/catalog/iceberg.properties**

```properties
# Iceberg Connector Configuration

connector.name=iceberg

# Iceberg catalog type
iceberg.catalog.type=hive_metastore

# Hive Metastore configuration
hive.metastore.uri=thrift://postgres:5432

# S3/MinIO configuration
s3.endpoint=http://minio:9000
s3.access-key=minioadmin
s3.secret-key=minioadmin
s3.path-style-access=true
s3.region=us-east-1

# Iceberg warehouse location (S3 path)
warehouse.location=s3://gates-pipeline-data/

# Table format
iceberg.table-format=ICEBERG

# Parquet settings
iceberg.parquet.use-column-names=true

# Null handling
iceberg.null-check=false

# Metrics
iceberg.metrics.mode=FULL
```

### **File 3: trino/catalog/postgresql.properties**

```properties
# PostgreSQL Connector (for config database)

connector.name=postgresql

connection-url=jdbc:postgresql://postgres:5432/gates_config
connection-user=postgres
connection-password=password

# Schema mapping
case-insensitive-name-matching=true

# Metadata
allow-drop-table=false
allow-rename-table=false
```

### **File 4: trino/etc/jvm.config**

```
-server
-Xmx4G
-Xms1G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ParallelRefProcEnabled
-XX:+UnlockDiagnosticVMOptions
-XX:G1SummarizeRSetStatsPeriod=1
```

---

## 📊 Trino Query Execution

### **Example 1: Query Bronze Layer (Raw Data)**

```sql
-- Connect to: http://localhost:8081
-- Catalog: iceberg
-- Schema: gates

SELECT 
    project_id,
    agency,
    region,
    budget_allocated_php,
    budget_utilized_php,
    _loaded_at,
    _dbt_run_id
FROM gates.bronze.project_monitoring
WHERE dt = '2026-09-14'
LIMIT 10;
```

**Expected Output:**
```
project_id    | agency | region | budget_allocated | budget_utilized | _loaded_at          | _dbt_run_id
PCHRD-2026-01 | PCHRD  | R4A    | 5200000.00      | 3100000.00      | 2026-09-14 09:15:00 | abc123-run-001
PCHRD-2026-02 | PCHRD  | R4A    | 8500000.00      | 7200000.00      | 2026-09-14 09:15:00 | abc123-run-001
...
```

### **Example 2: Query Silver Layer (Deduplicated with Soft Flags)**

```sql
SELECT 
    project_id,
    agency,
    region,
    budget_utilization_pct,
    quality_flag,
    _loaded_at
FROM gates.silver.project_monitoring
WHERE quality_flag != 'pass'  -- Show only violations
ORDER BY quality_flag, agency;
```

**Expected Output:**
```
project_id    | agency | region | utilization_pct | quality_flag                  | _loaded_at
PCHRD-2026-15 | PCHRD  | NCR    | 125.50         | soft_violation_over_budget    | 2026-09-14 09:15:00
PCHRD-2026-22 | PCHRD  | R4A    | NULL           | soft_violation_end_before_start | 2026-09-14 09:15:00
...
```

### **Example 3: Query Gold Layer (Business Metrics)**

```sql
SELECT 
    quarter,
    agency,
    program_area,
    region_name,
    status,
    project_count,
    total_budget_allocated_php,
    total_budget_utilized_php,
    utilization_rate_pct
FROM gates.gold.rd_portfolio_performance
WHERE quarter >= '2026-Q3'
ORDER BY quarter DESC, utilization_rate_pct DESC;
```

**Expected Output:**
```
quarter | agency | program_area | region_name | status    | project_count | total_allocated | total_utilized | utilization_pct
2026-Q3 | PCHRD  | Health       | Region4A    | Ongoing   | 45           | 250000000.00   | 198000000.00  | 79.2
2026-Q3 | PCHRD  | Agriculture  | Region4A    | Ongoing   | 28           | 180000000.00   | 145000000.00  | 80.6
...
```

### **Example 4: Time-Travel Query (Iceberg Feature)**

```sql
-- Query data as it was yesterday (time-travel)
SELECT 
    COUNT(*) as row_count,
    COUNT(DISTINCT project_id) as unique_projects
FROM gates.bronze.project_monitoring FOR VERSION AS OF '2026-09-13'
WHERE dt = '2026-09-13';
```

**Output:**
```
row_count | unique_projects
2015      | 1950
```

### **Example 5: Cross-Database Federation**

```sql
-- Query across Iceberg (data lake) and PostgreSQL (config)
SELECT 
    d.dataset_name,
    d.owner,
    s.row_count,
    s.status,
    s.ingestion_duration_seconds
FROM postgresql.public.dataset_registry d
JOIN (
    SELECT 
        dataset_id,
        COUNT(*) as row_count,
        status
    FROM (
        SELECT 
            project_id,
            _ingestion_date,
            'success' as status
        FROM gates.bronze.project_monitoring
    )
    GROUP BY dataset_id
) s ON d.dataset_id = s.dataset_id
ORDER BY s.row_count DESC;
```

---

## 🎨 Trino UI Features

### **1. Query Editor**

```
http://localhost:8081

┌─────────────────────────────────────────────────────┐
│ Trino Web UI                          [?] [+] [...]│
├─────────────────────────────────────────────────────┤
│                                                     │
│ SELECT * FROM gates.bronze.project_monitoring     │
│ WHERE dt = '2026-09-14'                          │
│ LIMIT 10;                                         │
│                                                     │
│ [Execute] [⏹ Cancel] [💾 Save] [📋 Format]        │
│                                                     │
├─────────────────────────────────────────────────────┤
│                                                     │
│ Results (10 rows)                                 │
│ ┌────────────────────────────────────────────────┐ │
│ │ project_id | agency | region | budget_... │ │ │
│ ├────────────────────────────────────────────────┤ │
│ │ PCH-001    | PCHRD  | R4A    | 5200000  │ │ │
│ │ PCH-002    | PCHRD  | R4A    | 8500000  │ │ │
│ │ ...                                      │ │ │
│ └────────────────────────────────────────────────┘ │
│                                                     │
│ Query Stats:                                       │
│ ├─ Execution Time: 2.34s                          │
│ ├─ Data Scanned: 125 MB                           │
│ ├─ Rows Returned: 10                              │
│ └─ Peak Memory: 512 MB                            │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### **2. Catalog Browser**

```
Catalogs                              Tables
├─ iceberg                ├─ gates
│  ├─ gates                │  ├─ bronze
│  │  ├─ project_monitoring(14 tables)
│  │  │  ├─ project_id (string)
│  │  │  ├─ agency (string)
│  │  │  └─ ...
│  ├─ rd_equipment
│  └─ finance_projects
├─ postgresql
│  ├─ public
│  │  ├─ dataset_registry
│  │  ├─ config_audit_log
│  │  └─ ...
└─ hive (optional)
```

### **3. Query History**

```
Recent Queries:
┌───────────────────────────────────────────────────┐
│ Query                    | Status | Duration | User│
├───────────────────────────────────────────────────┤
│ SELECT * FROM gates.... | ✅ OK  | 2.34s   | admin
│ SELECT COUNT(*) FROM... | ✅ OK  | 0.12s   | admin
│ SELECT * FROM iceber... | ✅ OK  | 1.87s   | data_eng
│ SELECT * FROM gate....  | ❌ FAIL| 3.42s   | admin
│ (10 more queries)       |        |         |      │
└───────────────────────────────────────────────────┘
```

---

## 🔌 Trino Integration with Airflow

### **Option 1: Execute Trino Queries from Airflow**

```python
# ingestion/trino_queries.py

from pyhive import presto
from pyhive.exc import DatabaseError

def execute_trino_query(query: str, catalog: str = 'iceberg', schema: str = 'gates'):
    """Execute query on Trino."""
    
    try:
        conn = presto.connect(
            host='trino',
            port=8080,
            username='trino',
            catalog=catalog,
            schema=schema,
        )
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return results
    except DatabaseError as e:
        print(f"[Trino] Query failed: {e}")
        raise

# Usage in Airflow:
from airflow.operators.python import PythonOperator

def validate_gold_metrics(**context):
    """Validate that gold layer has metrics."""
    
    query = """
        SELECT COUNT(*) as metric_count
        FROM gates.gold.rd_portfolio_performance
        WHERE quarter >= '2026-Q3'
    """
    
    results = execute_trino_query(query)
    metric_count = results[0][0]
    
    if metric_count == 0:
        raise ValueError("Gold layer is empty!")
    
    print(f"[Trino] Validated: {metric_count} metrics in gold layer")

validate_task = PythonOperator(
    task_id='validate_gold_metrics',
    python_callable=validate_gold_metrics,
    provide_context=True,
)
```

### **Option 2: dbt uses Trino as Backend**

```yaml
# dbt_project/profiles.yml

gates:
  outputs:
    dev:
      type: trino
      method: none  # No auth for local dev
      host: trino
      port: 8080
      catalog: iceberg
      schema: gates_dev
      threads: 4
      
    prod:
      type: trino
      method: ldap  # LDAP auth for production
      host: trino-prod.company.com
      port: 8080
      user: dbt_user
      password: "{{ env_var('TRINO_PASSWORD') }}"
      catalog: iceberg
      schema: gates
      threads: 8
      
  target: dev
```

**dbt uses Trino to:**
- Execute Bronze/Silver/Gold models
- Create/update Iceberg tables
- Run tests on data
- Generate documentation

---

## 📊 Trino Monitoring

### **1. Query Performance**

```python
# monitoring/trino_monitoring.py

def monitor_trino_queries():
    """Monitor slow queries on Trino."""
    
    query = """
        SELECT 
            query_id,
            query,
            execution_time_seconds,
            data_scanned_bytes,
            peak_memory_bytes,
            state
        FROM system.runtime.tasks
        WHERE execution_time_seconds > 10
        ORDER BY execution_time_seconds DESC
        LIMIT 20
    """
    
    results = execute_trino_query(query, catalog='system', schema='runtime')
    
    for row in results:
        print(f"[Trino] Slow query: {row[0]} took {row[2]}s, scanned {row[3]} bytes")
```

### **2. Connector Health**

```python
def check_trino_connectors():
    """Check if all connectors are healthy."""
    
    connectors = ['iceberg', 'postgresql', 'hive']
    
    for connector in connectors:
        try:
            execute_trino_query(
                f"SELECT 1 FROM {connector}.information_schema.tables LIMIT 1"
            )
            print(f"[Trino] {connector} connector: ✅ OK")
        except Exception as e:
            print(f"[Trino] {connector} connector: ❌ FAILED - {e}")
```

---

## 🎯 Demo Points for Trino

1. **Query Bronze**: Show raw ingested data
2. **Query Silver**: Show deduplicated data with soft rule flags
3. **Query Gold**: Show aggregated business metrics
4. **Time-Travel**: Query data as it was yesterday
5. **Performance**: Show query execution time and data scanned
6. **Federation**: Query across Iceberg and PostgreSQL
7. **UI**: Browse catalogs and run interactive queries
8. **Results**: Export results as CSV/JSON

---

## ✅ Trino + Iceberg Benefits

| Aspect | Benefit |
|--------|---------|
| **Querying** | Standard SQL over data lake (no data movement) |
| **Performance** | Distributed execution across workers |
| **Schema Evolution** | Add/drop columns without migration |
| **Time-Travel** | Query historical versions of data |
| **ACID** | No data corruption or partial writes |
| **Open Format** | Iceberg is vendor-agnostic (not Hive) |
| **Caching** | Metadata caching for faster queries |
| **Partitioning** | Intelligent partition pruning |

---

## 🚀 Complete Stack

```
Query Layer (Trino)
       ↑
SQL Transformation (dbt)
       ↑
Data Storage (MinIO + Iceberg)
       ↓
Data Ingestion (Airflow)
       ↓
Data Sources (API, File, Kafka CDC)
```

This creates a **complete, production-grade data platform** where:
- ✅ Data flows from sources to lake
- ✅ Data is transformed via dbt (using Trino)
- ✅ Data is queryable via Trino SQL
- ✅ Data is cataloged in DataHub
- ✅ All orchestrated by Airflow
- ✅ All monitored and logged

**That's the complete GATES pipeline!**
