# GATES Production Prototype: Hybrid Configuration Approach

**Updated: 2026-09-14**  
**Approach: YAML + Database (Both Supported)**

---

## 🎯 Why Hybrid Configuration?

```
The best approach is NOT choosing between YAML or Database...
It's supporting BOTH with automatic switching!

┌─────────────────────────────────────────────────────────┐
│                 ConfigRegistry                          │
│                                                          │
│  def get_config(dataset):                              │
│    try:                                                 │
│      return load_from_database(dataset)  # Fast        │
│    except:                                              │
│      return load_from_yaml(dataset)      # Resilient   │
│                                                          │
│  # Team gets: Scalability + Simplicity + Resilience   │
└─────────────────────────────────────────────────────────┘
```

---

## 📋 Configuration Architecture

### **Three Demo Scenarios**

#### **Scenario 1: YAML-Only (Development)**
```
docker-compose.yml (DEV)
├─ No PostgreSQL
├─ Config directory: config/
├─ ConfigRegistry:
│  ├─ use_db=False (database disabled)
│  └─ Always loads from YAML files
└─ Team sees:
   ✅ Simple setup
   ✅ Git version control
   ❌ No dynamic updates (requires commit)
   ❌ Scales to ~100 datasets
```

**Files to show:**
- `config/schema_project_monitoring.yaml`
- `config/column_aliasing_project_monitoring.yaml`
- `config/common_quality_rules.yaml`

#### **Scenario 2: Database-Only (Enterprise)**
```
AWS Production
├─ RDS PostgreSQL
├─ No YAML (optional backup)
├─ ConfigRegistry:
│  ├─ use_db=True
│  ├─ db_url="postgresql://..."
│  └─ Always loads from database
└─ Team sees:
   ✅ Dynamic updates (no redeploy)
   ✅ Scales to 1000+ datasets
   ✅ Audit trail (who changed what when)
   ❌ Database dependency
   ❌ Complex to set up
```

**Tables to show:**
- `dataset_registry` (metadata)
- `ingestion_source` (configs)
- `field_mapping` (aliasing)
- `config_audit_log` (audit trail)

#### **Scenario 3: Hybrid with Fallback (Best Practice)**
```
Production (Recommended)
├─ RDS PostgreSQL (primary)
├─ YAML files (backup)
├─ ConfigRegistry:
│  ├─ use_db=True
│  ├─ db_url="postgresql://..."
│  └─ Fallback to YAML if DB unavailable
└─ Team sees:
   ✅ Dynamic updates (from DB)
   ✅ Scalable (1000+ datasets)
   ✅ Resilient (YAML fallback)
   ✅ Audit trail (from DB)
   ✅ Never stops due to config access
```

---

## 🛠️ Implementation: What Gets Built

### **Phase 1: Unified ConfigRegistry Class**

**File:** `common/config_registry.py`

```python
class ConfigRegistry:
    """
    Single interface supporting both YAML and Database configs.
    Transparent backend selection with automatic fallback.
    """
    
    def __init__(self, yaml_dir, db_url=None, use_db=True):
        self.yaml_dir = yaml_dir
        self.db_url = db_url
        self.use_db = use_db and db_url is not None
        self.cache = TTLCache(maxsize=5000, ttl=60)
    
    def get_dataset(self, dataset_name: str) -> dict:
        """
        Automatic source selection:
        1. Try database (if configured)
        2. Fall back to YAML (if available)
        3. Cache result (60 seconds)
        """
        # Returns exact same dict regardless of source!
        # Caller doesn't know if it came from DB or YAML
```

### **Phase 2: Three Ingestion Handlers** (Work with both backends)

```
ingestion/
├─ ingest_api_airbyte.py
│  ├─ Loads config: registry.get_source_by_type(dataset, "airbyte")
│  └─ Works with both YAML and DB
│
├─ ingest_file_navi_gates.py
│  ├─ Loads config: registry.get_source_by_type(dataset, "file")
│  └─ Works with both YAML and DB
│
└─ ingest_kafka_cdc.py
   ├─ Loads config: registry.get_source_by_type(dataset, "kafka")
   └─ Works with both YAML and DB
```

**Key insight:** Handlers are config-agnostic. They just call `ConfigRegistry.get_*()` and don't care about the backend.

### **Phase 3: Airflow DAG Factory**

```python
# orchestration/dag_factory.py

def build_dag_for_dataset(dataset_name, config_registry):
    """
    Generate DAG from config (YAML or DB, doesn't matter).
    
    Pseudo-code:
    1. config = config_registry.get_dataset(dataset_name)
    2. Create ingestion tasks based on config['sources']
    3. Add validation, staging, dbt, etc tasks
    4. Return DAG (works with both config sources)
    """
```

**Result:** 
- One DAG factory handles all datasets
- Works seamlessly with YAML or DB config
- Team can switch backends without code changes

### **Phase 4-9: Rest of Pipeline**

(Unchanged from main architecture, works with both backends)

---

## 📊 Demo Walkthrough: Showing Both Approaches

### **Demo Flow (45 minutes)**

```
PART 1: Configuration Management (10 min)

1. YAML Config (5 min)
   ├─ Show: config/ folder
   ├─ Files: schema_*.yaml, column_aliasing_*.yaml
   ├─ Live edit: Change a field in YAML
   ├─ Show: Git tracks the change
   └─ Explain: Simple for dev/test

2. Database Config (5 min)
   ├─ Connect: pgAdmin
   ├─ Query: SELECT * FROM dataset_registry;
   ├─ Insert: Add new dataset row
   ├─ Show: Instant config available (no commit needed)
   └─ Explain: Scalable for enterprise

PART 2: ConfigRegistry In Action (10 min)

3. Show ConfigRegistry code
   ├─ Explain: Unified interface
   ├─ Show: Priority logic (DB first, YAML fallback)
   └─ Show: Caching (TTL=60s)

4. Run: config_registry.get_dataset("project_monitoring")
   ├─ Output with YAML backend: "_config_source": "yaml"
   ├─ Output with DB backend: "_config_source": "database"
   └─ Explain: Same config object, different source

5. Simulate DB unavailability
   ├─ Action: Stop PostgreSQL container
   ├─ Result: Automatic fallback to YAML
   ├─ Show: Pipeline continues running!
   └─ Explain: Resilience through hybrid approach

PART 3: Full Pipeline Execution (15 min)

6. Run Airflow DAG (works with both configs)
   ├─ Ingest from API (config from DB or YAML)
   ├─ Ingest from file (config from DB or YAML)
   ├─ Validate hard rules (config from DB or YAML)
   ├─ Stage to MinIO
   ├─ Run dbt (Bronze → Silver → Gold)
   └─ Explain: All works transparently

7. Show monitoring
   ├─ CloudWatch logs (structured JSON)
   ├─ Grafana dashboard (metrics)
   ├─ DataHub lineage (metadata)
   └─ Airflow UI (task execution)

PART 4: Team Discussion (10 min)

8. Ask team:
   ├─ "Prefer simple YAML for dev?"
   ├─ "Need dynamic DB updates for prod?"
   ├─ "Want both for flexibility?"
   └─ Recommendation: Use hybrid (best of both)
```

---

## 🚀 Demo Environments: What You'll Deploy

### **Environment 1: YAML-Only (Docker Compose - Dev)**

```yaml
# docker-compose-yaml-only.yml

version: '3.8'
services:
  airflow:
    image: apache/airflow:latest
    environment:
      - CONFIG_REGISTRY_USE_DB=false
      - CONFIG_YAML_DIR=/opt/airflow/config
    volumes:
      - ./config:/opt/airflow/config
    ports:
      - "8080:8080"  # Airflow UI

  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"

  trino:
    image: trinodb/trino:latest
    environment:
      - MINIO_ENDPOINT=minio:9000

  # No PostgreSQL!
```

**Team sees:**
- ✅ Simple setup (5 services)
- ✅ YAML configs visible in Git
- ✅ Perfect for dev/test
- ✅ No database dependency

### **Environment 2: Database-Only (Docker Compose - Staging)**

```yaml
# docker-compose-db-only.yml

version: '3.8'
services:
  postgres:
    image: postgres:14
    environment:
      - POSTGRES_DB=gates_config
    ports:
      - "5432:5432"

  pgadmin:
    image: dpage/pgadmin4:latest
    ports:
      - "5050:80"  # pgAdmin UI

  airflow:
    image: apache/airflow:latest
    environment:
      - CONFIG_REGISTRY_USE_DB=true
      - DATABASE_URL=postgresql://user:pass@postgres:5432/gates_config
    depends_on:
      - postgres

  minio:
    image: minio/minio:latest

  trino:
    image: trinodb/trino:latest

  # No YAML files (or minimal)!
```

**Team sees:**
- ✅ Database-driven config
- ✅ pgAdmin UI for browsing tables
- ✅ Dynamic updates (INSERT/UPDATE)
- ✅ Audit trail visible in tables

### **Environment 3: Hybrid (Docker Compose + AWS - Production)**

```yaml
# docker-compose-hybrid.yml

version: '3.8'
services:
  postgres:
    image: postgres:14
    # RDS equivalent in AWS

  airflow:
    image: apache/airflow:latest
    environment:
      - CONFIG_REGISTRY_USE_DB=true
      - CONFIG_REGISTRY_DB_URL=${DB_URL}
      - CONFIG_YAML_DIR=/opt/airflow/config  # Fallback
    volumes:
      - ./config:/opt/airflow/config

  # ... minio, trino, etc
```

**Team sees:**
- ✅ Best of both worlds
- ✅ DB for performance/scalability
- ✅ YAML for resilience
- ✅ Automatic fallback if DB unavailable

---

## 📋 Checklist: What You'll Demonstrate

### **For the Team**

✅ **Configuration Management**
- [ ] Show YAML files (simple, git-based)
- [ ] Show Database tables (dynamic, scalable)
- [ ] Show ConfigRegistry code (unified interface)

✅ **Three Ingestion Types**
- [ ] Airbyte API (works with YAML or DB config)
- [ ] File-based (works with YAML or DB config)
- [ ] Kafka CDC (works with YAML or DB config)

✅ **Hard & Soft Validation**
- [ ] Show hard rules (quarantine on failure)
- [ ] Show soft rules (flag but promote)
- [ ] Show quarantine zone (failed rows)

✅ **Storage Layer**
- [ ] MinIO browser (see data files)
- [ ] Iceberg metadata (schema, snapshots)
- [ ] Time-travel query (query old data)

✅ **Transformation**
- [ ] dbt Bronze model (typed, partitioned)
- [ ] dbt Silver model (deduped, soft flags)
- [ ] dbt Gold model (aggregated)

✅ **Data Cataloging**
- [ ] DataHub search (find datasets)
- [ ] LineageGraph (source → bronze → silver → gold)
- [ ] Column-level metrics (null rates, cardinality)

✅ **Observability**
- [ ] CloudWatch logs (structured)
- [ ] Grafana dashboards (KPIs)
- [ ] Airflow UI (task execution)
- [ ] Failure handling (retries, alerts)

✅ **Infrastructure**
- [ ] Terraform code (AWS resources)
- [ ] Docker Compose local setup
- [ ] Database schema (config tables)

---

## 🎁 Deliverables Structure

```
gates-pipeline-production/
│
├─ 00_PLAN.md (this architecture plan)
├─ 01_HYBRID_CONFIG_GUIDE.md (ConfigRegistry + both approaches)
├─ 02_CONFIG_EXAMPLES.md (YAML vs DB side-by-side)
│
├─ common/
│  ├─ config_registry.py (Unified config interface)
│  └─ ingestion_framework.py (Shared logic)
│
├─ ingestion/
│  ├─ ingest_api_airbyte.py (Works with both)
│  ├─ ingest_file_navi_gates.py (Works with both)
│  └─ ingest_kafka_cdc.py (Works with both)
│
├─ config/  ← YAML configuration (Option A)
│  ├─ schema_project_monitoring.yaml
│  ├─ column_aliasing_project_monitoring.yaml
│  ├─ common_quality_rules.yaml
│  └─ airbyte_manifest.yaml
│
├─ database/  ← Database schema (Option B)
│  ├─ init_config_db.sql
│  ├─ sample_data.sql
│  └─ migrations/
│
├─ orchestration/
│  ├─ dag_factory.py (DAG factory for all datasets)
│  ├─ task_definitions.py
│  └─ failure_handling.py
│
├─ transformation/dbt_project/
│  ├─ models/bronze/
│  ├─ models/silver/
│  └─ models/gold/
│
├─ terraform/  ← AWS infrastructure
│  ├─ main.tf
│  ├─ rds.tf
│  ├─ s3.tf
│  └─ docker-compose.yml
│
└─ demo/
   ├─ demo-yaml-only.md (YAML scenario)
   ├─ demo-database-only.md (DB scenario)
   ├─ demo-hybrid.md (Best practice)
   └─ team_presentation.pptx
```

---

## ✅ Advantages of Hybrid Approach

| Aspect | YAML Only | DB Only | **Hybrid ✅** |
|--------|-----------|---------|-----------|
| Dev Simplicity | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ |
| Prod Scalability | ⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Dynamic Updates | ⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Resilience | ⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Audit Trail | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Git Control | ⭐⭐⭐⭐⭐ | ⭐ | ⭐⭐⭐⭐ |
| Operational Overhead | ⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ |

**Winner:** Hybrid (perfect balance of all factors)

---

## 🚦 What's Ready to Build

### ✅ Complete Plan
- Architecture design (all 10 components)
- Database schema
- Config examples (YAML + SQL)
- Demo scenarios
- Terraform structure

### 🔨 Ready to Code
1. ConfigRegistry class (3 hours)
2. Ingestion handlers (YAML + DB support) (6 hours)
3. DAG factory (4 hours)
4. dbt models (3 hours)
5. Monitoring setup (3 hours)
6. Terraform (AWS deployment) (4 hours)
7. Docker Compose (local dev) (2 hours)

**Total Development: ~25 hours = 3-4 weeks full-time**

---

## 🎬 Next Step: Approval

**Should I proceed with building the complete working prototype with:**

✅ **Hybrid ConfigRegistry** (YAML + Database)  
✅ **Three Ingestion Types** (Airbyte API, File, Kafka CDC)  
✅ **Hard & Soft Validation** (quarantine + flags)  
✅ **MinIO + Iceberg** (storage)  
✅ **dbt Transformation** (Bronze/Silver/Gold)  
✅ **Airflow Orchestration** (DAG factory)  
✅ **DataHub Cataloging** (metadata + lineage)  
✅ **Full Observability** (logging, monitoring, dashboards)  
✅ **Terraform Deployment** (AWS infrastructure)  
✅ **Demo Ready** (local + cloud)  

### Approval Needed For:
- ✅ Proceed with Phase 1: ConfigRegistry + DB schema
- ✅ Approval to build all 9 phases
- ✅ AWS account details for Terraform deployment
- ✅ Team's preference on demo environments (all 3 or subset?)

**Shall I start Phase 1? 👉**
