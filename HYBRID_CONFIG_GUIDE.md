# GATES Configuration Management: Hybrid Approach (YAML + Database)

## 📋 Overview

This document shows how to **run the prototype with BOTH configuration approaches**:
- **YAML-based** (Git-controlled, version history)
- **Database-based** (Dynamic, scalable, audit trail)
- **Hybrid ConfigRegistry** (Unified interface, automatic fallback)

---

## 🔄 Hybrid ConfigRegistry Class

```python
# common/config_registry.py

from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Any
import yaml
import json
from cachetools import TTLCache
from sqlalchemy import create_engine, text

class ConfigRegistry:
    """
    Unified config accessor supporting YAML + Database backends.
    
    Priority: Database (if available) → YAML (fallback)
    Usage: ConfigRegistry is backend-agnostic. Caller doesn't care which source.
    """
    
    def __init__(self, 
                 yaml_dir: str = "config/",
                 db_url: Optional[str] = None,
                 use_db: bool = True,
                 cache_ttl_seconds: int = 60):
        """
        Initialize config registry.
        
        Args:
            yaml_dir: Path to YAML config directory
            db_url: PostgreSQL connection URL (optional)
            use_db: If True, try database first before YAML
            cache_ttl_seconds: Cache TTL for performance
        """
        self.yaml_dir = Path(yaml_dir)
        self.db_url = db_url
        self.use_db = use_db and db_url is not None
        self.cache = TTLCache(maxsize=5000, ttl=cache_ttl_seconds)
        
        print(f"[ConfigRegistry] Initialized")
        print(f"  - YAML directory: {yaml_dir}")
        print(f"  - Database: {'Enabled' if self.use_db else 'Disabled'}")
        print(f"  - Fallback: {'YAML' if self.use_db else 'None'}")
    
    def get_dataset(self, dataset_name: str) -> Dict[str, Any]:
        """
        Get complete configuration for a dataset.
        Tries database first, falls back to YAML.
        """
        
        # Check cache
        cache_key = f"dataset_{dataset_name}"
        if cache_key in self.cache:
            cached = self.cache[cache_key]
            print(f"[ConfigRegistry] Cache hit: {dataset_name}")
            return cached
        
        config = None
        source = None
        
        # Try database first
        if self.use_db:
            try:
                config = self._load_from_db(dataset_name)
                source = "database"
                print(f"[ConfigRegistry] Loaded {dataset_name} from DATABASE")
            except Exception as e:
                print(f"[ConfigRegistry] ⚠️  DB load failed for {dataset_name}: {e}")
                print(f"[ConfigRegistry] Attempting YAML fallback...")
        
        # Fallback to YAML
        if config is None:
            try:
                config = self._load_from_yaml(dataset_name)
                source = "yaml"
                print(f"[ConfigRegistry] Loaded {dataset_name} from YAML")
            except Exception as e:
                raise ValueError(f"Failed to load config for {dataset_name} from both DB and YAML") from e
        
        # Add metadata
        config['_config_source'] = source  # Track which backend was used
        config['_loaded_at'] = datetime.now().isoformat()
        
        # Cache
        self.cache[cache_key] = config
        
        return config
    
    def _load_from_yaml(self, dataset_name: str) -> Dict[str, Any]:
        """
        Load configuration from YAML files.
        
        Expected files:
        - config/schema_{dataset_name}.yaml
        - config/column_aliasing_{dataset_name}.yaml
        - config/validation_rules_{dataset_name}.yaml
        """
        
        schema_file = self.yaml_dir / f"schema_{dataset_name}.yaml"
        aliasing_file = self.yaml_dir / f"column_aliasing_{dataset_name}.yaml"
        rules_file = self.yaml_dir / f"common_quality_rules.yaml"
        
        # Load schema
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file}")
        
        with open(schema_file, 'r') as f:
            schema = yaml.safe_load(f)
        
        # Load aliasing
        if not aliasing_file.exists():
            raise FileNotFoundError(f"Aliasing file not found: {aliasing_file}")
        
        with open(aliasing_file, 'r') as f:
            aliasing = yaml.safe_load(f)
        
        # Load validation rules
        if not rules_file.exists():
            raise FileNotFoundError(f"Rules file not found: {rules_file}")
        
        with open(rules_file, 'r') as f:
            rules = yaml.safe_load(f)
        
        return {
            'dataset': schema.get('dataset'),
            'version': schema.get('version'),
            'canonical_schema': schema.get('canonical_schema'),
            'sources': schema.get('sources', []),
            'field_aliasing': aliasing.get('sources', []),
            'validation_rules': rules.get('rule_categories', []),
            'validation_failures': rules.get('on_failure', {}),
        }
    
    def _load_from_db(self, dataset_name: str) -> Dict[str, Any]:
        """
        Load configuration from PostgreSQL database.
        
        Tables queried:
        - dataset_registry (metadata)
        - ingestion_schema (canonical schema)
        - ingestion_source (source configs)
        - field_mapping (aliasing)
        - validation_rule (validation rules)
        """
        
        if not self.db_url:
            raise RuntimeError("Database URL not configured")
        
        engine = create_engine(self.db_url)
        
        with engine.connect() as conn:
            # 1. Get dataset metadata
            dataset_row = conn.execute(
                text("""
                    SELECT * FROM dataset_registry 
                    WHERE dataset_name = :name AND status = 'active'
                    LIMIT 1
                """),
                {"name": dataset_name}
            ).mappings().first()
            
            if not dataset_row:
                raise KeyError(f"Dataset {dataset_name} not found in database")
            
            dataset_id = dataset_row['dataset_id']
            
            # 2. Get schema
            schema_row = conn.execute(
                text("""
                    SELECT * FROM ingestion_schema 
                    WHERE dataset_id = :id AND status = 'active'
                    ORDER BY schema_version DESC LIMIT 1
                """),
                {"id": dataset_id}
            ).mappings().first()
            
            # 3. Get ingestion sources (multiple)
            sources_rows = conn.execute(
                text("""
                    SELECT * FROM ingestion_source 
                    WHERE dataset_id = :id AND status = 'active'
                    ORDER BY priority ASC
                """),
                {"id": dataset_id}
            ).mappings().all()
            
            # 4. Get field mappings
            mappings_rows = conn.execute(
                text("""
                    SELECT * FROM field_mapping 
                    WHERE schema_id = :id
                """),
                {"id": schema_row['schema_id']}
            ).mappings().all()
            
            # 5. Get validation rules
            rules_rows = conn.execute(
                text("""
                    SELECT * FROM validation_rule 
                    WHERE dataset_id = :id
                """),
                {"id": dataset_id}
            ).mappings().all()
        
        # Format as same structure as YAML
        return {
            'dataset': dataset_name,
            'version': schema_row['schema_version'],
            'dataset_id': dataset_id,
            'canonical_schema': {
                'unique_fields': schema_row['unique_fields'],
                'fields': json.loads(schema_row['fields']),
            },
            'sources': [
                {
                    'source_id': row['source_id'],
                    'source_type': row['source_type'],
                    'config': json.loads(row['config']),
                }
                for row in sources_rows
            ],
            'field_aliasing': [
                {
                    'source_id': row['source_id'],
                    'aliases': [{
                        'source_field': row['raw_field'],
                        'canonical_field': row['canonical_field'],
                        'transform': row['transform'],
                    } for row in mappings_rows if row['source_id'] == row['source_id']]
                }
                for row in set((r['source_id'],) for r in mappings_rows)
            ],
            'validation_rules': [
                {
                    'rule_id': row['rule_id'],
                    'field_name': row['field_name'],
                    'rule_type': row['rule_type'],
                    'severity': row['severity'],
                    'config': json.loads(row['rule_config']),
                }
                for row in rules_rows
            ],
        }
    
    def list_datasets(self) -> List[str]:
        """
        List all available datasets from both YAML and database.
        Returns combined, deduplicated list.
        """
        
        datasets_db = set()
        datasets_yaml = set()
        
        # From database
        if self.use_db:
            try:
                engine = create_engine(self.db_url)
                with engine.connect() as conn:
                    rows = conn.execute(
                        text("SELECT dataset_name FROM dataset_registry WHERE status='active'")
                    ).fetchall()
                    datasets_db = {row[0] for row in rows}
                print(f"[ConfigRegistry] Found {len(datasets_db)} datasets in database")
            except Exception as e:
                print(f"[ConfigRegistry] ⚠️  Failed to list DB datasets: {e}")
        
        # From YAML files
        for yaml_file in self.yaml_dir.glob("schema_*.yaml"):
            dataset = yaml_file.name.replace("schema_", "").replace(".yaml", "")
            datasets_yaml.add(dataset)
        
        print(f"[ConfigRegistry] Found {len(datasets_yaml)} datasets in YAML")
        
        # Merge (prefer DB, but include YAML)
        all_datasets = list(datasets_db | datasets_yaml)
        
        return sorted(all_datasets)
    
    def update_config(self, dataset_name: str, section: str, updates: Dict, reason: str = ""):
        """
        Update configuration (database only).
        
        Args:
            dataset_name: Dataset to update
            section: Which section (schema, source, validation_rule, etc)
            updates: Changes to apply
            reason: Audit reason
        """
        
        if not self.use_db:
            raise RuntimeError("Database not configured. Cannot update config.")
        
        engine = create_engine(self.db_url)
        
        with engine.connect() as conn:
            if section == "ingestion_source":
                # Update source config
                conn.execute(
                    text("""
                        UPDATE ingestion_source 
                        SET config = :config, modified_at = NOW(), modified_by = :user
                        WHERE source_id = :id
                    """),
                    {
                        'config': json.dumps(updates['config']),
                        'user': 'admin',  # Should be from context
                        'id': updates['source_id'],
                    }
                )
            
            # Log to audit trail
            conn.execute(
                text("""
                    INSERT INTO config_audit_log 
                    (dataset_id, table_modified, change_type, changed_by, reason, new_value, changed_at)
                    SELECT dataset_id, :section, 'UPDATE', :user, :reason, :updates, NOW()
                    FROM dataset_registry WHERE dataset_name = :dataset
                """),
                {
                    'dataset': dataset_name,
                    'section': section,
                    'user': 'admin',
                    'reason': reason,
                    'updates': json.dumps(updates),
                }
            )
            
            conn.commit()
        
        # Invalidate cache
        self.cache.clear()
        print(f"[ConfigRegistry] Updated {dataset_name}.{section}")
    
    def sync_yaml_to_db(self, dataset_name: str):
        """
        Seed database from YAML file.
        Used during migration from YAML-only to hybrid setup.
        """
        
        if not self.use_db:
            raise RuntimeError("Database not configured.")
        
        yaml_config = self._load_from_yaml(dataset_name)
        engine = create_engine(self.db_url)
        
        with engine.connect() as conn:
            # Insert into dataset_registry
            conn.execute(
                text("""
                    INSERT INTO dataset_registry 
                    (dataset_id, dataset_name, version, status, created_at)
                    VALUES (gen_random_uuid(), :name, :version, 'active', NOW())
                    ON CONFLICT(dataset_name) DO NOTHING
                """),
                {'name': dataset_name, 'version': yaml_config.get('version')}
            )
            
            # Insert schema
            # Insert sources
            # Insert field mappings
            # Insert validation rules
            
            conn.commit()
        
        print(f"[ConfigRegistry] ✅ Synced {dataset_name} from YAML to database")

# Usage Examples
```

---

## 🎯 Demo Scenarios: YAML vs Database

### **Demo 1: YAML-Only Setup (Dev Environment)**

```python
# docker-compose.yml (development)
# No database service

# app configuration
config_registry = ConfigRegistry(
    yaml_dir="config/",
    db_url=None,  # No database
    use_db=False,  # Force YAML-only
)

# Result: Always loads from YAML
config = config_registry.get_dataset("project_monitoring")
print(config['_config_source'])  # Output: "yaml"
```

**What Team Sees:**
```
✅ Config loaded from: YAML
✅ Files: config/schema_*.yaml, config/column_aliasing_*.yaml
✅ Version control: Yes (Git history)
✅ Dynamic updates: No (requires code commit)
✅ Use case: Development, small teams
```

---

### **Demo 2: Database-Only Setup (Production)**

```python
# production configuration
config_registry = ConfigRegistry(
    yaml_dir="config/",  # Ignored
    db_url="postgresql://user:pass@rds.amazonaws.com:5432/gates_config",
    use_db=True,
)

# Result: Always loads from Database (YAML not accessed)
config = config_registry.get_dataset("project_monitoring")
print(config['_config_source'])  # Output: "database"
```

**What Team Sees:**
```
✅ Config loaded from: DATABASE
✅ Storage: PostgreSQL RDS
✅ Version control: No (but audit trail in DB)
✅ Dynamic updates: Yes (instant)
✅ Use case: Enterprise, 1000+ datasets
✅ Audit trail: config_audit_log table tracks all changes
```

---

### **Demo 3: Hybrid Setup (Fallback Enabled)**

```python
# hybrid configuration
config_registry = ConfigRegistry(
    yaml_dir="config/",
    db_url="postgresql://user:pass@localhost:5432/gates_config",
    use_db=True,  # Prefer DB, fallback to YAML
)

# Scenario A: Database available
config = config_registry.get_dataset("project_monitoring")
print(config['_config_source'])  # Output: "database"

# Scenario B: Database temporarily down
# (e.g., network issue, RDS restarting)
config = config_registry.get_dataset("project_monitoring")
print(config['_config_source'])  # Output: "yaml" (fallback!)
print("[ConfigRegistry] ⚠️  DB load failed. Falling back to YAML")
```

**What Team Sees:**
```
✅ Primary: DATABASE (fast, dynamic)
✅ Fallback: YAML (resilient, always available)
✅ Best of both: Scalability + Resilience
✅ Use case: Mission-critical production
✅ Benefit: Pipeline never stops due to config access
```

---

## 📊 Configuration Comparison Table

| Aspect | YAML-Only | Database-Only | Hybrid |
|--------|-----------|---------------|--------|
| **Setup Complexity** | Simple (1 file) | Medium (DB + schema) | Medium (both) |
| **Config Updates** | Code commit (2 hrs) | SQL/UI (5 min) | SQL/UI (5 min) |
| **Disaster Recovery** | Git history | DB backups | Both |
| **Scalability** | 100s datasets | 1000s datasets | 1000s datasets |
| **Access Control** | Repo-level | DB roles/granular | DB roles/granular |
| **Audit Trail** | Git commits | DB audit log | DB audit log + Git |
| **Resilience** | No fallback | DB dependency | YAML fallback |
| **Recommended** | Dev/test | Enterprise | Best for prod |

---

## 🛠️ Implementation: Show Both in Demo

### **Demo Walkthrough**

```
DEMO PART 1: YAML Configuration (5 min)
  1. Show config/schema_*.yaml files
  2. Show how they're loaded
  3. Show version history (git log)
  4. Change file locally → DAG picks up change
  5. Explain: Simple but requires redeploy

DEMO PART 2: Database Configuration (5 min)
  1. Show PostgreSQL database (pgAdmin)
  2. Show dataset_registry, ingestion_source tables
  3. Show live update (INSERT/UPDATE)
  4. Show audit_log (who changed what when)
  5. Explain: Dynamic but requires DB

DEMO PART 3: Hybrid ConfigRegistry (5 min)
  1. Show ConfigRegistry class code
  2. Show fallback in action:
     - Database available → loads from DB
     - Simulate DB down → loads from YAML
  3. Show cache hit/miss
  4. Explain: Best of both worlds
  5. Show metrics: "Config source: [database/yaml]"

DEMO PART 4: Choose Your Path (5 min)
  1. Dev environment (YAML-only)
     - docker-compose.yml without DB
     - Show YAML configs work
  2. Production environment (Database)
     - AWS RDS PostgreSQL
     - Show dynamic updates
  3. Hybrid (Recommended)
     - Both available
     - DB preferred, YAML fallback
```

---

## 📝 Team Discussion Points

**Ask your team:**
1. Do you prefer version-controlled YAML or dynamic DB updates?
2. How often do configs change post-deployment?
3. Do you need audit trail for compliance?
4. How many datasets will you manage?

**Recommendations:**
- **Dev/Test:** YAML-only (simple, git-based)
- **Staging:** Hybrid (test both approaches)
- **Production:** Database + YAML fallback (resilience + scalability)

---

## ✅ Next Steps

1. ✅ Implement ConfigRegistry class (3 hours)
2. ✅ Create YAML config files for demo (1 hour)
3. ✅ Set up PostgreSQL schema (2 hours)
4. ✅ Run local demo with both backends (2 hours)
5. ✅ Deploy to AWS with hybrid approach (3 hours)
