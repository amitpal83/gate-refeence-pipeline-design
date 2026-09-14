# DataHub Integration Guide

**Purpose:** Register all datasets with DataHub for discovery, lineage, and metadata management

---

## 🎯 What DataHub Does in GATES Pipeline

```
GATES Pipeline
├─ Source (Airbyte API, File, Kafka CDC)
├─ Staging (MinIO Bronze layer)
├─ Transformation (dbt Silver/Gold)
└─ Analytics (BI Tools)

DataHub Role:
├─ Discover: Search for datasets, browse schemas
├─ Lineage: Track data flow (API → Bronze → Silver → Gold)
├─ Quality: Track validation metrics, soft rule violations
├─ Ownership: Track who owns each dataset
├─ SLA: Track freshness, completeness, accuracy
└─ Catalog: All metadata visible in UI
```

---

## 📊 DataHub Architecture in GATES

```
┌─────────────────────────────────────┐
│    GATES Ingestion Pipeline         │
│  (Airflow, Python, dbt)             │
└──────────────┬──────────────────────┘
               │ 1. Emit events
               ↓
┌─────────────────────────────────────┐
│    DataHub MCE Emitter              │
│  (metadata_emit.py)                 │
└──────────────┬──────────────────────┘
               │ 2. Send aspects
               ↓
┌─────────────────────────────────────┐
│    DataHub Metadata Service         │
│  (GraphQL API)                      │
└──────────────┬──────────────────────┘
               │ 3. Index & store
               ↓
┌─────────────────────────────────────┐
│    DataHub Frontend UI              │
│  (Search, Lineage, Details)         │
└─────────────────────────────────────┘
```

---

## 🔧 DataHub Setup

### **1. Docker Compose Configuration**

```yaml
# Part of docker-compose.yml

datahub:
  image: acryldata/datahub:latest
  depends_on:
    - elasticsearch
    - neo4j
    - mysql
    - kafka
  environment:
    ELASTICSEARCH_HOST: elasticsearch
    ELASTICSEARCH_PORT: 9200
    NEO4J_HOST: neo4j
    NEO4J_PORT: 7687
    MYSQL_HOST: mysql
    MYSQL_PORT: 3306
    KAFKA_BOOTSTRAP_SERVER: kafka:9092
  ports:
    - "9002:9002"  # DataHub Frontend
  volumes:
    - datahub-data:/var/datahub

elasticsearch:
  image: docker.elastic.co/elasticsearch/elasticsearch:7.10.0
  environment:
    - discovery.type=single-node
    - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
  ports:
    - "9200:9200"

neo4j:
  image: neo4j:4.0.6
  environment:
    NEO4J_AUTH: neo4j/datahub
  ports:
    - "7687:7687"

mysql:
  image: mysql:5.7
  environment:
    MYSQL_ROOT_PASSWORD: datahub
    MYSQL_DATABASE: datahub
  ports:
    - "3306:3306"

# In same file as Airflow, Trino, MinIO, etc.
```

### **2. Python DataHub Emitter Setup**

```bash
pip install acryl-datahub
pip install 'acryl-datahub[airflow]'  # Airflow plugin
```

---

## 📝 Implementation: metadata/datahub_emit.py

```python
"""
DataHub emission from GATES pipeline.
Called during Airflow tasks to register datasets.
"""

import json
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from uuid import uuid4

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    UpstreamLineageClass,
    UpstreamClass,
    DatasetLineageTypeClass,
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
    GlobalTagsClass,
    TagAssociationClass,
    DatasetProfileClass,
    ColumnProfileClass,
)

DATAHUB_GMS_URL = "http://localhost:8080"


class GATESDataHubEmitter:
    """Emit GATES pipeline metadata to DataHub."""
    
    def __init__(self, gms_url: str = DATAHUB_GMS_URL):
        self.emitter = DatahubRestEmitter(gms_url)
        self.gms_url = gms_url
        
        # Test connection
        try:
            self.emitter.test_connection()
            print(f"[DataHub] Connected to {gms_url}")
        except Exception as e:
            print(f"[DataHub] ⚠️  Connection failed: {e}")
    
    def emit_bronze_dataset(self, 
                           dataset_name: str,
                           schema_config: Dict[str, Any],
                           lineage_info: Dict[str, Any],
                           quality_metrics: Dict[str, Any]):
        """Emit Bronze layer dataset with lineage."""
        
        # Dataset URN
        platform = "iceberg"
        dataset_fqn = f"gates.bronze.{dataset_name}"
        dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_fqn},PROD)"
        
        # 1. Dataset Properties
        print(f"[DataHub] Emitting Bronze: {dataset_name}")
        
        properties_aspect = DatasetPropertiesClass(
            description=f"Bronze: Raw ingested data from {lineage_info['source_id']}",
            customProperties={
                "layer": "bronze",
                "source_id": lineage_info.get('source_id', 'unknown'),
                "row_count": str(lineage_info.get('row_count', 0)),
                "ingestion_time": lineage_info.get('timestamp', ''),
                "validation_status": lineage_info.get('validation_status', 'unknown'),
            }
        )
        
        self.emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=properties_aspect,
        ))
        
        # 2. Schema Metadata
        fields = schema_config.get('canonical_schema', {}).get('fields', [])
        
        schema_metadata = SchemaMetadataClass(
            schemaName=dataset_name,
            platform=f"urn:li:dataPlatform:{platform}",
            version=0,
            hash="",
            platformSchema=None,
            fields=[
                SchemaFieldClass(
                    fieldPath=field.get('name'),
                    type=SchemaFieldDataTypeClass(
                        type=self._map_dtype_to_datahub(field.get('dtype'))
                    ),
                    nativeDataType=field.get('dtype'),
                    description=field.get('description', ''),
                    nullable=not field.get('required', False),
                    isPartOfKey=field.get('name') in schema_config.get('unique_fields', []),
                )
                for field in fields
            ]
        )
        
        self.emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        ))
        
        # 3. Upstream Lineage
        upstreams = []
        
        # Add upstream based on source type
        source_id = lineage_info.get('source_id')
        if source_id:
            if source_id == 'dost_pms_api':
                # Airbyte source
                upstream_urn = "urn:li:dataset:(urn:li:dataPlatform:airbyte,dost-pms-api,PROD)"
            elif source_id == 'pchrd_regional_file':
                # File source
                upstream_urn = "urn:li:dataset:(urn:li:dataPlatform:sftp,pchrd-regional-drops,PROD)"
            elif source_id == 'kafka_cdc_updates':
                # Kafka CDC
                upstream_urn = "urn:li:dataset:(urn:li:dataPlatform:kafka,db-changelog.project_monitoring,PROD)"
            else:
                upstream_urn = f"urn:li:dataset:(urn:li:dataPlatform:unknown,{source_id},PROD)"
            
            upstreams.append(UpstreamClass(
                dataset=upstream_urn,
                type=DatasetLineageTypeClass.COPY,
            ))
        
        upstream_lineage = UpstreamLineageClass(upstreams=upstreams)
        
        self.emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=upstream_lineage,
        ))
        
        # 4. Ownership
        owner_slug = "pchrd-team"  # Should come from config
        
        ownership = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=f"urn:li:corpGroup:{owner_slug}",
                    type=OwnershipTypeClass.DATAOWNER,
                )
            ]
        )
        
        self.emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=ownership,
        ))
        
        # 5. Tags
        tags = GlobalTagsClass(
            tags=[
                TagAssociationClass(tag=f"urn:li:tag:ingestion"),
                TagAssociationClass(tag=f"urn:li:tag:bronze"),
                TagAssociationClass(tag=f"urn:li:tag:{dataset_name}"),
            ]
        )
        
        self.emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=tags,
        ))
        
        # 6. Data Profile (Quality Metrics)
        null_counts = quality_metrics.get('null_counts', {})
        
        profile = DatasetProfileClass(
            timestampMillis=int(datetime.now(timezone.utc).timestamp() * 1000),
            rowCount=lineage_info.get('row_count', 0),
            columnProfiles=[
                ColumnProfileClass(
                    columnName=field.get('name'),
                    nullCount=null_counts.get(field.get('name'), 0),
                    nullProportion=null_counts.get(field.get('name'), 0) / max(lineage_info.get('row_count', 1), 1),
                )
                for field in fields
            ]
        )
        
        self.emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=profile,
        ))
        
        print(f"[DataHub] ✅ Emitted Bronze dataset: {dataset_fqn}")
        return dataset_urn
    
    def emit_silver_dataset(self,
                           dataset_name: str,
                           schema_config: Dict[str, Any],
                           soft_violations: Dict[str, int]):
        """Emit Silver layer with soft rule violations."""
        
        platform = "iceberg"
        dataset_fqn = f"gates.silver.{dataset_name}"
        dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_fqn},PROD)"
        
        print(f"[DataHub] Emitting Silver: {dataset_name}")
        
        # Properties
        violation_summary = json.dumps({
            "total_violations": sum(soft_violations.values()),
            "by_rule": soft_violations,
        })
        
        properties_aspect = DatasetPropertiesClass(
            description=f"Silver: Deduplicated, soft-validated data",
            customProperties={
                "layer": "silver",
                "soft_violations": violation_summary,
                "quality_flag_column": "quality_flag",
            }
        )
        
        self.emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=properties_aspect,
        ))
        
        # Upstream: Bronze
        bronze_fqn = f"gates.bronze.{dataset_name}"
        bronze_urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{bronze_fqn},PROD)"
        
        upstream_lineage = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=bronze_urn,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            ]
        )
        
        self.emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=upstream_lineage,
        ))
        
        print(f"[DataHub] ✅ Emitted Silver dataset: {dataset_fqn}")
        return dataset_urn
    
    def emit_gold_dataset(self,
                         dataset_name: str,
                         metric_fields: List[str],
                         dimension_fields: List[str]):
        """Emit Gold layer with business metrics."""
        
        platform = "iceberg"
        dataset_fqn = f"gates.gold.{dataset_name}_metrics"
        dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_fqn},PROD)"
        
        print(f"[DataHub] Emitting Gold: {dataset_name}")
        
        properties_aspect = DatasetPropertiesClass(
            description=f"Gold: Business-ready aggregated metrics",
            customProperties={
                "layer": "gold",
                "metric_fields": json.dumps(metric_fields),
                "dimension_fields": json.dumps(dimension_fields),
                "use_case": "Portfolio Performance Dashboard",
            }
        )
        
        self.emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=properties_aspect,
        ))
        
        # Upstream: Silver
        silver_fqn = f"gates.silver.{dataset_name}"
        silver_urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{silver_fqn},PROD)"
        
        upstream_lineage = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=silver_urn,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            ]
        )
        
        self.emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=upstream_lineage,
        ))
        
        print(f"[DataHub] ✅ Emitted Gold dataset: {dataset_fqn}")
        return dataset_urn
    
    @staticmethod
    def _map_dtype_to_datahub(dtype: str) -> str:
        """Map pandas/SQL dtype to DataHub type."""
        mapping = {
            'string': 'string',
            'varchar': 'string',
            'int': 'int',
            'integer': 'int',
            'float': 'float',
            'decimal': 'decimal',
            'date': 'date',
            'timestamp': 'timestamp',
            'datetime': 'timestamp',
            'bool': 'boolean',
            'array': 'array',
            'map': 'map',
        }
        return mapping.get(dtype.lower(), 'unknown')


# Usage in Airflow DAG:

def publish_to_datahub(**context):
    """Airflow task that emits metadata to DataHub."""
    
    dag_run = context['dag_run']
    dataset_name = context['dag'].dag_id.split('_', 1)[1]
    
    emitter = GATESDataHubEmitter()
    
    # Get lineage info from previous task
    lineage_info = context['task_instance'].xcom_pull(
        task_ids='stage_to_minio',
        key='lineage_info'
    )
    
    # Get quality metrics
    quality_metrics = context['task_instance'].xcom_pull(
        task_ids='validate_ingestion',
        key='quality_metrics'
    )
    
    # Load schema config
    from common.config_registry import ConfigRegistry
    config_registry = ConfigRegistry(yaml_dir="config/")
    schema_config = config_registry.get_dataset(dataset_name)
    
    # Emit Bronze dataset
    bronze_urn = emitter.emit_bronze_dataset(
        dataset_name=dataset_name,
        schema_config=schema_config,
        lineage_info=lineage_info,
        quality_metrics=quality_metrics,
    )
    
    # Emit Silver dataset (after dbt run)
    silver_urn = emitter.emit_silver_dataset(
        dataset_name=dataset_name,
        schema_config=schema_config,
        soft_violations=quality_metrics.get('soft_violations', {}),
    )
    
    # Emit Gold dataset
    gold_urn = emitter.emit_gold_dataset(
        dataset_name=dataset_name,
        metric_fields=['project_count', 'total_budget_allocated', 'utilization_rate_pct'],
        dimension_fields=['agency', 'program_area', 'region', 'quarter'],
    )
    
    print(f"[DataHub] Complete lineage registered")
```

---

## 🎨 DataHub UI Features

### **1. Search & Discovery**

```
Search: "project_monitoring"
Results:
├─ gates.bronze.project_monitoring (Table)
├─ gates.silver.project_monitoring (Table)
└─ gates.gold.project_monitoring_metrics (Table)

Click on dataset → See:
├─ Schema (12 fields with types)
├─ Lineage (Bronze ← API/File/CDC)
├─ Ownership (PCHRD Team)
├─ Tags (ingestion, bronze, project_monitoring)
├─ Quality Metrics (null rates per column)
└─ Documentation
```

### **2. Lineage View**

```
     DOST PMS API          PCHRD File         Kafka CDC
     (Airbyte)          (Regional Drops)    (db-changes)
           │                  │                  │
           └──────────────────┼──────────────────┘
                              │
                              ↓
                 gates.bronze.project_monitoring
                              │
                              ↓ (dbt transformation)
                 gates.silver.project_monitoring
                              │
                              ↓ (dbt aggregation)
          gates.gold.project_monitoring_metrics
                              │
                              ↓ (consumed by)
                   Portfolio Performance BI Dashboard
```

### **3. Column-Level Metadata**

```
Column: project_id
├─ Type: string
├─ Nullable: false
├─ Part of Key: true
├─ Description: Unique project identifier (e.g., PCHRD-2026-0142)
├─ Null Rate: 0% (quality metric)
├─ Cardinality: 1250 unique values
└─ Tags: [primary_key, required]

Column: budget_allocated_php
├─ Type: decimal(18,2)
├─ Nullable: false
├─ Min Value: 100000 (from profile)
├─ Max Value: 50000000 (from profile)
├─ Average: 5200000
└─ Tags: [financial, validated]
```

### **4. Dataset Profile**

```
gates.bronze.project_monitoring
├─ Row Count: 2,040
├─ Last Updated: 2026-09-14 09:15 UTC
├─ Freshness: 2 hours old
├─ Completeness: 99.2%
├─ Validation Status: PASS
└─ Quality Issues:
   ├─ 12 rows with soft validation violations
   └─ 0 hard rule violations (already quarantined)
```

---

## 🔌 Airflow Integration

```python
# In orchestration/dag_factory.py

from airflow.operators.python import PythonOperator
from metadata.datahub_emit import publish_to_datahub

def build_dag_for_dataset(dataset_name, config_registry):
    dag = DAG(f"gates_{dataset_name}", ...)
    
    # ... other tasks ...
    
    # New task: Emit to DataHub
    publish_datahub = PythonOperator(
        task_id='publish_to_datahub',
        python_callable=publish_to_datahub,
        provide_context=True,
        dag=dag,
    )
    
    # After dbt completes
    dbt_gold >> publish_datahub
    
    return dag
```

---

## 📊 Monitoring DataHub Health

```python
# Monitor/check_datahub_health.py

from datahub.emitter.rest_emitter import DatahubRestEmitter

def check_datahub_connectivity():
    """Airflow sensor to check DataHub availability."""
    
    emitter = DatahubRestEmitter("http://localhost:8080")
    
    try:
        emitter.test_connection()
        return True
    except Exception as e:
        print(f"[DataHub Health] Connection failed: {e}")
        return False

# Use in DAG:
from airflow.sensors.python import PythonSensor

wait_for_datahub = PythonSensor(
    task_id='wait_for_datahub',
    python_callable=check_datahub_connectivity,
    poke_interval=10,
    timeout=120,
)
```

---

## ✅ Demo Points for DataHub

1. **Search**: Show searching for "project_monitoring"
2. **Lineage**: Click and show full lineage graph (API → Bronze → Silver → Gold)
3. **Schema**: Expand and show field details with types and descriptions
4. **Quality**: Show data profile with null rates and cardinality
5. **Ownership**: Show team ownership and SLA information
6. **History**: Show that data was loaded 2 hours ago
7. **Documentation**: Show that docs are auto-generated from config

---

## 🎯 Key Takeaway

**DataHub transforms GATES from a data pipeline into a DATA PLATFORM:**
- ✅ Team can **discover** datasets via UI
- ✅ Team can see **lineage** (where did this data come from?)
- ✅ Team can see **quality** (are there validation issues?)
- ✅ Team can see **ownership** (who should I contact?)
- ✅ Team can see **freshness** (when was this last updated?)

This is what makes it "enterprise-ready" — not just a working pipeline, but a **discoverable, traceable, monitored data platform**.
