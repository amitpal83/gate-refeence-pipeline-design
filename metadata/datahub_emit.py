"""Registers dataset metadata in DataHub."""
import os
from datetime import datetime, timezone

DATAHUB_GMS_SERVER = "http://datahub-gms:8080"


def _emit_via_real_datahub(metadata: dict) -> bool:
    """
    Emit typed DataHub aspects through the REST emitter.

    URN/platform choices made here, worth reviewing against your actual
    DataHub deployment:
      - platform="trino", dataset qualified as
        "{TRINO_CATALOG}.{layer_schema}.<table>" — each pipeline stage maps
        directly to its own physical Trino schema (bronze/silver/gold/
        staging; see transformation/dbt_project/dbt_project.yml's +schema
        per layer and common/trino_loader.py's staging schema), matching
        the actual medallion-per-schema layout dbt writes to, not a single
        flat schema. TRINO_CATALOG defaults to "gates" to match
        transformation/dbt_project/profiles.yml.
      - `dataset_table` (physical table name) defaults to `dataset` (the
        pipeline's dataset key, e.g. "project_monitoring") for
        bronze/silver, where the table is a 1:1 promotion of that entity —
        but Gold tables are aggregates with their own names (e.g.
        "agg_rd_portfolio_performance"), so callers registering a Gold
        stage must pass the real table name or this URN points at a table
        that doesn't exist.
      - env="PROD" — DataHub's FabricType; hardcoded, not parameterized.
      - Owner is turned into a corpGroup URN by slugifying the free-text
        `owner` string (e.g. "PCHRD M&E unit" -> "pchrd_m&e_unit"). In a
        real deployment this should instead reference a corpGroup/corpuser
        URN that already exists in DataHub, not a slug guessed here.
      - Lineage entries are turned into dataset URNs on a generic "gates"
        platform, since staging/bronze aren't necessarily registered as
        their own DataHub datasets on a recognized platform.
    """
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.metadata.schema_classes import (
        DatasetPropertiesClass, GlobalTagsClass, TagAssociationClass,
        OwnershipClass, OwnerClass, OwnershipTypeClass,
        UpstreamLineageClass, UpstreamClass, DatasetLineageTypeClass,
        SchemaMetadataClass, SchemaFieldClass, SchemaFieldDataTypeClass,
        OtherSchemaClass, StringTypeClass, NumberTypeClass, DateTypeClass, BooleanTypeClass,
    )

    emitter = DatahubRestEmitter(gms_server=DATAHUB_GMS_SERVER)
    emitter.test_connection()

    platform = "trino"
    env = "PROD"
    catalog = os.getenv("TRINO_CATALOG", "gates")
    # Each pipeline stage is a real, separate Trino schema — see
    # dbt_project.yml's +schema per layer and trino_loader.py's staging
    # schema — so the URN's schema segment maps directly to the stage,
    # rather than one flat TRINO_SCHEMA for everything past ingestion.
    dataset_schema = {"raw_ingested": "staging", "bronze": "bronze",
                       "silver": "silver", "gold": "gold"}.get(metadata["stage"], "default")
    table_name = metadata.get("dataset_table") or metadata["dataset"]
    dataset_fqn = f"{catalog}.{dataset_schema}.{table_name}"
    dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_fqn},{env})"

    # --- Aspect 1: DatasetProperties (schema_ref, DQI, rule summary as custom properties) ---
    custom_props = {
        "schema_ref": metadata["schema_ref"],
        "data_quality_index": str(metadata["data_quality_index"]),
        "pipeline_stage": metadata["stage"],
        "registered_at": metadata["registered_at"],
    }
    custom_props.update({
        f"rule_summary.{k}": str(v) for k, v in metadata["rule_results_summary"].items()
    })
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=DatasetPropertiesClass(
            description=f"GATES dataset: {metadata['dataset']}",
            customProperties=custom_props,
        ),
    ))

    # --- Aspect 2: Ownership ---
    owner_slug = metadata["owner"].lower().replace(" ", "_")
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=OwnershipClass(owners=[
            OwnerClass(owner=f"urn:li:corpGroup:{owner_slug}", type=OwnershipTypeClass.DATAOWNER)
        ]),
    ))

    # --- Aspect 3: GlobalTags (classification) ---
    tag_slug = metadata["classification"].lower().replace(" ", "_").replace("&", "and")
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=dataset_urn,
        aspect=GlobalTagsClass(tags=[
            TagAssociationClass(tag=f"urn:li:tag:{tag_slug}")
        ]),
    ))

    # --- Aspect 4: SchemaMetadata (column list — makes the dataset's
    # "Schema" tab in the DataHub UI actually show something; without this
    # aspect a dataset is still searchable by name/description/tags, but
    # its columns are invisible in the UI). Optional: callers that don't
    # have a field list handy (e.g. Gold aggregates with no canonical
    # schema of their own) can omit it. dtype names match
    # config/schema_*.yaml's canonical_schema.fields — see ConfigRegistry.
    if metadata.get("fields"):
        type_map = {
            "string": StringTypeClass(), "float": NumberTypeClass(),
            "integer": NumberTypeClass(), "date": DateTypeClass(),
            "boolean": BooleanTypeClass(),
        }
        schema_fields = [
            SchemaFieldClass(
                fieldPath=field["name"],
                type=SchemaFieldDataTypeClass(type=type_map.get(field.get("dtype"), StringTypeClass())),
                nativeDataType=field.get("dtype", "string"),
                description=field.get("description"),
            )
            for field in metadata["fields"]
        ]
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SchemaMetadataClass(
                schemaName=f"{metadata['dataset']}_schema",
                platform=f"urn:li:dataPlatform:{platform}",
                version=0,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=""),
                fields=schema_fields,
            ),
        ))

    # --- Aspect 5: UpstreamLineage ---
    if metadata["lineage"]:
        upstreams = [
            UpstreamClass(
                dataset=f"urn:li:dataset:(urn:li:dataPlatform:gates,{node},{env})",
                type=DatasetLineageTypeClass.TRANSFORMED,
            )
            for node in metadata["lineage"]
        ]
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=UpstreamLineageClass(upstreams=upstreams),
        ))

    return True


def emit_dataset_metadata(dataset: str, owner: str, classification: str,
                           schema_ref: str, lineage: list[str],
                           data_quality_index: float, rule_results_summary: dict,
                           stage: str = "bronze", dataset_table: str | None = None,
                           fields: list[dict] | None = None) -> dict:
    """dataset_table: the physical table name if it differs from `dataset`
    (Gold aggregates are named for their use case, not the source entity —
    see _emit_via_real_datahub's docstring). fields: canonical_schema.fields
    from ConfigRegistry, used to populate the DataHub Schema tab; omit for
    stages with no directly-corresponding field list."""
    metadata = {
        "dataset": dataset,
        "dataset_table": dataset_table,
        "owner": owner,
        "classification": classification,
        "schema_ref": schema_ref,
        "stage": stage,
        "lineage": lineage,
        "fields": fields,
        "data_quality_index": data_quality_index,
        "rule_results_summary": rule_results_summary,
        "registered_at": datetime.now(timezone.utc).isoformat(),
    }
    _emit_via_real_datahub(metadata)
    print(f"[datahub_emit] Metadata registered in DataHub for dataset={dataset}")
    return metadata


if __name__ == "__main__":
    emit_dataset_metadata(
        dataset="project_monitoring", owner="PCHRD M&E unit",
        classification="Project & Knowledge Management",
        schema_ref="schema_project_monitoring.yaml",
        stage="bronze",
        lineage=["dost_pms_api", "pchrd_regional_file_dropbox", "staging", "bronze"],
        data_quality_index=91.7,
        rule_results_summary={"hard_pass": True, "soft_flags": 1},
    )
