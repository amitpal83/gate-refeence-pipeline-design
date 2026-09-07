"""Registers dataset metadata in DataHub."""
from datetime import datetime, timezone

DATAHUB_GMS_SERVER = "http://datahub-gms:8080"


def _emit_via_real_datahub(metadata: dict) -> bool:
    """
    Emit typed DataHub aspects through the REST emitter.

    URN/platform choices made here, worth reviewing against your actual
    DataHub deployment:
      - platform="trino", dataset qualified as "iceberg.project_mgmt.<dataset>"
        — matches transformation/dbt_project/profiles.yml's catalog/schema.
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
    )

    emitter = DatahubRestEmitter(gms_server=DATAHUB_GMS_SERVER)
    emitter.test_connection()

    platform = "trino"
    env = "PROD"
    dataset_fqn = f"iceberg.project_mgmt.{metadata['dataset']}"
    dataset_urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_fqn},{env})"

    # --- Aspect 1: DatasetProperties (schema_ref, DQI, rule summary as custom properties) ---
    custom_props = {
        "schema_ref": metadata["schema_ref"],
        "data_quality_index": str(metadata["data_quality_index"]),
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

    # --- Aspect 4: UpstreamLineage ---
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
                           data_quality_index: float, rule_results_summary: dict) -> dict:
    metadata = {
        "dataset": dataset,
        "owner": owner,
        "classification": classification,
        "schema_ref": schema_ref,
        "lineage": lineage,
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
        lineage=["dost_pms_api", "pchrd_regional_file_dropbox", "staging", "bronze"],
        data_quality_index=91.7,
        rule_results_summary={"hard_pass": True, "soft_flags": 1},
    )
