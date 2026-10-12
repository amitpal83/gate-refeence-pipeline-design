"""Registers a geo layer's PostGIS serving table in DataHub.

metadata/datahub_emit.py is written for the Trino medallion tables (the Trino
platform and the staging, bronze, silver and gold schemas), so geo layers get
their own emitter. It follows the same pattern and the same custom-property
conventions.

The dataset is geo.<layer> in the geo database on the Postgres platform. Columns
that hold protected PII are tagged with how they were protected, so the catalog
shows where PII was found and what was done about it:

    pii_pseudonymized    keyed pseudonym (original is in the vault)
    pii_masked           partly hidden value
    pii_text_redacted    names, numbers and emails replaced inside free text
    pii_redacted         whole value replaced

The findings come from common/pii_protection.py and hold counts and entity types
only, never matched text.

Runs in the main Airflow environment, which has the DataHub client.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

DATAHUB_GMS_SERVER = os.getenv("DATAHUB_GMS_SERVER", "http://datahub-gms:8080")
PLATFORM = "postgres"
ENV = "PROD"

TREATMENT_TAGS = {
    "pseudonymize": "pii_pseudonymized",
    "mask": "pii_masked",
    "mask_last3": "pii_masked",
    "replace_in_text": "pii_text_redacted",
    "redact": "pii_redacted",
}


def geo_dataset_name(layer: str) -> str:
    return f"{os.getenv('GEO_DB_NAME', 'gates_geo')}.geo.{layer}"


def geo_dataset_urn(layer: str) -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{PLATFORM},{geo_dataset_name(layer)},{ENV})"


def _slug(value: str) -> str:
    return value.lower().replace(" ", "_").replace("&", "and")


def column_tags(findings: list[dict]) -> dict[str, str]:
    """{column: tag name} from the PII findings. A column protected in more than
    one way keeps the tag of its declared treatment."""
    tags: dict[str, str] = {}
    for finding in sorted(findings, key=lambda f: not f.get("declared", False)):
        tag = TREATMENT_TAGS.get(finding["treatment"])
        if tag:
            tags.setdefault(finding["column"], tag)
    return tags


def emit_geo_layer_metadata(
    layer_config: dict,
    owner: str,
    classification: str,
    data_quality_index: float,
    rows: int,
    batch_id: str,
    findings: list[dict],
    rule_summary: dict,
) -> dict:
    from datahub.emitter.mcp import MetadataChangeProposalWrapper
    from datahub.emitter.rest_emitter import DatahubRestEmitter
    from datahub.metadata.schema_classes import (
        DatasetPropertiesClass, GlobalTagsClass, OtherSchemaClass, OwnerClass, OwnershipClass,
        OwnershipTypeClass, SchemaFieldClass, SchemaFieldDataTypeClass, SchemaMetadataClass,
        StringTypeClass, TagAssociationClass,
    )

    layer = layer_config["layer"]
    urn = geo_dataset_urn(layer)
    workspace = (layer_config.get("geoserver") or {}).get("workspace", "gates")
    tags_by_column = column_tags(findings)

    emitter = DatahubRestEmitter(gms_server=DATAHUB_GMS_SERVER)
    emitter.test_connection()

    properties = {
        "layer": layer,
        "geometry_type": layer_config["geometry_type"],
        "crs": layer_config["target_crs"],
        "batch_id": batch_id,
        "row_count": str(rows),
        "data_quality_index": str(data_quality_index),
        "geoserver_layer": f"{workspace}:{layer}",
        "pii_vault": "pii_vault.contact_pii",
        "pii_columns_protected": ",".join(sorted(tags_by_column)) or "none",
        "registered_at": datetime.now(timezone.utc).isoformat(),
    }
    properties.update({f"rule_summary.{key}": str(value) for key, value in rule_summary.items()})
    for finding in findings:
        if finding["entity"] != "declared":
            properties[f"pii_findings.{finding['column']}.{finding['entity']}"] = str(finding["count"])

    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=DatasetPropertiesClass(
            description=layer_config.get("description") or f"GATES geo layer: {layer}",
            customProperties=properties,
        ),
    ))
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=OwnershipClass(owners=[
            OwnerClass(owner=f"urn:li:corpGroup:{_slug(owner)}", type=OwnershipTypeClass.DATAOWNER)
        ]),
    ))
    dataset_tags = [TagAssociationClass(tag=f"urn:li:tag:{_slug(classification)}")]
    if tags_by_column:
        dataset_tags.append(TagAssociationClass(tag="urn:li:tag:contains_pii"))
    emitter.emit(MetadataChangeProposalWrapper(entityUrn=urn, aspect=GlobalTagsClass(tags=dataset_tags)))

    fields = []
    declared = [a["name"] for a in layer_config.get("attributes", [])]
    for name in [*declared, "geometry", "_batch_id", "_ingested_at"]:
        native = f"geometry({layer_config['geometry_type']},4326)" if name == "geometry" else "text"
        field_tag = tags_by_column.get(name)
        fields.append(SchemaFieldClass(
            fieldPath=name,
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nativeDataType=native,
            globalTags=GlobalTagsClass(tags=[TagAssociationClass(tag=f"urn:li:tag:{field_tag}")]) if field_tag else None,
        ))
    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=SchemaMetadataClass(
            schemaName=f"{layer}_schema",
            platform=f"urn:li:dataPlatform:{PLATFORM}",
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        ),
    ))
    print(f"[datahub_emit_geo] Metadata registered in DataHub for layer={layer}")
    return {"urn": urn, "protected_columns": tags_by_column, **properties}
