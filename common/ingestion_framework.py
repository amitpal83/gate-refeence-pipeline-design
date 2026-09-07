"""
common/ingestion_framework.py

The Common Ingestion Framework referenced throughout the GATES LLD.
Both ingest_file_navi_gates.py and ingest_api_airbyte.py call the SAME
functions here — nothing dataset-specific is hardcoded. Given a different
dataset's config/schema_*.yaml + config/column_aliasing.yaml, this module
works unchanged.
"""
import json
import re
import hashlib
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


# --------------------------------------------------------------------------- #
# Config loading
# --------------------------------------------------------------------------- #

def load_contract(path: str | Path) -> dict:
    """Load the ingestion contract (canonical_schema + sources)."""
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_aliasing(path: str | Path) -> dict:
    """Load the column_aliasing.yaml mapping file."""
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_source_aliases(aliasing: dict, source_id: str) -> list[dict]:
    for src in aliasing["sources"]:
        if src["source_id"] == source_id:
            return src.get("aliases", [])
    raise KeyError(f"No aliasing block found for source_id={source_id!r}")


def get_source_meta(contract: dict, source_id: str) -> dict:
    """Batch-level metadata (agency, region, etc.) declared once per source
    in the ingestion contract — NOT a per-row raw column. File submissions
    are one file = one agency = one region; this is where that lives."""
    for src in contract.get("sources", []):
        if src["source_id"] == source_id:
            return src
    raise KeyError(f"No source block found for source_id={source_id!r}")


def get_canonical_fields(contract: dict) -> list[dict]:
    return contract["canonical_schema"]["fields"]


def get_unique_fields(contract: dict) -> list[str]:
    return contract["canonical_schema"].get("unique_fields", [])


# --------------------------------------------------------------------------- #
# Transform registry — the small vocabulary of transforms column_aliasing.yaml
# can reference by name. Adding a new transform = adding one function here;
# no ingestion script needs to change.
# --------------------------------------------------------------------------- #

def _transform_parse_date_mmddyyyy_to_iso(value: Any) -> Any:
    if value is None or (isinstance(value, float) and pd.isna(value)) or str(value).strip() == "":
        return None
    try:
        return datetime.strptime(str(value).strip(), "%m/%d/%Y").date().isoformat()
    except ValueError:
        # already ISO, or unparseable — pass through so validation catches it
        return str(value).strip()


def _transform_strip_currency_to_float(value: Any) -> Any:
    if value is None or (isinstance(value, float) and pd.isna(value)) or str(value).strip() == "":
        return None
    cleaned = re.sub(r"[^\d.\-]", "", str(value))
    return float(cleaned) if cleaned else None


TRANSFORM_REGISTRY = {
    "parse_date(mm/dd/yyyy -> ISO 8601)": _transform_parse_date_mmddyyyy_to_iso,
    "strip_currency_formatting -> float": _transform_strip_currency_to_float,
}


def apply_transform(value: Any, transform_name: str | None) -> Any:
    if not transform_name:
        return value
    fn = TRANSFORM_REGISTRY.get(transform_name)
    if fn is None:
        raise KeyError(f"Unknown transform {transform_name!r} — add it to TRANSFORM_REGISTRY")
    return fn(value)


# --------------------------------------------------------------------------- #
# The actual "apply aliasing" step — raw DataFrame (source column names) in,
# canonical DataFrame (canonical field names) out.
# --------------------------------------------------------------------------- #

def apply_aliasing(raw_df: pd.DataFrame, aliases: list[dict]) -> pd.DataFrame:
    """
    Rename raw columns to canonical field names and apply any declared
    transform, using ONLY what's in column_aliasing.yaml — no dataset-
    specific logic lives in this function.
    """
    out = pd.DataFrame(index=raw_df.index)
    for alias in aliases:
        src_col = alias["source_field"]
        canon_col = alias["canonical_field"]
        transform_name = alias.get("transform")
        if src_col not in raw_df.columns:
            raise KeyError(f"Expected raw column {src_col!r} not found in source data")
        out[canon_col] = raw_df[src_col].apply(lambda v: apply_transform(v, transform_name))
    return out


def identity_pass_through(raw_df: pd.DataFrame, canonical_fields: list[dict]) -> pd.DataFrame:
    """For sources with an empty aliases: [] (e.g. the API channel, whose
    field names already match canonical 1:1) — just select/order columns."""
    field_names = [f["name"] for f in canonical_fields]
    missing = [c for c in field_names if c not in raw_df.columns]
    if missing:
        raise KeyError(f"API payload missing expected canonical fields: {missing}")
    return raw_df[field_names].copy()


# --------------------------------------------------------------------------- #
# Manifest + staging writer — same for every channel
# --------------------------------------------------------------------------- #

def write_staged(df: pd.DataFrame, staging_dir: str | Path, dataset: str,
                  agency: str, run_date: str, source_id: str) -> dict:
    """
    Writes the canonicalized dataframe to the staging zone as CSV plus a
    manifest.json. The partitioning convention is
    /staging/{agency}/{dataset}/{date}/.
    """
    out_dir = Path(staging_dir) / agency / dataset / run_date
    out_dir.mkdir(parents=True, exist_ok=True)

    data_path = out_dir / f"raw_{source_id}.csv"
    df.to_csv(data_path, index=False)

    checksum = hashlib.sha256(data_path.read_bytes()).hexdigest()
    manifest = {
        "dataset": dataset,
        "source_id": source_id,
        "agency": agency,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "row_count": len(df),
        "schema_ref": "schema_project_monitoring.yaml",
        "alias_ref": "column_aliasing.yaml",
        "checksum": f"sha256:{checksum[:16]}...",
        "path": str(data_path),
    }
    manifest_path = out_dir / f"manifest_{source_id}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    return {"data_path": str(data_path), "manifest_path": str(manifest_path), "manifest": manifest}
