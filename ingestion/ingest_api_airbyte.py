"""
ingest_api_airbyte.py

Mirrors what Airbyte's low-code connector (config/airbyte_manifest.yaml)
does: read the requester config, make the HTTP GET with bearer auth, extract
records via the declared field_path, then hand off to the Common Ingestion
Framework for canonicalization + staging. Point AIRBYTE_MANIFEST's
requester.url_base at the real PMS API in production — nothing else changes.
"""
import sys
from pathlib import Path
from datetime import date

import pandas as pd
import requests
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.ingestion_framework import (
    load_contract, load_aliasing, get_source_aliases, get_canonical_fields,
    identity_pass_through, apply_aliasing, write_staged,
)

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
STAGING_DIR = Path(__file__).resolve().parents[1] / "staging"

# Defaults only — an Airflow task should pass dataset/source_id explicitly for
# any dataset other than this reference one. Nothing below this line should
# assume "project_monitoring" or "dost_pms_api" specifically.
DEFAULT_DATASET = "project_monitoring"
DEFAULT_SOURCE_ID = "dost_pms_api"


def run(base_url_override: str | None = None,
        dataset: str = DEFAULT_DATASET,
        source_id: str = DEFAULT_SOURCE_ID,
        agency: str = "PCHRD") -> dict:
    manifest = yaml.safe_load((CONFIG_DIR / "airbyte_manifest.yaml").read_text())
    requester = manifest["definitions"]["requester"]
    base_url = base_url_override or requester["url_base"]
    path = manifest["definitions"]["project_monitoring_stream"]["$parameters"]["path"]
    field_path = manifest["definitions"]["selector"]["extractor"]["field_path"]

    print(f"[ingest_api_airbyte] GET {base_url}{path}")
    resp = requests.get(
        f"{base_url}{path}",
        headers={"Authorization": "Bearer demo-token-not-a-real-secret"},
        timeout=10,
    )
    resp.raise_for_status()
    payload = resp.json()

    records = payload
    for key in field_path:
        records = records[key]
    print(f"[ingest_api_airbyte] Retrieved {len(records)} records "
          f"(server reports total_records={payload.get('total_records')})")

    raw_df = pd.DataFrame(records)

    contract = load_contract(CONFIG_DIR / "schema_project_monitoring.yaml")
    aliasing = load_aliasing(CONFIG_DIR / "column_aliasing.yaml")
    aliases = get_source_aliases(aliasing, source_id)

    if aliases:
        canonical_df = apply_aliasing(raw_df, aliases)
    else:
        canonical_df = identity_pass_through(raw_df, get_canonical_fields(contract))

    result = write_staged(
        canonical_df, STAGING_DIR, dataset,
        agency=agency, run_date=date.today().isoformat(), source_id=source_id,
    )
    print(f"[ingest_api_airbyte] Staged {len(canonical_df)} rows -> {result['data_path']}")
    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the Airbyte-style API ingestion step.")
    parser.add_argument("--base-url", default=None, help="Explicit API base URL override.")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="Dataset name (config/schema_<dataset>.yaml).")
    parser.add_argument("--source-id", default=DEFAULT_SOURCE_ID, help="source_id as declared in column_aliasing.yaml.")
    parser.add_argument("--agency", default="PCHRD", help="Agency partition for the staging path.")
    args = parser.parse_args()

    run(base_url_override=args.base_url, dataset=args.dataset, source_id=args.source_id, agency=args.agency)

