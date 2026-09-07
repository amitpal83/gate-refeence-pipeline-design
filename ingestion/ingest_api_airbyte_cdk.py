"""
ingest_api_airbyte_cdk.py

*** NOT VERIFIED IN THIS ENVIRONMENT *** (no network to install airbyte-cdk
and actually run this). This is the intended replacement for
ingest_api_airbyte.py's hand-rolled manifest reader: instead of us
interpreting a handful of manifest keys ourselves, this shells out to the
REAL connector (airbyte_cdk_connector/main.py, built on airbyte_cdk's
YamlDeclarativeSource) and parses its actual Airbyte Protocol output —
newline-delimited AirbyteMessage JSON on stdout — then hands the extracted
records to the SAME Common Ingestion Framework functions every other
ingestion path uses. Nothing past `_read_records_via_real_connector()` is
new or unverified; only that one function depends on airbyte-cdk actually
being installed and working.
"""
import sys
import json
import subprocess
from pathlib import Path
from datetime import date

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.ingestion_framework import (
    load_contract, load_aliasing, get_source_aliases, get_canonical_fields,
    identity_pass_through, apply_aliasing, write_staged,
)

CONNECTOR_DIR = Path(__file__).resolve().parent / "airbyte_cdk_connector"
CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
STAGING_DIR = Path(__file__).resolve().parents[1] / "staging"
DEFAULT_DATASET = "project_monitoring"
DEFAULT_SOURCE_ID = "dost_pms_api"


def _read_records_via_real_connector(config_path: Path, catalog_path: Path) -> list[dict]:
    """Runs the real Airbyte connector's `read` command and parses its
    Airbyte Protocol output. Requires `pip install -r
    airbyte_cdk_connector/requirements.txt` first."""
    proc = subprocess.run(
        [sys.executable, str(CONNECTOR_DIR / "main.py"), "read",
         "--config", str(config_path), "--catalog", str(catalog_path)],
        capture_output=True, text=True, cwd=str(CONNECTOR_DIR),
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"Real airbyte-cdk connector exited {proc.returncode}.\n"
            f"--- stdout ---\n{proc.stdout}\n--- stderr ---\n{proc.stderr}"
        )

    records = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue  # some CDK versions interleave non-JSON log lines; skip them
        if msg.get("type") == "RECORD":
            records.append(msg["record"]["data"])
    return records


def run(dataset: str = DEFAULT_DATASET, source_id: str = DEFAULT_SOURCE_ID,
        agency: str = "PCHRD") -> dict:
    config_path = CONNECTOR_DIR / "secrets" / "config.json"
    catalog_path = CONNECTOR_DIR / "integration_tests" / "configured_catalog.json"

    print(f"[ingest_api_airbyte_cdk] Running real connector: "
          f"python {CONNECTOR_DIR / 'main.py'} read --config {config_path} --catalog {catalog_path}")
    records = _read_records_via_real_connector(config_path, catalog_path)
    print(f"[ingest_api_airbyte_cdk] Parsed {len(records)} RECORD messages from Airbyte Protocol output")

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
    print(f"[ingest_api_airbyte_cdk] Staged {len(canonical_df)} rows -> {result['data_path']}")
    return result


if __name__ == "__main__":
    print(
        "NOTE: this requires `pip install -r airbyte_cdk_connector/requirements.txt`.\n"
        "This has not been executed in the environment that built it — see this "
        "file's docstring.\n",
        file=sys.stderr,
    )
    run()
