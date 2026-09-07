"""
ingest_file_navi_gates.py

The file-channel counterpart to ingest_api_airbyte.py. Reads a regional
office file (CSV/XLSX, agency-specific column names and formatting) and
calls the SAME Common Ingestion Framework functions to canonicalize it.
"""
import sys
from pathlib import Path
from datetime import date

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common.ingestion_framework import (
    load_contract, load_aliasing, get_source_aliases, get_source_meta, apply_aliasing, write_staged,
)

CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
STAGING_DIR = Path(__file__).resolve().parents[1] / "staging"

# Defaults only — same reasoning as ingest_api_airbyte.py. The pipeline
# should pass dataset/source_id explicitly for any dataset other than this
# reference one.
DEFAULT_DATASET = "project_monitoring"
DEFAULT_SOURCE_ID = "pchrd_regional_file_dropbox"


def run(file_path: str | Path,
        dataset: str = DEFAULT_DATASET,
        source_id: str = DEFAULT_SOURCE_ID) -> dict:
    file_path = Path(file_path)
    print(f"[ingest_file_navi_gates] Reading {file_path.name}")

    if file_path.suffix.lower() == ".csv":
        raw_df = pd.read_csv(file_path, dtype=str, keep_default_na=False, na_values=[""])
    else:
        raw_df = pd.read_excel(file_path, dtype=str)

    contract = load_contract(CONFIG_DIR / "schema_project_monitoring.yaml")
    aliasing = load_aliasing(CONFIG_DIR / "column_aliasing.yaml")
    aliases = get_source_aliases(aliasing, source_id)
    source_meta = get_source_meta(contract, source_id)

    canonical_df = apply_aliasing(raw_df, aliases)

    # agency/region are declared once per file submission (source metadata),
    # not carried as a per-row raw column — fill them in for every row.
    canonical_df["agency"] = source_meta["agency"]
    canonical_df["region"] = source_meta["region"]

    result = write_staged(
        canonical_df, STAGING_DIR, dataset,
        agency=source_meta["agency"], run_date=date.today().isoformat(), source_id=source_id,
    )
    print(f"[ingest_file_navi_gates] Staged {len(canonical_df)} rows -> {result['data_path']}")
    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the NAVI-GATES-style file ingestion step.")
    parser.add_argument(
        "--file", default=None,
        help="Path to the raw file to ingest. Defaults to the bundled sample_data CSV.",
    )
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="Dataset name (config/schema_<dataset>.yaml).")
    parser.add_argument("--source-id", default=DEFAULT_SOURCE_ID, help="source_id as declared in column_aliasing.yaml.")
    args = parser.parse_args()

    file_path = Path(args.file) if args.file else (
        Path(__file__).resolve().parents[1] / "sample_data" / "PCHRD_ProjectMonitoring_Q3_2026.csv"
    )
    run(file_path, dataset=args.dataset, source_id=args.source_id)

