"""Rebuild a layer in PostGIS from its archive in MinIO.

Each DAG run archives the protected layer (part.parquet) and the vault's
ciphertext (vault.parquet) under gates-geo/<layer>/batch=<run id>/, and loads
PostGIS from them. This script runs the same load from an archived batch, so a
lost or damaged PostGIS can be rebuilt without the original upload.

It rebuilds geo.<layer>, geo_serving.<layer> and the layer's rows in the vault.
GeoServer keeps its own layer settings. Run the DAG again if GeoServer was lost too.

Needs the roles and schemas from scripts/geo_setup.py to exist, so on a new database
run that first. Batches written before the vault archive existed cannot be used.

    docker compose -f docker-compose.demo.yml -f docker-compose.geo.yml exec -T \
      -e PYTHONPATH=/opt/airflow/dags [-e BATCH=<batch directory name>] \
      airflow /opt/airflow/geo-venv/bin/python - < scripts/geo_reload.py

BATCH is the directory name under gates-geo/<layer>/batch=, for example
manual__2026-10-12T18_24_30_00_00. It defaults to the newest complete batch.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from common.geo_config import load_layer_config
from common.geo_standardize import latest_batch, read_geoparquet, read_vault_archive
from common.openbao_client import OpenBaoClient
from common.postgis_loader import connect, load_layer

ROOT = Path(os.getenv("GATES_APP_ROOT", "/opt/airflow/dags"))
LAYER = os.getenv("LAYER", "critical_facilities")


def main() -> int:
    client = OpenBaoClient.from_env()
    config = load_layer_config(LAYER, ROOT / "config")
    minio = client.read_secret("geo/minio")

    batch = os.getenv("BATCH") or latest_batch(LAYER, minio)
    if not batch:
        print(f"No complete archived batch for {LAYER}. A batch needs both part.parquet and vault.parquet.")
        return 2
    print(f"Rebuilding {LAYER} from batch {batch}")

    try:
        gdf = read_geoparquet(LAYER, batch, minio)
        vault = read_vault_archive(LAYER, batch, minio)
    except Exception as exc:  # boto3 raises botocore ClientError for a missing object
        print(f"Could not read the archive for batch {batch}: {exc}")
        print("Batches written before the vault archive existed have no vault.parquet and cannot be reloaded.")
        return 2

    run_id = str(gdf["_batch_id"].iloc[0]) if "_batch_id" in gdf.columns and len(gdf) else batch
    result = load_layer(connect(client), gdf, config, vault, run_id)
    print(f"Loaded {result['rows']} features and {result['vault_rows']} vault rows into {result['table']}; view {result['view']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
