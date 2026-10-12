"""Standardize a validated, PII-protected geo layer and write it as GeoParquet.

Runs after protect_pii, so everything here handles the serving copy only.
GeoParquet goes to the MinIO `gates-geo` bucket (Ceph RGW in production) with
bounding-box covering metadata, so readers can skip files by location.

    gates-geo/<layer>/batch=<run id>/part.parquet

Runs in the geo environment (geopandas, pyarrow, boto3). Storage credentials are
passed in as a dict read from OpenBao (kv/geo/minio) by the caller.
"""
from __future__ import annotations

import io
import re
from datetime import datetime, timezone

import geopandas as gpd
import pandas as pd
from pyproj import CRS

_UNSAFE_KEY_CHARS = re.compile(r"[^A-Za-z0-9._=-]")


def safe_batch_id(batch_id: str) -> str:
    """Airflow run ids contain ':' and '+', which are awkward in object keys."""
    return _UNSAFE_KEY_CHARS.sub("_", batch_id)


def standardize(gdf: gpd.GeoDataFrame, layer_config: dict, batch_id: str) -> gpd.GeoDataFrame:
    """Reproject to the target CRS (a no-op for WGS 84 layers) and add the
    technical columns _batch_id and _ingested_at."""
    if gdf.crs is None:
        raise ValueError("The layer has no CRS. Ingest should have set one.")
    target = CRS.from_user_input(layer_config["target_crs"])
    out = gdf if gdf.crs.equals(target, ignore_axis_order=True) else gdf.to_crs(target)
    out = out.copy()
    out["_batch_id"] = batch_id
    out["_ingested_at"] = datetime.now(timezone.utc)
    return out


def _s3(minio: dict):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=minio["endpoint"],
        aws_access_key_id=minio["access_key"],
        aws_secret_access_key=minio["secret_key"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def geoparquet_key(layer: str, batch_id: str) -> str:
    return f"{layer}/batch={safe_batch_id(batch_id)}/part.parquet"


def write_geoparquet(gdf: gpd.GeoDataFrame, layer: str, batch_id: str, minio: dict) -> str:
    """Write the layer and return its s3:// URI. Writing the same batch again
    replaces the object, so a retry is safe."""
    buffer = io.BytesIO()
    gdf.to_parquet(buffer, write_covering_bbox=True)
    key = geoparquet_key(layer, batch_id)
    _s3(minio).put_object(Bucket=minio["bucket"], Key=key, Body=buffer.getvalue())
    return f"s3://{minio['bucket']}/{key}"


def read_geoparquet(layer: str, batch_id: str, minio: dict) -> gpd.GeoDataFrame:
    body = _s3(minio).get_object(Bucket=minio["bucket"], Key=geoparquet_key(layer, batch_id))["Body"].read()
    return gpd.read_parquet(io.BytesIO(body))


def delete_geoparquet(layer: str, batch_id: str, minio: dict) -> None:
    _s3(minio).delete_object(Bucket=minio["bucket"], Key=geoparquet_key(layer, batch_id))


# --- vault archive ---------------------------------------------------------
# The vault lives in PostGIS, so the GeoParquet alone could not rebuild it. Each
# batch also archives the vault's ciphertext next to the serving copy, which makes
# the archive a complete copy of what PostGIS holds. Ciphertext is safe to store:
# only OpenBao can decrypt it.

def vault_archive_key(layer: str, batch_id: str) -> str:
    return f"{layer}/batch={safe_batch_id(batch_id)}/vault.parquet"


def write_vault_archive(vault: pd.DataFrame, layer: str, batch_id: str, minio: dict) -> str:
    """Write the vault rows (record_id, column_name, ciphertext, key_version) and
    return their s3:// URI."""
    buffer = io.BytesIO()
    vault.to_parquet(buffer, index=False)
    key = vault_archive_key(layer, batch_id)
    _s3(minio).put_object(Bucket=minio["bucket"], Key=key, Body=buffer.getvalue())
    return f"s3://{minio['bucket']}/{key}"


def read_vault_archive(layer: str, batch_id: str, minio: dict) -> pd.DataFrame:
    body = _s3(minio).get_object(Bucket=minio["bucket"], Key=vault_archive_key(layer, batch_id))["Body"].read()
    return pd.read_parquet(io.BytesIO(body))


def latest_batch(layer: str, minio: dict) -> str | None:
    """The newest archived batch that has both its layer and its vault archive,
    as the batch directory name. None if there is none. Batches written before
    the vault archive existed are skipped, because they cannot rebuild the vault."""
    client = _s3(minio)
    found: dict[str, dict[str, object]] = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=minio["bucket"], Prefix=f"{layer}/batch="):
        for item in page.get("Contents", []):
            batch = item["Key"].split("/")[1].removeprefix("batch=")
            found.setdefault(batch, {})[item["Key"].rsplit("/", 1)[1]] = item["LastModified"]
    complete = {b: files["part.parquet"] for b, files in found.items() if {"part.parquet", "vault.parquet"} <= set(files)}
    return max(complete, key=complete.get) if complete else None
