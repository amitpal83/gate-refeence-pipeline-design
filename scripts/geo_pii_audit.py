"""Final audit for readable PII in every place it could have leaked.

Run after the DAG has loaded the critical_facilities layer. It checks, with
Presidio and with a search for the known invented values:

  - every text column in the geo and geo_serving schemas of PostGIS
  - every GeoParquet object in the gates-geo bucket
  - the Airflow task logs of the geo DAG, where a leak is easy to miss
  - that gates-geo holds only protected GeoParquet, never source files
  - that no working files are left in the container

The quarantine bucket is reported but not failed: it holds the original files of
rejected batches on purpose, so it is the one place plaintext PII can exist.

    docker compose -f docker-compose.demo.yml -f docker-compose.geo.yml exec -T \
      -e PYTHONPATH=/opt/airflow/dags airflow /opt/airflow/geo-venv/bin/python - < scripts/geo_pii_audit.py

Findings are reported as columns, files and counts. Matched text is never printed.
"""
from __future__ import annotations

import io
import os
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from common.geo_config import load_layer_config
from common.geo_standardize import _s3
from common.openbao_client import OpenBaoClient
from common.pii_protection import build_analyzer, detect_pii, load_policy
from common.postgis_loader import connect

ROOT = Path(os.getenv("GATES_APP_ROOT", "/opt/airflow/dags"))
LOG_ROOT = Path(os.getenv("GEO_LOG_ROOT", "/opt/airflow/logs"))
WORK_ROOT = Path(os.getenv("GEO_WORK_ROOT", "/tmp/geo-work"))
LAYER = "critical_facilities"
SOURCE_SUFFIXES = (".shp", ".dbf", ".shx", ".prj", ".cpg", ".zip", ".gpkg", ".csv", ".json")

SEEDED = [
    "Juan", "dela Cruz", "Maria Santos", "Pedro Reyes", "Ana Lim", "Liza Ramos", "Jose Mercado",
    "0917 123 4567", "0928 765 4321", "(02) 8123 4567", "0939 555 0123", "0905 222 3344", "0917 000 1111",
    "juan.delacruz@example.org", "maria.santos@example.org", "pedro.reyes@example.org",
    "ana.lim@example.org", "liza.ramos@example.org", "123-456-789-000",
]
# Masked emails such as j***@example.org look like emails to Presidio, so emails are not
# re-scanned. The known values above still catch a real email.
SCAN_ENTITIES = ["PERSON", "PH_PHONE", "PH_TIN", "PH_SSS", "PH_PHILSYS"]
failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not ok else ""))
    if not ok:
        failures.append(name)


def audit_frame(df: pd.DataFrame, analyzer, skip: set[str]) -> tuple[list[str], dict[str, int]]:
    """Returns (known values found, {column: findings}) for one table."""
    columns = [c for c in df.columns if c not in skip and df[c].dtype == object]
    blob = " ".join(str(v) for v in df[columns].to_numpy().ravel()) if columns else ""
    known = [s for s in SEEDED if s in blob]
    policy = load_policy({"scan": "all_text", "entities": SCAN_ENTITIES})
    found = detect_pii(df, policy, analyzer, columns) if columns else {}
    return known, {c: len(rows) for c, rows in found.items() if rows}


def scan_postgis(engine, analyzer, skip: set[str]) -> list[tuple[str, list[str], dict[str, int]]]:
    with engine.connect() as conn:
        columns = conn.execute(text(
            "SELECT table_schema, table_name, column_name FROM information_schema.columns "
            "WHERE table_schema IN ('geo', 'geo_serving') AND data_type IN ('text', 'character varying') "
            "ORDER BY table_schema, table_name, ordinal_position"
        )).all()
        tables: dict[tuple[str, str], list[str]] = {}
        for schema, table, column in columns:
            if not table.endswith("__stage"):
                tables.setdefault((schema, table), []).append(column)
        results = []
        for (schema, table), names in tables.items():
            selected = ", ".join(f'"{n}"' for n in names)
            frame = pd.read_sql(text(f'SELECT {selected} FROM "{schema}"."{table}"'), conn)
            known, hits = audit_frame(frame, analyzer, skip)
            results.append((f"{schema}.{table}", known, hits))
    return results


def scan_geoparquet(s3, bucket: str, analyzer, skip: set[str]) -> list[tuple[str, list[str], dict[str, int]]]:
    results = []
    for item in s3.list_objects_v2(Bucket=bucket).get("Contents", []):
        if not item["Key"].endswith(".parquet"):
            continue
        body = s3.get_object(Bucket=bucket, Key=item["Key"])["Body"].read()
        # Plain pandas reads both kinds of object: the spatial layer and the vault
        # archive, which has no geometry. The vault archive holds ciphertext only.
        frame = pd.read_parquet(io.BytesIO(body)).drop(columns=["geometry"], errors="ignore")
        known, hits = audit_frame(frame, analyzer, skip | {"ciphertext"})
        results.append((item["Key"], known, hits))
    return results


def scan_task_logs(root: Path) -> tuple[int, list[str]]:
    """Returns (files read, log files containing a known value)."""
    leaking, read = [], 0
    for path in root.glob(f"dag_id={LAYER}_geo_pipeline/**/*.log"):
        read += 1
        content = path.read_text(encoding="utf-8", errors="replace")
        if any(s in content for s in SEEDED):
            leaking.append(str(path.relative_to(root)))
    return read, leaking


def main() -> int:
    client = OpenBaoClient.from_env()
    config = load_layer_config(LAYER, ROOT / "config")
    minio = client.read_secret("geo/minio")
    s3 = _s3(minio)
    skip = set(config["pii"].get("skip_columns", [])) | {config["unique_key"]}

    print("Loading Presidio and the spaCy model...")
    analyzer = build_analyzer()

    tables = scan_postgis(connect(client), analyzer, skip)
    check("PostGIS has the layer's table and serving view", {"geo." + LAYER, "geo_serving." + LAYER} <= {t[0] for t in tables}, str([t[0] for t in tables]))
    for name, known, hits in tables:
        check(f"no readable PII in {name}", not known and not hits, f"known values: {len(known)}, findings: {hits}")

    parquet = scan_geoparquet(s3, minio["bucket"], analyzer, skip)
    check("GeoParquet objects exist", bool(parquet))
    check("at least one batch has its vault archive", any(key.endswith("vault.parquet") for key, _, _ in parquet))
    for key, known, hits in parquet:
        check(f"no readable PII in {key}", not known and not hits, f"known values: {len(known)}, findings: {hits}")

    objects = s3.list_objects_v2(Bucket=minio["bucket"]).get("Contents", [])
    sources = [o["Key"] for o in objects if o["Key"].lower().endswith(SOURCE_SUFFIXES)]
    check("the gates-geo bucket holds no source files", not sources, str(sources))

    read, leaking = scan_task_logs(LOG_ROOT)
    check(f"no known PII in the geo DAG task logs ({read} files read)", read > 0 and not leaking, str(leaking) if leaking else "no log files found")

    leftovers = [p for p in (WORK_ROOT / LAYER).rglob("*") if p.is_file()] if (WORK_ROOT / LAYER).exists() else []
    check("no working files are left in the container", not leftovers, str([str(p) for p in leftovers]))

    quarantined = s3.list_objects_v2(Bucket=minio["quarantine_bucket"]).get("KeyCount", 0)
    print(f"NOTE  the quarantine bucket holds {quarantined} object(s). It keeps rejected files with readable PII on "
          "purpose. They are encrypted at rest (SSE-KMS) and expire after the retention period, and access to the "
          "bucket must stay restricted.")

    print(f"\n{'ALL CHECKS PASSED' if not failures else str(len(failures)) + ' CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
