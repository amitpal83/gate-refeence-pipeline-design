"""End-to-end test of the geo load path (S9): ingest, validate, protect PII,
standardize, write GeoParquet, load PostGIS and the vault.

Needs scripts/geo_setup.py to have been run, and the sample files from
sample_data/geo/make_sample.py. Loads the real geo.critical_facilities table
with batch id "smoke" and removes its GeoParquet object afterwards.

    docker compose -f docker-compose.demo.yml -f docker-compose.geo.yml exec -T \
      -e PYTHONPATH=/opt/airflow/dags -e GEO_DB_PASSWORD \
      -e OPENBAO_READER_APPROLE="$(sudo cat /opt/gates-demo/data/openbao-init/approle-pii-reader.env)" \
      airflow /opt/airflow/geo-venv/bin/python - < scripts/geo_load_test.py

GEO_DB_PASSWORD (the PostGIS admin password) is optional. With it, the test also
reads the vault as admin, decrypts it as the reader role and compares every
value with the original file.
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import psycopg2
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from common.geo_config import load_layer_config
from common.geo_standardize import delete_geoparquet, read_geoparquet, standardize, write_geoparquet
from common.openbao_client import OpenBaoClient
from common.pii_protection import build_analyzer, detect_pii, load_policy, protect_dataframe, reveal_contacts
from common.postgis_loader import PostgisLoadError, connect, load_layer
from ingestion.ingest_shapefile import ingest_layer, remove_work_dir
from validation.geo_validation import validate_geo

ROOT = Path(os.getenv("GATES_APP_ROOT", "/opt/airflow/dags"))
SAMPLE = Path(os.getenv("GEO_SAMPLE_DIR", ROOT / "sample_data" / "geo")) / "critical_facilities"
LAYER = "critical_facilities"
BATCH = "smoke"
SEEDED = [
    "Juan", "dela Cruz", "Maria Santos", "Pedro Reyes", "Ana Lim", "Liza Ramos", "Jose Mercado",
    "0917 123 4567", "0928 765 4321", "(02) 8123 4567", "0939 555 0123", "0905 222 3344", "0917 000 1111",
    "juan.delacruz@example.org", "maria.santos@example.org", "pedro.reyes@example.org",
    "ana.lim@example.org", "liza.ramos@example.org", "123-456-789-000",
]
failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not ok else ""))
    if not ok:
        failures.append(name)


def denied(call) -> bool:
    try:
        call()
    except DBAPIError as exc:
        return "permission denied" in str(exc).lower()
    return False


def main() -> int:
    work_root = Path(tempfile.mkdtemp(prefix="geo-load-"))
    client = OpenBaoClient.from_env()
    config = load_layer_config(LAYER, ROOT / "config")
    minio = client.read_secret("geo/minio")
    try:
        manifest = ingest_layer(config, SAMPLE, "*.shp", BATCH, work_root)
        original = gpd.read_parquet(manifest["work_file"])
        report, validated = validate_geo(original, config)
        check("good sample passes validation", report["hard_rule_pass"])

        print("Loading Presidio and the spaCy model...")
        analyzer = build_analyzer()
        policy = load_policy(config["pii"])
        protected = protect_dataframe(validated, policy, config["unique_key"], client, analyzer)
        remove_work_dir(work_root, LAYER, BATCH)

        layer_gdf = standardize(protected.serving, config, BATCH)
        uri = write_geoparquet(layer_gdf, LAYER, BATCH, minio)
        check("GeoParquet is written to the gates-geo bucket", uri == f"s3://{minio['bucket']}/{LAYER}/batch={BATCH}/part.parquet", uri)

        engine = connect(client)
        result = load_layer(engine, layer_gdf, config, protected.vault, BATCH)
        check("PostGIS table has 5 rows and the vault 19 (15 contact values + 4 remarks)",result["rows"] == 5 and result["vault_rows"] == 19, str(result))

        again = load_layer(engine, layer_gdf, config, protected.vault, BATCH)
        check("re-running the same batch leaves the counts unchanged", again["rows"] == 5 and again["vault_rows"] == 19, str(again))

        # a failed load must leave the previous version intact
        broken = layer_gdf.assign(extra_column="x")
        try:
            load_layer(engine, broken, config, protected.vault, "broken")
        except PostgisLoadError as exc:
            refused = "changed" in str(exc)
        else:
            refused = False
        with engine.connect() as conn:
            rows = conn.execute(text(f"SELECT count(*) FROM geo.{LAYER}")).scalar()
            batches = conn.execute(text(f"SELECT DISTINCT \"_batch_id\" FROM geo.{LAYER}")).scalars().all()
            vault_count = conn.execute(text("SELECT count(*) FROM pii_vault.contact_pii WHERE layer = :l"), {"l": LAYER}).scalar()
        check("a load with changed columns is refused", refused)
        check("a failed load leaves the previous version intact", rows == 5 and batches == [BATCH] and vault_count == 19, f"{rows} {batches} {vault_count}")

        # access control
        with engine.connect() as conn:
            check("loader cannot read vault ciphertext", denied(lambda: conn.execute(text("SELECT ciphertext FROM pii_vault.contact_pii"))))
        reader_engine = connect(client, "geo/postgis_geoserver")
        with reader_engine.connect() as conn:
            view_columns = list(conn.execute(text(f"SELECT * FROM geo_serving.{LAYER}")).keys())
        expected = [*config["geoserver"]["expose_columns"], "geometry"]
        check("geoserver role reads only the exposed columns", view_columns == expected, str(view_columns))
        for statement in (f"SELECT * FROM geo.{LAYER}", "SELECT * FROM pii_vault.contact_pii"):
            with reader_engine.connect() as conn:
                check(f"geoserver role is denied: {statement}", denied(lambda s=statement: conn.execute(text(s))))

        # nothing readable is stored
        stored = pd.read_sql(text(f"SELECT * FROM geo.{LAYER}"), engine.connect())
        stored_blob = " ".join(str(v) for v in stored.to_numpy().ravel())
        parquet = read_geoparquet(LAYER, BATCH, minio)
        parquet_blob = " ".join(str(v) for v in parquet.drop(columns="geometry").to_numpy().ravel())
        leaked = [s for s in SEEDED if s in stored_blob or s in parquet_blob]
        check("no seeded PII in PostGIS or GeoParquet", not leaked, f"found: {leaked}")
        check("GeoParquet reads back with 5 features in WGS 84", len(parquet) == 5 and parquet.crs.to_epsg() == 4326)

        scan = stored.drop(columns=["geometry", "name", "address", "contact_email"], errors="ignore")
        scan_policy = load_policy({"scan": "all_text", "entities": ["PERSON", "PH_PHONE", "PH_TIN", "PH_SSS", "PH_PHILSYS"]})
        text_columns = [c for c in scan.columns if scan[c].dtype == object]
        found = detect_pii(scan, scan_policy, analyzer, text_columns)
        hits = {c: len(rows) for c, rows in found.items() if rows}
        check("a second Presidio scan of the stored data finds nothing", not hits, str(hits))

        # the vault round trip
        admin_password = os.getenv("GEO_DB_PASSWORD")
        if not admin_password:
            print("SKIP  vault round trip (set GEO_DB_PASSWORD to run it)")
        else:
            secret = client.read_secret("geo/postgis")
            with psycopg2.connect(host=secret["host"], port=secret["port"], dbname=secret["dbname"],
                                  user=os.getenv("GEO_DB_USER", "gates_geo"), password=admin_password) as db:
                vault = pd.read_sql("SELECT record_id, column_name, ciphertext, key_version FROM pii_vault.contact_pii WHERE layer = %s", db, params=(LAYER,))
            reader = OpenBaoClient.from_approle_text(os.environ["OPENBAO_READER_APPROLE"])
            revealed = reveal_contacts(reader, vault)
            matches = all(
                revealed.get(str(row.facility_id), {}).get(col) == getattr(row, col)
                for row in original.itertuples()
                for col in ("contact_person", "contact_phone", "contact_email")
            )
            check("vault in PostGIS decrypts to the original contact values", matches and vault["ciphertext"].str.startswith("vault:v").all())
    finally:
        shutil.rmtree(work_root, ignore_errors=True)
        try:
            delete_geoparquet(LAYER, BATCH, minio)
        except Exception:
            pass

    print(f"\n{'ALL CHECKS PASSED' if not failures else str(len(failures)) + ' CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
