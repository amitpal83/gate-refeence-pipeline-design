"""Check the end state after a run of the critical_facilities_geo_pipeline DAG.

Two modes, chosen with GEO_VERIFY_MODE:

  success (default)  after a run on the good sample: 5 features in PostGIS and 19
                     vault rows, a GeoParquet object, GeoServer serving the 5
                     features without contact fields, no readable PII anywhere,
                     no leftover working files, and the dataset in DataHub.
  failure            after a run with a bad file in the drop folder: the batch
                     is in the quarantine bucket with the expected failed rule,
                     and the previously loaded data is still in PostGIS.
                     EXPECT_RULE names the rule (default unique_key_unique).

    docker compose -f docker-compose.demo.yml -f docker-compose.geo.yml exec -T \
      -e PYTHONPATH=/opt/airflow/dags -e GEO_VERIFY_MODE=success \
      airflow /opt/airflow/geo-venv/bin/python - < scripts/geo_verify.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests
from sqlalchemy import text

from common.geo_config import load_layer_config
from common.geo_standardize import _s3
from common.geoserver_publisher import verify_ows
from common.openbao_client import OpenBaoClient
from common.postgis_loader import connect
from metadata.datahub_emit_geo import DATAHUB_GMS_SERVER, geo_dataset_urn

ROOT = Path(os.getenv("GATES_APP_ROOT", "/opt/airflow/dags"))
WORK_ROOT = Path(os.getenv("GEO_WORK_ROOT", "/tmp/geo-work"))
LAYER = "critical_facilities"
MODE = os.getenv("GEO_VERIFY_MODE", "success")
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


def main() -> int:
    client = OpenBaoClient.from_env()
    config = load_layer_config(LAYER, ROOT / "config")
    minio = client.read_secret("geo/minio")
    s3 = _s3(minio)
    engine = connect(client)

    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT count(*) FROM geo.{LAYER}")).scalar()
        batch = conn.execute(text(f'SELECT max("_batch_id") FROM geo.{LAYER}')).scalar()
        vault = conn.execute(text("SELECT count(*) FROM pii_vault.contact_pii WHERE layer = :l"), {"l": LAYER}).scalar()
        stored = pd.read_sql(text(f"SELECT * FROM geo.{LAYER}"), conn)
    print(f"PostGIS: {rows} rows, vault {vault}, batch {batch}")

    leftovers = [str(p) for p in (WORK_ROOT / LAYER).rglob("*") if p.is_file()] if (WORK_ROOT / LAYER).exists() else []
    check("no working files are left behind", not leftovers, str(leftovers))

    if MODE == "failure":
        expected_rule = os.getenv("EXPECT_RULE", "unique_key_unique")
        listing = s3.list_objects_v2(Bucket=minio["quarantine_bucket"], Prefix=f"{LAYER}/").get("Contents", [])
        reports = sorted((o for o in listing if o["Key"].endswith("failure.json")), key=lambda o: o["LastModified"])
        check("a failed batch is in the quarantine bucket", bool(reports))
        if reports:
            body = json.loads(s3.get_object(Bucket=minio["quarantine_bucket"], Key=reports[-1]["Key"])["Body"].read())
            check(f"the quarantine report names {expected_rule}", expected_rule in body["reason"], body["reason"])
            check("the failed source files are in quarantine", any(f.endswith(".shp") for f in body["files"]), str(body["files"]))
        check("the previous data is still in PostGIS", rows == 5 and vault == 19, f"{rows} rows, vault {vault}")
    else:
        check("PostGIS holds 5 features and the vault 19 rows", rows == 5 and vault == 19, f"{rows} rows, vault {vault}")

        objects = s3.list_objects_v2(Bucket=minio["bucket"], Prefix=f"{LAYER}/").get("Contents", [])
        check("a GeoParquet object exists", any(o["Key"].endswith("part.parquet") for o in objects), str([o["Key"] for o in objects]))

        geoserver = client.read_secret("geo/geoserver")
        served = verify_ows(geoserver["url"], config["geoserver"]["workspace"], LAYER, config["bbox_ph"],
                            (geoserver["user"], geoserver["password"]))
        exposed = set(config["geoserver"]["expose_columns"])
        check("GeoServer serves the 5 features", served["wms_status"] == 200 and served["wfs_features"] == 5, str(served))
        check("GeoServer shows only the exposed columns", set(served["wfs_properties"]) <= exposed, str(served["wfs_properties"]))

        blob = " ".join(str(v) for v in stored.drop(columns="geometry").to_numpy().ravel())
        check("no seeded PII in the stored table", not [s for s in SEEDED if s in blob])

        urn = geo_dataset_urn(LAYER)
        try:
            found = requests.get(f"{DATAHUB_GMS_SERVER}/entities/{quote(urn, safe='')}", timeout=15).status_code == 200
        except requests.RequestException:
            found = False
        check("the dataset is registered in DataHub", found, urn)

    print(f"\n{'ALL CHECKS PASSED' if not failures else str(len(failures)) + ' CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
