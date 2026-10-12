"""Test of GeoServer publishing (S10): publish the critical_facilities view, then
check WMS, WFS, the style, idempotency and the anonymous-access restriction.

Needs scripts/geo_setup.py and scripts/geo_load_test.py to have run first, so
geo_serving.critical_facilities exists with 5 features.

    docker compose -f docker-compose.demo.yml -f docker-compose.geo.yml exec -T \
      -e PYTHONPATH=/opt/airflow/dags airflow /opt/airflow/geo-venv/bin/python - < scripts/geoserver_test.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from sqlalchemy import text

from common.geo_config import load_layer_config
from common.geoserver_publisher import GeoServer, counts, publish, verify_ows
from common.openbao_client import OpenBaoClient
from common.postgis_loader import connect

ROOT = Path(os.getenv("GATES_APP_ROOT", "/opt/airflow/dags"))
LAYER = "critical_facilities"
failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not ok else ""))
    if not ok:
        failures.append(name)


def main() -> int:
    client = OpenBaoClient.from_env()
    config = load_layer_config(LAYER, ROOT / "config")
    workspace = config["geoserver"]["workspace"]
    geoserver = client.read_secret("geo/geoserver")
    postgis = client.read_secret("geo/postgis_geoserver")
    auth = (geoserver["user"], geoserver["password"])

    with connect(client, "geo/postgis_geoserver").connect() as conn:
        loaded = conn.execute(text(f"SELECT count(*) FROM geo_serving.{LAYER}")).scalar()
    if loaded != 5:
        print(f"geo_serving.{LAYER} has {loaded} rows, expected 5. Run scripts/geo_load_test.py first.")
        return 2

    print("Waiting for GeoServer and publishing...")
    first = publish(config, geoserver, postgis)
    print(f"first publish: {first}")

    gs = GeoServer(geoserver["url"], geoserver["user"], geoserver["password"])
    before = counts(gs, workspace)
    second = publish(config, geoserver, postgis)
    after = counts(gs, workspace)
    print(f"second publish: {second}")
    check("second publish creates nothing", "created" not in second.values(), str(second))
    check("second publish adds no workspaces, stores, layers or styles", before == after, f"{before} vs {after}")
    check("anonymous access was closed", second.get("anonymous_access") == "unchanged" and first.get("anonymous_access") in ("restricted", "unchanged"))

    signed_in = verify_ows(geoserver["url"], workspace, LAYER, config["bbox_ph"], auth)
    check("WMS GetMap returns a PNG", signed_in["wms_status"] == 200 and "image/png" in signed_in["wms_type"], str(signed_in))
    check("WFS GetFeature returns the 5 facilities", signed_in["wfs_status"] == 200 and signed_in["wfs_features"] == 5, str(signed_in))

    exposed = set(config["geoserver"]["expose_columns"])
    properties = set(signed_in["wfs_properties"])
    check("WFS shows only the exposed columns", properties <= exposed, str(sorted(properties - exposed)))
    check("WFS shows no contact fields", not [p for p in properties if p.startswith("contact") or p == "remarks"], str(sorted(properties)))

    anonymous = verify_ows(geoserver["url"], workspace, LAYER, config["bbox_ph"], None)
    check("anonymous WFS request gets no features", anonymous["wfs_features"] == 0, str(anonymous))
    check("anonymous WMS request gets no map", "image/png" not in anonymous["wms_type"], str(anonymous))

    sld = gs.call("GET", f"/workspaces/{workspace}/styles/{LAYER}.sld").text
    colors = config["geoserver"]["style"]["colors"].values()
    check("the generated style uses the configured colours", all(c in sld for c in colors))

    layer_info = gs.call("GET", f"/workspaces/{workspace}/layers/{LAYER}.json", headers={"Accept": "application/json"}).json()
    check("the layer's default style is the generated one", layer_info["layer"]["defaultStyle"]["name"].endswith(LAYER), str(layer_info["layer"]["defaultStyle"]))

    print(f"\n{'ALL CHECKS PASSED' if not failures else str(len(failures)) + ' CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
