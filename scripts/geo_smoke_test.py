"""Smoke test for the geo config, ingest and validation steps (S5 to S8).

Runs in the geo environment inside the Airflow container. It needs the sample
files from sample_data/geo/make_sample.py. Nothing is written to the repo: the
working copies go to a temporary folder.

    docker compose -f docker-compose.demo.yml -f docker-compose.geo.yml exec -T \
      -e PYTHONPATH=/opt/airflow/dags airflow /opt/airflow/geo-venv/bin/python - < scripts/geo_smoke_test.py
"""
from __future__ import annotations

import copy
import os
import shutil
import sys
import tempfile
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Polygon

from common.geo_config import GeoConfigError, load_geo_registry, load_layer_config, validate_layer_config
from ingestion.ingest_shapefile import GeoIngestError, ingest_layer, remove_work_dir
from validation.geo_validation import validate_geo

ROOT = Path(os.getenv("GATES_APP_ROOT", "/opt/airflow/dags"))
SAMPLES = Path(os.getenv("GEO_SAMPLE_DIR", ROOT / "sample_data" / "geo"))
failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not ok else ""))
    if not ok:
        failures.append(name)


def rejects(name: str, config: dict, expect: str) -> None:
    try:
        validate_layer_config(config)
    except GeoConfigError as exc:
        check(name, expect in str(exc), str(exc))
    else:
        check(name, False, "the config was accepted")


def run_case(config: dict, folder: Path, work_root: Path, run_id: str):
    manifest = ingest_layer(config, folder, "*.shp", run_id, work_root)
    gdf = gpd.read_parquet(manifest["work_file"])
    report, repaired = validate_geo(gdf, config)
    return manifest, report, repaired


def failed(report: dict) -> dict[str, dict]:
    return {r["rule"]: r for r in report["rule_results"] if not r["success"] and r["severity"] == "hard"}


def main() -> int:
    work_root = Path(tempfile.mkdtemp(prefix="geo-smoke-"))
    try:
        config = load_layer_config("critical_facilities", ROOT / "config")
        check("layer config loads and validates", config["layer"] == "critical_facilities")

        registry = load_geo_registry(ROOT / "orchestration" / "geo_registry.yaml")
        check("registry lists critical_facilities", [e["layer"] for e in registry] == ["critical_facilities"])

        broken = copy.deepcopy(config); del broken["unique_key"]
        rejects("config without a unique_key is rejected", broken, "unique_key")
        broken = copy.deepcopy(config); broken["source_crs"] = "EPSG:3857"
        rejects("config with a non-WGS 84 CRS is rejected", broken, "EPSG:4326")
        broken = copy.deepcopy(config); broken["bbox_ph"] = [4.5, 116.9, 21.2, 126.7]
        rejects("swapped bounding box is rejected", broken, "swapped")
        broken = copy.deepcopy(config); broken["pii"]["columns"]["remarks"]["serving"] = "shred"
        rejects("unknown PII treatment is rejected", broken, "shred")
        broken = copy.deepcopy(config); broken["field_aliases"]["facity_Id"] = "no_such_column"
        rejects("alias to an unknown column is rejected", broken, "no_such_column")

        good = SAMPLES / "critical_facilities"
        manifest, report, repaired = run_case(config, good, work_root, "good")
        check("good sample: 5 point features", manifest["feature_count"] == 5 and manifest["geometry_types"] == ["Point"], str(manifest["geometry_types"]))
        check("good sample: facity_Id restored to facility_id", "facility_id" in manifest["columns"] and "facity_Id" not in manifest["columns"], str(manifest["columns"]))
        check("good sample: alias map is recorded in the manifest", manifest["aliases_applied"].get("facity_Id") == "facility_id", str(manifest["aliases_applied"]))
        check("good sample: .prj is WGS 84", any(t in str(manifest["crs_in_file"]) for t in ("4326", "WGS")), str(manifest["crs_in_file"]))
        check("good sample passes validation", report["hard_rule_pass"] and report["data_quality_index"] == 100.0, str(failed(report)))
        remove_work_dir(work_root, "critical_facilities", "good")
        check("working copy is removed on request", not (work_root / "critical_facilities" / "good").exists())

        cases = {
            "wrong_crs": ("ingest", "CRS_MISMATCH"),
            "swapped_coordinates": ("validate", {"bbox"}),
            "wrong_geometry_type": ("validate", {"geometry_type"}),
            "duplicate_key": ("validate", {"unique_key_unique"}),
            "bad_attributes": ("validate", {"required_values", "permissible_values"}),
        }
        for case, (stage, expected) in cases.items():
            folder = SAMPLES / "failure_demo" / case
            try:
                _, case_report, _ = run_case(config, folder, work_root, case)
            except GeoIngestError as exc:
                check(f"{case}: fails at ingest with {expected}", stage == "ingest" and exc.code == expected, str(exc))
            else:
                got = set(failed(case_report))
                check(f"{case}: fails validation with {sorted(expected)}", stage == "validate" and expected <= got and not case_report["hard_rule_pass"], f"got {sorted(got)}")

        # a shapefile without a .prj uses the config CRS, one without a .dbf is refused
        tmp = work_root / "variants"
        tmp.mkdir()
        for sub in ("no_prj", "no_dbf"):
            shutil.copytree(good, tmp / sub)
        for path in (tmp / "no_prj").glob("*.prj"):
            path.unlink()
        for path in (tmp / "no_dbf").glob("*.dbf"):
            path.unlink()
        manifest, report, _ = run_case(config, tmp / "no_prj", work_root, "no_prj")
        check("missing .prj falls back to the config CRS", "none" in str(manifest["crs_in_file"]) and report["hard_rule_pass"], str(manifest["crs_in_file"]))
        try:
            ingest_layer(config, tmp / "no_dbf", "*.shp", "no_dbf", work_root)
        except GeoIngestError as exc:
            check("missing .dbf is refused", exc.code == "MISSING_SIDECAR", str(exc))
        else:
            check("missing .dbf is refused", False, "it was ingested")

        # invalid geometry repair: a polygon layer with one bow-tie
        poly_cfg = {
            "layer": "repair_test", "geometry_type": "Polygon", "unique_key": "id",
            "bbox_ph": [116.9, 4.5, 126.7, 21.2],
            "attributes": [{"name": "id", "required": True}],
            "thresholds": {"max_null_geom_pct": 0, "max_repaired_pct": 60},
        }
        bowtie = Polygon([(121.0, 14.0), (121.2, 14.2), (121.2, 14.0), (121.0, 14.2)])
        square = Polygon([(122.0, 14.0), (122.1, 14.0), (122.1, 14.1), (122.0, 14.1)])
        polys = gpd.GeoDataFrame({"id": ["a", "b"]}, geometry=[bowtie, square], crs="EPSG:4326")
        rep, fixed = validate_geo(polys, poly_cfg)
        soft = [r for r in rep["rule_results"] if r["rule"] == "invalid_geometries_repaired"][0]
        check("invalid geometry is repaired and reported as a warning", rep["hard_rule_pass"] and not soft["success"] and fixed.geometry.is_valid.all(), soft["detail"])
        poly_cfg["thresholds"]["max_repaired_pct"] = 0
        rep, _ = validate_geo(polys, poly_cfg)
        check("repairs above the threshold fail the batch", "repair_threshold" in failed(rep))
    finally:
        shutil.rmtree(work_root, ignore_errors=True)

    print(f"\n{'ALL CHECKS PASSED' if not failures else str(len(failures)) + ' CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
