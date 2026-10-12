"""
geo_airflow_dag.py: DAG FACTORY for geo vector layers, one DAG per entry in
geo_registry.yaml. Adding a layer means adding config/geo/<layer>.yaml and a
registry entry, not editing this file.

Per-DAG task chain:
    start_job -> ingest_shapefile -> validate_geometry -> protect_pii
      -> standardize -> write_geoparquet -> load_postgis -> publish_geoserver
      -> register_metadata -> finish_job
    cleanup_work runs after publish_geoserver whatever happened upstream.

The heavy tasks run in the geo Python environment (GEO_PYTHON, built in
deployment/airflow-geo/Dockerfile) through ExternalPythonOperator, so geopandas,
Presidio and spaCy never share an environment with dbt and Great Expectations.
The start, cleanup, metadata and finish tasks run in the normal Airflow
environment, which has the monitoring helpers and the DataHub client.

Plaintext PII exists only between ingest_shapefile and the end of protect_pii,
in GEO_WORK_ROOT (inside the container, not on a host mount). protect_pii
deletes the plaintext files as soon as the protected copy is written. Every
task that can leave files behind is covered by cleanup_work and by the failure
callback. A hard validation failure sends the source files to the quarantine
bucket and stops the run.

Functions named _task_* run inside the geo environment: they are copied into a
subprocess, so each one imports what it needs and uses no module-level names.
"""
from datetime import datetime, timedelta
import os
import re
import shutil
import sys
from pathlib import Path

from airflow import DAG
from airflow.operators.python import ExternalPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from common.geo_config import load_geo_registry  # noqa: E402

REGISTRY_PATH = Path(__file__).parent / "geo_registry.yaml"
GEO_PYTHON = os.getenv("GEO_PYTHON", "/opt/airflow/geo-venv/bin/python")
WORK_ROOT = os.getenv("GEO_WORK_ROOT", "/tmp/geo-work")

DEFAULT_ARGS = {
    "owner": "gates-data-platform",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

_UNSAFE = re.compile(r"[^A-Za-z0-9._=-]")


def _work_id(run_id: str) -> str:
    """Same rule as common.geo_standardize.safe_batch_id, which cannot be
    imported here because the main environment has no geopandas."""
    return _UNSAFE.sub("_", run_id)


def geo_failure_callback(context):
    """Log the failure and finish the job_execution row (the existing callback),
    then delete this run's plaintext working copy."""
    from orchestration.monitoring import failure_callback

    failure_callback(context)
    layer = context["dag"].dag_id.removesuffix("_geo_pipeline")
    shutil.rmtree(Path(WORK_ROOT) / layer / _work_id(context["run_id"]), ignore_errors=True)


# --- tasks that run in the geo environment ---------------------------------

def _task_ingest(layer, run_id, repo_root, work_root, source_folder, file_pattern):
    import sys
    from pathlib import Path

    sys.path.insert(0, repo_root)
    from common.geo_config import load_layer_config
    from common.geo_standardize import safe_batch_id
    from ingestion.ingest_shapefile import ingest_layer

    config = load_layer_config(layer, Path(repo_root) / "config")
    manifest = ingest_layer(config, Path(repo_root) / source_folder, file_pattern, safe_batch_id(run_id), work_root)
    keep = ("layer", "run_id", "source_file", "source_sha256", "feature_count", "crs_in_file",
            "geometry_types", "columns", "aliases_applied", "other_candidates")
    result = {key: manifest[key] for key in keep}
    result["source_path"] = str(Path(repo_root) / source_folder / manifest["source_file"])
    return result


def _task_validate(layer, run_id, repo_root, work_root, source_path):
    import sys
    from pathlib import Path

    sys.path.insert(0, repo_root)
    import geopandas as gpd
    from common.geo_config import load_layer_config
    from common.geo_quarantine import collect_source_files, quarantine_geo_batch
    from common.geo_standardize import safe_batch_id
    from common.openbao_client import OpenBaoClient
    from ingestion.ingest_shapefile import remove_work_dir, work_dir_for
    from validation.geo_validation import failed_rules, validate_geo

    config = load_layer_config(layer, Path(repo_root) / "config")
    work_id = safe_batch_id(run_id)
    work_dir = work_dir_for(work_root, layer, work_id)

    report, validated = validate_geo(gpd.read_parquet(work_dir / "ingested.parquet"), config)
    if not report["hard_rule_pass"]:
        failed = ", ".join(failed_rules(report))
        minio = OpenBaoClient.from_env().read_secret("geo/minio")
        location = quarantine_geo_batch(
            layer, run_id, collect_source_files(source_path),
            f"Geometry or attribute validation failed: {failed}", report, minio,
        )
        remove_work_dir(work_root, layer, work_id)
        raise ValueError(f"Validation failed for {layer}: {failed}. The batch was quarantined at {location}")

    validated.to_parquet(work_dir / "validated.parquet")
    return {
        "data_quality_index": report["data_quality_index"],
        "row_count": report["row_count"],
        "soft_warnings": [r["rule"] for r in report["rule_results"] if r["severity"] == "soft" and not r["success"]],
    }


def _task_protect(layer, run_id, repo_root, work_root):
    import shutil
    import sys
    from pathlib import Path

    sys.path.insert(0, repo_root)
    import geopandas as gpd
    from common.geo_config import load_layer_config
    from common.geo_standardize import safe_batch_id
    from common.openbao_client import OpenBaoClient
    from common.pii_protection import build_analyzer, load_policy, protect_dataframe
    from ingestion.ingest_shapefile import remove_work_dir, work_dir_for

    config = load_layer_config(layer, Path(repo_root) / "config")
    work_id = safe_batch_id(run_id)
    work_dir = work_dir_for(work_root, layer, work_id)
    try:
        validated = gpd.read_parquet(work_dir / "validated.parquet")
        result = protect_dataframe(
            validated, load_policy(config["pii"]), config["unique_key"],
            OpenBaoClient.from_env(), build_analyzer(),
        )
        result.serving.to_parquet(work_dir / "serving.parquet")
        result.vault.to_parquet(work_dir / "vault.parquet")
    except Exception:
        # Fail closed: nothing readable stays behind.
        remove_work_dir(work_root, layer, work_id)
        raise

    # The protected copy is written. Delete everything that held readable PII.
    (work_dir / "ingested.parquet").unlink(missing_ok=True)
    (work_dir / "validated.parquet").unlink(missing_ok=True)
    shutil.rmtree(work_dir / "unzipped", ignore_errors=True)
    return {"findings": result.findings, "stats": result.stats}


def _task_standardize(layer, run_id, repo_root, work_root):
    import sys
    from pathlib import Path

    sys.path.insert(0, repo_root)
    import geopandas as gpd
    from common.geo_config import load_layer_config
    from common.geo_standardize import safe_batch_id, standardize
    from ingestion.ingest_shapefile import work_dir_for

    config = load_layer_config(layer, Path(repo_root) / "config")
    work_dir = work_dir_for(work_root, layer, safe_batch_id(run_id))
    out = standardize(gpd.read_parquet(work_dir / "serving.parquet"), config, run_id)
    out.to_parquet(work_dir / "standardized.parquet")
    return {"rows": int(len(out))}


def _task_write_geoparquet(layer, run_id, repo_root, work_root):
    import sys

    sys.path.insert(0, repo_root)
    import geopandas as gpd
    from common.geo_standardize import safe_batch_id, write_geoparquet
    from common.openbao_client import OpenBaoClient
    from ingestion.ingest_shapefile import work_dir_for

    work_dir = work_dir_for(work_root, layer, safe_batch_id(run_id))
    minio = OpenBaoClient.from_env().read_secret("geo/minio")
    return {"geoparquet": write_geoparquet(gpd.read_parquet(work_dir / "standardized.parquet"), layer, run_id, minio)}


def _task_load_postgis(layer, run_id, repo_root, work_root):
    import sys
    from pathlib import Path

    sys.path.insert(0, repo_root)
    import geopandas as gpd
    import pandas as pd
    from common.geo_config import load_layer_config
    from common.geo_standardize import safe_batch_id
    from common.openbao_client import OpenBaoClient
    from common.postgis_loader import connect, load_layer
    from ingestion.ingest_shapefile import work_dir_for

    config = load_layer_config(layer, Path(repo_root) / "config")
    work_dir = work_dir_for(work_root, layer, safe_batch_id(run_id))
    gdf = gpd.read_parquet(work_dir / "standardized.parquet")
    vault = pd.read_parquet(work_dir / "vault.parquet")
    return load_layer(connect(OpenBaoClient.from_env()), gdf, config, vault, run_id)


def _task_publish(layer, run_id, repo_root, expected_rows):
    import sys
    from pathlib import Path

    sys.path.insert(0, repo_root)
    from common.geo_config import load_layer_config
    from common.geoserver_publisher import publish, verify_ows
    from common.openbao_client import OpenBaoClient

    config = load_layer_config(layer, Path(repo_root) / "config")
    client = OpenBaoClient.from_env()
    geoserver = client.read_secret("geo/geoserver")
    summary = publish(config, geoserver, client.read_secret("geo/postgis_geoserver"))

    checked = verify_ows(geoserver["url"], config["geoserver"]["workspace"], layer, config["bbox_ph"],
                         (geoserver["user"], geoserver["password"]))
    if checked["wms_status"] != 200 or checked["wfs_features"] != int(expected_rows):
        raise RuntimeError(f"GeoServer check failed: expected {expected_rows} features, got {checked}")
    return {"publish": summary, "wfs_features": checked["wfs_features"]}


# --- tasks that run in the main environment --------------------------------

def _make_start_job(layer):
    def _start(**context):
        from orchestration.monitoring import start_job_execution

        execution_id = start_job_execution(layer, context["run_id"], batch_id=context["run_id"])
        context["ti"].xcom_push(key="execution_id", value=execution_id)
    return _start


def _make_cleanup(layer):
    def _cleanup(**context):
        shutil.rmtree(Path(WORK_ROOT) / layer / _work_id(context["run_id"]), ignore_errors=True)
    return _cleanup


def _make_register_metadata(layer, agency):
    def _register(**context):
        from common.geo_config import load_layer_config
        from metadata.datahub_emit_geo import emit_geo_layer_metadata

        ti = context["ti"]
        validation = ti.xcom_pull(task_ids="validate_geometry")
        protection = ti.xcom_pull(task_ids="protect_pii")
        loaded = ti.xcom_pull(task_ids="load_postgis")
        emit_geo_layer_metadata(
            layer_config=load_layer_config(layer, REPO_ROOT / "config"),
            owner=f"{agency} GIS unit",
            classification="Geospatial Data",
            data_quality_index=validation["data_quality_index"],
            rows=loaded["rows"],
            batch_id=context["run_id"],
            findings=protection["findings"],
            rule_summary={"hard_pass": True, "soft_warnings": len(validation["soft_warnings"])},
        )
    return _register


def _make_finish_job(layer):
    def _finish(**context):
        from orchestration.monitoring import finish_job_execution, log_batch_result

        ti = context["ti"]
        validation = ti.xcom_pull(task_ids="validate_geometry")
        loaded = ti.xcom_pull(task_ids="load_postgis")
        execution_id = ti.xcom_pull(task_ids="start_job", key="execution_id")
        finish_job_execution(
            execution_id, status="success", rows_received=loaded["rows"],
            hard_failures=0, soft_warnings=len(validation["soft_warnings"]),
        )
        log_batch_result(layer, context["run_id"], "success", rows_received=loaded["rows"])
    return _finish


def build_geo_dag(layer: str, agency: str, source_folder: str, file_pattern: str,
                  schedule_interval: str, start_date: datetime) -> DAG:
    dag = DAG(
        dag_id=f"{layer}_geo_pipeline",
        default_args={**DEFAULT_ARGS, "on_failure_callback": geo_failure_callback},
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=False,
        max_active_runs=1,  # every load replaces the whole layer
        tags=["gates", "geo", layer],
    )

    def geo_task(task_id, func, retries=0, **op_kwargs):
        return ExternalPythonOperator(
            task_id=task_id,
            python=GEO_PYTHON,
            python_callable=func,
            op_kwargs={"layer": layer, "run_id": "{{ run_id }}", "repo_root": str(REPO_ROOT), **op_kwargs},
            expect_airflow=False,
            retries=retries,
        )

    with dag:
        start_job = PythonOperator(task_id="start_job", python_callable=_make_start_job(layer))

        ingest = geo_task("ingest_shapefile", _task_ingest, work_root=WORK_ROOT,
                          source_folder=source_folder, file_pattern=file_pattern)
        validate = geo_task("validate_geometry", _task_validate, work_root=WORK_ROOT,
                            source_path="{{ ti.xcom_pull(task_ids='ingest_shapefile')['source_path'] }}")
        protect = geo_task("protect_pii", _task_protect, retries=2, work_root=WORK_ROOT)
        standardize = geo_task("standardize", _task_standardize, work_root=WORK_ROOT)
        write_geoparquet = geo_task("write_geoparquet", _task_write_geoparquet, retries=2, work_root=WORK_ROOT)
        load_postgis = geo_task("load_postgis", _task_load_postgis, retries=2, work_root=WORK_ROOT)
        publish = geo_task("publish_geoserver", _task_publish, retries=2,
                           expected_rows="{{ ti.xcom_pull(task_ids='load_postgis')['rows'] }}")

        cleanup = PythonOperator(
            task_id="cleanup_work", python_callable=_make_cleanup(layer), trigger_rule=TriggerRule.ALL_DONE,
        )
        register = PythonOperator(task_id="register_metadata", python_callable=_make_register_metadata(layer, agency))
        finish = PythonOperator(task_id="finish_job", python_callable=_make_finish_job(layer))

        start_job >> ingest >> validate >> protect >> standardize >> write_geoparquet >> load_postgis >> publish
        publish >> cleanup
        publish >> register >> finish

    return dag


# DAG discovery: Airflow scans this module's global namespace for DAG objects.
for _entry in load_geo_registry(REGISTRY_PATH):
    globals()[f"{_entry['layer']}_geo_pipeline"] = build_geo_dag(
        layer=_entry["layer"],
        agency=_entry["agency"],
        source_folder=_entry["source_folder"],
        file_pattern=_entry["file_pattern"],
        schedule_interval=_entry["schedule_interval"],
        start_date=datetime.fromisoformat(str(_entry["start_date"])),
    )
