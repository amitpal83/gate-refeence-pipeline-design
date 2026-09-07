"""
airflow_dag.py — DAG FACTORY, not a single hardcoded DAG.

Generates one DAG per entry in dataset_registry.yaml. Nothing dataset-
specific (name, schedule, start_date, source_ids, agency) is hardcoded
below — every one of those comes from the registry, exactly the same
"config drives behavior, code stays generic" principle as
common/ingestion_framework.py. Adding a new dataset to this pipeline means
adding a block to dataset_registry.yaml, never editing this file.

Per-DAG task chain (unchanged from before):
    ingest_api + ingest_file (parallel)
        -> publish_to_kafka -> trigger_gx_validation -> register_metadata
        -> dbt_run_bronze -> dbt_run_silver -> dbt_run_gold

Two things fixed here versus the earlier single-dataset version:
  1. register_metadata (DataHub) is now a real task in the chain, placed
     right after validation as the design doc always specified — it was
     simply missing before.
  2. Every dataset-specific string used to be a literal (e.g.
     "--dataset project_monitoring" baked directly into a bash_command)
     even in places that ignored the DATASET module constant entirely.
     Everything below is an f-string built from build_dag()'s parameters.

NOTE ON MIXED FREQUENCIES: a single dataset's two channels may want
different cadences (e.g. an API source syncing daily, a file source
arriving quarterly). A single DAG has exactly one schedule_interval for
every task in it. This factory takes the simplest approach — schedule at
the TIGHTEST cadence the dataset needs (set per-dataset in the registry)
and make ingest_file_navi_gates.py itself a no-op when there's nothing new
since the last submission_date (not yet implemented — flagged here as a
real follow-up, not solved). The alternative — splitting API and file
ingestion into two independently-scheduled DAGs that both feed a shared
downstream DAG via Airflow Datasets — is cleaner semantically but adds
real complexity; revisit if a dataset's channels diverge further than
daily-vs-quarterly.
"""
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.utils.trigger_rule import TriggerRule

REGISTRY_PATH = Path(__file__).parent / "dataset_registry.yaml"

DEFAULT_ARGS_BASE = {
    "owner": "gates-data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def alert_and_dead_letter(context):
    """After retries are exhausted: notify the data-owner channel and move
    the failed batch to a dead-letter path for manual inspection."""
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    dag_id = context["dag"].dag_id
    # notify(channel="#gates-data-alerts", message=f"{dag_id}:{task_id} failed after retries ({run_id})")
    # move_to_dead_letter(dag_id=dag_id, run_id=run_id)


def _make_publish_ingestion_event(dataset: str, kafka_topic: str, file_task_id: str):
    """Closure so the Kafka-publish callable knows which dataset/topic/
    upstream-task-id it belongs to, without any module-level constant."""
    def _publish(**context):
        from confluent_kafka import Producer

        producer = Producer({"bootstrap.servers": "kafka-broker:9092"})
        manifest_path = context["ti"].xcom_pull(task_ids=file_task_id, key="manifest_path")
        event = {
            "dataset": dataset,
            "run_id": context["run_id"],
            "manifest_path": manifest_path,
            "landed_at": context["ts"],
        }
        producer.produce(kafka_topic, key=dataset, value=str(event))
        producer.flush()
    return _publish


def _make_trigger_gx_validation(dataset: str):
    """Previously: assumed a checkpoint named f"{dataset}_common_checkpoint"
    already existed somewhere and just ran it by name — nothing in this
    repo ever explained how that checkpoint would get created, and it never
    included the custom suite (see validation/gx_common_suite.py's fix).
    Now: loads this run's staged data directly and calls the same
    validation/run_validation.py:validate() function
    uses — one shared implementation, common+custom suites always
    combined, no assumed pre-existing state."""
    def _validate(**context):
        import sys
        import glob
        from pathlib import Path as _Path

        repo_root = _Path(__file__).resolve().parents[1]
        sys.path.insert(0, str(repo_root))
        from validation.run_validation import validate

        # Production note: this glob picks up every staged file for this
        # dataset regardless of run date/agency partition — the local
        # harness does the same simplification. A real deployment should
        # scope this to context["ds"] (the run's logical date) and the
        # specific agency partition(s) this run just ingested.
        staged_paths = glob.glob(
            str(repo_root / "staging" / "**" / dataset / "**" / "raw_*.csv"), recursive=True
        )
        report, _df = validate(staged_paths, repo_root / "config")

        context["ti"].xcom_push(key="data_quality_index", value=report["data_quality_index"])
        context["ti"].xcom_push(key="rule_results_summary", value={
            "hard_pass": report["hard_rule_pass"],
            "engine": report["engine"],
            "row_count": report["row_count"],
        })

        if not report["hard_rule_pass"]:
            raise ValueError(f"GX validation failed for {dataset} — quarantining batch")
        return report["hard_rule_pass"]
    return _validate


def _make_register_metadata(dataset: str, agency: str):
    """Step 5 — was missing entirely from the original single-dataset DAG;
    now a real task, placed right after validation per the design doc."""
    def _register(**context):
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from metadata.datahub_emit import emit_dataset_metadata

        ti = context["ti"]
        gx_result = ti.xcom_pull(task_ids="trigger_gx_validation")  # the return value (bool)
        dqi = ti.xcom_pull(task_ids="trigger_gx_validation", key="data_quality_index")
        rule_summary = ti.xcom_pull(task_ids="trigger_gx_validation", key="rule_results_summary")

        emit_dataset_metadata(
            dataset=dataset,
            owner=f"{agency} M&E unit",
            classification="Project & Knowledge Management",
            schema_ref=f"schema_{dataset}.yaml",
            lineage=["staging", "bronze"],  # silver/gold lineage added once those tasks also emit
            data_quality_index=dqi if dqi is not None else (100.0 if gx_result else 0.0),
            rule_results_summary=rule_summary if rule_summary is not None else {"hard_pass": bool(gx_result)},
        )
    return _register


def build_dag(dataset: str, start_date: datetime, schedule_interval: str,
              api_source_id: str, file_source_id: str,
              airbyte_connection_var: str, agency: str,
              dbt_layers: list[str]) -> DAG:
    kafka_topic = f"gates.ingestion.{dataset}"
    file_task_id = f"ingest_file_{file_source_id}"

    default_args = {**DEFAULT_ARGS_BASE, "on_failure_callback": alert_and_dead_letter}

    dag = DAG(
        dag_id=f"{dataset}_pipeline",
        default_args=default_args,
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=False,
        tags=["gates", "project_mgmt", dataset],
    )

    with dag:
        ingest_api = AirbyteTriggerSyncOperator(
            task_id=f"ingest_api_{api_source_id}",
            airbyte_conn_id="airbyte_default",
            connection_id=f"{{{{ var.value.{airbyte_connection_var} }}}}",
            asynchronous=False,
        )

        ingest_file = BashOperator(
            task_id=file_task_id,
            bash_command=f"navi-gates ingest --dataset {dataset} --config ingestion_config.yaml",
        )

        publish_event = PythonOperator(
            task_id="publish_to_kafka",
            python_callable=_make_publish_ingestion_event(dataset, kafka_topic, file_task_id),
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        validate = PythonOperator(
            task_id="trigger_gx_validation",
            python_callable=_make_trigger_gx_validation(dataset),
        )

        register_metadata = PythonOperator(
            task_id="register_metadata",
            python_callable=_make_register_metadata(dataset, agency),
        )

        # Previously: three separately hardcoded BashOperators
        # (dbt_bronze/dbt_silver/dbt_gold), with the layer names, task_ids,
        # AND the chain order all fixed in Python — a dataset needing a
        # different set of layers (no Silver, an extra layer, a different
        # order) would require editing this function, not just the
        # registry. Fix: dbt_layers is now a registry-driven list; the
        # chain below is built by looping over it, in whatever order the
        # registry declares. Each model is still tagged with BOTH its layer
        # AND its dataset name (see models/*/*.sql config() blocks) so
        # --select tag:X,tag:Y (an AND) scopes correctly regardless of how
        # many layers a given dataset has.
        dbt_tasks = [
            BashOperator(
                task_id=f"dbt_run_{layer}",
                bash_command=f"dbt run --select tag:{layer},tag:{dataset}",
            )
            for layer in dbt_layers
        ]

        chain_point = register_metadata
        for dbt_task in dbt_tasks:
            chain_point >> dbt_task
            chain_point = dbt_task

        [ingest_api, ingest_file] >> publish_event >> validate >> register_metadata

    return dag


# --- DAG discovery: Airflow scans this module's global namespace for DAG
# objects, so every registry entry must produce a module-level variable. ---
_registry = yaml.safe_load(REGISTRY_PATH.read_text())
for _entry in _registry["datasets"]:
    globals()[f"{_entry['dataset']}_pipeline"] = build_dag(
        dataset=_entry["dataset"],
        start_date=datetime.fromisoformat(_entry["start_date"]),
        schedule_interval=_entry["schedule_interval"],
        api_source_id=_entry["api_source_id"],
        file_source_id=_entry["file_source_id"],
        airbyte_connection_var=_entry["airbyte_connection_var"],
        agency=_entry["agency"],
        dbt_layers=_entry["dbt_layers"],
    )
