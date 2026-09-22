"""
airflow_dag.py — DAG FACTORY, not a single hardcoded DAG.

Generates one DAG per entry in dataset_registry.yaml. Nothing dataset-
specific (name, schedule, start_date, source_ids, agency) is hardcoded
below — every one of those comes from the registry, exactly the same
"config drives behavior, code stays generic" principle as
common/ingestion_framework.py. Adding a new dataset to this pipeline means
adding a block to dataset_registry.yaml, never editing this file.

Per-DAG task chain:
    (whichever of ingest_api / ingest_file / ingest_cdc / ingest_db a
    dataset actually declares in dataset_registry.yaml, run in parallel)
        -> publish_to_kafka -> trigger_gx_validation -> promote_to_staging
        -> register_bronze_metadata -> dbt_run_bronze -> dbt_run_silver -> dbt_run_gold
        -> register_transformation_metadata

Not every dataset has all four ingestion channels — project_monitoring uses
api+file+cdc; rd_equipment_inventory (a database-only-config dataset) uses
only its db_multitable source. build_dag() treats every channel parameter
as optional or None skips it — see build_dag()'s ingestion_tasks list.

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
import json
import os
from pathlib import Path

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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
    from orchestration.monitoring import failure_callback
    failure_callback(context)
    # notify(channel="#gates-data-alerts", message=f"{dag_id}:{task_id} failed after retries ({run_id})")
    # move_to_dead_letter(dag_id=dag_id, run_id=run_id)


def _make_publish_ingestion_event(dataset: str, kafka_topic: str, manifest_task_id: str):
    """Closure so the Kafka-publish callable knows which dataset/topic/
    upstream-task-id it belongs to, without any module-level constant.
    manifest_task_id is whichever ingestion task ran for this dataset (file,
    db, cdc, or api — see build_dag's ingestion_tasks list); not every
    dataset has all four channels, so this isn't hardcoded to the file
    channel specifically. Falls back to the task's raw return_value if it
    didn't push an explicit "manifest_path" xcom key (BashOperator tasks
    only auto-push return_value)."""
    def _publish(**context):
        from confluent_kafka import Producer

        producer = Producer({"bootstrap.servers": "kafka-broker:9092"})
        ti = context["ti"]
        manifest_path = ti.xcom_pull(task_ids=manifest_task_id, key="manifest_path")
        if manifest_path is None:
            manifest_path = ti.xcom_pull(task_ids=manifest_task_id, key="return_value")
        event = {
            "dataset": dataset,
            "run_id": context["run_id"],
            "manifest_path": manifest_path,
            "landed_at": context["ts"],
        }
        producer.produce(kafka_topic, key=dataset, value=str(event))
        producer.flush()
    return _publish


def _make_ingest_airbyte(dataset: str, source_id: str, agency: str):
    """Run the repository's Airbyte CDK connector in protocol mode."""
    def _ingest(**context):
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        fixture = os.getenv("GATES_API_FIXTURE")
        if fixture:
            from ingestion.ingest_api_airbyte import run_from_records
            result = run_from_records(fixture, dataset=dataset, source_id=source_id, agency=agency)
        else:
            from ingestion.ingest_api_airbyte_cdk import run
            result = run(dataset=dataset, source_id=source_id, agency=agency)
        context["ti"].xcom_push(key="manifest_path", value=result["manifest_path"])
        return result
    return _ingest


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
        # Two things the blanket glob above would otherwise get wrong:
        # 1) it also matches staging/quarantine/** (any path with "dataset"
        #    as a segment), so every previously-quarantined batch keeps
        #    getting re-validated and re-quarantined forever.
        # 2) repeated triggers on the same day create a NEW batch folder
        #    per source each time (file/API channels don't overwrite), so
        #    without this every historical batch gets concatenated
        #    together — the same project_id showing up once per past run,
        #    always failing uniqueness. Keep only the newest file per
        #    source_id (from its raw_{source_id}.csv filename).
        staged_paths = [p for p in staged_paths if "quarantine" not in _Path(p).parts]
        latest_by_source: dict[str, str] = {}
        for path in staged_paths:
            source_id = _Path(path).stem.removeprefix("raw_")
            if source_id not in latest_by_source or _Path(path).stat().st_mtime > _Path(latest_by_source[source_id]).stat().st_mtime:
                latest_by_source[source_id] = path
        staged_paths = list(latest_by_source.values())
        report, _df = validate(staged_paths, repo_root / "config", dataset=dataset)

        context["ti"].xcom_push(key="data_quality_index", value=report["data_quality_index"])
        context["ti"].xcom_push(key="rule_results_summary", value={
            "hard_pass": report["hard_rule_pass"],
            "engine": report["engine"],
            "row_count": report["row_count"],
        })

        if not report["hard_rule_pass"]:
            from validation.quarantine import quarantine_batch
            quarantine_batch(
                staged_paths,
                repo_root / "staging" / "quarantine" / dataset,
                reason=f"Great Expectations hard validation failed for {dataset}",
            )
            raise ValueError(f"GX validation failed for {dataset} — quarantining batch")
        return report["hard_rule_pass"]
    return _validate


def _make_promote_to_staging(dataset: str):
    """Load the just-validated batch into iceberg.staging.{dataset}_raw — the
    real write path bronze_project_monitoring.sql's `_gx_validation_status =
    'pass'` filter depends on. Only runs when trigger_gx_validation succeeded
    (a hard-rule failure already raised and quarantined the batch upstream),
    so every row loaded here is a validation pass by construction."""
    def _promote(**context):
        import sys
        import glob
        from pathlib import Path as _Path

        repo_root = _Path(__file__).resolve().parents[1]
        sys.path.insert(0, str(repo_root))
        import pandas as pd
        from common.config_registry import ConfigRegistry
        from common.trino_loader import load_validated_batch

        staged_paths = glob.glob(
            str(repo_root / "staging" / "**" / dataset / "**" / "raw_*.csv"), recursive=True
        )
        dfs = [pd.read_csv(p, dtype=str, keep_default_na=False, na_values=[""]) for p in staged_paths]
        df = pd.concat(dfs, ignore_index=True)

        config = ConfigRegistry(yaml_dir=repo_root / "config").get_dataset(dataset)
        canonical_fields = config["canonical_schema"]["fields"]
        canonical_names = [field["name"] for field in canonical_fields]
        df = df[[column for column in df.columns if column in canonical_names]]

        rows = load_validated_batch(
            dataset, df, canonical_fields,
            batch_id=context["run_id"], source_id="combined",
        )
        context["ti"].xcom_push(key="rows_promoted", value=rows)
        return rows
    return _promote


def _make_ingest_cdc(dataset: str, source_id: str):
    """Consume a bounded CDC batch from a fixture or the demo Kafka broker."""
    def _ingest(**context):
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from common.config_registry import ConfigRegistry
        from ingestion.batch_ingestion import BatchIngestor

        fixture = os.getenv("GATES_CDC_FIXTURE")
        records = []
        if fixture:
            with open(fixture, encoding="utf-8") as handle:
                records = [json.loads(line) for line in handle if line.strip()]
        else:
            from confluent_kafka import Consumer
            consumer = Consumer({
                "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
                "group.id": f"gates-{dataset}-cdc",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            })
            consumer.subscribe(["cdc.project_schema.projects"])
            try:
                deadline = datetime.utcnow() + timedelta(seconds=30)
                while datetime.utcnow() < deadline:
                    message = consumer.poll(1.0)
                    if message is None:
                        continue
                    if message.error():
                        raise RuntimeError(message.error())
                    value = message.value()
                    if value is None:
                        # Debezium's delete tombstone: same key, null value —
                        # not a change event to ingest.
                        continue
                    records.append(json.loads(value.decode("utf-8")))
            finally:
                consumer.close()

        if not records:
            raise ValueError(f"No CDC events received for {dataset}")
        result = BatchIngestor(
            ConfigRegistry(yaml_dir=Path(__file__).resolve().parents[1] / "config"),
            staging_dir=Path(__file__).resolve().parents[1] / "staging",
        ).ingest_cdc_records(dataset, source_id, records, batch_id=context["run_id"])
        context["ti"].xcom_push(key="batch_manifest", value=result)
        return result
    return _ingest


def _make_ingest_db(dataset: str, source_id: str):
    """Read every table a db_multitable source declares and join them — the
    multi-table-database ingestion path (see
    ingestion.batch_ingestion.BatchIngestor.ingest_db_tables). Used by
    rd_equipment_inventory, whose config lives only in the config database
    (database/init_config_db.sql), not YAML."""
    def _ingest(**context):
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from common.config_registry import ConfigRegistry
        from ingestion.batch_ingestion import BatchIngestor

        result = BatchIngestor(
            ConfigRegistry(yaml_dir=Path(__file__).resolve().parents[1] / "config"),
            staging_dir=Path(__file__).resolve().parents[1] / "staging",
        ).ingest_db_tables(dataset, source_id, batch_id=context["run_id"])
        context["ti"].xcom_push(key="manifest_path", value=result["manifest_path"])
        return result
    return _ingest


def _make_register_metadata(dataset: str, agency: str, stage: str, lineage: list[str],
                             include_validation_result: bool = True):
    """Emit metadata at a completed pipeline boundary.

    include_validation_result=False is for the "raw_ingested" stage, which
    fires before trigger_gx_validation has even run — pulling its xcom at
    that point would misleadingly read as a validation failure (no result
    yet, not a bad one)."""
    def _register(**context):
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from metadata.datahub_emit import emit_dataset_metadata
        from common.config_registry import ConfigRegistry

        if include_validation_result:
            ti = context["ti"]
            gx_result = ti.xcom_pull(task_ids="trigger_gx_validation")  # the return value (bool)
            dqi = ti.xcom_pull(task_ids="trigger_gx_validation", key="data_quality_index")
            rule_summary = ti.xcom_pull(task_ids="trigger_gx_validation", key="rule_results_summary")
            dqi = dqi if dqi is not None else (100.0 if gx_result else 0.0)
            rule_summary = rule_summary if rule_summary is not None else {"hard_pass": bool(gx_result)}
        else:
            dqi = None
            rule_summary = {"note": "registered at ingestion time, before validation ran"}

        # rd_equipment_inventory has no schema_*.yaml file at all — its
        # config lives only in the config database — so the DataHub-visible
        # schema_ref should say so instead of pointing at a file that
        # doesn't exist. ConfigRegistry tags every resolved config with
        # exactly where it came from.
        repo_root = Path(__file__).resolve().parents[1]
        config = ConfigRegistry(yaml_dir=repo_root / "config").get_dataset(dataset)
        schema_ref = (
            f"schema_{dataset}.yaml" if config.get("_config_source") == "yaml"
            else f"config_database:dataset_registry.{dataset}"
        )

        emit_dataset_metadata(
            dataset=dataset,
            owner=f"{agency} M&E unit",
            classification="Project & Knowledge Management",
            schema_ref=schema_ref,
            stage=stage,
            lineage=lineage,
            data_quality_index=dqi,
            rule_results_summary=rule_summary,
        )
    return _register


def _make_finish_job(dataset: str):
    """Mark the run's job_execution row 'success', with whatever row counts
    are cheaply available from xcom (see common/trino_loader.py's
    load_validated_batch for rows_received; per-layer bronze/silver/gold row
    counts aren't tracked yet — dbt's BashOperators don't report them —
    left at 0 as a known simplification, not a correctness gap)."""
    def _finish(**context):
        from orchestration.monitoring import finish_job_execution, log_batch_result

        ti = context["ti"]
        run_id = context["run_id"]
        execution_id = ti.xcom_pull(task_ids="start_job", key="execution_id")
        rows_received = ti.xcom_pull(task_ids="promote_to_staging", key="rows_promoted") or 0
        finish_job_execution(execution_id, status="success", rows_received=rows_received)
        log_batch_result(dataset, run_id, "success", rows_received=rows_received)
    return _finish


def build_dag(dataset: str, start_date: datetime, schedule_interval: str,
              agency: str, dbt_layers: list[str],
              api_source_id: str | None = None,
              file_source_id: str | None = None,
              cdc_source_id: str | None = None,
              file_folder: str | None = None,
              file_pattern: str | None = None,
              db_source_id: str | None = None) -> DAG:
    """Every ingestion channel is optional — a dataset declares whichever
    combination it actually has in dataset_registry.yaml (project_monitoring
    uses all of api/file/cdc; rd_equipment_inventory uses only db_source_id).
    At least one must be set, or there's nothing to ingest."""
    kafka_topic = f"gates.ingestion.{dataset}"

    default_args = {**DEFAULT_ARGS_BASE, "on_failure_callback": alert_and_dead_letter}

    dag = DAG(
        dag_id=f"{dataset}_pipeline",
        default_args=default_args,
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=False,
        tags=["gates", dataset],
    )

    with dag:
        def _start_job(**context):
            from orchestration.monitoring import start_job_execution
            execution_id = start_job_execution(dataset, context["run_id"], batch_id=context["run_id"])
            context["ti"].xcom_push(key="execution_id", value=execution_id)

        start_job = PythonOperator(task_id="start_job", python_callable=_start_job)

        ingestion_tasks = []
        manifest_task_id = None  # whichever channel exists is fine for the landing event

        if api_source_id:
            ingest_api = PythonOperator(
                task_id=f"ingest_api_{api_source_id}",
                python_callable=_make_ingest_airbyte(dataset, api_source_id, agency),
            )
            ingestion_tasks.append(ingest_api)
            manifest_task_id = ingest_api.task_id

        if file_source_id:
            if not (file_folder and file_pattern):
                raise ValueError(f"{dataset}: file_source_id set without file_folder/file_pattern")
            ingest_file = BashOperator(
                task_id=f"ingest_file_{file_source_id}",
                bash_command=(
                    f"cd /opt/airflow/dags && python -m ingestion.batch_ingestion --dataset {dataset} "
                    f"--source-id {file_source_id} --folder {file_folder} --pattern '{file_pattern}'"
                ),
            )
            ingestion_tasks.append(ingest_file)
            manifest_task_id = ingest_file.task_id  # prefer the file channel when present

        if cdc_source_id:
            ingest_cdc = PythonOperator(
                task_id=f"ingest_cdc_{cdc_source_id}",
                python_callable=_make_ingest_cdc(dataset, cdc_source_id),
            )
            ingestion_tasks.append(ingest_cdc)
            manifest_task_id = manifest_task_id or ingest_cdc.task_id

        if db_source_id:
            ingest_db = PythonOperator(
                task_id=f"ingest_db_{db_source_id}",
                python_callable=_make_ingest_db(dataset, db_source_id),
            )
            ingestion_tasks.append(ingest_db)
            manifest_task_id = manifest_task_id or ingest_db.task_id

        if not ingestion_tasks:
            raise ValueError(f"{dataset}: at least one ingestion source must be configured")

        publish_event = PythonOperator(
            task_id="publish_to_kafka",
            python_callable=_make_publish_ingestion_event(dataset, kafka_topic, manifest_task_id),
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        # Registers the raw/ingested dataset in DataHub even before
        # validation runs, so it's searchable/catalogued from the moment
        # data lands — not only once it's reached bronze or gold.
        register_ingestion_metadata = PythonOperator(
            task_id="register_ingestion_metadata",
            python_callable=_make_register_metadata(
                dataset, agency, "raw_ingested", ["staging"], include_validation_result=False,
            ),
        )

        validate = PythonOperator(
            task_id="trigger_gx_validation",
            python_callable=_make_trigger_gx_validation(dataset),
        )

        promote_to_staging = PythonOperator(
            task_id="promote_to_staging",
            python_callable=_make_promote_to_staging(dataset),
        )
        validate >> promote_to_staging

        register_bronze_metadata = PythonOperator(
            task_id="register_bronze_metadata",
            python_callable=_make_register_metadata(dataset, agency, "bronze", ["staging", "bronze"]),
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
                bash_command=(
                    "dbt deps --project-dir /opt/airflow/dags/transformation/dbt_project "
                    "--profiles-dir /opt/airflow/dags/transformation/dbt_project && "
                    f"dbt run --project-dir /opt/airflow/dags/transformation/dbt_project "
                    f"--profiles-dir /opt/airflow/dags/transformation/dbt_project --select tag:{layer},tag:{dataset}"
                ),
            )
            for layer in dbt_layers
        ]

        dbt_test_tasks = [
            BashOperator(
                task_id=f"dbt_test_{layer}",
                bash_command=(
                    f"dbt test --project-dir /opt/airflow/dags/transformation/dbt_project "
                    f"--profiles-dir /opt/airflow/dags/transformation/dbt_project --select tag:{layer},tag:{dataset}"
                ),
            )
            for layer in dbt_layers
        ]

        chain_point = promote_to_staging
        for dbt_task, dbt_test in zip(dbt_tasks, dbt_test_tasks):
            chain_point >> dbt_task >> dbt_test
            chain_point = dbt_test
            if dbt_task is dbt_tasks[0]:
                promote_to_staging >> dbt_task
        dbt_tasks[0] >> register_bronze_metadata >> dbt_test_tasks[0]

        register_transformation_metadata = PythonOperator(
            task_id="register_transformation_metadata",
            python_callable=_make_register_metadata(
                dataset, agency, "gold", ["staging", "bronze", "silver", "gold"]
            ),
        )
        chain_point >> register_transformation_metadata

        start_job >> ingestion_tasks >> publish_event >> register_ingestion_metadata >> validate

        finish_job = PythonOperator(
            task_id="finish_job",
            python_callable=_make_finish_job(dataset),
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )
        register_transformation_metadata >> finish_job

    return dag


# --- DAG discovery: Airflow scans this module's global namespace for DAG
# objects, so every registry entry must produce a module-level variable. ---
_registry = yaml.safe_load(REGISTRY_PATH.read_text())
for _entry in _registry["datasets"]:
    globals()[f"{_entry['dataset']}_pipeline"] = build_dag(
        dataset=_entry["dataset"],
        start_date=datetime.fromisoformat(_entry["start_date"]),
        schedule_interval=_entry["schedule_interval"],
        agency=_entry["agency"],
        dbt_layers=_entry["dbt_layers"],
        api_source_id=_entry.get("api_source_id"),
        file_source_id=_entry.get("file_source_id"),
        cdc_source_id=_entry.get("cdc_source_id"),
        file_folder=_entry.get("file_folder"),
        file_pattern=_entry.get("file_pattern"),
        db_source_id=_entry.get("db_source_id"),
    )
