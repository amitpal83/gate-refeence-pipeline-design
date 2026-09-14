"""Structured job logging used by Airflow/CloudWatch, plus job_execution
tracking in the config database (see database/init_config_db.sql's
job_execution table) — the queryable run-history view for the demo's
logging/monitoring/failure-handling story."""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("gates.pipeline")


def log_event(event: str, **fields: Any) -> dict[str, Any]:
    payload = {
        "event": event,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **fields,
    }
    logger.info(json.dumps(payload, default=str, sort_keys=True))
    return payload


def failure_callback(context: dict[str, Any]) -> None:
    task = context["task_instance"]
    # batch_id equals the DAG run_id by convention everywhere a batch_id is
    # generated in this pipeline (see BatchIngestor.ingest_cdc_records /
    # ingest_db_tables call sites in airflow_dag.py), so run_id doubles as
    # the batch reference here without depending on any one dataset's
    # specific task_id existing.
    run_id = context.get("run_id")
    log_event(
        "task_failed",
        dag_id=context.get("dag").dag_id if context.get("dag") else None,
        task_id=task.task_id,
        run_id=run_id,
        try_number=task.try_number,
        exception=str(context.get("exception")),
        batch_id=run_id,
    )
    execution_id = task.xcom_pull(task_ids="start_job", key="execution_id")
    finish_job_execution(execution_id, status="failed", error_message=str(context.get("exception")))


def log_batch_result(dataset: str, batch_id: str, status: str, **metrics: Any) -> None:
    log_event("batch_completed", dataset=dataset, batch_id=batch_id, status=status, **metrics)


def _config_db_engine():
    from sqlalchemy import create_engine

    return create_engine(os.environ["GATES_CONFIG_DB_URL"], pool_pre_ping=True)


def start_job_execution(dataset: str, dag_run_id: str, batch_id: str | None = None) -> str | None:
    """Insert a 'running' job_execution row and return its execution_id.

    Monitoring must never fail the pipeline: any error here (config DB
    unreachable, table missing) is logged and swallowed, returning None —
    finish_job_execution() then no-ops for that run instead of raising.
    """
    try:
        from sqlalchemy import text

        execution_id = str(uuid.uuid4())
        with _config_db_engine().begin() as connection:
            connection.execute(
                text("""
                    INSERT INTO job_execution (execution_id, dataset_name, dag_run_id, batch_id, status)
                    VALUES (:execution_id, :dataset_name, :dag_run_id, :batch_id, 'running')
                """),
                {"execution_id": execution_id, "dataset_name": dataset,
                 "dag_run_id": dag_run_id, "batch_id": batch_id},
            )
        return execution_id
    except Exception:
        logger.exception("start_job_execution failed for dataset=%s — continuing without job_execution tracking", dataset)
        return None


def finish_job_execution(execution_id: str | None, status: str, **metrics: Any) -> None:
    """Update a job_execution row with its final status/metrics. No-ops
    quietly if execution_id is None (start_job_execution already failed) or
    the config database is unreachable."""
    if execution_id is None:
        return
    try:
        from sqlalchemy import text

        with _config_db_engine().begin() as connection:
            connection.execute(
                text("""
                    UPDATE job_execution
                    SET status = :status,
                        rows_received = :rows_received,
                        rows_bronze = :rows_bronze,
                        rows_silver = :rows_silver,
                        rows_gold = :rows_gold,
                        hard_failures = :hard_failures,
                        soft_warnings = :soft_warnings,
                        error_message = :error_message,
                        finished_at = now()
                    WHERE execution_id = :execution_id
                """),
                {
                    "execution_id": execution_id,
                    "status": status,
                    "rows_received": metrics.get("rows_received", 0),
                    "rows_bronze": metrics.get("rows_bronze", 0),
                    "rows_silver": metrics.get("rows_silver", 0),
                    "rows_gold": metrics.get("rows_gold", 0),
                    "hard_failures": metrics.get("hard_failures", 0),
                    "soft_warnings": metrics.get("soft_warnings", 0),
                    "error_message": metrics.get("error_message"),
                },
            )
    except Exception:
        logger.exception("finish_job_execution failed for execution_id=%s", execution_id)
