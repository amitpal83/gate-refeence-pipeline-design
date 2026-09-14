"""Load GX-validated ingestion batches into a real Iceberg staging table via Trino.

Staging lives in the same `iceberg`/Nessie catalog dbt already targets for
bronze/silver/gold (see transformation/dbt_project/profiles.yml), in its own
`staging` schema — no second catalog or Hive Metastore is needed. This is
what makes "store data in MinIO + Iceberg" and "hard checks gate what
reaches bronze" both literally true: only a batch that already passed
validation/run_validation.py's Great Expectations suite is ever handed to
load_validated_batch(), and bronze_project_monitoring.sql's
`where _gx_validation_status = 'pass'` filters a column this module actually
populates (previously nothing wrote that column at all).
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd

TECHNICAL_COLUMNS = ("_gx_validation_status", "_source_id", "_batch_id", "_ingested_at")
INSERT_CHUNK_SIZE = 500


def _connect():
    import trino

    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "gates_svc_loader"),
        catalog=os.getenv("TRINO_CATALOG", "iceberg"),
        schema="staging",
        http_scheme="http",
    )


def _sql_literal(value: Any) -> str:
    if value is None or value == "" or (isinstance(value, float) and pd.isna(value)):
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def ensure_staging_table(dataset: str, canonical_fields: list[dict]) -> str:
    """Idempotently create iceberg.staging.{dataset}_raw.

    Every column is VARCHAR — Bronze's own CAST(...) does the typing,
    matching what ingestion actually writes (loosely-typed CSV output).
    """
    table = f"{dataset}_raw"
    columns = [field["name"] for field in canonical_fields] + list(TECHNICAL_COLUMNS)
    column_defs = ",\n        ".join(f'"{name}" VARCHAR' for name in columns)

    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.staging")
        cur.fetchall()
        cur.execute(
            f'CREATE TABLE IF NOT EXISTS iceberg.staging."{table}" (\n'
            f"        {column_defs}\n"
            f"    ) WITH (format = 'PARQUET')"
        )
        cur.fetchall()
    finally:
        conn.close()
    return f'iceberg.staging."{table}"'


def load_validated_batch(dataset: str, df: pd.DataFrame, canonical_fields: list[dict],
                          batch_id: str, source_id: str) -> int:
    """Stamp technical columns and insert every row of an already-GX-passed
    batch into its Iceberg staging table.

    Callers must only invoke this after validation/run_validation.py's
    validate() reports hard_rule_pass=True — validation/quarantine.py
    handles the failing case instead, so every row that reaches here is, by
    construction, a validation pass.
    """
    table_fqn = ensure_staging_table(dataset, canonical_fields)
    if df.empty:
        return 0

    stamped = df.copy()
    stamped["_gx_validation_status"] = "pass"
    stamped["_source_id"] = source_id
    stamped["_batch_id"] = batch_id
    stamped["_ingested_at"] = datetime.now(timezone.utc).isoformat()

    columns = [field["name"] for field in canonical_fields] + list(TECHNICAL_COLUMNS)
    stamped = stamped.reindex(columns=columns)

    conn = _connect()
    try:
        cur = conn.cursor()
        column_list = ", ".join(f'"{c}"' for c in columns)
        chunk: list[tuple] = []
        inserted = 0
        for row in stamped.itertuples(index=False, name=None):
            chunk.append(row)
            if len(chunk) == INSERT_CHUNK_SIZE:
                inserted += _insert_chunk(cur, table_fqn, column_list, chunk)
                chunk = []
        if chunk:
            inserted += _insert_chunk(cur, table_fqn, column_list, chunk)
        return inserted
    finally:
        conn.close()


def _insert_chunk(cur, table_fqn: str, column_list: str, rows: list[tuple]) -> int:
    values_sql = ",\n".join(
        "(" + ", ".join(_sql_literal(value) for value in row) + ")" for row in rows
    )
    cur.execute(f"INSERT INTO {table_fqn} ({column_list}) VALUES {values_sql}")
    cur.fetchall()
    return len(rows)
