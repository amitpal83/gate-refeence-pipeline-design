"""Source-neutral batch ingestion for folder files and Kafka CDC records."""
from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from common.config_registry import ConfigRegistry
from common.ingestion_framework import (
    apply_aliasing,
    get_canonical_fields,
    get_source_aliases,
    get_source_meta,
    identity_pass_through,
    load_aliasing,
    load_contract,
    write_staged,
)


class BatchIngestor:
    """Create one manifest for all source objects in a pipeline batch."""

    def __init__(self, registry: ConfigRegistry, staging_dir: str | Path = "staging") -> None:
        self.registry = registry
        self.staging_dir = Path(staging_dir)

    def ingest_folder(
        self,
        dataset: str,
        source_id: str,
        folder: str | Path,
        pattern: str = "*.csv",
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        config = self.registry.get_dataset(dataset)
        source = get_source_meta(config, source_id)
        aliases = self._aliases(config, source_id)
        files = sorted(Path(folder).glob(pattern))
        if not files:
            raise FileNotFoundError(f"No files matched {Path(folder) / pattern}")

        batch_id = batch_id or self._batch_id(dataset)
        staged = []
        for file_path in files:
            raw = self._read_file(file_path)
            canonical = self._canonicalize(raw, config, aliases, source)
            staged.append(write_staged(
                canonical,
                self.staging_dir,
                dataset,
                agency=source.get("agency", "unknown"),
                run_date=date.today().isoformat(),
                source_id=source_id,
                batch_id=batch_id,
                source_object=str(file_path),
            ))
        return self._write_batch_manifest(dataset, batch_id, source_id, staged)

    def ingest_cdc_records(
        self,
        dataset: str,
        source_id: str,
        records: Iterable[dict[str, Any]],
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        config = self.registry.get_dataset(dataset)
        source = get_source_meta(config, source_id)
        payload = list(records)
        if not payload:
            raise ValueError("Kafka CDC batch contained no records")

        rows = []
        for event in payload:
            operation = event.get("op", event.get("operation", "u"))
            if operation not in {"c", "u", "d", "r", "insert", "update", "delete", "read"}:
                raise ValueError(f"Unsupported CDC operation: {operation}")
            # Debezium delete events carry the old row in "before" with
            # "after": null — fall back to it so a delete doesn't blow up
            # with every canonical field missing.
            row = dict(event.get("after") or event.get("before") or event.get("data") or event)
            row["_cdc_operation"] = operation
            row["_source_table"] = event.get("source", {}).get("table", event.get("table"))
            row["_operation_timestamp"] = event.get("ts_ms", event.get("timestamp"))
            rows.append(row)

        canonical = self._canonicalize(pd.DataFrame(rows), config, self._aliases(config, source_id), source, keep_metadata=True)
        batch_id = batch_id or self._batch_id(dataset)
        result = write_staged(
            canonical,
            self.staging_dir,
            dataset,
            agency=source.get("agency", "unknown"),
            run_date=date.today().isoformat(),
            source_id=source_id,
            batch_id=batch_id,
            source_object=f"kafka:{source.get('config', {}).get('topic_pattern', source_id)}",
        )
        return self._write_batch_manifest(dataset, batch_id, source_id, [result])

    def ingest_db_tables(
        self,
        dataset: str,
        source_id: str,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        """Read every table a db_multitable source declares straight out of
        the operational database, left-join them on the source's join_key
        into one wide raw frame, and canonicalize — the multi-table-database
        counterpart to ingest_folder's multi-file-folder case. Reuses
        apply_aliasing() unchanged: once the tables are merged, it doesn't
        matter which original table a raw column came from."""
        import os

        from sqlalchemy import create_engine

        config = self.registry.get_dataset(dataset)
        source = get_source_meta(config, source_id)
        source_config = source.get("config") or {}
        tables = source_config["tables"]
        join_key = source_config["join_key"]
        schema_name = source_config.get("schema", "public")

        engine = create_engine(os.environ[source_config.get("connection_env", "GATES_SOURCE_DB_URL")])
        frames = [pd.read_sql(f"SELECT * FROM {schema_name}.{table}", engine) for table in tables]
        raw = frames[0]
        for frame in frames[1:]:
            raw = raw.merge(frame, on=join_key, how="left")

        canonical = self._canonicalize(raw, config, self._aliases(config, source_id), source)
        batch_id = batch_id or self._batch_id(dataset)
        result = write_staged(
            canonical,
            self.staging_dir,
            dataset,
            agency=source_config.get("agency", "unknown"),
            run_date=date.today().isoformat(),
            source_id=source_id,
            batch_id=batch_id,
            source_object=f"db:{schema_name}.{'+'.join(tables)}",
        )
        return self._write_batch_manifest(dataset, batch_id, source_id, [result])

    def _canonicalize(self, raw: pd.DataFrame, config: dict[str, Any], aliases: list[dict[str, Any]], source: dict[str, Any], keep_metadata: bool = False) -> pd.DataFrame:
        canonical = apply_aliasing(raw, aliases) if aliases else identity_pass_through(raw, get_canonical_fields(config))
        canonical_names = {field["name"] for field in get_canonical_fields(config)}
        # YAML source blocks carry agency/region as top-level keys; a
        # database-backed source (see ConfigRegistry._load_from_db) instead
        # nests them under "config" jsonb — check both. Only ever stamp a
        # column that's actually part of THIS dataset's canonical schema, so
        # a dataset with no "region" field (e.g. rd_equipment_inventory)
        # doesn't pick up an unexpected extra column.
        source_config = source.get("config") or {}
        if "agency" in canonical_names:
            canonical["agency"] = source.get("agency") or source_config.get("agency") or canonical.get("agency", "unknown")
        if "region" in canonical_names:
            canonical["region"] = source.get("region") or source_config.get("region") or canonical.get("region", "unknown")
        if keep_metadata:
            for column in ("_cdc_operation", "_source_table", "_operation_timestamp"):
                if column in raw:
                    canonical[column] = raw[column].values
            # A CDC topic is a stream of changes over time, not a
            # current-state snapshot — re-consuming it (this ingestion
            # re-reads from the earliest offset every run, for demo
            # repeatability) yields one row per historical event per key,
            # not one row per entity. Collapse to the latest event per
            # unique key (by _operation_timestamp) so this matches what the
            # file/API channels already produce, and so it doesn't fail the
            # dataset's hard uniqueness check downstream.
            unique_fields = config.get("canonical_schema", {}).get("unique_fields", [])
            if unique_fields and "_operation_timestamp" in canonical.columns:
                canonical = (
                    canonical.sort_values("_operation_timestamp")
                    .drop_duplicates(subset=unique_fields, keep="last")
                    .reset_index(drop=True)
                )
        for field in get_canonical_fields(config):
            if field.get("dtype") == "date" and field["name"] in canonical.columns:
                canonical[field["name"]] = self._decode_debezium_dates(canonical[field["name"]])
        return canonical

    @staticmethod
    def _decode_debezium_dates(column: pd.Series) -> pd.Series:
        """Debezium's io.debezium.time.Date logical type serializes a
        Postgres DATE column as days-since-epoch (a plain integer) rather
        than a formatted date string — e.g. 20802 for a 2026 date. Only CDC
        records carry this encoding (the file channel's aliasing already
        parses its own mm/dd/yyyy strings, and the API fixture already
        emits ISO dates), so this is a no-op whenever the column already
        holds strings. Left undecoded, dateutil's fuzzy parser reads a bare
        number like this as a literal year and blows up with "year 20802
        is out of range" deep inside Great Expectations' metric engine.
        """
        if not pd.api.types.is_numeric_dtype(column):
            return column
        epoch = date(1970, 1, 1)
        return column.apply(
            lambda days: (epoch + timedelta(days=int(days))).isoformat() if pd.notna(days) else days
        )

    @staticmethod
    def _read_file(path: Path) -> pd.DataFrame:
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
        if path.suffix.lower() in {".xlsx", ".xls"}:
            return pd.read_excel(path, dtype=str)
        raise ValueError(f"Unsupported file format: {path.suffix}")

    @staticmethod
    def _aliases(config: dict[str, Any], source_id: str) -> list[dict[str, Any]]:
        for source in config.get("field_aliasing", []):
            if source.get("source_id") == source_id:
                return source.get("aliases", [])
        raise KeyError(f"No aliases configured for source {source_id!r}")

    @staticmethod
    def _batch_id(dataset: str) -> str:
        return f"{dataset}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"

    def _write_batch_manifest(self, dataset: str, batch_id: str, source_id: str, staged: list[dict[str, Any]]) -> dict[str, Any]:
        manifest = {
            "dataset": dataset,
            "batch_id": batch_id,
            "source_id": source_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "objects": [item["manifest"] for item in staged],
            "row_count": sum(item["manifest"]["row_count"] for item in staged),
        }
        path = self.staging_dir / "batches" / dataset / f"{batch_id}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return {"batch_id": batch_id, "manifest_path": str(path), "manifest": manifest}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest all matching files as one GATES batch.")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--folder", required=True)
    parser.add_argument("--pattern", default="*.csv")
    parser.add_argument("--config-dir", default="config")
    parser.add_argument("--staging-dir", default="staging")
    args = parser.parse_args()
    result = BatchIngestor(
        ConfigRegistry(yaml_dir=args.config_dir),
        staging_dir=args.staging_dir,
    ).ingest_folder(args.dataset, args.source_id, args.folder, args.pattern)
    print(json.dumps(result, indent=2))