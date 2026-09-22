"""Unified YAML/PostgreSQL configuration access for the GATES pipeline."""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import yaml


class ConfigRegistry:
    """Resolve dataset configuration from PostgreSQL with YAML fallback.

    YAML remains the zero-dependency local mode. PostgreSQL is only imported
    when a database URL is configured, so the demo can start in either mode.
    """

    def __init__(
        self,
        yaml_dir: str | Path = "config",
        db_url: str | None = None,
        use_db: bool = True,
        cache_ttl_seconds: int = 60,
    ) -> None:
        self.yaml_dir = Path(yaml_dir)
        self.db_url = db_url or os.getenv("GATES_CONFIG_DB_URL")
        self.use_db = bool(use_db and self.db_url)
        self.cache_ttl_seconds = cache_ttl_seconds
        self._cache: dict[str, tuple[float, dict[str, Any]]] = {}

    def get_dataset(self, dataset_name: str) -> dict[str, Any]:
        cached = self._cache.get(dataset_name)
        if cached and time.monotonic() - cached[0] < self.cache_ttl_seconds:
            return cached[1]

        config: dict[str, Any] | None = None
        source = "yaml"
        if self.use_db:
            try:
                config = self._load_from_db(dataset_name)
                source = "database"
            except Exception:
                config = None

        if config is None:
            config = self._load_from_yaml(dataset_name)

        config = {**config, "_config_source": source}
        self._cache[dataset_name] = (time.monotonic(), config)
        return config

    def list_datasets(self) -> list[str]:
        datasets = {
            path.name.removeprefix("schema_").removesuffix(".yaml")
            for path in self.yaml_dir.glob("schema_*.yaml")
        }
        if self.use_db:
            try:
                datasets.update(self._list_from_db())
            except Exception:
                pass
        return sorted(datasets)

    def clear_cache(self) -> None:
        self._cache.clear()

    def _load_from_yaml(self, dataset_name: str) -> dict[str, Any]:
        schema_path = self.yaml_dir / f"schema_{dataset_name}.yaml"
        alias_path = self.yaml_dir / "column_aliasing.yaml"
        rules_path = self.yaml_dir / "common_quality_rules.yaml"
        if not schema_path.exists():
            raise FileNotFoundError(f"Dataset schema not found: {schema_path}")

        schema = self._read_yaml(schema_path)
        aliasing = self._read_yaml(alias_path) if alias_path.exists() else {"sources": []}
        rules = self._read_yaml(rules_path) if rules_path.exists() else {}
        if schema.get("dataset") != dataset_name:
            raise ValueError(f"Schema {schema_path} declares a different dataset")

        return {
            "dataset": dataset_name,
            "version": schema.get("version", "1.0"),
            "description": schema.get("description", ""),
            "canonical_schema": schema.get("canonical_schema", {}),
            "sources": schema.get("sources", []),
            "field_aliasing": aliasing.get("sources", []),
            "validation_rules": rules.get("rule_categories", []),
            "validation_failures": rules.get("on_failure", {}),
        }

    def _load_from_db(self, dataset_name: str) -> dict[str, Any]:
        try:
            from sqlalchemy import create_engine, text
        except ImportError as exc:
            raise RuntimeError("SQLAlchemy is required for database-backed configuration") from exc

        engine = create_engine(self.db_url, pool_pre_ping=True)
        with engine.connect() as connection:
            dataset = connection.execute(
                text("""
                    SELECT * FROM dataset_registry
                    WHERE dataset_name = :dataset AND status = 'active'
                    LIMIT 1
                """),
                {"dataset": dataset_name},
            ).mappings().first()
            if not dataset:
                raise KeyError(f"Dataset {dataset_name!r} is not active in the registry")

            schema = connection.execute(
                text("""
                    SELECT * FROM ingestion_schema
                    WHERE dataset_id = :dataset_id AND status = 'active'
                    ORDER BY schema_version DESC LIMIT 1
                """),
                {"dataset_id": dataset["dataset_id"]},
            ).mappings().first()
            if not schema:
                raise KeyError(f"No active schema for {dataset_name!r}")

            sources = connection.execute(
                text("SELECT * FROM ingestion_source WHERE dataset_id = :dataset_id AND status = 'active' ORDER BY priority"),
                {"dataset_id": dataset["dataset_id"]},
            ).mappings().all()
            mappings = connection.execute(
                text("SELECT * FROM field_mapping WHERE schema_id = :schema_id ORDER BY source_id, mapping_id"),
                {"schema_id": schema["schema_id"]},
            ).mappings().all()
            rules = connection.execute(
                text("SELECT * FROM validation_rule WHERE dataset_id = :dataset_id AND active = true"),
                {"dataset_id": dataset["dataset_id"]},
            ).mappings().all()

        by_source: dict[str, list[dict[str, Any]]] = {}
        for mapping in mappings:
            by_source.setdefault(mapping["source_id"], []).append({
                "source_field": mapping["raw_field"],
                "canonical_field": mapping["canonical_field"],
                "transform": mapping.get("transform"),
            })

        return {
            "dataset": dataset_name,
            "version": str(schema["schema_version"]),
            "dataset_id": str(dataset["dataset_id"]),
            "canonical_schema": {
                "unique_fields": _json_value(schema["unique_fields"], []),
                "fields": _json_value(schema["fields"], []),
            },
            "sources": [
                {
                    "source_id": row["source_id"],
                    "channel": row["source_type"],
                    "config": _json_value(row["config"], {}),
                }
                for row in sources
            ],
            # Built from `sources`, not just by_source.items() — a source
            # with no field_mapping rows (identity-mapped, e.g. dost_pms_api
            # and project_monitoring_cdc both map 1:1 onto the canonical
            # schema already) must still get a field_aliasing entry with an
            # empty aliases list, matching the YAML config path. Otherwise
            # BatchIngestor._aliases() can't find the source at all and
            # raises KeyError instead of correctly falling through to
            # identity_pass_through.
            "field_aliasing": [
                {"source_id": row["source_id"], "aliases": by_source.get(row["source_id"], [])}
                for row in sources
            ],
            "validation_rules": [
                {
                    "rule_id": row["rule_id"],
                    "field_name": row.get("field_name"),
                    "rule_type": row["rule_type"],
                    "severity": row["severity"],
                    "config": _json_value(row.get("rule_config"), {}),
                }
                for row in rules
            ],
        }

    def _list_from_db(self) -> set[str]:
        from sqlalchemy import create_engine, text

        engine = create_engine(self.db_url, pool_pre_ping=True)
        with engine.connect() as connection:
            rows = connection.execute(
                text("SELECT dataset_name FROM dataset_registry WHERE status = 'active'")
            )
            return {row[0] for row in rows}

    @staticmethod
    def _read_yaml(path: Path) -> dict[str, Any]:
        with path.open(encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}


def _json_value(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value
