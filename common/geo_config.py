"""Load and validate geo layer configuration.

    config/geo/<layer>.yaml       the layer contract
    orchestration/geo_registry.yaml   which layers exist, where their files land

Validation collects every problem and reports them together, so a bad config
is fixed in one pass. Nothing here needs geopandas, so it also runs in the main
Airflow environment when the DAG file is parsed.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from common.pii_protection import load_policy

GEOMETRY_TYPES = ("Point", "MultiPoint", "LineString", "MultiLineString", "Polygon", "MultiPolygon")
SUPPORTED_CRS = "EPSG:4326"
REQUIRED_KEYS = ("layer", "geometry_type", "source_crs", "target_crs", "bbox_ph", "unique_key", "attributes")
REGISTRY_KEYS = ("layer", "agency", "source_folder", "file_pattern", "schedule_interval", "start_date")
_SQL_NAME = re.compile(r"^[a-z][a-z0-9_]{0,40}$")


class GeoConfigError(ValueError):
    """The layer config or registry is invalid. The message lists every problem."""


def _crs_ok(value: Any) -> bool:
    return str(value).strip().upper() == SUPPORTED_CRS


def validate_layer_config(config: dict) -> dict:
    """Return the config unchanged if it is valid, otherwise raise GeoConfigError."""
    errors: list[str] = []

    for key in REQUIRED_KEYS:
        if key not in config or config[key] in (None, "", []):
            errors.append(f"missing required key: {key}")

    geometry_type = config.get("geometry_type")
    if geometry_type is not None and geometry_type not in GEOMETRY_TYPES:
        errors.append(f"geometry_type {geometry_type!r} is not one of {GEOMETRY_TYPES}")

    for key in ("source_crs", "target_crs"):
        if key in config and not _crs_ok(config[key]):
            errors.append(f"{key} must be {SUPPORTED_CRS} (WGS 84) for the PoC, got {config[key]!r}")

    bbox = config.get("bbox_ph")
    if bbox is not None:
        if not (isinstance(bbox, (list, tuple)) and len(bbox) == 4 and all(isinstance(v, (int, float)) for v in bbox)):
            errors.append("bbox_ph must be four numbers: [min_lon, min_lat, max_lon, max_lat]")
        else:
            min_lon, min_lat, max_lon, max_lat = bbox
            if not (-180 <= min_lon < max_lon <= 180 and -90 <= min_lat < max_lat <= 90):
                errors.append(
                    f"bbox_ph {list(bbox)} is not a valid [min_lon, min_lat, max_lon, max_lat] "
                    "(longitude within 180, latitude within 90, min below max). Latitude and longitude may be swapped"
                )

    attributes = config.get("attributes") or []
    names: list[str] = []
    for index, attribute in enumerate(attributes):
        if not isinstance(attribute, dict) or not attribute.get("name"):
            errors.append(f"attributes[{index}] needs a name")
            continue
        names.append(attribute["name"])
        allowed = attribute.get("permissible_values")
        if allowed is not None and (not isinstance(allowed, list) or not allowed):
            errors.append(f"attributes.{attribute['name']}.permissible_values must be a non-empty list")
    if len(set(names)) != len(names):
        errors.append("attributes has duplicate names")

    unique_key = config.get("unique_key")
    if unique_key and unique_key not in names:
        errors.append(f"unique_key {unique_key!r} is not listed in attributes")

    aliases = config.get("field_aliases") or {}
    if not isinstance(aliases, dict):
        errors.append("field_aliases must be a mapping of source name to canonical name")
    else:
        for source, target in aliases.items():
            if target not in names:
                errors.append(f"field_aliases maps {source!r} to {target!r}, which is not listed in attributes")

    thresholds = config.get("thresholds") or {}
    for key, value in thresholds.items():
        if not isinstance(value, (int, float)) or not 0 <= value <= 100:
            errors.append(f"thresholds.{key} must be a percentage between 0 and 100")

    if "pii" in config:
        try:
            policy = load_policy(config["pii"])
        except ValueError as exc:
            errors.append(f"pii: {exc}")
        else:
            for column in policy.columns:
                if column not in names:
                    errors.append(f"pii.columns names {column!r}, which is not listed in attributes")
            if unique_key in policy.columns:
                errors.append("the unique_key cannot be a PII column, it keys the vault")

    layer_name = config.get("layer")
    if layer_name and not _SQL_NAME.match(str(layer_name)):
        errors.append(f"layer {layer_name!r} must be lowercase letters, digits and underscores (it becomes a table name)")

    if "geoserver" in config:
        expose = (config["geoserver"] or {}).get("expose_columns")
        if not isinstance(expose, list) or not expose:
            errors.append("geoserver.expose_columns is required: the columns GeoServer may show")
        else:
            pii_columns = set(load_policy(config["pii"]).columns) if "pii" in config and not any(e.startswith("pii") for e in errors) else set()
            for column in expose:
                if column not in names:
                    errors.append(f"geoserver.expose_columns names {column!r}, which is not listed in attributes")
                elif not _SQL_NAME.match(column):
                    errors.append(f"geoserver.expose_columns {column!r} must be lowercase letters, digits and underscores")
                elif column in pii_columns:
                    errors.append(f"geoserver.expose_columns includes {column!r}, which is a PII column")

    if errors:
        raise GeoConfigError(f"Invalid geo layer config {config.get('layer', '?')!r}:\n  - " + "\n  - ".join(errors))
    return config


def load_layer_config(layer: str, config_dir: str | Path = "config") -> dict:
    """Read and validate config/geo/<layer>.yaml."""
    path = Path(config_dir) / "geo" / f"{layer}.yaml"
    if not path.exists():
        raise GeoConfigError(f"Geo layer config not found: {path}")
    with path.open(encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}
    if config.get("layer") != layer:
        raise GeoConfigError(f"{path} declares layer {config.get('layer')!r}, expected {layer!r}")
    return validate_layer_config(config)


def load_geo_registry(path: str | Path) -> list[dict]:
    """Read orchestration/geo_registry.yaml and return its layer entries."""
    with Path(path).open(encoding="utf-8") as handle:
        registry = yaml.safe_load(handle) or {}
    layers = registry.get("layers") or []
    errors: list[str] = []
    seen: set[str] = set()
    for index, entry in enumerate(layers):
        for key in REGISTRY_KEYS:
            if entry.get(key) in (None, ""):
                errors.append(f"layers[{index}] is missing {key}")
        name = entry.get("layer")
        if name in seen:
            errors.append(f"layer {name!r} appears more than once")
        seen.add(name)
    if errors:
        raise GeoConfigError("Invalid geo registry:\n  - " + "\n  - ".join(errors))
    return layers
