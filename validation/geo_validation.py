"""Geometry and attribute validation for geo layers.

validate_geo() returns (report, gdf) in the same report shape as
validation/run_validation.py:validate(): hard_rule_pass, data_quality_index,
row_count, rule_results. The returned GeoDataFrame has invalid geometries
repaired with shapely.make_valid.

Hard rules fail the batch. Soft rules are warnings and only lower the quality
index. Attribute rules run in plain pandas: Great Expectations is installed in
the main Airflow environment, not in the geo environment.

Not covered yet (needed once polygon boundary layers arrive): overlap and
sliver checks.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd

SAMPLE_IDS = 5


def _rule(rule: str, column: str, severity: str, success: bool, detail: str,
          count: int = 0, sample_ids: list[str] | None = None) -> dict[str, Any]:
    return {
        "rule": rule, "column": column, "severity": severity, "success": success,
        "detail": detail, "count": count, "sample_ids": sample_ids or [],
    }


def _ids(df: pd.DataFrame, mask: np.ndarray, key: str) -> list[str]:
    """Up to a few feature ids (or row numbers) for the flagged rows."""
    positions = np.flatnonzero(mask)[:SAMPLE_IDS]
    if key in df.columns:
        return [str(df[key].iloc[p]) for p in positions]
    return [f"row {p}" for p in positions]


def _blank(series: pd.Series) -> np.ndarray:
    return (series.isna() | (series.astype(str).str.strip() == "")).to_numpy()


def _result(rule: str, column: str, severity: str, mask: np.ndarray, rows: int, key: str, df: pd.DataFrame,
            ok_detail: str, fail_detail: str) -> tuple[dict[str, Any], np.ndarray]:
    count = int(mask.sum())
    if count == 0:
        return _rule(rule, column, severity, True, ok_detail), mask
    ids = _ids(df, mask, key)
    return _rule(rule, column, severity, False, f"{count} of {rows} features {fail_detail}: {', '.join(ids)}",
                 count, ids), mask


def validate_geo(gdf: gpd.GeoDataFrame, layer_config: dict) -> tuple[dict[str, Any], gpd.GeoDataFrame]:
    cfg = layer_config
    key = cfg["unique_key"]
    thresholds = cfg.get("thresholds") or {}
    rows = len(gdf)
    results: list[dict[str, Any]] = []
    flagged: list[np.ndarray] = []

    def add(item: tuple[dict[str, Any], np.ndarray]) -> None:
        results.append(item[0])
        flagged.append(item[1])

    if rows == 0:
        results.append(_rule("not_empty", "*", "hard", False, "the layer has no features"))
        return _report(cfg, rows, results, flagged), gdf

    geometry = gdf.geometry
    null_or_empty = (geometry.isna() | geometry.is_empty).to_numpy()
    present = ~null_or_empty

    # geometry type
    expected = cfg["geometry_type"]
    allowed = {expected} | ({"Multi" + expected} if cfg.get("allow_multi") else set())
    found_types = geometry.geom_type.fillna("").to_numpy()
    wrong_type = present & ~np.isin(found_types, list(allowed))
    add(_result("geometry_type", "geometry", "hard", wrong_type, rows, key, gdf,
                f"all geometries are {expected}",
                f"are not {expected} (found {sorted(set(found_types[wrong_type]))})"))

    # null or empty geometry
    pct = 100.0 * null_or_empty.sum() / rows
    limit = float(thresholds.get("max_null_geom_pct", 0))
    mask = null_or_empty
    item, mask = _result("null_or_empty_geometry", "geometry", "hard", mask, rows, key, gdf,
                         "no null or empty geometries", "have a null or empty geometry")
    if item["count"] and pct <= limit:
        item = {**item, "success": True, "detail": item["detail"] + f" (within the {limit}% limit)"}
    results.append(item)
    flagged.append(mask)

    # invalid geometries: repair, then check how many were needed
    invalid = (~geometry.is_valid).to_numpy() & present
    repaired = gdf
    if invalid.any():
        fixed = geometry.copy()
        fixed[invalid] = geometry[invalid].make_valid()
        repaired = gdf.copy()
        repaired[repaired.geometry.name] = fixed
    repaired_pct = 100.0 * invalid.sum() / rows
    max_repaired = float(thresholds.get("max_repaired_pct", 0))
    add(_result("invalid_geometries_repaired", "geometry", "soft", invalid, rows, key, gdf,
                "no invalid geometries", "had an invalid geometry and were repaired with make_valid"))
    results.append(_rule(
        "repair_threshold", "geometry", "hard", repaired_pct <= max_repaired,
        f"{repaired_pct:.1f}% repaired, limit {max_repaired}%", int(invalid.sum()),
        _ids(gdf, invalid, key) if repaired_pct > max_repaired else [],
    ))

    # bounding box
    min_lon, min_lat, max_lon, max_lat = cfg["bbox_ph"]
    bounds = repaired.geometry.bounds
    outside = (
        (bounds["minx"] < min_lon) | (bounds["maxx"] > max_lon) |
        (bounds["miny"] < min_lat) | (bounds["maxy"] > max_lat)
    ).to_numpy() & present
    add(_result("bbox", "geometry", "hard", outside, rows, key, gdf,
                "all geometries are inside the Philippine bounding box",
                f"are outside the Philippine bounding box {cfg['bbox_ph']} (latitude and longitude may be swapped)"))

    # unique key
    if key not in gdf.columns:
        results.append(_rule("unique_key_present", key, "hard", False, f"the unique key column {key!r} is missing"))
        flagged.append(np.zeros(rows, dtype=bool))
    else:
        add(_result("unique_key_present", key, "hard", _blank(gdf[key]), rows, key, gdf,
                    f"every feature has a {key}", f"have no {key}"))
        duplicated = gdf[key].duplicated(keep=False).to_numpy() & ~_blank(gdf[key])
        add(_result("unique_key_unique", key, "hard", duplicated, rows, key, gdf,
                    f"{key} is unique", f"share a duplicate {key}"))

    # required columns and values, permitted values
    attributes = cfg.get("attributes") or []
    missing_columns = [a["name"] for a in attributes if a.get("required") and a["name"] not in gdf.columns]
    results.append(_rule(
        "required_columns", ",".join(missing_columns) or "*", "hard", not missing_columns,
        "all required columns are present" if not missing_columns else f"missing required column(s): {missing_columns}",
        len(missing_columns),
    ))
    flagged.append(np.zeros(rows, dtype=bool))

    for attribute in attributes:
        name = attribute["name"]
        if name not in gdf.columns:
            continue
        blank = _blank(gdf[name])
        if attribute.get("required"):
            add(_result("required_values", name, "hard", blank, rows, key, gdf,
                        f"{name} is filled in everywhere", f"have an empty {name}"))
        allowed_values = attribute.get("permissible_values")
        if allowed_values:
            bad = (~blank) & (~gdf[name].astype(str).isin(allowed_values)).to_numpy()
            add(_result("permissible_values", name, "hard", bad, rows, key, gdf,
                        f"{name} only has permitted values",
                        f"have a {name} outside {allowed_values}"))

    # columns nobody declared
    known = {a["name"] for a in attributes} | {gdf.geometry.name}
    unexpected = [c for c in gdf.columns if c not in known]
    results.append(_rule(
        "unexpected_columns", ",".join(unexpected) or "*", "soft", not unexpected,
        "no unexpected columns" if not unexpected else f"columns not in the layer config: {unexpected}",
        len(unexpected),
    ))
    flagged.append(np.zeros(rows, dtype=bool))

    return _report(cfg, rows, results, flagged), repaired


def _report(cfg: dict, rows: int, results: list[dict[str, Any]], flagged: list[np.ndarray]) -> dict[str, Any]:
    hard_pass = all(r["success"] for r in results if r["severity"] == "hard")
    if rows:
        any_issue = np.logical_or.reduce(flagged) if flagged else np.zeros(rows, dtype=bool)
        dqi = round(100.0 * (1 - float(any_issue.sum()) / rows), 1)
    else:
        dqi = 0.0
    return {
        "layer": cfg["layer"],
        "engine": "shapely+geopandas",
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "row_count": rows,
        "hard_rule_pass": hard_pass,
        "data_quality_index": dqi,
        "rule_results": results,
    }


def failed_rules(report: dict[str, Any], severity: str = "hard") -> list[str]:
    """Names of the rules that failed, for logs and quarantine reasons."""
    return [r["rule"] for r in report["rule_results"] if r["severity"] == severity and not r["success"]]
