"""Read a vector layer drop (shapefile, zipped shapefile or GeoPackage) for the
geo pipeline.

    ingest_layer() picks the newest matching file, checks its sidecar files and
    CRS, reads it with GeoPandas (pyogrio), restores truncated column names from
    the layer's alias map, and writes a working copy plus a manifest.

No raw copy of the upload is kept. The working copy holds the data before PII
protection, so it is plaintext and short-lived: the protect_pii task must call
remove_work_dir() when it finishes, and the failure path must too.

CRS rule (PoC): WGS 84 only. A missing .prj falls back to the layer's
source_crs. A .prj that declares anything else fails the batch with
CRS_MISMATCH. Nothing is reprojected silently.

Runs in the geo environment (geopandas, pyogrio, pyproj).
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import geopandas as gpd
from pyproj import CRS

REQUIRED_SIDECARS = (".dbf", ".shx")


class GeoIngestError(RuntimeError):
    """code is one of: NO_SOURCE_FILE, BAD_ARCHIVE, MISSING_SIDECAR, UNREADABLE_PRJ,
    CRS_MISMATCH, ENCODING, UNREADABLE, ALIAS_COLLISION."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


def find_layer_files(folder: str | Path, pattern: str) -> list[Path]:
    """Files in the folder matching the pattern, newest first."""
    folder = Path(folder)
    if not folder.is_dir():
        return []
    return sorted((p for p in folder.glob(pattern) if p.is_file()), key=lambda p: p.stat().st_mtime, reverse=True)


def _sibling(path: Path, suffix: str) -> Path | None:
    """The file next to `path` with the same name and the given extension,
    matched ignoring case (.DBF and .dbf are both common)."""
    wanted = (path.stem + suffix).lower()
    for candidate in path.parent.iterdir():
        if candidate.name.lower() == wanted:
            return candidate
    return None


def check_sidecars(shp_path: str | Path) -> list[str]:
    """Return the required sidecar extensions that are missing."""
    shp_path = Path(shp_path)
    return [suffix for suffix in REQUIRED_SIDECARS if _sibling(shp_path, suffix) is None]


def read_prj_crs(shp_path: str | Path) -> CRS | None:
    """The CRS declared in the .prj file, or None if there is no .prj."""
    prj = _sibling(Path(shp_path), ".prj")
    if prj is None:
        return None
    try:
        return CRS.from_wkt(prj.read_text(encoding="utf-8", errors="replace"))
    except Exception as exc:  # pyproj raises CRSError, which is not worth importing
        raise GeoIngestError("UNREADABLE_PRJ", f"{prj.name} could not be parsed: {exc}") from exc


def _is_wgs84(crs: CRS) -> bool:
    return crs.to_epsg() == 4326 or crs.equals(CRS.from_epsg(4326), ignore_axis_order=True)


def check_crs(found: CRS | None, layer_config: dict) -> CRS:
    """The CRS to use for this file. Raises CRS_MISMATCH if the file declares one
    that is not WGS 84."""
    expected = CRS.from_user_input(layer_config["source_crs"])
    if found is None:
        return expected
    if not _is_wgs84(found):
        raise GeoIngestError(
            "CRS_MISMATCH",
            f"the file declares {found.name!r} (EPSG:{found.to_epsg()}), but this layer must be WGS 84 (EPSG:4326)",
        )
    return expected


def _resolve_source(path: Path, work_dir: Path) -> Path:
    """Unpack a .zip into the working folder and return the .shp or .gpkg inside."""
    if path.suffix.lower() != ".zip":
        return path
    destination = work_dir / "unzipped"
    destination.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(path) as archive:
            root = destination.resolve()
            for member in archive.namelist():
                target = (destination / member).resolve()
                if target != root and root not in target.parents:
                    raise GeoIngestError("BAD_ARCHIVE", f"archive member {member!r} points outside the extraction folder")
            archive.extractall(destination)
    except zipfile.BadZipFile as exc:
        raise GeoIngestError("BAD_ARCHIVE", f"{path.name} is not a valid zip file") from exc
    found = [p for p in destination.rglob("*") if p.is_file() and p.suffix.lower() in (".shp", ".gpkg")]
    if len(found) != 1:
        raise GeoIngestError("BAD_ARCHIVE", f"{path.name} must contain exactly one .shp or .gpkg, found {len(found)}")
    return found[0]


def _apply_aliases(gdf: gpd.GeoDataFrame, aliases: dict[str, str]) -> tuple[gpd.GeoDataFrame, dict[str, str]]:
    """Rename truncated or misspelled source columns to their canonical names,
    matching the source name without regard to case."""
    lookup = {source.lower(): target for source, target in (aliases or {}).items()}
    geometry_name = gdf.geometry.name
    rename = {
        column: lookup[str(column).lower()]
        for column in gdf.columns
        if column != geometry_name and str(column).lower() in lookup and lookup[str(column).lower()] != column
    }
    final = [rename.get(c, c) for c in gdf.columns]
    clashes = sorted({name for name in final if final.count(name) > 1})
    if clashes:
        raise GeoIngestError("ALIAS_COLLISION", f"renaming would create duplicate columns: {clashes}")
    return gdf.rename(columns=rename), rename


def read_layer(path: str | Path, layer_config: dict) -> tuple[gpd.GeoDataFrame, dict[str, Any]]:
    """Read one source file. Returns the GeoDataFrame (canonical column names, CRS
    set) and a dict describing what was found: crs_in_file and aliases_applied."""
    path = Path(path)
    suffix = path.suffix.lower()
    encoding = layer_config.get("encoding", "utf-8")
    info: dict[str, Any] = {}

    if suffix == ".shp":
        missing = check_sidecars(path)
        if missing:
            raise GeoIngestError("MISSING_SIDECAR", f"{path.name} is missing required file(s): {', '.join(missing)}")
        found = read_prj_crs(path)
        resolved = check_crs(found, layer_config)
        info["crs_in_file"] = found.to_string() if found else "none, used the layer's source_crs"
    elif suffix == ".gpkg":
        resolved = None
    else:
        raise GeoIngestError("UNREADABLE", f"unsupported file type {suffix!r}; expected .shp, .zip or .gpkg")

    try:
        kwargs: dict[str, Any] = {"engine": "pyogrio"}
        if suffix == ".shp":
            kwargs["encoding"] = encoding
        elif layer_config.get("gpkg_layer"):
            kwargs["layer"] = layer_config["gpkg_layer"]
        gdf = gpd.read_file(path, **kwargs)
    except UnicodeDecodeError as exc:
        raise GeoIngestError("ENCODING", f"{path.name} is not {encoding}; set 'encoding' in the layer config (for example cp1252)") from exc
    except Exception as exc:
        raise GeoIngestError("UNREADABLE", f"{path.name} could not be read: {exc}") from exc

    if suffix == ".gpkg":
        info["crs_in_file"] = gdf.crs.to_string() if gdf.crs else "none"
        resolved = check_crs(gdf.crs, layer_config)
    if gdf.crs is None:
        gdf = gdf.set_crs(resolved)

    gdf, renamed = _apply_aliases(gdf, layer_config.get("field_aliases") or {})
    info["aliases_applied"] = renamed
    return gdf, info


def build_manifest(layer: str, run_id: str, source: Path, gdf: gpd.GeoDataFrame, info: dict[str, Any]) -> dict[str, Any]:
    digest = hashlib.sha256()
    with source.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return {
        "layer": layer,
        "run_id": run_id,
        "source_file": source.name,
        "source_sha256": digest.hexdigest(),
        "source_bytes": source.stat().st_size,
        "feature_count": int(len(gdf)),
        "crs_in_file": info.get("crs_in_file"),
        "crs_used": gdf.crs.to_string() if gdf.crs else None,
        "geometry_types": sorted(str(t) for t in gdf.geom_type.dropna().unique()),
        "columns": [c for c in gdf.columns if c != gdf.geometry.name],
        "aliases_applied": info.get("aliases_applied", {}),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def work_dir_for(work_root: str | Path, layer: str, run_id: str) -> Path:
    return Path(work_root) / layer / run_id


def remove_work_dir(work_root: str | Path, layer: str, run_id: str) -> None:
    """Delete the plaintext working copy. Safe to call when it does not exist."""
    shutil.rmtree(work_dir_for(work_root, layer, run_id), ignore_errors=True)


def ingest_layer(
    layer_config: dict,
    source_folder: str | Path,
    file_pattern: str,
    run_id: str,
    work_root: str | Path,
) -> dict[str, Any]:
    """Ingest the newest file matching the pattern. Returns the manifest, with
    `work_file` and `manifest_file` paths added."""
    layer = layer_config["layer"]
    candidates = find_layer_files(source_folder, file_pattern)
    if not candidates:
        raise GeoIngestError("NO_SOURCE_FILE", f"no file matching {file_pattern!r} in {source_folder}")
    source = candidates[0]

    work_dir = work_dir_for(work_root, layer, run_id)
    work_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(work_dir, 0o700)
    try:
        resolved = _resolve_source(source, work_dir)
        gdf, info = read_layer(resolved, layer_config)
        manifest = build_manifest(layer, run_id, source, gdf, info)

        work_file = work_dir / "ingested.parquet"
        gdf.to_parquet(work_file)
        manifest["work_file"] = str(work_file)
        manifest["manifest_file"] = str(work_dir / "manifest.json")
        manifest["other_candidates"] = [p.name for p in candidates[1:]]
        Path(manifest["manifest_file"]).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    except Exception:
        remove_work_dir(work_root, layer, run_id)
        raise
    return manifest
