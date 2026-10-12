"""Quarantine for geo batches that fail validation.

Failed source files hold plaintext PII, so they go to the restricted MinIO
bucket `gates-geo-quarantine` (Ceph RGW in production), not to a local folder:

    gates-geo-quarantine/<layer>/<run id>/<source files>
    gates-geo-quarantine/<layer>/<run id>/failure.json

failure.json holds the reason and the rule results (rule names, counts and
feature ids, never attribute values). No retention period is set yet; add a
bucket lifecycle rule once the retention period is decided.

The original files are copied, not moved: the drop folder is mounted read-only
in Airflow. Runs in the geo environment (boto3).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from common.geo_standardize import _s3, safe_batch_id


def collect_source_files(source: str | Path) -> list[Path]:
    """The file that was dropped, plus the sidecars next to it for a shapefile
    (.dbf, .shx, .prj, .cpg and so on)."""
    source = Path(source)
    if source.suffix.lower() != ".shp":
        return [source] if source.exists() else []
    stem = source.stem.lower()
    return sorted(p for p in source.parent.iterdir() if p.is_file() and p.stem.lower() == stem)


def quarantine_geo_batch(
    layer: str,
    run_id: str,
    source_files: list[Path],
    reason: str,
    report: dict,
    minio: dict,
) -> str:
    """Copy the failed files and a failure report to the quarantine bucket.
    Returns the s3:// prefix."""
    client = _s3(minio)
    bucket = minio["quarantine_bucket"]
    prefix = f"{layer}/{safe_batch_id(run_id)}/"

    uploaded = []
    for path in source_files:
        client.put_object(Bucket=bucket, Key=prefix + path.name, Body=path.read_bytes())
        uploaded.append(path.name)

    failure = {
        "layer": layer,
        "run_id": run_id,
        "reason": reason,
        "quarantined_at": datetime.now(timezone.utc).isoformat(),
        "files": uploaded,
        "rule_results": report.get("rule_results", []),
    }
    client.put_object(Bucket=bucket, Key=prefix + "failure.json", Body=json.dumps(failure, indent=2, default=str).encode("utf-8"))
    return f"s3://{bucket}/{prefix}"
