"""Check that the geo buckets are encrypted at rest with SSE-KMS, using keys that
KES fetches from OpenBao.

For each geo bucket it checks that default encryption is on, and that every object
reports aws:kms. Run it after the DAG has written to the new minio-geo. It also
checks that the geo tasks point at minio-geo.

This shows MinIO's side. To see the encryption on disk, search the raw files on
the host (see docs/GEO_PIPELINE.md, "Encryption at rest").

    docker compose -f docker-compose.demo.yml -f docker-compose.geo.yml exec -T \
      -e PYTHONPATH=/opt/airflow/dags airflow /opt/airflow/geo-venv/bin/python - < scripts/sse_check.py
"""
from __future__ import annotations

import sys

from botocore.exceptions import ClientError

from common.geo_standardize import _s3
from common.openbao_client import OpenBaoClient

failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not ok else ""))
    if not ok:
        failures.append(name)


def main() -> int:
    minio = OpenBaoClient.from_env().read_secret("geo/minio")
    s3 = _s3(minio)
    check("the geo tasks use the encrypted minio-geo", "minio-geo" in minio["endpoint"], minio["endpoint"])

    for bucket in (minio["bucket"], minio["quarantine_bucket"]):
        try:
            rule = s3.get_bucket_encryption(Bucket=bucket)["ServerSideEncryptionConfiguration"]["Rules"][0]
            default = rule["ApplyServerSideEncryptionByDefault"]
        except ClientError as exc:
            check(f"{bucket} has default SSE-KMS encryption", False, str(exc))
            continue
        check(f"{bucket} has default SSE-KMS encryption", default.get("SSEAlgorithm") == "aws:kms", str(default))

        keys = [
            item["Key"]
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket)
            for item in page.get("Contents", [])
        ]
        plain = [k for k in keys if s3.head_object(Bucket=bucket, Key=k).get("ServerSideEncryption") != "aws:kms"]
        check(f"every object in {bucket} is SSE-KMS ({len(keys)} checked)", not plain, str(plain[:5]))

    print(f"\n{'ALL CHECKS PASSED' if not failures else str(len(failures)) + ' CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
