"""Smoke test for common/openbao_client.py and common/pii_protection.py.

Runs inside the Airflow container, in the geo environment, against the real
OpenBao. All people, phones, emails and the TIN below are invented.

    docker compose -f docker-compose.demo.yml -f docker-compose.geo.yml exec -T \
      -e PYTHONPATH=/opt/airflow/dags \
      -e OPENBAO_READER_APPROLE="$(sudo cat /opt/gates-demo/data/openbao-init/approle-pii-reader.env)" \
      airflow /opt/airflow/geo-venv/bin/python - < scripts/pii_smoke_test.py

The reader credentials are passed for this test only. They are not mounted
into Airflow, because normal pipeline tasks must not be able to decrypt.
"""
from __future__ import annotations

import json
import os
import sys

import pandas as pd

from common.openbao_client import OpenBaoClient, OpenBaoError
from common.pii_protection import build_analyzer, load_policy, protect_dataframe, reveal_contacts

POLICY = {
    "scan": "all_text",
    "score_threshold": 0.4,
    "skip_columns": ["name", "address"],
    "columns": {
        "contact_person": {"serving": "pseudonymize", "vault": True},
        "contact_email": {"serving": "mask", "vault": True},
        "contact_phone": {"serving": "mask_last3", "vault": True},
        "remarks": {"serving": "replace_in_text", "vault": True},
    },
}

DATA = pd.DataFrame({
    "feature_id": ["f1", "f2", "f3", "f4"],
    "name": ["Sample District Hospital", "Sample Central School", "Sample Rural Health Unit", "Sample Barangay Hall"],
    "address": ["1 Example Street", "2 Example Street", "3 Example Street", "4 Example Street"],
    "region": ["Region IV-A", "NCR", "Region IV-A", "NCR"],
    "contact_person": ["Juan dela Cruz", "Maria Santos", "Juan dela Cruz", "Pedro Reyes"],
    "contact_phone": ["0917 123 4567", "0928 765 4321", "0917 123 4567", "(02) 8123 4567"],
    "contact_email": ["juan.delacruz@example.org", "maria.santos@example.org", "juan.delacruz@example.org", "pedro.reyes@example.org"],
    "remarks": [
        "Call Engr. Juan dela Cruz at 0917 123 4567 about the generator",
        "Contact maria.santos@example.org, TIN 123-456-789-000",
        "No contact details recorded",
        "",
    ],
})

SEEDED = [
    "Juan", "dela Cruz", "Maria Santos", "Pedro Reyes",
    "0917 123 4567", "0928 765 4321", "(02) 8123 4567",
    "juan.delacruz@example.org", "maria.santos@example.org", "pedro.reyes@example.org",
    "123-456-789-000",
]

failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail and not ok else ""))
    if not ok:
        failures.append(name)


def expect_denied(name: str, call) -> None:
    try:
        call()
    except OpenBaoError as exc:
        check(name, exc.status == 403, f"HTTP {exc.status}: {exc}")
    else:
        check(name, False, "the call was allowed")


def main() -> int:
    loader = OpenBaoClient.from_env()
    reader = OpenBaoClient.from_approle_text(os.environ["OPENBAO_READER_APPROLE"])
    policy = load_policy(POLICY)

    print("Loading Presidio and the spaCy model...")
    analyzer = build_analyzer()

    first = protect_dataframe(DATA, policy, "feature_id", loader, analyzer)
    second = protect_dataframe(DATA, policy, "feature_id", loader, analyzer)
    serving = first.serving

    blob = " ".join(str(v) for v in serving.to_numpy().ravel())
    leaked = [s for s in SEEDED if s in blob]
    check("serving copy contains none of the seeded PII", not leaked, f"found: {leaked}")

    check("facility name and address are unchanged", serving[["name", "address"]].equals(DATA[["name", "address"]]))
    check("a clean column is unchanged", serving["region"].equals(DATA["region"]))

    check("same person gets the same pseudonym within a run", serving["contact_person"][0] == serving["contact_person"][2])
    check("different people get different pseudonyms", serving["contact_person"][0] != serving["contact_person"][1])
    check("pseudonyms are identical across runs", serving["contact_person"].tolist() == second.serving["contact_person"].tolist())
    check("emails are masked", serving["contact_email"][0] == "j***@example.org", serving["contact_email"][0])
    check("phones keep only the last 3 digits", serving["contact_phone"][0] == "**** *** *567", serving["contact_phone"][0])

    check("vault holds 15 rows (12 contact values + 3 remarks)", len(first.vault) == 15, str(len(first.vault)))
    check("vault values are ciphertext", first.vault["ciphertext"].str.startswith("vault:v1:").all())

    findings_text = json.dumps(first.findings)
    check("findings report contains no matched text", not [s for s in SEEDED if s in findings_text])

    expect_denied("loader role is denied decrypt", lambda: loader.decrypt_batch("pii-enc", ["vault:v1:AAAA"]))
    expect_denied("reader role is denied encrypt", lambda: reader.encrypt_batch("pii-enc", ["x"]))

    revealed = reveal_contacts(reader, first.vault)
    ok = all(
        revealed.get(rid, {}).get(col) == DATA.loc[i, col]
        for i, rid in enumerate(DATA["feature_id"])
        for col in ("contact_person", "contact_phone", "contact_email")
    )
    check("reader recovers every original contact value", ok)
    check("vaulted remarks keep names and phones", revealed["f1"]["remarks"] == DATA.loc[0, "remarks"], revealed["f1"].get("remarks", ""))
    check(
        "government ID is redacted even in the vault",
        "123-456-789-000" not in revealed["f2"]["remarks"] and "maria.santos@example.org" in revealed["f2"]["remarks"],
        revealed["f2"].get("remarks", ""),
    )
    check("empty remarks are not vaulted", "remarks" not in revealed.get("f4", {}))

    print("\nFindings (column, entity, count, scores, treatment):")
    for item in first.findings:
        print(f"  {item['column']:<15} {item['entity']:<14} {item['count']:>3}  {item['min_score']}-{item['max_score']}  {item['treatment']}")
    print("\nServing copy:")
    print(serving[["feature_id", "contact_person", "contact_phone", "contact_email", "remarks"]].to_string(index=False))

    print(f"\n{'ALL CHECKS PASSED' if not failures else str(len(failures)) + ' CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
