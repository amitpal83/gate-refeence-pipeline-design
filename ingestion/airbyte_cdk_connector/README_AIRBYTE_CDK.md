# Real Airbyte connector (airbyte-cdk) — for project_monitoring

## ⚠️ Not verified in the environment that built this

No network access was available here to `pip install airbyte-cdk` or run
any command below. Every file in this folder is written from the
documented low-code CDK pattern, not executed. Before relying on this,
run the commands below in order — `spec` first, since it needs no network
and no mock server, so it's the cheapest way to catch a manifest or API
mismatch immediately.

## Files

- `manifest.yaml` — a schema-valid low-code CDK manifest (adds the
  `type:`/`spec:` keys the real CDK requires, which the simplified
  `config/airbyte_manifest.yaml` one level up omits on purpose, since that
  one is read by our own hand-rolled parser, not by airbyte-cdk).
- `main.py` — the connector entrypoint (`YamlDeclarativeSource` +
  `AirbyteEntrypoint.launch`).
- `secrets/config.json` — connector config; `base_url` defaults to the
  local mock server, not the real DOST PMS API.
- `integration_tests/configured_catalog.json` — tells `read` which
  stream(s) to sync.
- `requirements.txt` — `airbyte-cdk` version range (best guess, unverified).

## Commands, in order

```bash
cd ingestion/airbyte_cdk_connector
pip install -r requirements.txt

# 1. spec — prints the connector's config JSON Schema from manifest.yaml's
#    spec: block. No network, no mock server needed. Run this first.
python main.py spec

# 2. check — validates secrets/config.json against spec, then attempts a
#    real connection. Needs the mock server running (see below).
python main.py check --config secrets/config.json

# 3. discover — lists available streams and their schema.
python main.py discover --config secrets/config.json

# 4. read — actually pulls records. Prints newline-delimited Airbyte
#    Protocol JSON (RECORD/LOG/STATE/TRACE messages) to stdout.
python main.py read --config secrets/config.json \
    --catalog integration_tests/configured_catalog.json
```

## Wiring this into the actual pipeline

`read`'s output is raw Airbyte Protocol — not staged CSVs, not canonical
columns. `../ingest_api_airbyte_cdk.py` (one level up) is the adapter: it
runs `main.py read` as a subprocess, parses only the `RECORD` messages out
of its stdout, and hands the extracted data to the exact same
`common/ingestion_framework.py` functions every other ingestion path in
this repo uses (`identity_pass_through`, `write_staged`, etc.). Once
`airbyte-cdk` is installed and steps 1–4 above succeed manually, run:

```bash
cd ../..   # back to gates_pipeline root
python ingestion/ingest_api_airbyte_cdk.py
```

This should behave identically to `ingest_api_airbyte.py` in terms of what
lands in `staging/` — same dataset, same source_id, same canonical columns —
the only difference is *how* the HTTP call and manifest interpretation
happened underneath (real airbyte-cdk vs. our hand-rolled reader).

## Known gaps versus a real Airbyte deployment

Even once this runs correctly, it is still not the full platform:
- No Docker isolation (connectors normally run in their own container)
- No connection state/checkpointing between syncs (each `read` is a fresh
  full sync; incremental sync via `last_updated_at` as declared in the
  original simplified manifest isn't wired up here)
- No destination connector — output is parsed and staged by our own code,
  not written by an Airbyte destination
- No Airbyte UI/scheduler — orchestration is still Airflow, calling this
  script directly
