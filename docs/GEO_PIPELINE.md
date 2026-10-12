# Geo pipeline

A separate pipeline that brings GIS vector layers (shapefiles, zipped shapefiles and GeoPackages) into the platform. It validates the geometry, protects personal data, stores the layer in PostGIS and publishes it through GeoServer as WMS and WFS. It reuses the platform's configuration pattern, job tracking and DataHub, and does not touch the existing medallion pipelines.

Contact details found in a layer are kept, encrypted, in a vault, so applications can later use them to send emails and messages. The tables that GeoServer and analysts see hold no readable personal data.

Production differences are in [PRODUCTION_SECURITY.md](PRODUCTION_SECURITY.md).

## Flow

```
shapefile / .zip / .gpkg dropped in the layer's source folder
  -> ingest_shapefile     read with GeoPandas (pyogrio), restore column names, check CRS (WGS 84 only)
  -> validate_geometry    geometry and attribute rules. A hard failure quarantines the batch and stops the run
  -> protect_pii          Presidio finds PII, OpenBao encrypts the originals into the vault, the serving copy is masked
  -> standardize          technical columns (_batch_id, _ingested_at)
  -> write_geoparquet     MinIO bucket gates-geo, with bounding-box metadata
  -> load_postgis         serving table, vault rows and serving view, replaced in one transaction
  -> publish_geoserver    workspace, datastore, layer, style. Anonymous reads closed
  -> register_metadata    DataHub, with a PII tag on each protected column
  -> finish_job           job_execution row
  cleanup_work            always runs after publish_geoserver
```

The DAG is `<layer>_geo_pipeline`, for example `critical_facilities_geo_pipeline`. It is built from `orchestration/geo_registry.yaml` by `orchestration/geo_airflow_dag.py`. Only one run of a layer can be active at a time.

## Files

| Area | File |
|---|---|
| Layer contract | `config/geo/<layer>.yaml` |
| Layer list | `orchestration/geo_registry.yaml` |
| DAG factory | `orchestration/geo_airflow_dag.py` |
| Config loading and validation | `common/geo_config.py` |
| Ingest | `ingestion/ingest_shapefile.py` |
| Validation | `validation/geo_validation.py` |
| PII protection (generic, reusable) | `common/pii_protection.py` |
| OpenBao client | `common/openbao_client.py` |
| GeoParquet | `common/geo_standardize.py` |
| PostGIS and vault loader | `common/postgis_loader.py` |
| GeoServer | `common/geoserver_publisher.py` |
| Quarantine | `common/geo_quarantine.py` |
| DataHub | `metadata/datahub_emit_geo.py` |
| Services | `docker-compose.geo.yml`, `deployment/openbao/`, `deployment/airflow-geo/` |
| One-time setup | `scripts/geo_setup.py` |
| Sample data | `sample_data/geo/make_sample.py` |

## Where data lives and who can read it

| Place | Holds | Read by |
|---|---|---|
| Source folder | The dropped file, with readable PII | Whoever manages the drop |
| `/tmp/geo-work` in the Airflow container | Working copies. Readable PII only until `protect_pii` finishes | The DAG tasks. Deleted at the end of the run and on failure |
| MinIO `gates-geo-quarantine` | Original files of rejected batches, with readable PII | Restricted. Objects expire after `GEO_QUARANTINE_RETENTION_DAYS` days (default 14) |
| MinIO `gates-geo` | GeoParquet, serving copy only | The loader |
| PostGIS `geo.<layer>` | Serving copy: pseudonyms, masked values | `geo_loader` |
| PostGIS `geo_serving.<layer>` | View with only `geoserver.expose_columns` | `geoserver_ro` |
| PostGIS `pii_vault.contact_pii` | Originals, encrypted | `vault_reader` reads the ciphertext. Only the OpenBao `pii-reader` role can decrypt it |
| OpenBao `kv/geo/*` | Credentials for PostGIS, GeoServer and MinIO | The `geo-loader` role |
| OpenBao `kv/apps/vault-reader` | Database login for the vault reader | Admin only |

Database roles (created by `scripts/geo_setup.py`):

- `geo_loader` owns the geo tables and can write the vault. It cannot read the vault, except the `layer` column, which `DELETE ... WHERE layer = ...` needs.
- `geoserver_ro` can read the serving views and nothing else.
- `vault_reader` can read the vault, for the future notification application.

OpenBao roles (created by `deployment/openbao/bootstrap.sh`):

- `geo-loader` can HMAC with `pii-hmac` and encrypt with `pii-enc`. It cannot decrypt.
- `pii-reader` can decrypt with `pii-enc`. It cannot encrypt.

## Personal data

Presidio scans every text column of a layer except the ones listed in `pii.skip_columns` (facility names and addresses are public data). It adds recognizers for Philippine mobile and Metro Manila numbers, PhilSys, TIN, SSS and GSIS.

| Treatment | Effect on the serving copy |
|---|---|
| `pseudonymize` | A keyed pseudonym (OpenBao HMAC). The same person gets the same token everywhere, so analysts can count and join without knowing who it is |
| `mask` | `j***@example.org` |
| `mask_last3` | Every digit but the last three replaced by `*` |
| `replace_in_text` | Names, numbers and emails inside free text replaced by `<PERSON>`, `<PH_PHONE>` and so on |
| `redact` | The whole value replaced |

Every protected value is also encrypted, in full, into the vault, unless the column has `vault: false`. Nothing is dropped. Government ID numbers are redacted even in the vault copy unless the policy sets `vault_government_ids: true`.

A column that is not declared in `pii.columns` but where Presidio finds PII is protected too: replaced in text, with the original in the vault. Presidio is risk reduction and does not guarantee detection. Review the findings counts when a new layer arrives.

### Reading contact details from an application

```python
from common.openbao_client import OpenBaoClient
from common.pii_protection import reveal_contacts

reader = OpenBaoClient.from_approle_text(approle_text)   # the pii-reader role
contacts = reveal_contacts(reader, vault_rows, record_ids=["F001"], columns=["contact_email"])
```

`vault_rows` come from `pii_vault.contact_pii`, read with the `vault_reader` database role. Ask for the narrowest record ids and columns the use needs. Every decrypt is written to the OpenBao audit log.

Sending messages to people is a new purpose for their data. Under the Philippine Data Privacy Act it needs a lawful basis such as consent, and a way to opt out. Add a consent record before any application reads the vault.

## Adding a layer

1. Copy `config/geo/critical_facilities.yaml` to `config/geo/<layer>.yaml` and edit it. The layer name must be lowercase letters, digits and underscores, because it becomes a table name. Set:
   - `geometry_type`, `bbox_ph`, `unique_key` and the `attributes`, with `required` and `permissible_values`.
   - `field_aliases` for truncated shapefile column names (DBF names are limited to 10 characters).
   - the `pii:` block, and `geoserver.expose_columns` (never a PII column).
2. Add an entry to `orchestration/geo_registry.yaml`.
3. Wait up to 5 minutes, or run `airflow dags reserialize`. The new DAG appears paused. Unpause it.
4. Drop a file in the source folder and trigger the DAG.

All layers must be WGS 84 (EPSG:4326) for now. A `.prj` that declares anything else fails the batch. A missing `.prj` falls back to the layer's `source_crs`.

## Running it on AWS

All commands run in an SSM session on the instance, from `/opt/gates-demo/app`. The host's `.env` needs `GATES_DATA_ROOT`, `GATES_ENV=aws`, `OPENBAO_POC_ACK=yes`, `GEO_DB_PASSWORD` and `GEOSERVER_ADMIN_PASSWORD` (see `geo.env.example`). Use `-f docker-compose.demo.yml -f docker-compose.geo.yml` on every compose command. It is written as `$GEO` below.

First time:

```bash
GEO="docker compose -f docker-compose.demo.yml -f docker-compose.geo.yml"
$GEO up -d openbao-perms openbao openbao-init      # the last line of the openbao-init log should say "OpenBao ready"
$GEO build airflow-init                            # several minutes: geo packages and the spaCy model
$GEO up -d postgis geoserver geo-init airflow
```

Run `scripts/geo_setup.py` once. It creates the database roles and schemas and stores the credentials in OpenBao. It needs the OpenBao root token and the admin passwords, passed for that one command:

```bash
export GEO_DB_PASSWORD=$(grep '^GEO_DB_PASSWORD=' .env | cut -d= -f2-)
export GEOSERVER_ADMIN_PASSWORD=$(grep '^GEOSERVER_ADMIN_PASSWORD=' .env | cut -d= -f2-)
export MINIO_ROOT_PASSWORD=$(grep '^MINIO_ROOT_PASSWORD=' .env | cut -d= -f2-)
export MINIO_ROOT_USER=$(grep '^MINIO_ROOT_USER=' .env | cut -d= -f2-); MINIO_ROOT_USER=${MINIO_ROOT_USER:-minioadmin}
export OPENBAO_ROOT_TOKEN=$(sudo awk -F= '/^ROOT_TOKEN=/{print $2}' /opt/gates-demo/data/openbao-init/keys.env)
$GEO exec -T -e PYTHONPATH=/opt/airflow/dags -e OPENBAO_ROOT_TOKEN -e GEO_DB_PASSWORD -e GEOSERVER_ADMIN_PASSWORD -e MINIO_ROOT_PASSWORD -e MINIO_ROOT_USER airflow /opt/airflow/geo-venv/bin/python - < scripts/geo_setup.py
unset OPENBAO_ROOT_TOKEN GEO_DB_PASSWORD GEOSERVER_ADMIN_PASSWORD MINIO_ROOT_PASSWORD
```

Generate the sample shapefiles, then run the DAG:

```bash
mkdir -p sample_data/geo
$GEO run --rm -T --no-deps --entrypoint /opt/airflow/geo-venv/bin/python --user "$(id -u):0" -e HOME=/tmp -e GEO_SAMPLE_OUT=/out -v "$PWD/sample_data/geo:/out" airflow - < sample_data/geo/make_sample.py
$GEO exec airflow airflow dags reserialize
$GEO exec airflow airflow dags unpause critical_facilities_geo_pipeline
$GEO exec airflow airflow dags trigger critical_facilities_geo_pipeline
$GEO exec airflow airflow dags list-runs -d critical_facilities_geo_pipeline
```

### Failure demo

```bash
cp sample_data/geo/failure_demo/duplicate_key/* sample_data/geo/critical_facilities/
$GEO exec airflow airflow dags trigger critical_facilities_geo_pipeline
# the run fails at validate_geometry. The batch is in the quarantine bucket and the loaded layer is untouched
rm sample_data/geo/critical_facilities/duplicate_key.*
```

The pipeline reads the newest file in the source folder, so remove the bad copy afterwards. The source folder is mounted read-only in Airflow, so the pipeline cannot move a failed file itself.

### After a restart or a reboot

OpenBao comes back sealed. The demo's own systemd unit does not start PostGIS or GeoServer, and it starts Airflow from the original image, without the geo environment. `deployment/start-geo.sh` fixes all three, and the unit `deployment/systemd/gates-geo.service` runs it after `gates-demo.service` starts and whenever that service restarts. Install the unit once on the host:

```bash
sudo cp /opt/gates-demo/app/deployment/systemd/gates-geo.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now gates-geo.service
systemctl status gates-geo.service
```

The unit assumes the demo unit is `gates-demo.service` and the repository is in `/opt/gates-demo/app`. A new instance built from `terraform/user_data.sh.tftpl` needs this install step once. To run the same steps by hand, use `bash deployment/start-geo.sh`.

## Tests

All of them run inside the Airflow container with `-e PYTHONPATH=/opt/airflow/dags`. The command is in each script's docstring.

| Script | Covers | Passing result |
|---|---|---|
| `scripts/pii_smoke_test.py` | OpenBao client and PII module, roles, vault round trip | 17 checks |
| `scripts/geo_smoke_test.py` | Config, ingest, validation, the five failure cases | 22 checks |
| `scripts/geo_load_test.py` | GeoParquet, PostGIS, vault, database roles, atomic replacement | 14 checks |
| `scripts/geoserver_test.py` | Publishing, WMS, WFS, style, anonymous access | 11 checks |
| `scripts/geo_verify.py` | End state after a DAG run (`GEO_VERIFY_MODE=success` or `failure`) | 7 and 5 checks |
| `scripts/geo_pii_audit.py` | Final scan for readable PII in tables, GeoParquet, logs and buckets | all checks pass |

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `DagNotFound` when triggering a new DAG | The scheduler scans for new files every 5 minutes. Run `airflow dags reserialize` |
| OpenBao: "address already in use" | The image's start script loads the config folder. Never add `-config` to the OpenBao command |
| `openbao-init` exits with 137 | Out of memory. It needs `mem_limit: 256m` |
| OpenBao cannot write its data | Bind-mounted folders are owned by root. `openbao-perms` fixes this, so start it first |
| `compose run` fails with "invalid choice" from `airflow` | The Airflow image treats other commands as `airflow` subcommands. Add `--entrypoint /opt/airflow/geo-venv/bin/python` |
| A compose command stops with a message about `GEO_DB_PASSWORD` | Compose reads the whole override file. Both passwords must be in `.env` |
| Geo tasks fail after a restart or reboot | OpenBao is sealed, or Airflow is running the original image. Run `bash deployment/start-geo.sh`, and install `gates-geo.service` so it happens automatically. See "After a restart or a reboot" |
| The newest run read the wrong file | The pipeline takes the newest file in the source folder. Remove leftover test files |
| A file named `.env*` cannot be created | The workspace's permission rules block those names. Use `geo.env.example` |
