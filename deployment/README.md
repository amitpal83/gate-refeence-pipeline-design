# GATES Demo Deployment

This deployment runs the integrated prototype on one EC2 host. Terraform creates the AWS host and persistent storage; Docker Compose runs the services inside it.

## Services

- Airflow: orchestration, retries, task logs, and DAG status
- Airbyte CDK connector: run embedded from an Airflow task (see below) — no separate Airbyte control-plane service
- Kafka + Kafka UI: landing-event topics and CDC change-event topics
- Kafka Connect (Debezium): captures row-level changes from `source-db`'s `project_schema.projects` table into `cdc.project_schema.projects`
- `source-db`: the operational Postgres CDC reads from, and the multi-table source `rd_equipment_inventory`'s DB ingestion reads directly (`equipment_schema.equipment` + `equipment_schema.equipment_assignment`) — see `database/seed_source_db.sql`
- MinIO: S3-compatible object storage backing every Iceberg table (staging, bronze, silver, gold)
- Nessie: Iceberg catalog service
- Trino: SQL engine used by dbt, `common/trino_loader.py`, and the team demo
- DataHub: metadata catalog, lineage, schema, and quality UI
- Elasticsearch and MySQL: DataHub dependencies
- PostgreSQL (`postgres`): Airflow database AND the GATES config database (`database/init_config_db.sql`) — separate from `source-db`

The demo runs the repository's real Airbyte CDK connector in embedded
Airbyte Protocol mode from an Airflow task. This avoids deploying the much
larger Airbyte control plane on the single demo host while preserving the
connector, manifest, record extraction, and canonical staging behavior. Set
`GATES_API_FIXTURE` for a deterministic offline demo; otherwise the CDK
connector calls the configured API (a mock API container in the compose
stack for the demo — see `deployment/mock-api/`).

The Debezium connector is registered automatically by the one-shot
`kafka-connect-init` service (`deployment/kafka-connect/debezium-project-schema.json`)
once `kafka-connect` is up — no manual registration step. To confirm it's
running: `curl http://localhost:8083/connectors/gates-project-schema/status`.

### Demoing the real Airbyte CDK connector (optional)

By default `GATES_API_FIXTURE` points the API channel at the deterministic
`sample_data/project_monitoring_api.json` fixture, so the DAG's happy path
works with no external dependency. To show the *real* Airbyte CDK connector
running instead (`ingestion/airbyte_cdk_connector`, via
`ingestion/ingest_api_airbyte_cdk.py`):

1. Edit `ingestion/airbyte_cdk_connector/secrets/config.json` (not tracked
   by this assistant — it's a secrets file) so `base_url` is
   `http://mock-api:8899` and `api_token` is any non-empty string.
2. In `.env`, set `GATES_API_FIXTURE=` (empty) so `_make_ingest_airbyte`
   falls through to the real connector.
3. Re-trigger the `project_monitoring_pipeline` DAG.

### Demoing a hard-check failure (quarantine)

The deliberately invalid regional file lives at
`sample_data/failure_demo/PCHRD_ProjectMonitoring_Q3_2026_BAD.csv` (a
duplicate `project_id`, a missing title, a missing end date, and an
end-date-before-start-date row) — kept out of `sample_data/`'s default glob
so the normal happy-path run doesn't quarantine every time. To demo the
failure path: copy it into `sample_data/` alongside the other CSVs,
re-trigger the DAG, watch `trigger_gx_validation` fail and the batch land
under `staging/quarantine/project_monitoring/`, then remove it again.

## Local validation

Copy `.env.demo.example` to `.env` and replace every placeholder with a local secret. Do not commit `.env`.

```powershell
Copy-Item .env.demo.example .env
docker compose -f docker-compose.demo.yml config
docker compose -f docker-compose.demo.yml up -d
```

Locally, the UIs bind to the host's own network interface (`0.0.0.0`), so
anyone on your machine's network can reach them too — fine for a laptop
during development, but be mindful on a shared network:

- Airflow: `http://localhost:8080`
- Trino: `http://localhost:8081`
- MinIO: `http://localhost:9001`
- Kafka UI: `http://localhost:8082`
- DataHub: `http://localhost:9002`

(MinIO's S3 API on 9000, Kafka Connect's REST API on 8083, and the mock API
on 8899 stay loopback-only — they're not meant to be browsed.)

## AWS deployment

1. Create an EC2 key pair and note its name.
2. Copy `terraform/terraform.tfvars.example` to `terraform/terraform.tfvars`.
3. Set `allowed_ssh_cidr` to the demo operator's public IP with `/32`.
4. Set `allowed_demo_cidr` to whatever network the team will actually watch
   the demo from (the venue's Wi-Fi range, or a wider `/24` if people join
   from different places) — this is what lets everyone open the UIs
   directly in a browser instead of tunneling over SSH. Defaults to
   `allowed_ssh_cidr` if left unset, which usually means only you can see it.
5. Set `ssh_key_name` and a globally unique backup bucket name.
6. Run Terraform validation and review the plan:

```powershell
terraform -chdir=terraform init
terraform -chdir=terraform validate
terraform -chdir=terraform plan -var-file=terraform.tfvars
```

No resources are created by `validate` or `plan`. Provision only after the plan has been reviewed:

```powershell
terraform -chdir=terraform apply -var-file=terraform.tfvars
```

After the EC2 host is ready, copy the repository and `.env` to the host. The EC2 bootstrap creates `/opt/gates-demo/start-demo.sh` and a guarded `gates-demo.service`; it will not start until both the repository and `.env` exist.

```powershell
$ip = terraform -chdir=terraform output -raw platform_public_ip
scp -i .\gates-demo.pem -r .\* ec2-user@${ip}:/opt/gates-demo/app/
scp -i .\gates-demo.pem .env ec2-user@${ip}:/opt/gates-demo/app/.env
ssh -i .\gates-demo.pem ec2-user@${ip} "sudo systemctl start gates-demo.service"
```

The Compose volumes use `GATES_DATA_ROOT=/opt/gates-demo/data`, which is the attached encrypted EBS volume.

Once the stack is up, run `terraform -chdir=terraform output demo_ui_urls` for
direct browser links to Airflow/Trino/Kafka UI/MinIO/DataHub — reachable
from `allowed_demo_cidr`, no SSH tunnel required. The `ssh_tunnel_command`
output is still there as a fallback for anyone outside that range, or for
solo debugging.

**After the demo**, tighten `allowed_demo_cidr` back down (re-apply with it
set to your own IP, or removed) — these are demo-grade services with
default/simple credentials, not hardened for open-ended internet exposure.

## Current scope

The full reference pipeline is wired end to end for two datasets:
`project_monitoring` (YAML-configured; Airbyte API + regional file + Kafka/Debezium
CDC channels) and `rd_equipment_inventory` (config-database-only; a
multi-table database join). Ingestion → Great Expectations hard checks →
quarantine-on-failure → a real Iceberg staging table on MinIO → dbt
bronze/silver/gold on Trino → DataHub metadata at both ingestion and
transformation stages → `job_execution` run tracking, all orchestrated by
one Airflow DAG factory per dataset. None of this has been run against a
real AWS deployment yet — validate locally via Compose first (see above)
before applying Terraform.
