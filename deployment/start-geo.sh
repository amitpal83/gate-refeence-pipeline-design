#!/bin/bash
# Brings up the geo layer on top of the demo stack. Safe to run at any time.
#
# Run by deployment/systemd/gates-geo.service after gates-demo.service starts, and
# again whenever gates-demo.service restarts. The demo's own start script knows
# nothing about the geo layer, and it starts Airflow from the original image, so
# without this the geo DAG would fail after a restart or a reboot.
#
#   1. OpenBao comes back sealed after a restart. openbao-init unseals it and
#      re-applies its setup. It is idempotent.
#   2. KES and the encrypted geo MinIO are started. They need OpenBao unsealed.
#   3. PostGIS, GeoServer and the bucket setup are started.
#   4. Airflow is switched back to the geo image (the one with the geo Python
#      environment).
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/gates-demo/app}"
cd "$APP_DIR"
GEO=(docker compose -f docker-compose.demo.yml -f docker-compose.geo.yml)

echo "[start-geo] starting OpenBao"
"${GEO[@]}" up -d openbao-perms openbao

echo "[start-geo] unsealing OpenBao and applying its setup"
"${GEO[@]}" run --rm --no-deps openbao-init

echo "[start-geo] starting KES and the encrypted geo MinIO (needs OpenBao unsealed)"
"${GEO[@]}" up -d kes-certs kes-bootstrap kes minio-geo

echo "[start-geo] starting PostGIS, GeoServer, bucket setup and the geo Airflow image"
"${GEO[@]}" up -d postgis geoserver geo-init airflow

echo "[start-geo] done"
