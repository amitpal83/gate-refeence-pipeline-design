#!/bin/sh
# One-shot, idempotent: gives KES what it needs in OpenBao and writes its config.
#
#   1. an OpenBao policy and AppRole for KES, limited to kv/kes/*
#   2. /kes/config.yml, rendered from config.yml.tmpl with the AppRole credentials
#      and MinIO's identity
#
# Run by the `kes-bootstrap` service in docker-compose.geo.yml, after OpenBao is
# unsealed (openbao-init) and the certificates exist (kes-certs):
#   /bootstrap  -> ./deployment/kes (read-only)
#   /init       -> .local-data/openbao-init (read-only: keys.env holds the root token)
#   /certs      -> .local-data/kes-certs (read-only)
#   /kes        -> .local-data/kes (output)
#
# Like deployment/openbao/bootstrap.sh this uses the root token, which is POC-only.
set -eu

: "${BAO_ADDR:?}"

if [ ! -f /init/keys.env ]; then
  echo "Missing /init/keys.env. Run openbao-init first." >&2
  exit 1
fi
. /init/keys.env
export BAO_TOKEN="$ROOT_TOKEN"

tries=0
until [ "$(bao status 2>/dev/null | awk '$1 == "Sealed" {print $2}')" = "false" ]; do
  tries=$((tries + 1))
  if [ "$tries" -gt 60 ]; then
    echo "OpenBao is not unsealed. Run openbao-init first." >&2
    exit 1
  fi
  sleep 2
done

bao policy write kes /bootstrap/kes-policy.hcl >/dev/null
bao write auth/approle/role/kes token_policies=kes token_ttl=1h token_max_ttl=24h >/dev/null
ROLE_ID=$(bao read -field=role_id auth/approle/role/kes/role-id)

# Keep the same secret id across runs, so KES keeps working after a restart.
if [ ! -f /kes/approle.env ]; then
  SECRET_ID=$(bao write -f -field=secret_id auth/approle/role/kes/secret-id)
  ( umask 077; printf 'SECRET_ID=%s\n' "$SECRET_ID" > /kes/approle.env )
fi
. /kes/approle.env

MINIO_IDENTITY=$(cat /certs/minio.identity)
( umask 077
  sed -e "s|__MINIO_IDENTITY__|$MINIO_IDENTITY|" \
      -e "s|__KES_ROLE_ID__|$ROLE_ID|" \
      -e "s|__KES_SECRET_ID__|$SECRET_ID|" \
      /bootstrap/config.yml.tmpl > /kes/config.yml )

echo "KES config written. KES may use kv/kes/* in OpenBao and nothing else."
