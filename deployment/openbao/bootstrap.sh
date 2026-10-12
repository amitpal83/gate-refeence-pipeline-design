#!/bin/sh
# POC bootstrap for OpenBao. Safe to re-run: every step checks before it acts.
#
# On AWS, the host's .env must set GATES_ENV=aws and OPENBAO_POC_ACK=yes,
# otherwise the guard below stops the run (or never fires, if GATES_ENV is unset).
#
# Run by the `openbao-init` service in docker-compose.geo.yml, with:
#   /bootstrap  -> ./deployment/openbao (read-only)
#   /init       -> .local-data/openbao-init (unseal key, root token, AppRole files)
set -eu

INIT_DIR=/init
KEYS_FILE="$INIT_DIR/keys.env"

if [ "${GATES_ENV:-local}" = "aws" ] && [ "${OPENBAO_POC_ACK:-}" != "yes" ]; then
  echo "Refusing to run: this bootstrap keeps the unseal key on the host, which is POC-only." >&2
  echo "Set OPENBAO_POC_ACK=yes to accept that on purpose." >&2
  exit 1
fi

status_field() {
  bao status 2>/dev/null | awk -v k="$1" '$1 == k {print $2}'
}

wait_for_api() {
  tries=0
  while :; do
    rc=0
    bao status >/dev/null 2>&1 || rc=$?
    # 0 = unsealed, 2 = sealed or uninitialized, 1 = cannot connect
    [ "$rc" -ne 1 ] && return 0
    tries=$((tries + 1))
    if [ "$tries" -gt 60 ]; then
      echo "OpenBao API is not reachable at $BAO_ADDR" >&2
      exit 1
    fi
    sleep 2
  done
}

echo "Waiting for OpenBao..."
wait_for_api

mkdir -p "$INIT_DIR"

if [ "$(status_field Initialized)" != "true" ]; then
  echo "Initializing (1 key share, POC only)..."
  OUT=$(bao operator init -key-shares=1 -key-threshold=1)
  UNSEAL_KEY=$(printf '%s\n' "$OUT" | awk '/^Unseal Key 1:/ {print $4}')
  ROOT_TOKEN=$(printf '%s\n' "$OUT" | awk '/^Initial Root Token:/ {print $4}')
  ( umask 077; printf 'UNSEAL_KEY=%s\nROOT_TOKEN=%s\n' "$UNSEAL_KEY" "$ROOT_TOKEN" > "$KEYS_FILE" )
fi

if [ ! -f "$KEYS_FILE" ]; then
  echo "OpenBao is initialized but $KEYS_FILE is missing. Restore it, or wipe .local-data/openbao to start over." >&2
  exit 1
fi
. "$KEYS_FILE"

if [ "$(status_field Sealed)" = "true" ]; then
  echo "Unsealing..."
  bao operator unseal "$UNSEAL_KEY" >/dev/null
fi

export BAO_TOKEN="$ROOT_TOKEN"

# Audit first, so everything after this point is recorded.
bao audit list 2>/dev/null | awk '{print $1}' | grep -qx 'file/' \
  || bao audit enable file file_path=/openbao/audit/audit.log

bao secrets list | awk '{print $1}' | grep -qx 'transit/' || bao secrets enable transit
bao secrets list | awk '{print $1}' | grep -qx 'kv/' || bao secrets enable -path=kv -version=2 kv
bao auth list | awk '{print $1}' | grep -qx 'approle/' || bao auth enable approle

# Two separate keys: one for pseudonyms (HMAC), one for recoverable encryption.
bao write transit/keys/pii-hmac type=aes256-gcm96 >/dev/null
bao write transit/keys/pii-enc type=aes256-gcm96 >/dev/null

create_role() {
  name="$1"
  bao policy write "$name" "/bootstrap/policies/$name.hcl" >/dev/null
  bao write "auth/approle/role/$name" token_policies="$name" token_ttl=1h token_max_ttl=4h >/dev/null
  cred_file="$INIT_DIR/approle-$name.env"
  if [ ! -f "$cred_file" ]; then
    role_id=$(bao read -field=role_id "auth/approle/role/$name/role-id")
    secret_id=$(bao write -f -field=secret_id "auth/approle/role/$name/secret-id")
    printf 'ROLE_ID=%s\nSECRET_ID=%s\n' "$role_id" "$secret_id" > "$cred_file"
    # POC: world-readable so a container running as another user (Airflow,
    # uid 50000) can read it. Production delivers these through an OpenBao
    # agent or a secrets mount, not a shared file.
    chmod 644 "$cred_file"
  fi
}

create_role geo-loader
create_role pii-reader

role_login() {
  ( . "$INIT_DIR/approle-$1.env"
    BAO_TOKEN="" bao write -field=token auth/approle/login role_id="$ROLE_ID" secret_id="$SECRET_ID" )
}

echo "Self-check of role permissions..."
LOADER_TOKEN=$(role_login geo-loader)
READER_TOKEN=$(role_login pii-reader)
PROBE=$(printf 'bootstrap-check' | base64)

CIPHERTEXT=$(BAO_TOKEN="$LOADER_TOKEN" bao write -field=ciphertext transit/encrypt/pii-enc plaintext="$PROBE")
BAO_TOKEN="$LOADER_TOKEN" bao write -field=hmac transit/hmac/pii-hmac input="$PROBE" >/dev/null

if BAO_TOKEN="$LOADER_TOKEN" bao write transit/decrypt/pii-enc ciphertext="$CIPHERTEXT" >/dev/null 2>&1; then
  echo "FAIL: geo-loader can decrypt. Its policy is too broad." >&2
  exit 1
fi

RECOVERED=$(BAO_TOKEN="$READER_TOKEN" bao write -field=plaintext transit/decrypt/pii-enc ciphertext="$CIPHERTEXT")
if [ "$RECOVERED" != "$PROBE" ]; then
  echo "FAIL: pii-reader could not recover the probe value." >&2
  exit 1
fi

if BAO_TOKEN="$READER_TOKEN" bao write transit/encrypt/pii-enc plaintext="$PROBE" >/dev/null 2>&1; then
  echo "FAIL: pii-reader can encrypt. Its policy is too broad." >&2
  exit 1
fi

if BAO_TOKEN="$LOADER_TOKEN" bao write transit/hmac/pii-enc input="$PROBE" >/dev/null 2>&1; then
  echo "FAIL: geo-loader can use a key it should not." >&2
  exit 1
fi

echo "OpenBao ready. Loader: encrypt + HMAC only. Reader: decrypt only."
