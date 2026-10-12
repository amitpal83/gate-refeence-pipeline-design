#!/bin/sh
# One-shot: creates the certificates KES and the geo MinIO use to authenticate
# each other (mutual TLS). Safe to re-run: existing certificates are kept.
#
# Run by the `kes-certs` service in docker-compose.geo.yml, with:
#   /bootstrap  -> ./deployment/kes (read-only)
#   /certs      -> .local-data/kes-certs
#
# Output in /certs:
#   kes.crt / kes.key        KES server certificate (self-signed, valid for the host name "kes")
#   minio.crt / minio.key    client certificate MinIO presents to KES
#   minio.identity           SHA-256 of the client certificate's public key. KES identifies
#                            and authorizes MinIO by this value.
#
# POC: self-signed, 10-year certificates. Production uses certificates from the
# organization's CA, with rotation.
set -eu

CERTS=/certs
mkdir -p "$CERTS"

if [ -f "$CERTS/kes.crt" ] && [ -f "$CERTS/minio.crt" ] && [ -f "$CERTS/minio.identity" ]; then
  echo "Certificates already exist in $CERTS"
  exit 0
fi

openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=kes" \
  -addext "subjectAltName=DNS:kes,DNS:localhost,IP:127.0.0.1" \
  -keyout "$CERTS/kes.key" -out "$CERTS/kes.crt" 2>/dev/null

openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=minio-geo" \
  -addext "extendedKeyUsage=clientAuth" \
  -keyout "$CERTS/minio.key" -out "$CERTS/minio.crt" 2>/dev/null

openssl x509 -in "$CERTS/minio.crt" -noout -pubkey \
  | openssl pkey -pubin -outform DER \
  | openssl dgst -sha256 -r | cut -d' ' -f1 > "$CERTS/minio.identity"

# Certificates are public. The private keys stay readable by root only, and both
# KES and MinIO run as root in this setup.
chmod 644 "$CERTS/kes.crt" "$CERTS/minio.crt" "$CERTS/minio.identity"
chmod 600 "$CERTS/kes.key" "$CERTS/minio.key"

echo "Created certificates. MinIO identity: $(cat "$CERTS/minio.identity")"
