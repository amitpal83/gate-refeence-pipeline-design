"""One-time admin setup for the geo stores. Safe to re-run.

Creates the PostGIS roles, schemas and the PII vault table, and stores the
service credentials in OpenBao's kv store so pipeline tasks read them from
there and never from .env.

    roles        geo_loader     owns the geo tables, can write the vault, cannot read it
                 geoserver_ro   can read the geo_serving views only
                 vault_reader   can read the vault (for the notification application)
    schemas      geo, geo_serving, pii_vault
    kv secrets   geo/postgis, geo/postgis_geoserver, geo/minio, geo/geoserver, apps/vault-reader

The loader's OpenBao policy reads kv/geo/* only, so it cannot read apps/vault-reader.

Needs admin secrets, passed for this one command and not stored anywhere:
OPENBAO_ROOT_TOKEN, GEO_DB_PASSWORD (the PostGIS superuser), MINIO_ROOT_PASSWORD,
GEOSERVER_ADMIN_PASSWORD. Role passwords are generated here, kept in OpenBao, and
reused on re-runs, so re-running never locks the tasks out.

POC gap: the MinIO secret stored here is the MinIO root credential. Use a
scoped service user in production (Ceph RGW).
"""
from __future__ import annotations

import os
import secrets
import sys

import psycopg2
import requests
from psycopg2 import sql

ROLES = ("geo_loader", "geoserver_ro", "vault_reader")

SCHEMA_STATEMENTS = (
    "CREATE EXTENSION IF NOT EXISTS postgis",
    "CREATE SCHEMA IF NOT EXISTS geo AUTHORIZATION geo_loader",
    "CREATE SCHEMA IF NOT EXISTS geo_serving AUTHORIZATION geo_loader",
    # The vault schema stays owned by the admin: a schema owner could drop its tables.
    "CREATE SCHEMA IF NOT EXISTS pii_vault",
    """CREATE TABLE IF NOT EXISTS pii_vault.contact_pii (
        layer       text        NOT NULL,
        record_id   text        NOT NULL,
        column_name text        NOT NULL,
        ciphertext  text        NOT NULL,
        key_version integer     NOT NULL,
        batch_id    text        NOT NULL,
        created_at  timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (layer, record_id, column_name)
    )""",
    "GRANT USAGE ON SCHEMA geo_serving TO geoserver_ro",
    "GRANT USAGE ON SCHEMA pii_vault TO geo_loader, vault_reader",
    "GRANT INSERT, DELETE ON pii_vault.contact_pii TO geo_loader",
    # DELETE ... WHERE layer = ... needs SELECT on that one column. Nothing else is readable.
    "GRANT SELECT (layer) ON pii_vault.contact_pii TO geo_loader",
    "GRANT SELECT ON pii_vault.contact_pii TO vault_reader",
)


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        sys.exit(f"Missing environment variable {name}. See the docstring at the top of this script.")
    return value


class Kv:
    """OpenBao kv v2 with the root token. Used only by this script."""

    def __init__(self, addr: str, token: str) -> None:
        self.addr = addr.rstrip("/")
        self.headers = {"X-Vault-Token": token}

    def get(self, path: str) -> dict | None:
        response = requests.get(f"{self.addr}/v1/kv/data/{path}", headers=self.headers, timeout=10)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()["data"]["data"]

    def put(self, path: str, data: dict) -> None:
        response = requests.post(f"{self.addr}/v1/kv/data/{path}", headers=self.headers, json={"data": data}, timeout=10)
        response.raise_for_status()


def ensure_role(cursor, role: str, password: str) -> str:
    cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
    exists = cursor.fetchone() is not None
    verb = "ALTER" if exists else "CREATE"
    cursor.execute(sql.SQL(verb + " ROLE {} WITH LOGIN PASSWORD {}").format(sql.Identifier(role), sql.Literal(password)))
    return "updated" if exists else "created"


def main() -> None:
    addr = required_env("OPENBAO_ADDR")
    kv = Kv(addr, required_env("OPENBAO_ROOT_TOKEN"))

    host = os.getenv("GEO_DB_HOST", "postgis")
    port = int(os.getenv("GEO_DB_PORT", "5432"))
    dbname = os.getenv("GEO_DB_NAME", "gates_geo")
    admin_user = os.getenv("GEO_DB_USER", "gates_geo")
    where = {"host": host, "port": port, "dbname": dbname}

    # Keep existing passwords, so a re-run never breaks running tasks.
    stored = {
        "geo_loader": kv.get("geo/postgis") or {},
        "geoserver_ro": kv.get("geo/postgis_geoserver") or {},
        "vault_reader": kv.get("apps/vault-reader") or {},
    }
    passwords = {role: stored[role].get("password") or secrets.token_urlsafe(24) for role in ROLES}

    kv.put("geo/postgis", {**where, "user": "geo_loader", "password": passwords["geo_loader"]})
    kv.put("geo/postgis_geoserver", {**where, "user": "geoserver_ro", "password": passwords["geoserver_ro"]})
    kv.put("apps/vault-reader", {**where, "user": "vault_reader", "password": passwords["vault_reader"]})
    kv.put("geo/minio", {
        "endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        "access_key": os.getenv("MINIO_ROOT_USER", "minioadmin"),
        "secret_key": required_env("MINIO_ROOT_PASSWORD"),
        "bucket": "gates-geo",
        "quarantine_bucket": "gates-geo-quarantine",
    })
    kv.put("geo/geoserver", {
        "url": os.getenv("GEOSERVER_URL", "http://geoserver:8080/geoserver"),
        "user": os.getenv("GEOSERVER_ADMIN_USER", "admin"),
        "password": required_env("GEOSERVER_ADMIN_PASSWORD"),
    })
    print("OpenBao kv: geo/postgis, geo/postgis_geoserver, geo/minio, geo/geoserver, apps/vault-reader written")

    connection = psycopg2.connect(host=host, port=port, dbname=dbname, user=admin_user, password=required_env("GEO_DB_PASSWORD"))
    connection.autocommit = True
    with connection.cursor() as cursor:
        for role in ROLES:
            print(f"role {role}: {ensure_role(cursor, role, passwords[role])}")
        for statement in SCHEMA_STATEMENTS:
            cursor.execute(statement)
    connection.close()
    print("schemas geo, geo_serving, pii_vault and table pii_vault.contact_pii are ready")


if __name__ == "__main__":
    main()
