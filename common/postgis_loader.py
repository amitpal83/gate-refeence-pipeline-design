"""Load a geo layer and its PII vault rows into PostGIS in one transaction.

    geo.<layer>             the serving copy (no readable PII), replaced on every load
    geo_serving.<layer>     a view with only the layer's `geoserver.expose_columns`
    pii_vault.contact_pii   the encrypted originals, replaced per layer

Replacement is latest-version-only: the new batch goes into a staging table,
then the real table is truncated and refilled from it, together with the vault
rows, inside one transaction. If anything fails, the previous version of the
table, the vault rows and the view are untouched. If a layer's columns change,
the load fails with a clear message rather than altering the table.

Roles and schemas are created once by scripts/geo_setup.py. This module connects
as geo_loader, with credentials read from OpenBao (kv/geo/postgis).
"""
from __future__ import annotations

import re
from typing import Any

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Connection, Engine

from common.openbao_client import OpenBaoClient

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,40}$")
GEOSERVER_ROLE = "geoserver_ro"


class PostgisLoadError(RuntimeError):
    """The load was refused or failed. The previous version is unchanged."""


def _ident(name: str) -> str:
    """Names come from config, so check them before they go into SQL text."""
    if not _IDENTIFIER.match(name):
        raise PostgisLoadError(f"{name!r} is not a safe SQL name (lowercase letters, digits and underscores)")
    return name


def connect(client: OpenBaoClient, secret_path: str = "geo/postgis") -> Engine:
    """An engine for the role whose credentials are stored at kv/<secret_path>.
    The default is geo_loader. Pass 'geo/postgis_geoserver' for geoserver_ro."""
    secret = client.read_secret(secret_path)
    if not secret:
        raise PostgisLoadError(f"No credentials at kv/{secret_path}. Run scripts/geo_setup.py first.")
    url = URL.create(
        "postgresql+psycopg2",
        username=secret["user"], password=secret["password"],
        host=secret["host"], port=int(secret["port"]), database=secret["dbname"],
    )
    return create_engine(url, pool_pre_ping=True)


def _columns(conn: Connection, schema: str, table: str) -> list[str]:
    rows = conn.execute(
        text("SELECT column_name FROM information_schema.columns "
             "WHERE table_schema = :schema AND table_name = :table ORDER BY ordinal_position"),
        {"schema": schema, "table": table},
    )
    return [row[0] for row in rows]


def write_vault(conn: Connection, layer: str, vault: pd.DataFrame, batch_id: str) -> int:
    """Replace this layer's vault rows. Part of the caller's transaction."""
    conn.execute(text("DELETE FROM pii_vault.contact_pii WHERE layer = :layer"), {"layer": layer})
    if vault.empty:
        return 0
    rows = [
        {
            "layer": layer, "record_id": str(record_id), "column_name": column,
            "ciphertext": ciphertext, "key_version": int(version), "batch_id": batch_id,
        }
        for record_id, column, ciphertext, version in zip(
            vault["record_id"], vault["column_name"], vault["ciphertext"], vault["key_version"]
        )
    ]
    conn.execute(
        text("INSERT INTO pii_vault.contact_pii (layer, record_id, column_name, ciphertext, key_version, batch_id) "
             "VALUES (:layer, :record_id, :column_name, :ciphertext, :key_version, :batch_id)"),
        rows,
    )
    return len(rows)


def create_serving_view(conn: Connection, layer: str, expose_columns: list[str], geometry_column: str) -> str:
    """A view exposing only the listed columns, readable by geoserver_ro. The view
    runs with its owner's rights, so geoserver_ro needs no access to geo.<layer>."""
    columns = ", ".join(f'"{_ident(c)}"' for c in [*expose_columns, geometry_column] if c)
    conn.execute(text(f"DROP VIEW IF EXISTS geo_serving.{layer}"))
    conn.execute(text(f"CREATE VIEW geo_serving.{layer} AS SELECT {columns} FROM geo.{layer}"))
    conn.execute(text(f"GRANT SELECT ON geo_serving.{layer} TO {GEOSERVER_ROLE}"))
    return f"geo_serving.{layer}"


def load_layer(
    engine: Engine,
    gdf: gpd.GeoDataFrame,
    layer_config: dict,
    vault: pd.DataFrame,
    batch_id: str,
) -> dict[str, Any]:
    """Replace the layer, its vault rows and its serving view in one transaction."""
    layer = _ident(layer_config["layer"])
    expose = (layer_config.get("geoserver") or {}).get("expose_columns")
    if not expose:
        raise PostgisLoadError("The layer config needs geoserver.expose_columns")
    stage = f"{layer}__stage"
    geometry_column = gdf.geometry.name

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS geo.{stage}"))
        gdf.to_postgis(stage, conn, schema="geo", if_exists="fail", index=False)
        stage_columns = _columns(conn, "geo", stage)

        exists = conn.execute(text("SELECT to_regclass(:name)"), {"name": f"geo.{layer}"}).scalar() is not None
        if not exists:
            conn.execute(text(f"CREATE TABLE geo.{layer} (LIKE geo.{stage} INCLUDING ALL)"))
        else:
            existing = _columns(conn, "geo", layer)
            if existing != stage_columns:
                raise PostgisLoadError(
                    f"The columns of {layer} changed. Existing: {existing}. New: {stage_columns}. "
                    f"Review the change, then drop geo.{layer} to reload it."
                )

        column_list = ", ".join(f'"{c}"' for c in stage_columns)
        conn.execute(text(f"TRUNCATE geo.{layer}"))
        conn.execute(text(f"INSERT INTO geo.{layer} ({column_list}) SELECT {column_list} FROM geo.{stage}"))
        conn.execute(text(f"DROP TABLE geo.{stage}"))
        vault_rows = write_vault(conn, layer, vault, batch_id)
        view = create_serving_view(conn, layer, expose, geometry_column)
        rows = conn.execute(text(f"SELECT count(*) FROM geo.{layer}")).scalar()

    # Outside the transaction: refresh planner statistics for the new data.
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"ANALYZE geo.{layer}"))

    return {"table": f"geo.{layer}", "view": view, "rows": int(rows), "vault_rows": vault_rows, "batch_id": batch_id}
