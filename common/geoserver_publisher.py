"""Publish a PostGIS serving view as a GeoServer WMS/WFS layer through the REST API.

    publish(layer_config, geoserver_secret, postgis_secret)

creates, or updates, in this order: the workspace, a PostGIS datastore, the
feature type (the view), an SLD style generated from the layer config, and
the layer's default style. It then closes anonymous read access. Running it
again changes nothing but refreshes the settings, and never duplicates anything.

The datastore connects as geoserver_ro to the geo_serving schema only, so
GeoServer can never reach the base table or the PII vault. The layer config's
`geoserver.expose_columns` already decided which columns the view holds.

Credentials come from OpenBao (kv/geo/geoserver and kv/geo/postgis_geoserver),
passed in by the caller. Needs only `requests`.
"""
from __future__ import annotations

import time
from typing import Any
from xml.sax.saxutils import escape

import requests

DATASTORE_NAME = "postgis_geo_serving"
SERVING_SCHEMA = "geo_serving"
JSON = {"Accept": "application/json", "Content-Type": "application/json"}
SLD_TYPE = "application/vnd.ogc.sld+xml"


class GeoServerError(RuntimeError):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class GeoServer:
    """A small REST client. `url` ends with /geoserver."""

    def __init__(self, url: str, user: str, password: str, timeout: float = 30) -> None:
        self.base = url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.auth = (user, password)

    def call(self, method: str, path: str, ok: tuple[int, ...] = (200, 201), **kwargs: Any) -> requests.Response:
        try:
            response = self.session.request(method, f"{self.base}/rest{path}", timeout=self.timeout, **kwargs)
        except requests.RequestException as exc:
            raise GeoServerError(f"GeoServer is unreachable at {self.base}: {exc}") from exc
        if response.status_code not in ok:
            raise GeoServerError(
                f"{method} {path} failed with HTTP {response.status_code}: {response.text[:300]}",
                status=response.status_code,
            )
        return response

    def exists(self, path: str) -> bool:
        response = self.call("GET", path, ok=(200, 404), headers={"Accept": "application/json"})
        return response.status_code == 200

    def wait_until_ready(self, timeout: float = 180) -> None:
        """GeoServer takes a minute or two to start. Fails at once on bad credentials."""
        deadline = time.monotonic() + timeout
        while True:
            try:
                response = self.session.get(f"{self.base}/rest/about/version.json", timeout=10)
                if response.status_code == 200:
                    return
                if response.status_code in (401, 403):
                    raise GeoServerError("GeoServer rejected the admin credentials", status=response.status_code)
            except requests.RequestException:
                pass
            if time.monotonic() > deadline:
                raise GeoServerError(f"GeoServer was not ready after {timeout:.0f}s at {self.base}")
            time.sleep(5)


# --- steps ---------------------------------------------------------------

def ensure_workspace(gs: GeoServer, workspace: str) -> str:
    if gs.exists(f"/workspaces/{workspace}.json"):
        return "exists"
    gs.call("POST", "/workspaces", json={"workspace": {"name": workspace}}, headers=JSON)
    return "created"


def ensure_datastore(gs: GeoServer, workspace: str, postgis: dict, name: str = DATASTORE_NAME) -> str:
    entries = {
        "dbtype": "postgis",
        "host": postgis["host"],
        "port": str(postgis["port"]),
        "database": postgis["dbname"],
        "schema": SERVING_SCHEMA,
        "user": postgis["user"],
        "passwd": postgis["password"],
        "Expose primary keys": "true",
        "validate connections": "true",
    }
    body = {"dataStore": {
        "name": name, "enabled": True,
        "connectionParameters": {"entry": [{"@key": key, "$": value} for key, value in entries.items()]},
    }}
    path = f"/workspaces/{workspace}/datastores"
    if gs.exists(f"{path}/{name}.json"):
        gs.call("PUT", f"{path}/{name}", json=body, headers=JSON)
        return "updated"
    gs.call("POST", path, json=body, headers=JSON)
    return "created"


def publish_layer(gs: GeoServer, workspace: str, layer: str, title: str, datastore: str = DATASTORE_NAME) -> str:
    base = f"/workspaces/{workspace}/datastores/{datastore}/featuretypes"
    body = {"featureType": {
        "name": layer, "nativeName": layer, "title": title,
        "srs": "EPSG:4326", "projectionPolicy": "FORCE_DECLARED", "enabled": True,
    }}
    action = "updated"
    if not gs.exists(f"{base}/{layer}.json"):
        gs.call("POST", base, json=body, headers=JSON)
        action = "created"
    # Always refresh the bounding boxes from the data that is loaded now.
    gs.call("PUT", f"{base}/{layer}", params={"recalculate": "nativebbox,latlonbbox"}, json=body, headers=JSON)
    return action


def _symbolizer(geometry_type: str, color: str, size: Any) -> str:
    color = escape(str(color))
    base = geometry_type.replace("Multi", "")
    if base == "Point":
        return (
            "<PointSymbolizer><Graphic><Mark><WellKnownName>circle</WellKnownName>"
            f'<Fill><CssParameter name="fill">{color}</CssParameter></Fill>'
            '<Stroke><CssParameter name="stroke">#ffffff</CssParameter><CssParameter name="stroke-width">1</CssParameter></Stroke>'
            f"</Mark><Size>{escape(str(size))}</Size></Graphic></PointSymbolizer>"
        )
    if base == "LineString":
        return (f'<LineSymbolizer><Stroke><CssParameter name="stroke">{color}</CssParameter>'
                '<CssParameter name="stroke-width">2</CssParameter></Stroke></LineSymbolizer>')
    return (f'<PolygonSymbolizer><Fill><CssParameter name="fill">{color}</CssParameter>'
            '<CssParameter name="fill-opacity">0.4</CssParameter></Fill>'
            f'<Stroke><CssParameter name="stroke">{color}</CssParameter><CssParameter name="stroke-width">1</CssParameter></Stroke>'
            "</PolygonSymbolizer>")


def build_sld(layer_config: dict) -> str:
    """An SLD style: one rule per value of style.attribute, coloured from
    style.colors, plus a grey rule for any other value."""
    layer = layer_config["layer"]
    style = (layer_config.get("geoserver") or {}).get("style") or {}
    geometry_type = layer_config["geometry_type"]
    size = style.get("point_size", 8)
    attribute = style.get("attribute")
    colors = style.get("colors") or {}

    rules = []
    for value, color in colors.items():
        rules.append(
            f"<Rule><Name>{escape(str(value))}</Name><Title>{escape(str(value))}</Title>"
            f"<ogc:Filter><ogc:PropertyIsEqualTo><ogc:PropertyName>{escape(str(attribute))}</ogc:PropertyName>"
            f"<ogc:Literal>{escape(str(value))}</ogc:Literal></ogc:PropertyIsEqualTo></ogc:Filter>"
            f"{_symbolizer(geometry_type, color, size)}</Rule>"
        )
    # Anything that matches no rule above (or every feature, if no colours are configured).
    else_filter = "<ElseFilter/>" if rules else ""
    rules.append(f"<Rule><Name>other</Name><Title>other</Title>{else_filter}"
                 f"{_symbolizer(geometry_type, '#888888', size)}</Rule>")

    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<StyledLayerDescriptor version="1.0.0" xmlns="http://www.opengis.net/sld" xmlns:ogc="http://www.opengis.net/ogc">'
        f"<NamedLayer><Name>{escape(layer)}</Name><UserStyle><Title>{escape(layer)}</Title>"
        f"<FeatureTypeStyle>{''.join(rules)}</FeatureTypeStyle></UserStyle></NamedLayer></StyledLayerDescriptor>"
    )


def apply_style(gs: GeoServer, workspace: str, layer: str, layer_config: dict) -> str:
    sld = build_sld(layer_config).encode("utf-8")
    action = "updated"
    if gs.exists(f"/workspaces/{workspace}/styles/{layer}.json"):
        gs.call("PUT", f"/workspaces/{workspace}/styles/{layer}", data=sld, headers={"Content-Type": SLD_TYPE})
    else:
        gs.call("POST", f"/workspaces/{workspace}/styles", params={"name": layer}, data=sld, headers={"Content-Type": SLD_TYPE})
        action = "created"
    gs.call(
        "PUT", f"/workspaces/{workspace}/layers/{layer}",
        json={"layer": {"defaultStyle": {"name": layer, "workspace": workspace}, "enabled": True}}, headers=JSON,
    )
    return action


def restrict_anonymous_access(gs: GeoServer) -> str:
    """GeoServer allows anonymous reads of every layer by default. Require a
    login instead. Other data-security rules are kept as they are."""
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    rules = gs.call("GET", "/security/acl/layers", headers=headers).json()
    if rules.get("*.*.r") == "ROLE_AUTHENTICATED":
        return "unchanged"
    rules["*.*.r"] = "ROLE_AUTHENTICATED"
    gs.call("PUT", "/security/acl/layers", json=rules, headers=headers)
    return "restricted"


def publish(layer_config: dict, geoserver: dict, postgis: dict) -> dict[str, str]:
    """Publish the layer's serving view. `geoserver` is kv/geo/geoserver (url,
    user, password) and `postgis` is kv/geo/postgis_geoserver. Returns what
    happened at each step: created, updated, exists, restricted or unchanged."""
    layer = layer_config["layer"]
    settings = layer_config.get("geoserver") or {}
    workspace = settings.get("workspace", "gates")

    gs = GeoServer(geoserver["url"], geoserver["user"], geoserver["password"])
    gs.wait_until_ready()
    result = {
        "workspace": ensure_workspace(gs, workspace),
        "datastore": ensure_datastore(gs, workspace, postgis),
        "layer": publish_layer(gs, workspace, layer, layer_config.get("description") or layer),
        "style": apply_style(gs, workspace, layer, layer_config),
    }
    if settings.get("restrict_anonymous", True):
        result["anonymous_access"] = restrict_anonymous_access(gs)
    return result


# --- checks --------------------------------------------------------------

def counts(gs: GeoServer, workspace: str) -> dict[str, int]:
    """How many workspaces, datastores, feature types and styles exist. Used to
    prove a second publish creates nothing new."""
    def listed(path: str, outer: str, inner: str) -> int:
        body = gs.call("GET", path, headers={"Accept": "application/json"}).json()
        items = body.get(outer)
        return len(items.get(inner, [])) if isinstance(items, dict) else 0

    return {
        "workspaces": listed("/workspaces", "workspaces", "workspace"),
        "datastores": listed(f"/workspaces/{workspace}/datastores", "dataStores", "dataStore"),
        "feature_types": listed(f"/workspaces/{workspace}/datastores/{DATASTORE_NAME}/featuretypes", "featureTypes", "featureType"),
        "styles": listed(f"/workspaces/{workspace}/styles", "styles", "style"),
    }


def verify_ows(base_url: str, workspace: str, layer: str, bbox: list[float], auth: tuple[str, str] | None) -> dict[str, Any]:
    """Request a WMS map and a WFS feature collection, with or without a login."""
    base = base_url.rstrip("/")
    min_lon, min_lat, max_lon, max_lat = bbox
    wms = requests.get(
        f"{base}/{workspace}/wms",
        params={
            "service": "WMS", "version": "1.1.1", "request": "GetMap", "layers": f"{workspace}:{layer}",
            "styles": "", "bbox": f"{min_lon},{min_lat},{max_lon},{max_lat}", "width": 256, "height": 256,
            "srs": "EPSG:4326", "format": "image/png",
        },
        auth=auth, timeout=60,
    )
    wfs = requests.get(
        f"{base}/{workspace}/ows",
        params={
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": f"{workspace}:{layer}", "outputFormat": "application/json",
        },
        auth=auth, timeout=60,
    )
    features: list[dict] = []
    if wfs.status_code == 200 and "json" in wfs.headers.get("Content-Type", ""):
        features = wfs.json().get("features", [])
    return {
        "wms_status": wms.status_code,
        "wms_type": wms.headers.get("Content-Type", ""),
        "wfs_status": wfs.status_code,
        "wfs_features": len(features),
        "wfs_properties": sorted({key for feature in features for key in feature.get("properties", {})}),
    }
