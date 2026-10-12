"""Generate sample shapefiles for the geo pipeline.

Writes a good critical_facilities layer (5 points, WGS 84) and five deliberately
bad ones, one defect each. Every person, phone number, email and TIN is invented.

The shapefile column names are the 10-character DBF names an agency file would
really have, including the misspelled facity_Id. config/geo/critical_facilities.yaml
maps them back to the canonical names.

Run in the geo environment (it needs geopandas). sample_data is mounted read-only
in Airflow, so write to a mounted output folder:

    mkdir -p sample_data/geo
    docker compose -f docker-compose.demo.yml -f docker-compose.geo.yml run --rm -T --no-deps \
      --entrypoint /opt/airflow/geo-venv/bin/python \
      --user "$(id -u):0" -e HOME=/tmp -e GEO_SAMPLE_OUT=/out -v "$PWD/sample_data/geo:/out" \
      airflow - < sample_data/geo/make_sample.py

(--entrypoint is needed because the Airflow image's start script otherwise treats
the command as an `airflow` subcommand.)
"""
from __future__ import annotations

import os
from pathlib import Path

import geopandas as gpd
from shapely.geometry import LineString, Point

OUT = Path(os.getenv("GEO_SAMPLE_OUT") or Path(__file__).resolve().parent)

# (id, name, type, address, lon, lat, contact, phone, email, remarks)
FACILITIES = [
    ("F001", "Sample District Hospital", "hospital", "1 Example Street, Manila", 120.9842, 14.5995,
     "Juan dela Cruz", "0917 123 4567", "juan.delacruz@example.org",
     "Call Engr. Juan dela Cruz at 0917 123 4567 about the generator"),
    ("F002", "Sample Central School", "school", "2 Example Street, Quezon City", 121.0437, 14.6760,
     "Maria Santos", "0928 765 4321", "maria.santos@example.org",
     "Contact maria.santos@example.org, TIN 123-456-789-000"),
    ("F003", "Sample Rural Health Unit", "government", "3 Example Street, Laguna", 121.1653, 14.2117,
     "Pedro Reyes", "(02) 8123 4567", "pedro.reyes@example.org",
     "No contact details recorded"),
    ("F004", "Sample Barangay Hall", "government", "4 Example Street, Cebu", 123.8854, 10.3157,
     "Ana Lim", "0939 555 0123", "ana.lim@example.org",
     "Night guard is Jose Mercado, mobile 0917 000 1111"),
    ("F005", "Sample Provincial Hospital", "hospital", "5 Example Street, Davao", 125.6128, 7.0731,
     "Liza Ramos", "0905 222 3344", "liza.ramos@example.org",
     ""),
]


def frame(rows=FACILITIES, geometry=None, crs="EPSG:4326") -> gpd.GeoDataFrame:
    """Column names are the DBF names, at most 10 characters."""
    data = {
        "facity_Id": [r[0] for r in rows],
        "name": [r[1] for r in rows],
        "facility_t": [r[2] for r in rows],
        "address": [r[3] for r in rows],
        "contact_pe": [r[6] for r in rows],
        "contact_ph": [r[7] for r in rows],
        "contact_em": [r[8] for r in rows],
        "remarks": [r[9] for r in rows],
    }
    geoms = geometry if geometry is not None else [Point(r[4], r[5]) for r in rows]
    return gpd.GeoDataFrame(data, geometry=geoms, crs=crs)


def write(gdf: gpd.GeoDataFrame, folder: Path, name: str) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    gdf.to_file(folder / f"{name}.shp", driver="ESRI Shapefile", engine="pyogrio", encoding="utf-8")
    print(f"wrote {folder / (name + '.shp')}  ({len(gdf)} features, CRS {gdf.crs.to_string() if gdf.crs else 'none'})")


def main() -> None:
    write(frame(), OUT / "critical_facilities", "critical_facilities")

    bad = OUT / "failure_demo"

    # .prj says Web Mercator, not WGS 84
    write(frame().to_crs("EPSG:3857"), bad / "wrong_crs", "wrong_crs")

    # latitude and longitude swapped: every point lands outside the Philippines
    swapped = [Point(r[5], r[4]) for r in FACILITIES]
    write(frame(geometry=swapped), bad / "swapped_coordinates", "swapped_coordinates")

    # lines where the contract says points
    lines = [LineString([(r[4], r[5]), (r[4] + 0.01, r[5] + 0.01)]) for r in FACILITIES]
    write(frame(geometry=lines), bad / "wrong_geometry_type", "wrong_geometry_type")

    # F001 twice
    duplicated = [FACILITIES[0]] + FACILITIES[:4]
    write(frame(rows=duplicated), bad / "duplicate_key", "duplicate_key")

    # an empty name and a facility type that is not allowed
    broken = list(FACILITIES)
    broken[1] = (broken[1][0], "", *broken[1][2:])
    broken[2] = (broken[2][0], broken[2][1], "airport", *broken[2][3:])
    write(frame(rows=broken), bad / "bad_attributes", "bad_attributes")


if __name__ == "__main__":
    main()
