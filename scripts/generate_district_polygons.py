#!/usr/bin/env python3
"""Generate approximate Singapore postal-district polygons.

Official D01-D28 postal-district boundary polygons are not exposed by data.gov.sg.
This script derives a lightweight choropleth geography from:
  1. data.gov.sg URA Master Plan Planning Area Boundary (No Sea), used as the land mask
  2. OneMap postal-code point dump from xkjyeah/singapore-postal-codes
  3. SingPost postal-sector -> district mapping

Method: assign a fine lon/lat grid over Singapore land to the nearest postal-code
point's district, dissolve grid cells by D-code, and simplify for browser use.
Only the main island is retained for the dashboard map.
"""

from __future__ import annotations

import json
import math
import urllib.request
from collections import defaultdict
from pathlib import Path

from scipy.spatial import cKDTree
from shapely.geometry import Point, box, shape, mapping
from shapely.ops import unary_union
from shapely.prepared import prep

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "assets" / "postal-districts-derived.geojson"
CACHE = ROOT / "data" / "geo_cache"
PLANNING_AREA_DATASET = "d_2cc750190544007400b2cfd5d7f53209"
PLANNING_AREA_CACHE = CACHE / "MasterPlan2025PlanningAreaBoundaryNoSea.geojson"
POSTAL_POINTS_CACHE = CACHE / "onemap-postal-code-points.json"
POSTAL_POINTS_URL = "https://raw.githubusercontent.com/xkjyeah/singapore-postal-codes/master/buildings.json"

SECTOR_TO_DISTRICT = {
    **dict.fromkeys(["01", "02", "03", "04", "05", "06"], "D01"),
    **dict.fromkeys(["07", "08"], "D02"),
    **dict.fromkeys(["14", "15", "16"], "D03"),
    **dict.fromkeys(["09", "10"], "D04"),
    **dict.fromkeys(["11", "12", "13"], "D05"),
    "17": "D06",
    **dict.fromkeys(["18", "19"], "D07"),
    **dict.fromkeys(["20", "21"], "D08"),
    **dict.fromkeys(["22", "23"], "D09"),
    **dict.fromkeys(["24", "25", "26", "27"], "D10"),
    **dict.fromkeys(["28", "29", "30"], "D11"),
    **dict.fromkeys(["31", "32", "33"], "D12"),
    **dict.fromkeys(["34", "35", "36", "37"], "D13"),
    **dict.fromkeys(["38", "39", "40", "41"], "D14"),
    **dict.fromkeys(["42", "43", "44", "45"], "D15"),
    **dict.fromkeys(["46", "47", "48"], "D16"),
    **dict.fromkeys(["49", "50", "81"], "D17"),
    **dict.fromkeys(["51", "52"], "D18"),
    **dict.fromkeys(["53", "54", "55", "82"], "D19"),
    **dict.fromkeys(["56", "57"], "D20"),
    **dict.fromkeys(["58", "59"], "D21"),
    **dict.fromkeys(["60", "61", "62", "63", "64"], "D22"),
    **dict.fromkeys(["65", "66", "67", "68"], "D23"),
    **dict.fromkeys(["69", "70", "71"], "D24"),
    **dict.fromkeys(["72", "73"], "D25"),
    **dict.fromkeys(["77", "78"], "D26"),
    **dict.fromkeys(["75", "76"], "D27"),
    **dict.fromkeys(["79", "80"], "D28"),
}


def fetch_json(url: str) -> dict | list:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=90) as resp:
        return json.load(resp)


def download_data_gov_geojson(dataset_id: str, path: Path) -> None:
    poll_url = f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/poll-download"
    payload = fetch_json(poll_url)
    url = payload["data"]["url"]
    path.write_text(json.dumps(fetch_json(url)))


def ensure_sources() -> None:
    CACHE.mkdir(parents=True, exist_ok=True)
    if not PLANNING_AREA_CACHE.exists():
        download_data_gov_geojson(PLANNING_AREA_DATASET, PLANNING_AREA_CACHE)
    if not POSTAL_POINTS_CACHE.exists():
        POSTAL_POINTS_CACHE.write_text(json.dumps(fetch_json(POSTAL_POINTS_URL)))


def grid_range(lo: float, hi: float, step: float):
    x = math.floor(lo / step) * step
    while x <= hi:
        yield x
        x += step


def main_island_only(geometry):
    """Return the largest contiguous land polygon, dropping offshore islands."""
    if geometry.geom_type == "Polygon":
        return geometry
    if geometry.geom_type == "MultiPolygon":
        return max(geometry.geoms, key=lambda geom: geom.area)
    polygons = [geom for geom in getattr(geometry, "geoms", []) if geom.geom_type == "Polygon"]
    if not polygons:
        raise ValueError(f"Cannot extract main island from {geometry.geom_type}")
    return max(polygons, key=lambda geom: geom.area)


def main() -> None:
    ensure_sources()
    planning = json.loads(PLANNING_AREA_CACHE.read_text())
    land = main_island_only(unary_union([shape(f["geometry"]) for f in planning["features"]]))
    prepared_land = prep(land)
    minx, miny, maxx, maxy = land.bounds

    points = []
    districts = []
    for row in json.loads(POSTAL_POINTS_CACHE.read_text()):
        postal = str(row.get("POSTAL", "")).zfill(6)
        district = SECTOR_TO_DISTRICT.get(postal[:2])
        if not district:
            continue
        try:
            lon = float(row.get("LONGITUDE") or row.get("LONGTITUDE"))
            lat = float(row["LATITUDE"])
        except (TypeError, ValueError, KeyError):
            continue
        if 103.55 <= lon <= 104.15 and 1.15 <= lat <= 1.55 and prepared_land.contains(Point(lon, lat)):
            points.append((lon, lat))
            districts.append(district)

    tree = cKDTree(points)
    # ~165m latitude. Small enough to look geographic, large enough to stay lightweight.
    step = 0.0015
    cells_by_district: dict[str, list] = defaultdict(list)
    for x in grid_range(minx, maxx, step):
        for y in grid_range(miny, maxy, step):
            center = Point(x + step / 2, y + step / 2)
            if not prepared_land.contains(center):
                continue
            _, idx = tree.query((center.x, center.y), k=1)
            cell = box(x, y, x + step, y + step).intersection(land)
            if not cell.is_empty:
                cells_by_district[districts[int(idx)]].append(cell)

    features = []
    for code in sorted(cells_by_district):
        geom = unary_union(cells_by_district[code]).simplify(0.0009, preserve_topology=True)
        features.append({
            "type": "Feature",
            "properties": {"district_code": code},
            "geometry": mapping(geom),
        })

    output = {
        "type": "FeatureCollection",
        "metadata": {
            "name": "Derived Singapore postal district polygons",
            "method": "Nearest-postal-code grid dissolved by SingPost postal-sector mapping; clipped to the largest contiguous polygon from data.gov.sg URA Master Plan 2025 Planning Area Boundary (No Sea), so offshore islands are excluded.",
            "caveat": "Derived analytical geography, not an official postal-district boundary dataset.",
            "sources": [
                "data.gov.sg d_2cc750190544007400b2cfd5d7f53209 Master Plan 2025 Planning Area Boundary (No Sea)",
                "xkjyeah/singapore-postal-codes OneMap postal-code point dump",
                "SingPost postal sector to district mapping",
            ],
        },
        "features": features,
    }
    OUT.write_text(json.dumps(output, separators=(",", ":")))
    print(f"wrote {OUT} ({OUT.stat().st_size / 1024:.1f} KiB, {len(features)} features)")


if __name__ == "__main__":
    main()
