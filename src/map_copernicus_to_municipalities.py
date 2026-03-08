from __future__ import annotations

import argparse
import ast
import csv
import json
import os
import sys
from pathlib import Path

import geopandas as gpd
import psycopg2
from shapely.geometry import Point, shape


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "data" / "fires" / "copernicus_2025.csv"
DEFAULT_GEOJSON = ROOT / "data" / "geo" / "municipalities.geojson"
DEFAULT_OUTPUT = ROOT / "data" / "fires" / "copernicus_2025_with_municipality.csv"
WORKING_CRS = "EPSG:2100"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Attach municipality codes to Copernicus fire records by spatial overlap.",
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT, help="Input Copernicus CSV")
    parser.add_argument("--geojson", type=Path, default=DEFAULT_GEOJSON, help="Municipalities GeoJSON")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output CSV with municipality fields")
    parser.add_argument("--db-path", type=str, default=None, help="Optional DATABASE_URL override")
    return parser.parse_args()


def parse_shape(raw: str | None):
    text = str(raw or "").strip()
    if not text:
      return None
    try:
        return shape(ast.literal_eval(text))
    except Exception:
        return None


def parse_centroid(raw: str | None):
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        payload = ast.literal_eval(text)
    except Exception:
        return None
    coords = payload.get("coordinates") if isinstance(payload, dict) else None
    if not isinstance(coords, (list, tuple)) or len(coords) != 2:
        return None
    lon, lat = coords
    try:
        return Point(float(lon), float(lat))
    except Exception:
        return None


def load_fires(path: Path) -> gpd.GeoDataFrame:
    csv.field_size_limit(sys.maxsize)
    with path.open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    geometry = []
    methods = []
    normalized_rows = []
    for row in rows:
        fire_shape = parse_shape(row.get("shape"))
        fire_centroid = parse_centroid(row.get("centroid"))
        if fire_shape is not None:
            geometry.append(fire_shape)
            methods.append("shape")
        else:
            geometry.append(fire_centroid)
            methods.append("centroid")
        normalized_rows.append(dict(row))

    gdf = gpd.GeoDataFrame(normalized_rows, geometry=geometry, crs="EPSG:4326")
    gdf["match_geometry"] = methods
    return gdf[gdf.geometry.notnull()].copy()


def load_municipalities(path: Path) -> gpd.GeoDataFrame:
    municipalities = gpd.read_file(path)[["municipality_code", "name", "geometry"]].copy()
    municipalities["municipality_code"] = municipalities["municipality_code"].astype(str)
    municipalities["name"] = municipalities["name"].astype(str)
    return municipalities


def resolve_database_url(db_path: str | None) -> str:
    if db_path:
        return db_path

    env_value = os.getenv("DATABASE_URL", "").strip()
    if env_value:
        return env_value

    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() == "DATABASE_URL" and value.strip():
                return value.strip().strip("'\"")

    raise ValueError("Δεν βρέθηκε DATABASE_URL ούτε δόθηκε db_path.")


def load_normalized_name_lookup(db_path: str | None) -> dict[str, str]:
    db_url = resolve_database_url(db_path)
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute("""
        SELECT municipality_key, municipality_normalized_value
        FROM public.municipality_normalized_name
        WHERE municipality_key IS NOT NULL
          AND municipality_normalized_value IS NOT NULL
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {
        str(municipality_key).strip(): str(municipality_normalized_value).strip()
        for municipality_key, municipality_normalized_value in rows
        if municipality_key is not None and municipality_normalized_value is not None
    }


def assign_municipalities(
    fires: gpd.GeoDataFrame,
    municipalities: gpd.GeoDataFrame,
    normalized_name_lookup: dict[str, str],
) -> gpd.GeoDataFrame:
    fires_metric = fires.to_crs(WORKING_CRS)
    municipalities_metric = municipalities.to_crs(WORKING_CRS)

    output = fires.copy()
    output["municipality_key"] = None
    output["municipality_name"] = None
    output["municipality_normalized_value"] = None
    output["municipality_match_method"] = None
    output["municipality_overlap_ratio"] = None

    sindex = municipalities_metric.sindex

    for idx, fire_row in fires_metric.iterrows():
        geom = fire_row.geometry
        if geom is None or geom.is_empty:
            continue

        candidate_idx = list(sindex.intersection(geom.bounds))
        if not candidate_idx:
            continue

        candidates = municipalities_metric.iloc[candidate_idx]
        intersections = candidates[candidates.intersects(geom)]
        if intersections.empty:
            continue

        if geom.geom_type in {"Polygon", "MultiPolygon"}:
            best_code = None
            best_name = None
            best_area = 0.0
            total_area = geom.area if geom.area > 0 else None
            for _, municipality_row in intersections.iterrows():
                overlap_area = geom.intersection(municipality_row.geometry).area
                if overlap_area > best_area:
                    best_area = overlap_area
                    best_code = municipality_row["municipality_code"]
                    best_name = municipality_row["name"]

            if best_code is not None:
                output.at[idx, "municipality_key"] = best_code
                output.at[idx, "municipality_name"] = best_name
                output.at[idx, "municipality_normalized_value"] = normalized_name_lookup.get(str(best_code).strip(), best_name)
                output.at[idx, "municipality_match_method"] = "shape_max_overlap"
                if total_area:
                    output.at[idx, "municipality_overlap_ratio"] = round(best_area / total_area, 6)
            continue

        containing = intersections[intersections.contains(geom)]
        target = containing.iloc[0] if not containing.empty else intersections.iloc[0]
        output.at[idx, "municipality_key"] = target["municipality_code"]
        output.at[idx, "municipality_name"] = target["name"]
        output.at[idx, "municipality_normalized_value"] = normalized_name_lookup.get(
            str(target["municipality_code"]).strip(),
            target["name"],
        )
        output.at[idx, "municipality_match_method"] = "centroid_contains"
        output.at[idx, "municipality_overlap_ratio"] = 1.0

    return output


def main() -> None:
    args = parse_args()
    fires = load_fires(args.input)
    municipalities = load_municipalities(args.geojson)
    normalized_name_lookup = load_normalized_name_lookup(args.db_path)
    enriched = assign_municipalities(fires, municipalities, normalized_name_lookup)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    enriched = enriched.drop(columns=["geometry"])
    enriched.to_csv(args.output, index=False)

    matched = enriched["municipality_key"].notna().sum()
    total = len(enriched)
    unmatched = total - matched
    by_method = enriched["municipality_match_method"].fillna("unmatched").value_counts().to_dict()

    print(json.dumps({
        "input": str(args.input),
        "output": str(args.output),
        "total_rows": total,
        "matched_rows": int(matched),
        "unmatched_rows": int(unmatched),
        "match_methods": by_method,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
