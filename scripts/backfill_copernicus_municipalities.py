from __future__ import annotations

import argparse
import json

import geopandas as gpd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

from src.map_copernicus_to_municipalities import (
    DEFAULT_GEOJSON,
    assign_municipalities,
    load_municipalities,
    parse_centroid,
    parse_shape,
    resolve_database_url,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild Copernicus-to-municipality overlaps from geometries already stored in Postgres.",
    )
    parser.add_argument("--db-path", default=None, help="Optional DATABASE_URL override")
    parser.add_argument("--geojson", default=DEFAULT_GEOJSON, help="Municipality boundary GeoJSON")
    parser.add_argument("--apply", action="store_true", help="Apply the replacement transaction")
    return parser.parse_args()


def load_source_rows(conn) -> tuple[list[dict], dict[str, str]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SET statement_timeout = '60s'")
        cur.execute(
            """
            SELECT copernicus_id AS id, area_ha, shape, centroid
            FROM public.copernicus
            ORDER BY copernicus_id
            """,
        )
        rows = [dict(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT municipality_key, municipality_normalized_value
            FROM public.municipality_normalized_name
            WHERE municipality_key IS NOT NULL
              AND municipality_normalized_value IS NOT NULL
            """,
        )
        normalized_names = {
            str(row["municipality_key"]).strip(): str(row["municipality_normalized_value"]).strip()
            for row in cur.fetchall()
        }
    conn.rollback()
    return rows, normalized_names


def build_fire_geodataframe(rows: list[dict]) -> tuple[gpd.GeoDataFrame, int]:
    records = []
    geometries = []
    skipped = 0
    for row in rows:
        geometry = parse_shape(row.get("shape")) or parse_centroid(row.get("centroid"))
        if geometry is None or geometry.is_empty:
            skipped += 1
            continue
        records.append({"id": row["id"], "area_ha": row.get("area_ha")})
        geometries.append(geometry)
    return gpd.GeoDataFrame(records, geometry=geometries, crs="EPSG:4326"), skipped


def flatten_matches(enriched: gpd.GeoDataFrame) -> list[tuple]:
    values = []
    for _, row in enriched.iterrows():
        copernicus_id = int(row["id"])
        for match in row["municipality_matches"]:
            values.append((
                copernicus_id,
                match["municipality_key"],
                match["match_method"],
                match["overlap_ratio"],
                match["overlap_area_ha"],
            ))
    return values


def apply_backfill(conn, processed_ids: list[int], matches: list[tuple]) -> None:
    if not processed_ids or not matches:
        raise RuntimeError("Refusing to replace municipality matches with an empty result.")

    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '120s'")
        cur.execute(
            "DELETE FROM public.copernicus_municipality WHERE copernicus_id = ANY(%s)",
            (processed_ids,),
        )
        execute_values(
            cur,
            """
            INSERT INTO public.copernicus_municipality (
              copernicus_id,
              municipality_key,
              match_method,
              overlap_ratio,
              overlap_area_ha
            ) VALUES %s
            """,
            matches,
            page_size=1000,
        )
    conn.commit()


def main() -> None:
    args = parse_args()
    conn = psycopg2.connect(resolve_database_url(args.db_path))
    try:
        rows, normalized_names = load_source_rows(conn)
        fires, skipped = build_fire_geodataframe(rows)
        municipalities = load_municipalities(args.geojson)
        enriched = assign_municipalities(fires, municipalities, normalized_names)
        matches = flatten_matches(enriched)
        processed_ids = [int(value) for value in enriched["id"].tolist()]
        fires_with_multiple_municipalities = sum(
            1 for municipality_matches in enriched["municipality_matches"] if len(municipality_matches) > 1
        )
        target_matches = [
            {
                "municipality_key": match[1],
                "overlap_ratio": match[3],
                "overlap_area_ha": match[4],
            }
            for match in matches
            if match[0] == 601665
        ]

        summary = {
            "mode": "apply" if args.apply else "dry-run",
            "source_rows": len(rows),
            "processed_fires": len(processed_ids),
            "skipped_without_geometry": skipped,
            "municipality_matches": len(matches),
            "fires_with_multiple_municipalities": fires_with_multiple_municipalities,
            "target_601665_matches": target_matches,
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))

        if args.apply:
            apply_backfill(conn, processed_ids, matches)
            print(json.dumps({"applied": True, "inserted_matches": len(matches)}, ensure_ascii=False))
    finally:
        conn.rollback()
        conn.close()


if __name__ == "__main__":
    main()
