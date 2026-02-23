"""
ingest_municipalities.py
------------------------
Loads municipality geometries and forest area data into Supabase.

Sources:
  data/geo/municipalities.geojson        → municipalities.id, name, geom
  data/geo/municipalities_forest_ha.csv  → municipalities.forest_ha

Usage:
  python ingest/ingest_municipalities.py

Prerequisites:
  - 001_init_schema.sql applied in Supabase SQL Editor
  - 002_webapp_schema.sql applied in Supabase SQL Editor
  - .env with DATABASE_URL (direct Postgres connection string)
"""

from __future__ import annotations

import json
import os
import unicodedata
from pathlib import Path

import psycopg2
import psycopg2.extras
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

REPO_DIR   = Path(__file__).resolve().parent.parent
GEOJSON    = REPO_DIR / "data" / "geo" / "municipalities.geojson"
FOREST_CSV = REPO_DIR / "data" / "geo" / "municipalities_forest_ha.csv"


def strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", str(s))
        if unicodedata.category(c) != "Mn"
    )


def norm(s: str) -> str:
    return strip_accents(s).upper().strip()


def main() -> None:
    db_url = os.environ["DATABASE_URL"]
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()

    # ── Load forest area lookup ──────────────────────────────────────────────
    forest_df = pd.read_csv(FOREST_CSV)
    forest_lookup: dict[str, float] = {
        norm(str(r["municipality_name"])): float(r["forest_area_ha"])
        for _, r in forest_df.iterrows()
    }
    print(f"Forest area entries: {len(forest_lookup)}")

    # ── Load GeoJSON ─────────────────────────────────────────────────────────
    with open(GEOJSON) as f:
        gj = json.load(f)

    features = gj["features"]
    print(f"GeoJSON features: {len(features)}")

    rows: list[tuple] = []
    skipped = 0
    forest_matched = 0

    for ft in features:
        props = ft["properties"]
        muni_id = props.get("municipality_code")
        if not muni_id:
            print(f"  [skip] {props.get('name')!r} — no municipality_code")
            skipped += 1
            continue

        name = props["name"]
        geom_geojson = json.dumps(ft["geometry"])
        forest_ha = forest_lookup.get(norm(name))
        if forest_ha is not None:
            forest_matched += 1

        rows.append((muni_id, name, geom_geojson, forest_ha))

    print(f"To insert: {len(rows)} (skipped: {skipped}, with forest data: {forest_matched})")

    # ── Upsert ───────────────────────────────────────────────────────────────
    upsert_sql = """
        INSERT INTO public.municipalities (id, name, geom, forest_ha)
        VALUES (
            %s,
            %s,
            ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)),
            %s
        )
        ON CONFLICT (id) DO UPDATE SET
            name      = EXCLUDED.name,
            geom      = EXCLUDED.geom,
            forest_ha = EXCLUDED.forest_ha;
    """

    psycopg2.extras.execute_batch(cur, upsert_sql, rows, page_size=50)
    conn.commit()

    # ── Verify ───────────────────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM public.municipalities;")
    count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM public.municipalities WHERE geom IS NOT NULL;")
    with_geom = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM public.municipalities WHERE forest_ha IS NOT NULL;")
    with_forest = cur.fetchone()[0]

    print(f"\n[done] municipalities table:")
    print(f"  Total rows:       {count}")
    print(f"  With geometry:    {with_geom}")
    print(f"  With forest data: {with_forest}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
