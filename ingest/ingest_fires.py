"""
ingest_fires.py
---------------
Loads harmonized fire incidents (2000–2024) into Supabase.

Source:
  data/fires/fire_incidents_unified.csv  (84,693 rows)

Usage:
  python ingest/ingest_fires.py

Prerequisites:
  - 002_webapp_schema.sql applied
  - municipalities table populated (ingest_municipalities.py)
  - .env with DATABASE_URL
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

REPO_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = REPO_DIR / "data" / "fires" / "fire_incidents_unified.csv"


def clean_municipality_raw(val: str | float) -> str | None:
    """Strip list syntax: "['ΑΘΗΝΑΣ']" → "ΑΘΗΝΑΣ". Return None if empty."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    # Remove leading/trailing brackets and quotes from list repr
    s = re.sub(r"^\[", "", s)
    s = re.sub(r"\]$", "", s)
    s = s.strip("'\"").strip()
    return s if s else None


def coerce_float(val) -> float | None:
    try:
        f = float(val)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


def coerce_date(val) -> str | None:
    if pd.isna(val) or str(val).strip() in ("", "nan", "NaT"):
        return None
    return str(val).strip()


def main() -> None:
    db_url = os.environ["DATABASE_URL"]
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()

    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False, na_values=[""])
    print(f"Loaded {len(df)} rows from {CSV_PATH.name}")

    rows: list[tuple] = []
    for _, r in df.iterrows():
        rows.append((
            int(r["fire_id"]),
            int(r["year"]) if r["year"] else None,
            coerce_date(r.get("date_start")),
            coerce_date(r.get("date_end")),
            r.get("nomos") or None,
            clean_municipality_raw(r.get("municipality_raw")),
            r.get("area_name") or None,
            coerce_float(r.get("lat")),
            coerce_float(r.get("lon")),
            coerce_float(r.get("burned_forest_stremata")),
            coerce_float(r.get("burned_woodland_stremata")),
            coerce_float(r.get("burned_grove_stremata")),
            coerce_float(r.get("burned_grassland_stremata")),
            coerce_float(r.get("burned_other_stremata")),
            coerce_float(r.get("burned_total_stremata")),
            coerce_float(r.get("burned_total_ha")),
            r.get("source") or None,
        ))

    upsert_sql = """
        INSERT INTO public.fire_incidents (
            fire_id, year, date_start, date_end,
            nomos, municipality_raw, area_name,
            lat, lon,
            burned_forest_stremata, burned_woodland_stremata,
            burned_grove_stremata, burned_grassland_stremata,
            burned_other_stremata, burned_total_stremata,
            burned_total_ha, source
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s
        )
        ON CONFLICT (fire_id) DO UPDATE SET
            year                      = EXCLUDED.year,
            date_start                = EXCLUDED.date_start,
            date_end                  = EXCLUDED.date_end,
            nomos                     = EXCLUDED.nomos,
            municipality_raw          = EXCLUDED.municipality_raw,
            area_name                 = EXCLUDED.area_name,
            lat                       = EXCLUDED.lat,
            lon                       = EXCLUDED.lon,
            burned_forest_stremata    = EXCLUDED.burned_forest_stremata,
            burned_woodland_stremata  = EXCLUDED.burned_woodland_stremata,
            burned_grove_stremata     = EXCLUDED.burned_grove_stremata,
            burned_grassland_stremata = EXCLUDED.burned_grassland_stremata,
            burned_other_stremata     = EXCLUDED.burned_other_stremata,
            burned_total_stremata     = EXCLUDED.burned_total_stremata,
            burned_total_ha           = EXCLUDED.burned_total_ha,
            source                    = EXCLUDED.source;
    """

    print("Inserting...", flush=True)
    psycopg2.extras.execute_batch(cur, upsert_sql, rows, page_size=500)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM public.fire_incidents;")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM public.fire_incidents WHERE lat IS NOT NULL;")
    with_coords = cur.fetchone()[0]
    cur.execute("SELECT MIN(year), MAX(year) FROM public.fire_incidents;")
    yr_min, yr_max = cur.fetchone()

    print(f"\n[done] fire_incidents table:")
    print(f"  Total rows:    {total}")
    print(f"  With coords:   {with_coords}")
    print(f"  Year range:    {yr_min}–{yr_max}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
