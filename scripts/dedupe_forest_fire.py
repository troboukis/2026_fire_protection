from __future__ import annotations

import argparse
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.map_copernicus_to_municipalities import resolve_database_url


DEDUP_CTE = """
WITH ranked AS (
  SELECT
    id,
    ROW_NUMBER() OVER (
      PARTITION BY
        municipality_key,
        region_key,
        year,
        date_start,
        date_end,
        nomos,
        area_name,
        lat,
        lon,
        burned_forest_stremata,
        burned_woodland_stremata,
        burned_grassland_stremata,
        burned_grove_stremata,
        burned_other_stremata,
        burned_total_stremata,
        burned_total_ha,
        source
      ORDER BY id
    ) AS rn
  FROM public.forest_fire
)
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Remove duplicate rows from public.forest_fire")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Delete duplicate rows. Default is dry-run preview only.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn = psycopg2.connect(resolve_database_url(None))
    cur = conn.cursor()

    cur.execute(
        DEDUP_CTE
        + """
        SELECT COUNT(*)
        FROM ranked
        WHERE rn > 1
        """
    )
    duplicate_rows = int(cur.fetchone()[0])
    print(f"duplicate_rows={duplicate_rows}")

    if not args.apply:
        conn.rollback()
        cur.close()
        conn.close()
        return

    cur.execute(
        DEDUP_CTE
        + """
        DELETE FROM public.forest_fire AS ff
        USING ranked
        WHERE ff.id = ranked.id
          AND ranked.rn > 1
        """
    )
    deleted_rows = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    print(f"deleted_rows={deleted_rows}")


if __name__ == "__main__":
    main()
