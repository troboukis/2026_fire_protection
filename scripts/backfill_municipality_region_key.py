from __future__ import annotations

import csv
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_batch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.map_copernicus_to_municipalities import resolve_database_url


CSV_PATH = ROOT / "data" / "mappings" / "region_to_municipalities.csv"


def load_rows() -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    with CSV_PATH.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            municipality_key = str(row.get("municipality_id") or "").strip()
            region_key = str(row.get("region_id") or "").strip()
            pair = (municipality_key, region_key)
            if not municipality_key or not region_key or pair in seen:
                continue
            seen.add(pair)
            rows.append(pair)
    return rows


def main() -> None:
    rows = load_rows()
    conn = psycopg2.connect(resolve_database_url(None))
    cur = conn.cursor()
    execute_batch(
        cur,
        """
        UPDATE public.municipality
        SET region_key = %s,
            updated_at = NOW()
        WHERE municipality_key = %s
        """,
        [(region_key, municipality_key) for municipality_key, region_key in rows],
        page_size=500,
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"backfilled_rows={len(rows)}")


if __name__ == "__main__":
    main()
