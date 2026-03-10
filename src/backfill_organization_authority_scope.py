from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from ingest.stage2_load_erd import authority_scope_key_map
from src.map_copernicus_to_municipalities import resolve_database_url


def main() -> None:
    db_url = resolve_database_url(None)
    org_map_path = ROOT / "data" / "mappings" / "org_to_municipality.csv"
    org_map = pd.read_csv(org_map_path, dtype=str).fillna("")
    updates = authority_scope_key_map(org_map)

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute("ALTER TABLE public.organization ADD COLUMN IF NOT EXISTS authority_scope TEXT")
    updated = 0
    for org_key, authority_scope in updates.items():
        cur.execute(
            """
            UPDATE public.organization
            SET authority_scope = %s
            WHERE organization_key = %s
            """,
            (authority_scope, org_key),
        )
        updated += cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    print(json.dumps({
        "organization_keys_with_scope": len(updates),
        "rows_updated": updated,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
