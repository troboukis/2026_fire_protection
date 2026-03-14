from __future__ import annotations

import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_batch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingest.stage2_load_erd import (
    build_maps,
    build_municipality_lookup,
    build_municipality_region_lookup,
    build_organization_lookup,
    build_region_lookup,
    procurement_rows,
    read_csvs,
    seed_municipality_rows,
    seed_organization_rows,
    seed_region_rows,
)
from src.map_copernicus_to_municipalities import resolve_database_url


def build_rows() -> list[tuple[str | None, str | None, str | None, str | None, str]]:
    bundle = read_csvs()
    org_map = build_maps(bundle.org_map, bundle.expanded_map)
    municipality_seed = seed_municipality_rows(bundle)
    procurement_data = procurement_rows(
        raw=bundle.raw,
        org_map=org_map,
        organization_lookup=build_organization_lookup(seed_organization_rows(bundle)),
        region_lookup=build_region_lookup(seed_region_rows(bundle)),
        municipality_lookup=build_municipality_lookup(municipality_seed),
        municipality_region_lookup=build_municipality_region_lookup(municipality_seed),
    )
    rows: list[tuple[str | None, str | None, str | None, str | None, str]] = []
    for row in procurement_data:
        reference_number = str(row[1] or "").strip()
        if not reference_number:
            continue
        rows.append((row[48], row[49], row[50], row[51], reference_number))
    return rows


def main() -> None:
    rows = build_rows()
    conn = psycopg2.connect(resolve_database_url(None))
    cur = conn.cursor()
    execute_batch(
        cur,
        """
        UPDATE public.procurement
        SET region_key = %s,
            organization_key = %s,
            municipality_key = %s,
            canonical_owner_scope = %s,
            updated_at = NOW()
        WHERE reference_number = %s
        """,
        rows,
        page_size=500,
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"backfilled_rows={len(rows)}")


if __name__ == "__main__":
    main()
