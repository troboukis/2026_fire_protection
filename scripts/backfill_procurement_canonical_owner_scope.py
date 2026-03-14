from __future__ import annotations

import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingest.stage2_load_erd import (
    build_maps,
    build_municipality_alias_lookup,
    build_municipality_lookup,
    build_municipality_region_lookup,
    build_org_municipality_coverage_lookup,
    build_organization_lookup,
    build_region_lookup,
    procurement_rows,
    read_csvs,
    seed_municipality_rows,
    seed_organization_rows,
    seed_region_rows,
)
from src.map_copernicus_to_municipalities import resolve_database_url


def build_scope_rows() -> list[tuple[str, str | None]]:
    bundle = read_csvs()
    org_map = build_maps(bundle.org_map, bundle.expanded_map)
    municipality_seed = seed_municipality_rows(bundle)
    organization_lookup = build_organization_lookup(seed_organization_rows(bundle))
    region_lookup = build_region_lookup(seed_region_rows(bundle))
    municipality_lookup = build_municipality_lookup(municipality_seed)
    municipality_region_lookup = build_municipality_region_lookup(municipality_seed)
    municipality_alias_lookup = build_municipality_alias_lookup(municipality_seed)
    org_municipality_coverage_lookup = build_org_municipality_coverage_lookup(bundle.expanded_map)
    procurement_data = procurement_rows(
        raw=bundle.raw,
        org_map=org_map,
        organization_lookup=organization_lookup,
        region_lookup=region_lookup,
        municipality_lookup=municipality_lookup,
        municipality_region_lookup=municipality_region_lookup,
        municipality_alias_lookup=municipality_alias_lookup,
        org_municipality_coverage_lookup=org_municipality_coverage_lookup,
    )
    return [
        (str(reference_number).strip(), canonical_owner_scope)
        for _, reference_number, *rest, canonical_owner_scope in procurement_data
        if str(reference_number).strip()
    ]


def main() -> None:
    rows = build_scope_rows()
    conn = psycopg2.connect(resolve_database_url(None))
    cur = conn.cursor()
    execute_values(
        cur,
        """
        UPDATE public.procurement AS p
        SET canonical_owner_scope = src.canonical_owner_scope,
            updated_at = NOW()
        FROM (VALUES %s) AS src(reference_number, canonical_owner_scope)
        WHERE p.reference_number = src.reference_number
        """,
        rows,
        page_size=1000,
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"backfilled_rows={len(rows)}")


if __name__ == "__main__":
    main()
