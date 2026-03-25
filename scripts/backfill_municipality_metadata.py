from __future__ import annotations

import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingest.stage2_load_erd import (
    apply_procurement_chain_dedup,
    build_maps,
    build_municipality_alias_lookup,
    build_municipality_lookup,
    build_municipality_metadata_rows,
    build_municipality_region_lookup,
    build_organization_lookup,
    build_org_municipality_coverage_lookup,
    build_region_lookup,
    execute_values,
    read_csvs,
    seed_municipality_rows,
    seed_organization_rows,
    seed_region_rows,
)
from src.map_copernicus_to_municipalities import resolve_database_url


def main() -> None:
    bundle = read_csvs()
    bundle.raw = apply_procurement_chain_dedup(bundle.raw)

    org_map = build_maps(bundle.org_map, bundle.expanded_map)
    region_seed = seed_region_rows(bundle)
    municipality_seed = seed_municipality_rows(bundle)
    organization_seed = seed_organization_rows(bundle)

    rows = build_municipality_metadata_rows(
        raw=bundle.raw,
        org_map=org_map,
        organization_lookup=build_organization_lookup(organization_seed),
        region_lookup=build_region_lookup(region_seed),
        municipality_lookup=build_municipality_lookup(municipality_seed),
        municipality_region_lookup=build_municipality_region_lookup(municipality_seed),
        municipality_alias_lookup=build_municipality_alias_lookup(municipality_seed),
        org_municipality_coverage_lookup=build_org_municipality_coverage_lookup(bundle.expanded_map),
    )

    conn = psycopg2.connect(resolve_database_url(None))
    cur = conn.cursor()
    execute_values(
        cur,
        """
        UPDATE public.municipality AS m
        SET organization_key = COALESCE(src.organization_key, m.organization_key),
            municipality_afm = COALESCE(src.municipality_afm, m.municipality_afm),
            nuts_postal_code = COALESCE(src.nuts_postal_code, m.nuts_postal_code),
            nuts_city = COALESCE(src.nuts_city, m.nuts_city),
            nuts_code_value = COALESCE(src.nuts_code_value, m.nuts_code_value),
            nuts_code_key = COALESCE(src.nuts_code_key, m.nuts_code_key),
            updated_at = NOW()
        FROM (VALUES %s) AS src (
          municipality_key,
          organization_key,
          municipality_afm,
          nuts_postal_code,
          nuts_city,
          nuts_code_value,
          nuts_code_key
        )
        WHERE m.municipality_key = src.municipality_key
        """,
        rows,
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"backfilled_municipality_keys={len(rows)}")


if __name__ == "__main__":
    main()
