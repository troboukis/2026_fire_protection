#!/usr/bin/env python3
"""Correct Diavgeia geography produced from ambiguous organization coverage."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ingest.stage2_load_erd import (  # noqa: E402
    build_diavgeia_maps,
    build_municipality_lookup,
    build_municipality_region_lookup,
    build_region_lookup,
    read_csvs,
    resolve_diavgeia_context,
    seed_municipality_rows,
    seed_region_rows,
)


def load_database_url() -> str:
    value = os.environ.get("DATABASE_URL", "").strip()
    if value:
        return value
    for raw_line in (REPO_ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, candidate = line.split("=", 1)
        if key.strip() == "DATABASE_URL" and candidate.strip():
            return candidate.strip()
    raise RuntimeError("DATABASE_URL is required")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Apply corrections; default is dry-run")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    bundle = read_csvs()
    geography_map = build_diavgeia_maps(bundle.org_map, bundle.expanded_map)
    region_lookup = build_region_lookup(seed_region_rows(bundle))
    municipality_rows = seed_municipality_rows(bundle)
    municipality_lookup = build_municipality_lookup(municipality_rows)
    municipality_region_lookup = build_municipality_region_lookup(municipality_rows)

    conn = psycopg2.connect(load_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, org_type, org_name_clean, region_key, municipality_key
                FROM public.diavgeia
                ORDER BY id
                """
            )
            corrections: list[tuple[str | None, str | None, int]] = []
            samples: list[tuple] = []
            for row_id, org_type, org_name, old_region, old_municipality in cur.fetchall():
                new_region, _, new_municipality = resolve_diavgeia_context(
                    org_type=org_type or "",
                    org_name=org_name or "",
                    org_map=geography_map,
                    organization_lookup={},
                    region_lookup=region_lookup,
                    municipality_lookup=municipality_lookup,
                    municipality_region_lookup=municipality_region_lookup,
                )
                if (old_region, old_municipality) == (new_region, new_municipality):
                    continue
                corrections.append((new_region, new_municipality, row_id))
                if len(samples) < 10:
                    samples.append(
                        (row_id, org_type, org_name, old_region, old_municipality, new_region, new_municipality)
                    )

            print(f"Corrections required: {len(corrections)}")
            for sample in samples:
                print(sample)

            if not args.apply:
                conn.rollback()
                print("Dry run only; use --apply to update the database")
                return 0

            psycopg2.extras.execute_values(
                cur,
                """
                UPDATE public.diavgeia AS d
                SET region_key = changes.region_key,
                    municipality_key = changes.municipality_key,
                    updated_at = NOW()
                FROM (VALUES %s) AS changes(region_key, municipality_key, id)
                WHERE d.id = changes.id
                """,
                corrections,
                page_size=500,
            )
        conn.commit()
        print(f"Applied corrections: {len(corrections)}")
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
