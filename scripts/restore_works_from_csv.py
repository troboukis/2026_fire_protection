#!/usr/bin/env python3
"""
Restore the works table from a CSV backup.

Usage:
  ./.fireprotection/bin/python scripts/restore_works_from_csv.py
  ./.fireprotection/bin/python scripts/restore_works_from_csv.py --csv backupDB/works.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import psycopg2
from psycopg2 import sql

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.map_copernicus_to_municipalities import resolve_database_url

DEFAULT_CSV = ROOT / "backupDB" / "works.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Restore works table from CSV backup.")
    parser.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV,
        help="Path to the works CSV backup file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print row count that would be restored without writing anything.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    csv_path = args.csv.resolve()

    if not csv_path.exists():
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        return 1

    if args.dry_run:
        with csv_path.open(encoding="utf-8") as fh:
            row_count = sum(1 for _ in fh) - 1  # subtract header
        print(f"dry_run: would restore {row_count} rows from {csv_path}")
        return 0

    conn = psycopg2.connect(resolve_database_url(None))
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE public.works RESTART IDENTITY CASCADE")

            # Load into a temp table first so we can filter on the FK
            cur.execute(
                """
                CREATE TEMP TABLE works_restore (LIKE public.works INCLUDING ALL)
                ON COMMIT DROP
                """
            )
            with csv_path.open(encoding="utf-8") as fh:
                cur.copy_expert(
                    sql.SQL("COPY works_restore FROM STDIN WITH CSV HEADER"),
                    fh,
                )
            cur.execute("SELECT COUNT(*) FROM works_restore")
            csv_total = cur.fetchone()[0]

            cur.execute(
                """
                INSERT INTO public.works
                SELECT r.*
                FROM works_restore r
                WHERE EXISTS (
                    SELECT 1 FROM public.procurement p
                    WHERE p.reference_number = r.reference_number
                )
                """
            )
            cur.execute(
                "SELECT setval('public.works_id_seq', COALESCE(MAX(id), 1)) FROM public.works"
            )
            cur.execute("SELECT COUNT(*) FROM public.works")
            row_count = cur.fetchone()[0]

        conn.commit()
        skipped = csv_total - row_count
        print(f"restored rows={row_count} skipped={skipped} (missing procurement FK) from {csv_path}")
        return 0
    except Exception as exc:
        conn.rollback()
        print(f"Restore failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
