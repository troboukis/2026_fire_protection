#!/usr/bin/env python3
"""
Backfill public.beneficiary.gemi from beneficiary_vat_number using the GEMI API.
The lookup uses the exact digit sequence stored in beneficiary_vat_number.
Rows that are not found in GEMI are marked with gemi = '-1' when --apply is used.

Dry-run is the default. Use --apply to write updates.

Examples:
  ./.fireprotection/bin/python scripts/backfill_beneficiary_gemi.py --limit 25
  ./.fireprotection/bin/python scripts/backfill_beneficiary_gemi.py --limit 25 --apply
  ./.fireprotection/bin/python scripts/backfill_beneficiary_gemi.py --afm 043170596 --apply
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.gemi_api_extract import get_gemi_number_by_afm, normalize_afm
from src.map_copernicus_to_municipalities import resolve_database_url


@dataclass(frozen=True)
class BeneficiaryRow:
    beneficiary_vat_number: str
    beneficiary_name: str | None
    existing_gemi: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill public.beneficiary.gemi from GEMI search by AFM.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write successful GEMI lookups to the database. Default is dry-run.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum number of beneficiaries to process. Use 0 for no limit.",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Offset for selecting beneficiaries from the database.",
    )
    parser.add_argument(
        "--afm",
        action="append",
        default=[],
        help="Specific beneficiary AFM to process. Repeat for multiple AFMs.",
    )
    parser.add_argument(
        "--include-existing",
        action="store_true",
        help="Also process beneficiaries that already have gemi populated.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Seconds to sleep between GEMI API requests.",
    )
    parser.add_argument(
        "--language",
        choices=("el", "en"),
        default="el",
        help="Language sent to the GEMI endpoint.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Optional DATABASE_URL override.",
    )
    return parser.parse_args()


def is_valid_afm(value: str | None) -> bool:
    digits = "".join(re.findall(r"\d", str(value or "")))
    return len(digits) >= 8


def fetch_beneficiaries(conn, args: argparse.Namespace) -> list[BeneficiaryRow]:
    params: list[object] = []
    filters = [
        "beneficiary_vat_number IS NOT NULL",
        "LENGTH(REGEXP_REPLACE(beneficiary_vat_number, '\\D', '', 'g')) >= 8",
    ]

    if args.afm:
        normalized_afms = [normalize_afm(afm) for afm in args.afm if is_valid_afm(afm)]
        if not normalized_afms:
            raise ValueError("No valid --afm values were provided.")
        filters.append("REGEXP_REPLACE(beneficiary_vat_number, '\\D', '', 'g') = ANY(%s)")
        params.append(normalized_afms)
    elif not args.include_existing:
        filters.append("NULLIF(BTRIM(gemi), '') IS NULL")

    limit_clause = ""
    if args.limit > 0:
        limit_clause = "LIMIT %s"
        params.append(args.limit)

    query = f"""
        SELECT beneficiary_vat_number, beneficiary_name, gemi
        FROM public.beneficiary
        WHERE {' AND '.join(filters)}
        ORDER BY beneficiary_vat_number
        {limit_clause}
        OFFSET %s
    """
    params.append(args.offset)

    with conn.cursor() as cur:
        cur.execute(query, params)
        return [
            BeneficiaryRow(
                beneficiary_vat_number=str(row[0]),
                beneficiary_name=row[1],
                existing_gemi=row[2],
            )
            for row in cur.fetchall()
            if is_valid_afm(str(row[0]))
        ]


def update_beneficiary_gemi(conn, beneficiary_vat_number: str, gemi: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.beneficiary
            SET gemi = %s,
                updated_at = NOW()
            WHERE beneficiary_vat_number = %s
            """,
            (gemi, beneficiary_vat_number),
        )


def main() -> int:
    args = parse_args()
    conn = psycopg2.connect(resolve_database_url(args.db_path))
    conn.autocommit = False

    processed = 0
    updated = 0
    not_found = 0
    failed = 0

    try:
        rows = fetch_beneficiaries(conn, args)
        mode = "apply" if args.apply else "dry-run"
        print(f"mode={mode} candidates={len(rows)}")

        for index, row in enumerate(rows, start=1):
            afm = normalize_afm(row.beneficiary_vat_number)
            label = row.beneficiary_name or row.beneficiary_vat_number
            processed += 1

            try:
                gemi = get_gemi_number_by_afm(afm, language=args.language)
            except LookupError as exc:
                not_found += 1
                print(f"[{index}/{len(rows)}] NOT_FOUND afm={afm} name={label!r} error={exc}")
                if args.apply:
                    update_beneficiary_gemi(conn, row.beneficiary_vat_number, "-1")
                    conn.commit()
                    updated += 1
            except Exception as exc:
                failed += 1
                print(f"[{index}/{len(rows)}] ERROR afm={afm} name={label!r} error={exc}")
            else:
                print(
                    f"[{index}/{len(rows)}] OK afm={afm} gemi={gemi} "
                    f"existing={row.existing_gemi or '-'} name={label!r}"
                )
                if args.apply:
                    update_beneficiary_gemi(conn, row.beneficiary_vat_number, gemi)
                    conn.commit()
                    updated += 1

            if args.delay > 0 and index < len(rows):
                time.sleep(args.delay)

        if not args.apply:
            conn.rollback()

        print(f"processed={processed} updated={updated} not_found={not_found} failed={failed}")
        if not args.apply:
            print("No database changes were written. Re-run with --apply to update gemi.")
        return 0 if failed == 0 else 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
