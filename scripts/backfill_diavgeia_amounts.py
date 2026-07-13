from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.diavgeia_amounts import resolve_diavgeia_amount_text
from src.map_copernicus_to_municipalities import resolve_database_url


DEFAULT_CSV = ROOT / "data" / "2026_diavgeia.csv"


@dataclass(frozen=True)
class RawAmountSummary:
    raw_rows_with_amount: int
    amounts_by_ada: dict[str, str]
    duplicate_adas: int
    conflicting_duplicate_adas: int


def load_raw_amounts(csv_path: Path) -> RawAmountSummary:
    csv.field_size_limit(sys.maxsize)
    amounts_by_ada: dict[str, str] = {}
    values_by_ada: dict[str, set[str]] = {}
    raw_rows_with_amount = 0

    with csv_path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            amount = resolve_diavgeia_amount_text(row)
            if amount is None:
                continue
            ada = str(row.get("ada") or "").strip()
            if not ada:
                raise ValueError("Raw Diavgeia row with an amount is missing ADA")
            raw_rows_with_amount += 1
            amounts_by_ada[ada] = amount  # Match loader semantics: last raw row wins.
            values_by_ada.setdefault(ada, set()).add(amount)

    duplicate_adas = raw_rows_with_amount - len(amounts_by_ada)
    conflicting_duplicate_adas = sum(len(values) > 1 for values in values_by_ada.values())
    return RawAmountSummary(
        raw_rows_with_amount=raw_rows_with_amount,
        amounts_by_ada=amounts_by_ada,
        duplicate_adas=duplicate_adas,
        conflicting_duplicate_adas=conflicting_duplicate_adas,
    )


def inspect_database(conn, amounts_by_ada: dict[str, str]) -> dict[str, int]:
    adas = list(amounts_by_ada)
    amounts = [amounts_by_ada[ada] for ada in adas]
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '20s'")
        cur.execute(
            """
            WITH source(ada, amount) AS (
              SELECT * FROM unnest(%s::text[], %s::text[])
            )
            SELECT
              COUNT(*) FILTER (WHERE d.id IS NOT NULL) AS matched,
              COUNT(*) FILTER (WHERE d.id IS NULL) AS missing,
              COUNT(*) FILTER (
                WHERE d.id IS NOT NULL
                  AND d.spending_contractors_value IS DISTINCT FROM source.amount
              ) AS differing
            FROM source
            LEFT JOIN public.diavgeia d USING (ada)
            """,
            (adas, amounts),
        )
        matched, missing, differing = cur.fetchone()
    return {"matched": int(matched), "missing": int(missing), "differing": int(differing)}


def apply_backfill(conn, amounts_by_ada: dict[str, str]) -> int:
    rows = list(amounts_by_ada.items())
    with conn.cursor() as cur:
        updated_rows = execute_values(
            cur,
            """
            UPDATE public.diavgeia AS d
            SET spending_contractors_value = source.amount,
                updated_at = NOW()
            FROM (VALUES %s) AS source(ada, amount)
            WHERE d.ada = source.ada
              AND d.spending_contractors_value IS DISTINCT FROM source.amount
            RETURNING d.ada
            """,
            rows,
            page_size=500,
            fetch=True,
        )
        return len(updated_rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill the single public.diavgeia amount text column from the raw CSV.",
    )
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--apply", action="store_true", help="Apply updates. Default is read-only dry-run.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = load_raw_amounts(args.csv)
    print(f"Raw rows with amount: {summary.raw_rows_with_amount}")
    print(f"Unique ADAs with amount: {len(summary.amounts_by_ada)}")
    print(f"Duplicate ADA rows: {summary.duplicate_adas}")
    print(f"Conflicting duplicate ADAs: {summary.conflicting_duplicate_adas}")

    conn = psycopg2.connect(resolve_database_url(None))
    try:
        if not args.apply:
            conn.set_session(readonly=True)
        db_summary = inspect_database(conn, summary.amounts_by_ada)
        print(f"Matched DB rows: {db_summary['matched']}")
        print(f"Missing DB rows: {db_summary['missing']}")
        print(f"Rows requiring update: {db_summary['differing']}")

        if args.apply:
            updated = apply_backfill(conn, summary.amounts_by_ada)
            conn.commit()
            print(f"Updated DB rows: {updated}")
        else:
            conn.rollback()
            print("Dry-run only; no database changes made.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
