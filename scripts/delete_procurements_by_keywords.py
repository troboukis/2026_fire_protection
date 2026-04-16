#!/usr/bin/env python3
"""
Delete procurement rows whose text fields contain specific keywords.

Dry-run is the default behavior. Use --apply to execute the deletion.

Examples:
  ./.fireprotection/bin/python scripts/delete_procurements_by_keywords.py \
    --keyword καθαρισμός --keyword αποψίλωση

  ./.fireprotection/bin/python scripts/delete_procurements_by_keywords.py \
    --keywords-file data/keywords/to_remove.txt --apply
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import unicodedata
from pathlib import Path

import psycopg2


REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = REPO_ROOT / ".env"
DEFAULT_COLUMNS = ("title", "short_descriptions")
ALLOWED_COLUMNS = (
    "title",
    "short_descriptions",
    "contract_type",
    "award_procedure",
    "procedure_type_value",
    "contracting_authority_activity",
)
GREEK_ACCENTED_SOURCE = "άέήίϊΐόύϋΰώς"
GREEK_ACCENTED_TARGET = "αεηιιιουυυωσ"


def normalize_database_url(raw: str | None) -> str:
    value = str(raw or "").strip().strip("'\"")
    if not value:
        return ""
    if value.startswith("DATABASE_URL="):
        value = value.split("=", 1)[1].strip().strip("'\"")
    return value


def load_database_url(db_path: str | None) -> str:
    if db_path:
        db_url = normalize_database_url(db_path)
        if db_url:
            return db_url

    env_db_url = normalize_database_url(os.getenv("DATABASE_URL"))
    if env_db_url:
        return env_db_url

    if not ENV_PATH.exists():
        raise RuntimeError("Missing DATABASE_URL and .env file not found.")

    for raw in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() == "DATABASE_URL":
            db_url = normalize_database_url(value)
            if db_url:
                return db_url

    raise RuntimeError("DATABASE_URL not found in environment or .env")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Delete matching rows from public.procurement based on keywords in text columns.",
    )
    parser.add_argument(
        "--keyword",
        dest="keywords",
        action="append",
        default=[],
        help="Keyword or phrase to match. Repeat the flag for multiple values.",
    )
    parser.add_argument(
        "--keywords-file",
        type=Path,
        help="Optional text file with one keyword or phrase per line.",
    )
    parser.add_argument(
        "--column",
        dest="columns",
        action="append",
        choices=ALLOWED_COLUMNS,
        help="Column to search. Defaults to title and short_descriptions.",
    )
    parser.add_argument(
        "--match-mode",
        choices=("any", "all"),
        default="any",
        help="Match any keyword or require all keywords across the selected columns.",
    )
    parser.add_argument(
        "--preview-limit",
        type=int,
        default=20,
        help="Number of matching rows to preview in the output.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Optional DATABASE_URL override.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute the delete. Default is dry-run preview only.",
    )
    return parser.parse_args()


def load_keywords_from_file(path: Path | None) -> list[str]:
    if path is None:
        return []
    if not path.exists():
        raise FileNotFoundError(f"Keywords file not found: {path}")

    keywords: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        keywords.append(line)
    return keywords


def normalize_keyword(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    decomposed = unicodedata.normalize("NFD", text)
    without_marks = "".join(char for char in decomposed if unicodedata.category(char) != "Mn")
    folded = without_marks.casefold().replace("ς", "σ")
    return "".join(char for char in folded if char.isalnum())


def prepare_keywords(inline_keywords: list[str], keywords_file: Path | None) -> list[str]:
    combined = [*inline_keywords, *load_keywords_from_file(keywords_file)]
    prepared: list[str] = []
    seen: set[str] = set()

    for raw in combined:
        keyword = normalize_keyword(raw)
        if not keyword:
            continue
        if keyword in seen:
            continue
        seen.add(keyword)
        prepared.append(keyword)

    if not prepared:
        raise ValueError("Provide at least one keyword via --keyword or --keywords-file.")

    return prepared


def prepare_columns(raw_columns: list[str] | None) -> list[str]:
    columns = list(raw_columns or DEFAULT_COLUMNS)
    prepared: list[str] = []
    seen: set[str] = set()

    for column in columns:
        if column not in ALLOWED_COLUMNS:
            raise ValueError(f"Unsupported search column: {column}")
        if column in seen:
            continue
        seen.add(column)
        prepared.append(column)

    if not prepared:
        raise ValueError("At least one search column is required.")

    return prepared


def database_supports_unaccent(cur) -> bool:
    cur.execute("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'unaccent')")
    return bool(cur.fetchone()[0])


def build_normalized_sql_expression(column: str, use_unaccent: bool) -> str:
    base_expr = f"LOWER(COALESCE(p.{column}, ''))"
    if use_unaccent:
        base_expr = f"UNACCENT({base_expr})"
    else:
        base_expr = (
            f"TRANSLATE({base_expr}, '{GREEK_ACCENTED_SOURCE}', '{GREEK_ACCENTED_TARGET}')"
        )

    return f"REGEXP_REPLACE({base_expr}, '[^[:alnum:]]+', '', 'g')"


def build_keyword_filter(
    columns: list[str],
    keywords: list[str],
    match_mode: str,
    use_unaccent: bool,
) -> tuple[str, list[str]]:
    groups: list[str] = []
    params: list[str] = []

    for keyword in keywords:
        column_checks = []
        for column in columns:
            normalized_expr = build_normalized_sql_expression(column, use_unaccent)
            column_checks.append(f"{normalized_expr} LIKE %s")
            params.append(f"%{keyword}%")
        groups.append("(" + " OR ".join(column_checks) + ")")

    joiner = " OR " if match_mode == "any" else " AND "
    return joiner.join(groups), params


def fetch_summary(cur, where_sql: str, params: list[str]) -> dict[str, int]:
    cur.execute(
        f"""
        WITH matched AS (
          SELECT p.id, p.reference_number
          FROM public.procurement AS p
          WHERE {where_sql}
        ),
        matched_payments AS (
          SELECT id
          FROM public.payment
          WHERE procurement_id IN (SELECT id FROM matched)
        )
        SELECT
          (SELECT COUNT(*) FROM matched) AS procurement_rows,
          (SELECT COUNT(*) FROM public.payment WHERE id IN (SELECT id FROM matched_payments)) AS payment_rows,
          (SELECT COUNT(*) FROM public.payment_beneficiary WHERE payment_id IN (SELECT id FROM matched_payments)) AS payment_beneficiary_rows,
          (SELECT COUNT(*) FROM public.cpv WHERE procurement_id IN (SELECT id FROM matched)) AS cpv_rows,
          (SELECT COUNT(*) FROM public.diavgeia_procurement WHERE procurement_id IN (SELECT id FROM matched)) AS diavgeia_procurement_rows,
          (SELECT COUNT(*) FROM public.works WHERE reference_number IN (
            SELECT reference_number
            FROM matched
            WHERE reference_number IS NOT NULL
          )) AS works_rows
        """,
        params,
    )
    row = cur.fetchone()
    return {
        "procurement_rows": int(row[0]),
        "payment_rows": int(row[1]),
        "payment_beneficiary_rows": int(row[2]),
        "cpv_rows": int(row[3]),
        "diavgeia_procurement_rows": int(row[4]),
        "works_rows": int(row[5]),
    }


def fetch_preview(cur, where_sql: str, params: list[str], limit: int) -> list[dict[str, object]]:
    cur.execute(
        f"""
        SELECT
          p.id,
          p.reference_number,
          p.title,
          p.contract_signed_date
        FROM public.procurement AS p
        WHERE {where_sql}
        ORDER BY p.contract_signed_date DESC NULLS LAST, p.id DESC
        LIMIT %s
        """,
        [*params, limit],
    )

    preview: list[dict[str, object]] = []
    for procurement_id, reference_number, title, contract_signed_date in cur.fetchall():
        preview.append(
            {
                "id": int(procurement_id),
                "reference_number": reference_number,
                "title": title,
                "contract_signed_date": (
                    contract_signed_date.isoformat() if contract_signed_date is not None else None
                ),
            }
        )
    return preview


def delete_matching_procurements(cur, where_sql: str, params: list[str]) -> int:
    cur.execute("SET CONSTRAINTS ALL DEFERRED")
    cur.execute(
        f"""
        WITH matched AS (
          SELECT p.id
          FROM public.procurement AS p
          WHERE {where_sql}
        )
        DELETE FROM public.procurement AS p
        USING matched
        WHERE p.id = matched.id
        """,
        params,
    )
    return cur.rowcount


def main() -> int:
    args = parse_args()

    try:
        keywords = prepare_keywords(args.keywords, args.keywords_file)
        columns = prepare_columns(args.columns)
    except Exception as exc:
        print(f"Invalid arguments: {exc}", file=sys.stderr)
        return 2

    if args.preview_limit < 0:
        print("Invalid arguments: --preview-limit must be >= 0", file=sys.stderr)
        return 2

    try:
        db_url = load_database_url(args.db_path)

        conn = psycopg2.connect(db_url)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                use_unaccent = database_supports_unaccent(cur)
                where_sql, params = build_keyword_filter(columns, keywords, args.match_mode, use_unaccent)
                summary = fetch_summary(cur, where_sql, params)
                preview = fetch_preview(cur, where_sql, params, args.preview_limit)

                output = {
                    "applied": bool(args.apply),
                    "keywords": keywords,
                    "columns": columns,
                    "match_mode": args.match_mode,
                    "normalization": {
                        "case_sensitive": False,
                        "accent_sensitive": False,
                        "special_character_sensitive": False,
                        "database_unaccent": use_unaccent,
                    },
                    "preview_limit": args.preview_limit,
                    "matches": summary,
                    "preview": preview,
                }

                if not args.apply:
                    conn.rollback()
                    print(json.dumps(output, ensure_ascii=False, indent=2))
                    return 0

                deleted_rows = delete_matching_procurements(cur, where_sql, params)
                conn.commit()
                output["deleted_procurement_rows"] = int(deleted_rows)
                print(json.dumps(output, ensure_ascii=False, indent=2))
                return 0
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    except Exception as exc:
        print(f"Delete failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
