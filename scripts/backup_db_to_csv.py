#!/usr/bin/env python3
"""
Export all public schema base tables to CSV files under backupDB/.

Usage:
  ./.fireprotection/bin/python scripts/backup_db_to_csv.py
  ./.fireprotection/bin/python scripts/backup_db_to_csv.py --output-dir backupDB
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2 import sql


REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = REPO_ROOT / ".env"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "backupDB"


def load_database_url() -> str:
    if "DATABASE_URL" in os.environ and os.environ["DATABASE_URL"].strip():
        return os.environ["DATABASE_URL"].strip()

    if not ENV_PATH.exists():
        raise RuntimeError("Missing DATABASE_URL and .env file not found.")

    for raw in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() == "DATABASE_URL" and value.strip():
            return value.strip()

    raise RuntimeError("DATABASE_URL not found in environment or .env")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backup public schema tables to CSV files.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where the CSV backup files will be written.",
    )
    parser.add_argument(
        "--schema",
        default="public",
        help="Schema to export. Defaults to public.",
    )
    return parser.parse_args()


def list_base_tables(cur, schema_name: str) -> list[str]:
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (schema_name,),
    )
    return [row[0] for row in cur.fetchall()]


def export_table_to_csv(cur, schema_name: str, table_name: str, output_path: Path) -> int:
    with output_path.open("w", encoding="utf-8", newline="") as fh:
        query = sql.SQL("COPY {}.{} TO STDOUT WITH CSV HEADER").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )
        cur.copy_expert(query, fh)

    cur.execute(
        sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )
    )
    return int(cur.fetchone()[0])


def main() -> int:
    args = parse_args()
    db_url = load_database_url()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            table_names = list_base_tables(cur, args.schema)
            if not table_names:
                raise RuntimeError(f"No base tables found in schema {args.schema!r}.")

            manifest: dict[str, object] = {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "schema": args.schema,
                "output_dir": str(output_dir),
                "tables": [],
            }

            for table_name in table_names:
                output_path = output_dir / f"{table_name}.csv"
                row_count = export_table_to_csv(cur, args.schema, table_name, output_path)
                print(f"Exported {args.schema}.{table_name} -> {output_path.name} ({row_count} rows)")
                manifest["tables"].append(
                    {
                        "table_name": table_name,
                        "csv_file": output_path.name,
                        "row_count": row_count,
                    }
                )

            manifest_path = output_dir / "manifest.json"
            manifest_path.write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            print(f"Wrote manifest -> {manifest_path}")
        return 0
    except Exception as exc:
        print(f"Backup failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
