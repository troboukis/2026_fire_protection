"""
ingest_org_municipality_coverage.py
----------------------------------
Loads org -> municipality coverage mapping into Supabase.

Source:
  data/mappings/org_to_municipality_coverage.csv

Target:
  public.org_municipality_coverage

Prerequisites:
  - sql/006_org_municipality_coverage.sql applied
  - municipalities table populated
  - .env with DATABASE_URL
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - optional dependency
    tqdm = None

load_dotenv()

REPO_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = REPO_DIR / "data" / "mappings" / "org_to_municipality_coverage.csv"


def _clean_text(val) -> str | None:
    s = str(val or "").strip()
    if not s or s.lower() == "nan":
        return None
    return s


def main() -> None:
    db_url = os.environ["DATABASE_URL"]
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("SELECT id FROM public.municipalities;")
    valid_ids = {str(r[0]) for r in cur.fetchall()}

    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False, na_values=[""])
    print(f"Loaded {len(df)} rows from {CSV_PATH.name}")

    rows: list[tuple] = []
    skipped_invalid = 0
    row_iter = df.iterrows()
    if tqdm is not None:
        row_iter = tqdm(row_iter, total=len(df), desc="org_coverage", unit="row")

    for _, r in row_iter:
        municipality_id = _clean_text(r.get("municipality_id"))
        if not municipality_id or municipality_id not in valid_ids:
            skipped_invalid += 1
            continue
        rows.append((
            _clean_text(r.get("org_type")),
            _clean_text(r.get("org_name_clean")),
            _clean_text(r.get("authority_level")),
            _clean_text(r.get("region_id")),
            municipality_id,
            _clean_text(r.get("municipality_name")),
            _clean_text(r.get("coverage_method")) or "unknown",
        ))

    print(f"Prepared {len(rows)} rows (skipped invalid municipality_id: {skipped_invalid})")

    cur.execute("TRUNCATE TABLE public.org_municipality_coverage RESTART IDENTITY;")

    insert_sql = """
        INSERT INTO public.org_municipality_coverage (
            org_type, org_name_clean, authority_level, region_id,
            municipality_id, municipality_name, coverage_method
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s
        );
    """

    print("Inserting...", flush=True)
    psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=1000)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM public.org_municipality_coverage;")
    total = cur.fetchone()[0]
    cur.execute(
        "SELECT coverage_method, COUNT(*) "
        "FROM public.org_municipality_coverage "
        "GROUP BY coverage_method ORDER BY 2 DESC, 1;"
    )
    by_method = cur.fetchall()
    cur.execute(
        "SELECT COUNT(DISTINCT (org_type, org_name_clean)) "
        "FROM public.org_municipality_coverage;"
    )
    distinct_orgs = cur.fetchone()[0]

    print("\n[done] org_municipality_coverage table:")
    print(f"  Total rows:          {total}")
    print(f"  Distinct orgs:       {distinct_orgs}")
    for method, count in by_method:
        print(f"  {method}: {count}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
