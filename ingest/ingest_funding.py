"""
ingest_funding.py
-----------------
Loads KAP fire protection funding allocations into Supabase.

Source:
  data/funding/municipal_funding.csv  (~2,378 rows)

Municipality matching strategy (in order):
  1. recipient_raw name → strip accents + uppercase → match against municipality names
  2. (fallback) municipality_code from PDF → match Kallikratis ID (rarely works — different coding)

Usage:
  python ingest/ingest_funding.py

Prerequisites:
  - 002_webapp_schema.sql applied
  - municipalities table populated
  - .env with DATABASE_URL
"""

from __future__ import annotations

import os
import re
import unicodedata
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

REPO_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = REPO_DIR / "data" / "funding" / "municipal_funding.csv"


def strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", str(s))
        if unicodedata.category(c) != "Mn"
    )


def norm(s: str) -> str:
    return strip_accents(s).upper().strip()


# Remove common prefixes that appear in recipient_raw but not in municipality names
_PREFIXES = re.compile(
    r"^(ΔΗΜΟΣ|ΔΗΜΟΥ|∆ΗΜΟΣ|∆ΗΜΟΥ|Δ\.|∆\.)\s+",
    re.UNICODE,
)


def normalize_recipient(raw: str) -> str:
    """Strip 'ΔΗΜΟΣ ' / 'Δ. ' prefix and normalize for name matching."""
    s = norm(raw)
    s = _PREFIXES.sub("", s).strip()
    return s


def build_name_lookup(cur) -> dict[str, str]:
    """
    Build {normalized_name: municipality_id} from DB.
    Normalization: strip_accents + upper.
    """
    cur.execute("SELECT id, name FROM public.municipalities;")
    lookup: dict[str, str] = {}
    for mid, name in cur.fetchall():
        lookup[norm(name)] = mid
    return lookup


def match_recipient(raw: str, name_lookup: dict[str, str]) -> str | None:
    """Try name-based matching for a recipient_raw string."""
    if not raw:
        return None
    normalized = normalize_recipient(raw)
    # Direct match after prefix stripping
    if normalized in name_lookup:
        return name_lookup[normalized]
    # Some PDFs include full forms like "ΑΘΗΝΑΙΩΝ" matching "Αθηναίων"
    full_norm = norm(raw)
    if full_norm in name_lookup:
        return name_lookup[full_norm]
    return None


def main() -> None:
    db_url = os.environ["DATABASE_URL"]
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()

    # Build name lookup from DB
    name_lookup = build_name_lookup(cur)
    valid_ids = set(name_lookup.values())
    print(f"Municipality name lookup: {len(name_lookup)} entries")

    # Truncate existing data so re-runs are idempotent
    cur.execute("TRUNCATE TABLE public.funding_allocations RESTART IDENTITY;")

    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False, na_values=[""])
    print(f"Loaded {len(df)} rows from {CSV_PATH.name}")

    rows: list[tuple] = []
    matched = 0
    unmatched_recipients: list[str] = []

    for _, r in df.iterrows():
        recipient_raw = str(r.get("recipient_raw") or "").strip()
        recipient_type = str(r.get("recipient_type") or "").strip()

        municipality_id: str | None = None

        # Only attempt municipality matching for δήμος entries
        if recipient_type == "δήμος":
            municipality_id = match_recipient(recipient_raw, name_lookup)
            if municipality_id:
                matched += 1
            else:
                unmatched_recipients.append(recipient_raw)

        rows.append((
            int(r["year"]),
            r["allocation_type"],
            recipient_type,
            recipient_raw,
            r.get("nomos") or None,
            municipality_id,
            float(r["amount_eur"]),
            r.get("source_ada") or None,
        ))

    dimos_total = sum(1 for r in rows if r[2] == "δήμος")
    print(f"  Matched {matched}/{dimos_total} δήμοι to municipalities")
    if unmatched_recipients:
        unique_unmatched = sorted(set(unmatched_recipients))
        print(f"  Unmatched ({len(unique_unmatched)} unique): {unique_unmatched[:20]}")

    insert_sql = """
        INSERT INTO public.funding_allocations (
            year, allocation_type, recipient_type, recipient_raw,
            nomos, municipality_id, amount_eur, source_ada
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    print("Inserting...", flush=True)
    psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=200)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM public.funding_allocations;")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM public.funding_allocations WHERE municipality_id IS NOT NULL;")
    with_muni = cur.fetchone()[0]
    cur.execute("SELECT MIN(year), MAX(year) FROM public.funding_allocations;")
    yr_min, yr_max = cur.fetchone()

    print(f"\n[done] funding_allocations table:")
    print(f"  Total rows:        {total}")
    print(f"  With municipality: {with_muni}")
    print(f"  Year range:        {yr_min}–{yr_max}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
