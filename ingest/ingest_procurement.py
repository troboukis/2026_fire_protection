"""
ingest_procurement.py
---------------------
Loads Diavgeia fire-protection procurement decisions into Supabase.

Sources:
  data/2026_diavgeia_filtered.csv       (22,886 rows)
  data/mappings/org_to_municipality.csv (600 rows)

Usage:
  python ingest/ingest_procurement.py

Prerequisites:
  - 002_webapp_schema.sql applied
  - municipalities table populated
  - .env with DATABASE_URL
"""

from __future__ import annotations

import ast
import os
from datetime import datetime
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

REPO_DIR     = Path(__file__).resolve().parent.parent
DIAVGEIA_CSV = REPO_DIR / "data" / "2026_diavgeia_filtered.csv"
MAPPING_CSV  = REPO_DIR / "data" / "mappings" / "org_to_municipality.csv"


def parse_greek_float(s: str) -> float | None:
    """Parse Greek-formatted number: '10.925,00' → 10925.0"""
    s = str(s).strip()
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def parse_amount_field(val) -> float | None:
    """Parse amount from plain string or list-repr string. Returns sum for lists."""
    if not val or str(val).strip() in ("", "nan"):
        return None
    val = str(val).strip()
    if val.startswith("["):
        try:
            items = ast.literal_eval(val)
            total = sum(
                f for item in items
                if item is not None
                for f in [parse_greek_float(str(item))]
                if f is not None
            )
            return total if total > 0 else None
        except (ValueError, SyntaxError):
            return None
    return parse_greek_float(val)


def derive_amount(r: pd.Series) -> float | None:
    """
    Derive the best amount for display/storage.

    Primary behavior:
    - choose the amount field that matches the row's decisionType
    - then fall back to the legacy priority order for robustness

    """
    decision_type = str(r.get("decisionType") or "").upper().strip()

    preferred_cols: list[str]
    if decision_type.startswith("ΕΓΚΡΙΣΗ"):
        preferred_cols = ["spending_contractors_value"]
    elif decision_type.startswith("ΑΝΑΛΗΨΗ"):
        preferred_cols = ["commitment_amount_with_vat"]
    elif decision_type.startswith("ΑΝΑΘΕΣΗ"):
        preferred_cols = ["direct_value"]
    elif decision_type.startswith("ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ") or decision_type.startswith("ΠΛΗΡΩΜΗ"):
        preferred_cols = ["payment_value"]
    else:
        preferred_cols = []

    candidate_cols = preferred_cols + [
        col for col in [
            "spending_contractors_value",
            "commitment_amount_with_vat",
            "direct_value",
            "payment_value",
        ]
        if col not in preferred_cols
    ]

    for col in candidate_cols:
        raw_val = r.get(col)
        amt = parse_amount_field(raw_val)
        if amt is None or amt <= 0:
            continue
        return amt
    return None


def parse_contractor(r: pd.Series) -> str | None:
    """Extract contractor name: prefer direct_name, then payment_beneficiary_name."""
    for col in ["direct_name", "payment_beneficiary_name", "spending_contractors_name"]:
        val = str(r.get(col) or "").strip()
        if not val or val in ("nan", "[]", "[None]"):
            continue
        if val.startswith("["):
            try:
                items = ast.literal_eval(val)
                names = [str(i).strip() for i in items if i is not None and str(i).strip()]
                if names:
                    return names[0][:500]
            except (ValueError, SyntaxError):
                pass
        else:
            return val[:500]
    return None


def parse_keywords(subj, pdf) -> list[str] | None:
    """Combine subject + PDF keywords into a deduplicated list."""
    kws: set[str] = set()
    for val in [subj, pdf]:
        val = str(val or "").strip()
        if not val or val == "nan":
            continue
        if val.startswith("["):
            try:
                items = ast.literal_eval(val)
                kws.update(str(i) for i in items if i)
                continue
            except (ValueError, SyntaxError):
                pass
        kws.add(val)
    return list(kws) if kws else None


def parse_date(val) -> str | None:
    val = str(val or "").strip()
    if not val or val == "nan":
        return None
    try:
        return datetime.strptime(val[:10], "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        try:
            return datetime.strptime(val[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            return None


def parse_bool(val) -> bool | None:
    s = str(val or "").strip().lower()
    if not s or s == "nan":
        return None
    if s in {"true", "1", "yes"}:
        return True
    if s in {"false", "0", "no"}:
        return False
    return None


def main() -> None:
    db_url = os.environ["DATABASE_URL"]
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()

    # Valid municipality IDs from DB
    cur.execute("SELECT id FROM public.municipalities;")
    valid_ids: set[str] = {row[0] for row in cur.fetchall()}

    # Load org → municipality mapping
    map_df = pd.read_csv(MAPPING_CSV, dtype=str, keep_default_na=False, na_values=[""])
    org_map: dict[tuple, dict] = {}
    for _, r in map_df.iterrows():
        key = (r["org_type"], r["org_name_clean"])
        muni_id = str(r.get("municipality_id") or "").strip()
        muni_id = muni_id if muni_id and muni_id != "nan" and muni_id in valid_ids else None
        org_map[key] = {
            "authority_level": r.get("authority_level") or None,
            "municipality_id": muni_id,
        }

    # Load Diavgeia decisions
    df = pd.read_csv(DIAVGEIA_CSV, dtype=str, keep_default_na=False, na_values=[""])
    print(f"Loaded {len(df)} rows from {DIAVGEIA_CSV.name}")

    rows: list[tuple] = []
    row_iter = df.iterrows()
    if tqdm is not None:
        row_iter = tqdm(row_iter, total=len(df), desc="procurement_headers", unit="row")

    for _, r in row_iter:
        org_type      = str(r.get("org_type") or "").strip() or None
        org_name      = str(r.get("org_name_clean") or "").strip() or None
        if not org_type or not org_name:
            continue

        mapping = org_map.get((org_type, org_name), {})
        authority_level = mapping.get("authority_level")
        municipality_id = mapping.get("municipality_id")

        # region_name: use org_name_clean for region-level decisions
        region_name = org_name if authority_level == "region" else None

        rows.append((
            str(r["ada"]).strip(),
            org_type,
            org_name,
            authority_level,
            municipality_id,
            region_name,
            parse_date(r.get("issueDate")),
            str(r.get("subject") or "")[:2000] or None,
            str(r.get("documentUrl") or "") or None,
            str(r.get("decisionType") or "") or None,
            parse_bool(r.get("subject_has_anatrop_or_anaklis")),
            parse_bool(r.get("subject_has_budget_balance_report_terms")),
            parse_keywords(r.get("matched_keywords_subject"), r.get("matched_keywords_pdf")),
            derive_amount(r),
            parse_contractor(r),
        ))

    upsert_sql = """
        INSERT INTO public.procurement_decisions (
            ada, org_type, org_name_clean, authority_level, municipality_id,
            region_name, issue_date, subject, document_url, decision_type,
            subject_has_anatrop_or_anaklis, subject_has_budget_balance_report_terms,
            matched_keywords, amount_eur, contractor_name
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT (ada) DO UPDATE SET
            org_type        = EXCLUDED.org_type,
            org_name_clean  = EXCLUDED.org_name_clean,
            authority_level = EXCLUDED.authority_level,
            municipality_id = EXCLUDED.municipality_id,
            region_name     = EXCLUDED.region_name,
            issue_date      = EXCLUDED.issue_date,
            subject         = EXCLUDED.subject,
            document_url    = EXCLUDED.document_url,
            decision_type   = EXCLUDED.decision_type,
            subject_has_anatrop_or_anaklis = EXCLUDED.subject_has_anatrop_or_anaklis,
            subject_has_budget_balance_report_terms = EXCLUDED.subject_has_budget_balance_report_terms,
            matched_keywords= EXCLUDED.matched_keywords,
            amount_eur      = EXCLUDED.amount_eur,
            contractor_name = EXCLUDED.contractor_name;
    """

    print("Inserting...", flush=True)
    psycopg2.extras.execute_batch(cur, upsert_sql, rows, page_size=500)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM public.procurement_decisions;")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM public.procurement_decisions WHERE municipality_id IS NOT NULL;")
    with_muni = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM public.procurement_decisions WHERE amount_eur IS NOT NULL;")
    with_amt = cur.fetchone()[0]
    cur.execute("SELECT MIN(issue_date), MAX(issue_date) FROM public.procurement_decisions;")
    d_min, d_max = cur.fetchone()

    print(f"\n[done] procurement_decisions table:")
    print(f"  Total rows:        {total}")
    print(f"  With municipality: {with_muni}")
    print(f"  With amount:       {with_amt}")
    print(f"  Date range:        {d_min} – {d_max}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
