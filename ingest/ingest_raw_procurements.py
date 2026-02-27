"""
ingest_raw_procurements.py
--------------------------
Load data/raw_procurements.csv into public.raw_procurements (Supabase/Postgres).

Usage:
  python ingest/ingest_raw_procurements.py
  python ingest/ingest_raw_procurements.py --dry-run
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path
from typing import Callable

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
SOURCE_CSV = REPO_DIR / "data" / "raw_procurements.csv"


def parse_text(val) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return None if not s or s.lower() == "nan" else s


def parse_bool(val) -> bool | None:
    s = parse_text(val)
    if s is None:
        return None
    lowered = s.lower()
    if lowered in {"true", "1", "yes"}:
        return True
    if lowered in {"false", "0", "no"}:
        return False
    return None


def parse_int(val) -> int | None:
    s = parse_text(val)
    if s is None:
        return None
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return None


def parse_numeric(val) -> float | None:
    s = parse_text(val)
    if s is None:
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def parse_date(val) -> str | None:
    s = parse_text(val)
    if s is None:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_timestamp(val) -> str | None:
    s = parse_text(val)
    if s is None:
        return None
    if "T" in s and not s.endswith("Z"):
        # Keep as UTC-like timestamp for postgres casting.
        return f"{s}Z"
    return s


COLUMN_SPECS: list[tuple[str, str, Callable]] = [
    ("title", "title", parse_text),
    ("referenceNumber", "reference_number", parse_text),
    ("submissionDate", "submission_at", parse_timestamp),
    ("contractSignedDate", "contract_signed_date", parse_date),
    ("startDate", "start_date", parse_date),
    ("noEndDate", "no_end_date", parse_bool),
    ("endDate", "end_date", parse_date),
    ("cancelled", "cancelled", parse_bool),
    ("cancellationDate", "cancellation_date", parse_date),
    ("cancellationType", "cancellation_type", parse_text),
    ("cancellationReason", "cancellation_reason", parse_text),
    ("decisionRelatedAda", "decision_related_ada", parse_text),
    ("contractNumber", "contract_number", parse_text),
    ("organizationVatNumber", "organization_vat_number", parse_text),
    ("greekOrganizationVatNumber", "greek_organization_vat_number", parse_bool),
    ("diavgeiaADA", "diavgeia_ada", parse_text),
    ("budget", "budget", parse_numeric),
    ("contractBudget", "contract_budget", parse_numeric),
    ("bidsSubmitted", "bids_submitted", parse_int),
    ("maxBidsSubmitted", "max_bids_submitted", parse_int),
    ("numberOfSections", "number_of_sections", parse_int),
    ("centralGovernmentAuthority", "central_government_authority", parse_text),
    ("nutsCode_key", "nuts_code_key", parse_text),
    ("nutsCode_value", "nuts_code_value", parse_text),
    ("organization_key", "organization_key", parse_text),
    ("organization_value", "organization_value", parse_text),
    ("procedureType_key", "procedure_type_key", parse_text),
    ("procedureType_value", "procedure_type_value", parse_text),
    ("awardProcedure", "award_procedure", parse_text),
    ("nutsCity", "nuts_city", parse_text),
    ("nutsPostalCode", "nuts_postal_code", parse_text),
    ("centralizedMarkets", "centralized_markets", parse_text),
    ("contractType", "contract_type", parse_text),
    ("assignCriteria", "assign_criteria", parse_text),
    (
        "classificationOfPublicLawOrganization",
        "classification_of_public_law_organization",
        parse_text,
    ),
    ("typeOfContractingAuthority", "type_of_contracting_authority", parse_text),
    ("contractingAuthorityActivity", "contracting_authority_activity", parse_text),
    ("contractDuration", "contract_duration", parse_int),
    ("contractDurationUnitOfMeasure", "contract_duration_unit_of_measure", parse_text),
    ("contractRelatedADA", "contract_related_ada", parse_text),
    ("fundingDetails_cofund", "funding_details_cofund", parse_text),
    ("fundingDetails_selfFund", "funding_details_self_fund", parse_text),
    ("fundingDetails_espa", "funding_details_espa", parse_text),
    ("fundingDetails_regularBudget", "funding_details_regular_budget", parse_text),
    ("unitsOperator", "units_operator", parse_text),
    ("signers", "signers", parse_text),
    ("firstMember_vatNumber", "first_member_vat_number", parse_text),
    ("firstMember_name", "first_member_name", parse_text),
    ("totalCostWithVAT", "total_cost_with_vat", parse_numeric),
    ("totalCostWithoutVAT", "total_cost_without_vat", parse_numeric),
    ("shortDescriptions", "short_descriptions", parse_text),
    ("cpv_keys", "cpv_keys", parse_text),
    ("cpv_values", "cpv_values", parse_text),
    ("greenContracts", "green_contracts", parse_text),
    ("auctionRefNo", "auction_ref_no", parse_text),
    ("paymentRefNo", "payment_ref_no", parse_text),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest raw_procurements.csv into Supabase")
    parser.add_argument("--csv", default=str(SOURCE_CSV), help="Path to raw_procurements.csv")
    parser.add_argument("--dry-run", action="store_true", help="Parse input without writing to DB")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_csv = Path(args.csv).resolve()

    if not source_csv.exists():
        raise FileNotFoundError(f"CSV not found: {source_csv}")

    df = pd.read_csv(source_csv, dtype=str, keep_default_na=False, na_values=[""])
    print(f"Loaded {len(df)} rows from {source_csv}")

    rows: list[tuple] = []
    row_iter = df.iterrows()
    if tqdm is not None:
        row_iter = tqdm(row_iter, total=len(df), desc="raw_procurements", unit="row")

    for _, row in row_iter:
        parsed_row = tuple(parser(row.get(csv_col)) for csv_col, _, parser in COLUMN_SPECS)
        rows.append(parsed_row)

    if args.dry_run:
        print(f"[dry-run] Parsed {len(rows)} rows; no database writes performed.")
        return

    db_url = os.environ["DATABASE_URL"]
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()

    db_columns = [db_col for _, db_col, _ in COLUMN_SPECS]
    insert_sql = f"""
        INSERT INTO public.raw_procurements ({", ".join(db_columns)})
        VALUES %s
    """

    try:
        cur.execute("TRUNCATE TABLE public.raw_procurements RESTART IDENTITY;")
        psycopg2.extras.execute_values(
            cur,
            insert_sql,
            rows,
            page_size=500,
        )
        cur.execute("SELECT COUNT(*) FROM public.raw_procurements;")
        count = cur.fetchone()[0]
        conn.commit()
        print(f"[done] raw_procurements rows in DB: {count}")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
