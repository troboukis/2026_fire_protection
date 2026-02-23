"""
ingest_procurement_lines.py
---------------------------
Loads line-level procurement details into Supabase (multiple rows per ADA).

Source:
  data/2026_diavgeia_filtered.csv

Target:
  public.procurement_decision_lines

Prerequisites:
  - procurement_decisions already ingested (parent rows by ADA)
  - sql/005_procurement_decision_lines.sql applied
  - .env with DATABASE_URL
"""

from __future__ import annotations

import ast
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

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
CSV_PATH = REPO_DIR / "data" / "2026_diavgeia_filtered.csv"


def parse_structured(val: Any) -> Any:
    if isinstance(val, (list, dict)):
        return val
    s = str(val or "").strip()
    if not s or s.lower() == "nan":
        return None
    if s.startswith("[") or s.startswith("{"):
        try:
            return ast.literal_eval(s)
        except (ValueError, SyntaxError):
            return None
    return s


def json_safe(value: Any) -> Any:
    """Recursively convert pandas/NaN values to JSON-safe Python values."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_safe(v) for v in value]
    return value


def parse_greek_float(s: Any) -> float | None:
    txt = str(s or "").strip()
    if not txt or txt.lower() == "nan":
        return None
    txt = txt.replace(".", "").replace(",", ".")
    try:
        return float(txt)
    except ValueError:
        return None


def _counterparty_from_any(value: Any) -> tuple[str | None, str | None]:
    parsed = parse_structured(value)
    if isinstance(parsed, dict):
        afm = str(parsed.get("ΑΦΜ") or "").strip() or None
        name = str(parsed.get("Επωνυμία") or "").strip() or None
        return afm, name
    s = str(parsed or "").strip()
    return None, (s or None)


def as_list(value: Any) -> list[Any]:
    """Return a normalized list from scalar/list/stringified-list values."""
    parsed = parse_structured(value)
    if parsed is None:
        return []
    if isinstance(parsed, list):
        return parsed
    return [parsed]


def build_line_rows(r: pd.Series) -> list[tuple]:
    ada = str(r.get("ada") or "").strip()
    if not ada:
        return []

    out: list[tuple] = []

    # 1) Spending approvals -> contractor lines
    spending = parse_structured(r.get("spending_contractors_details"))
    if isinstance(spending, list):
        for i, item in enumerate(spending):
            if not isinstance(item, dict):
                continue
            amount_raw = item.get("Αξία")
            out.append((
                ada,
                "spending_contractor",
                i,
                "spending_contractors_details",
                str(item.get("ΑΦΜ") or "").strip() or None,
                str(item.get("Επωνυμία") or "").strip() or None,
                str(amount_raw).strip() if amount_raw not in (None, "") else None,
                parse_greek_float(amount_raw),
                str(item.get("Νόμισμα") or "").strip() or None,
                None,
                None,
                None,
                json.dumps(json_safe(item), ensure_ascii=False, allow_nan=False),
            ))

    # 2) Payment finalization -> beneficiary lines
    payment = parse_structured(r.get("payment_beneficiaries_details"))
    if isinstance(payment, list):
        for i, item in enumerate(payment):
            if not isinstance(item, dict):
                continue
            amount_raw = item.get("Αξία")
            out.append((
                ada,
                "payment_beneficiary",
                i,
                "payment_beneficiaries_details",
                str(item.get("ΑΦΜ") or "").strip() or None,
                str(item.get("Επωνυμία") or "").strip() or None,
                str(amount_raw).strip() if amount_raw not in (None, "") else None,
                parse_greek_float(amount_raw),
                None,
                None,
                None,
                None,
                json.dumps(json_safe(item), ensure_ascii=False, allow_nan=False),
            ))

    # 3) Commitments -> KAE/ALE lines
    commitments = parse_structured(r.get("commitment_lines_details"))
    if isinstance(commitments, list) and len(commitments) > 0:
        for i, item in enumerate(commitments):
            if not isinstance(item, dict):
                continue
            cp_afm, cp_name = _counterparty_from_any(item.get("ΑΦΜ / Επωνυμία"))
            amount_raw = item.get("Ποσό με ΦΠΑ")
            out.append((
                ada,
                "commitment_kae_line",
                i,
                "commitment_lines_details",
                cp_afm,
                cp_name,
                str(amount_raw).strip() if amount_raw not in (None, "") else None,
                parse_greek_float(amount_raw),
                None,
                str(item.get("Αριθμός ΚΑΕ/ΑΛΕ") or "").strip() or None,
                str(item.get("Υπόλοιπο ΚΑΕ/ΑΛΕ") or "").strip() or None,
                str(item.get("Υπόλοιπο διαθέσιμης πίστωσης") or "").strip() or None,
                json.dumps(json_safe(item), ensure_ascii=False, allow_nan=False),
            ))
    else:
        # Fallback when no line-details exist (e.g. only "Συνολικό ποσό" available)
        cparties = as_list(r.get("commitment_counterparty"))
        amounts = as_list(r.get("commitment_amount_with_vat"))
        kaes = as_list(r.get("commitment_kae_ale_number"))
        rem_kae = as_list(r.get("commitment_remaining_kae_ale"))
        rem_credit = as_list(r.get("commitment_remaining_available_credit"))
        n = max(len(cparties), len(amounts), len(kaes), len(rem_kae), len(rem_credit))
        for i in range(n):
            cp = cparties[i] if i < len(cparties) else None
            amount_raw = amounts[i] if i < len(amounts) else None
            kae = kaes[i] if i < len(kaes) else None
            rkae = rem_kae[i] if i < len(rem_kae) else None
            rcred = rem_credit[i] if i < len(rem_credit) else None
            if all(v in (None, "", "nan", [], {}) for v in [cp, amount_raw, kae, rkae, rcred]):
                continue
            cp_afm, cp_name = _counterparty_from_any(cp)
            raw_item = {
                "ΑΦΜ / Επωνυμία": cp,
                "Ποσό με ΦΠΑ": amount_raw,
                "Αριθμός ΚΑΕ/ΑΛΕ": kae,
                "Υπόλοιπο ΚΑΕ/ΑΛΕ": rkae,
                "Υπόλοιπο διαθέσιμης πίστωσης": rcred,
            }
            out.append((
                ada,
                "commitment_kae_line",
                i,
                "commitment_* (fallback)",
                cp_afm,
                cp_name,
                str(amount_raw).strip() if str(amount_raw or "").strip() not in ("", "nan") else None,
                parse_greek_float(amount_raw),
                None,
                str(kae).strip() if str(kae or "").strip() not in ("", "nan") else None,
                str(rkae).strip() if str(rkae or "").strip() not in ("", "nan") else None,
                str(rcred).strip() if str(rcred or "").strip() not in ("", "nan") else None,
                json.dumps(json_safe(raw_item), ensure_ascii=False, allow_nan=False),
            ))

    # 4) Direct assignments -> single line (header-level amount/person)
    direct_value = r.get("direct_value")
    direct_people = parse_structured(r.get("direct_people_details"))
    if isinstance(direct_people, list) and len(direct_people) > 0:
        for i, person in enumerate(direct_people):
            person = person if isinstance(person, dict) else {}
            # Put amount only on the first direct row to avoid double-counting in aggregates.
            amount_raw = direct_value if i == 0 else None
            raw_json = {
                "person": person,
                "direct_related_commitment": r.get("direct_related_commitment"),
                "direct_see_also": r.get("direct_see_also"),
            }
            out.append((
                ada,
                "direct_assignment",
                i,
                "direct_people_details",
                str(person.get("ΑΦΜ") or "").strip() or None,
                str(person.get("Επωνυμία") or "").strip() or None,
                str(amount_raw).strip() if str(amount_raw or "").strip() not in ("", "nan") else None,
                parse_greek_float(amount_raw),
                None,
                None,
                None,
                None,
                json.dumps(json_safe(raw_json), ensure_ascii=False, allow_nan=False),
            ))
    else:
        direct_name = str(r.get("direct_name") or "").strip() or None
        direct_afm = str(r.get("direct_afm") or "").strip() or None
        if any(v not in (None, "", "nan") for v in [direct_value, direct_name, direct_afm]):
            raw_json = {
                "direct_afm": r.get("direct_afm"),
                "direct_name": r.get("direct_name"),
                "direct_people_details": parse_structured(r.get("direct_people_details")),
                "direct_related_commitment": r.get("direct_related_commitment"),
                "direct_see_also": r.get("direct_see_also"),
            }
            out.append((
                ada,
                "direct_assignment",
                0,
                "direct_*",
                direct_afm,
                direct_name,
                str(direct_value).strip() if str(direct_value or "").strip() not in ("", "nan") else None,
                parse_greek_float(direct_value),
                None,
                None,
                None,
                None,
                json.dumps(json_safe(raw_json), ensure_ascii=False, allow_nan=False),
            ))

    return out


def renumber_line_indices(rows: list[tuple]) -> list[tuple]:
    """
    Ensure line_index is unique per (ada, line_type) across the whole dataset.

    The raw CSV may contain multiple rows for the same ADA, so local per-row
    indices (e.g. direct_assignment always 0) can collide.
    """
    counters: dict[tuple[str, str], int] = defaultdict(int)
    out: list[tuple] = []
    for row in rows:
        ada = row[0]
        line_type = row[1]
        new_idx = counters[(ada, line_type)]
        counters[(ada, line_type)] += 1
        out.append((row[0], row[1], new_idx, *row[3:]))
    return out


def main() -> None:
    db_url = os.environ["DATABASE_URL"]
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()

    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False, na_values=[""])
    print(f"Loaded {len(df)} rows from {CSV_PATH.name}")

    rows: list[tuple] = []
    row_iter = df.iterrows()
    if tqdm is not None:
        row_iter = tqdm(row_iter, total=len(df), desc="procurement_lines", unit="row")

    for _, r in row_iter:
        rows.extend(build_line_rows(r))
    rows = renumber_line_indices(rows)

    print(f"Built {len(rows)} line rows")

    # Keep reruns idempotent and synced with parent table reruns.
    cur.execute("TRUNCATE TABLE public.procurement_decision_lines RESTART IDENTITY;")

    insert_sql = """
        INSERT INTO public.procurement_decision_lines (
            ada, line_type, line_index, source_field,
            counterparty_afm, counterparty_name,
            amount_raw, amount_eur, currency,
            kae_ale_number, remaining_kae_ale, remaining_available_credit,
            raw_line_json
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s::jsonb
        );
    """

    print("Inserting...", flush=True)
    psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=1000)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM public.procurement_decision_lines;")
    total = cur.fetchone()[0]
    cur.execute("SELECT line_type, COUNT(*) FROM public.procurement_decision_lines GROUP BY line_type ORDER BY 1;")
    by_type = cur.fetchall()

    print("\n[done] procurement_decision_lines table:")
    print(f"  Total rows: {total}")
    for line_type, count in by_type:
        print(f"  {line_type}: {count}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
