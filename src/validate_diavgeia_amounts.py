"""
Notebook-friendly validation of Diavgeia amount values against parsed PDF text.

Use case:
- read amount value as stored in raw `data/2026_diavgeia.csv`
- find the corresponding PDF text by ADA from `data/pdf_pages_dataset.csv`
- check exact presence
- if not present, find amount-like strings in the PDF with the same digits but
  different separators (., spaces, etc.)

This does NOT modify any data.
"""

from __future__ import annotations

import argparse
import ast
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


REPO_DIR = Path(__file__).resolve().parent.parent
DEFAULT_RAW_CSV = REPO_DIR / "data" / "2026_diavgeia.csv"
DEFAULT_PDF_DATASET = REPO_DIR / "data" / "pdf_pages_dataset.csv"

AMOUNT_FIELDS = [
    "spending_contractors_value",
    "commitment_amount_with_vat",
    "direct_value",
    "payment_value",
]


@dataclass
class AmountValidationResult:
    ada: str
    field: str
    raw_value: str | None
    raw_value_candidates: list[str]
    pdf_available: bool
    exact_match_found: bool
    exact_matches: list[str]
    same_digits_candidates: list[str]


def _is_blank(v: Any) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    return s == "" or s.lower() == "nan"


def _parse_csv_value(v: Any) -> Any:
    """Parse list-repr strings like \"['8.755,24']\"; otherwise return original."""
    if isinstance(v, (list, dict)):
        return v
    if _is_blank(v):
        return None
    s = str(v).strip()
    if s.startswith("[") or s.startswith("{"):
        try:
            return ast.literal_eval(s)
        except Exception:
            return s
    return s


def _flatten_amount_strings(v: Any) -> list[str]:
    parsed = _parse_csv_value(v)
    if parsed is None:
        return []
    if isinstance(parsed, list):
        out: list[str] = []
        for item in parsed:
            if _is_blank(item):
                continue
            out.append(str(item).strip())
        return out
    if isinstance(parsed, dict):
        return []
    s = str(parsed).strip()
    return [s] if s else []


def amount_digits(value: str) -> str:
    return "".join(ch for ch in str(value) if ch.isdigit())


def extract_amount_like_strings(text: str) -> list[str]:
    """
    Extract amount-like strings from PDF text.

    Accepts common Greek/OCR variants:
    - 8.755,24
    - 8755,24
    - 8 755,24
    - 8,755.24 (rare OCR/format issue)
    """
    if not text:
        return []

    pattern = re.compile(
        r"(?<!\d)"
        r"(?:\d{1,3}(?:[.,\s]\d{3})+(?:[.,]\d{2})|\d+(?:[.,]\d{2}))"
        r"(?!\d)"
    )
    matches = pattern.findall(text)

    # De-duplicate preserving order
    seen: set[str] = set()
    out: list[str] = []
    for m in matches:
        s = re.sub(r"\s+", " ", m).strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def build_same_digits_regex(raw_amount: str) -> re.Pattern[str]:
    """
    Build a permissive regex that matches the same digits with optional separators.

    Example raw '8.755,24' -> matches '8755,24', '8 755,24', '8.755,24', etc.
    """
    digits = amount_digits(raw_amount)
    if not digits:
        # no digits -> regex that matches nothing
        return re.compile(r"(?!x)x")

    sep = r"[\s.,]*"
    body = sep.join(re.escape(d) for d in digits)
    return re.compile(rf"(?<!\d){body}(?!\d)")


def load_pdf_text_lookup(pdf_pages_dataset: Path = DEFAULT_PDF_DATASET) -> dict[str, str]:
    if not pdf_pages_dataset.exists():
        raise FileNotFoundError(f"PDF dataset not found: {pdf_pages_dataset}")

    lookup: dict[str, str] = {}
    for chunk in pd.read_csv(pdf_pages_dataset, usecols=["ada", "text"], chunksize=5000):
        for _, row in chunk.iterrows():
            ada = str(row.get("ada") or "").strip()
            if not ada:
                continue
            text = "" if pd.isna(row.get("text")) else str(row.get("text"))
            prev = lookup.get(ada, "")
            if len(text) > len(prev):
                lookup[ada] = text
    return lookup


def load_raw_rows(raw_csv: Path = DEFAULT_RAW_CSV) -> pd.DataFrame:
    if not raw_csv.exists():
        raise FileNotFoundError(f"Raw CSV not found: {raw_csv}")
    return pd.read_csv(raw_csv, dtype=str, keep_default_na=False, na_values=[""])


def validate_amount_for_ada(
    ada: str,
    *,
    field: str = "direct_value",
    raw_df: pd.DataFrame | None = None,
    pdf_text_by_ada: dict[str, str] | None = None,
    raw_csv: Path = DEFAULT_RAW_CSV,
    pdf_pages_dataset: Path = DEFAULT_PDF_DATASET,
) -> AmountValidationResult:
    if field not in AMOUNT_FIELDS:
        raise ValueError(f"Unsupported field '{field}'. Choose one of: {AMOUNT_FIELDS}")

    if raw_df is None:
        raw_df = load_raw_rows(raw_csv)
    if pdf_text_by_ada is None:
        pdf_text_by_ada = load_pdf_text_lookup(pdf_pages_dataset)

    rows = raw_df[raw_df["ada"].astype(str) == ada]
    if rows.empty:
        raise KeyError(f"ADA not found in raw CSV: {ada}")

    # If duplicates exist, use the first row; notebook users can inspect all rows separately.
    row = rows.iloc[0]
    raw_values = _flatten_amount_strings(row.get(field))
    raw_value = raw_values[0] if raw_values else None

    pdf_text = pdf_text_by_ada.get(ada, "")
    pdf_available = bool(pdf_text.strip())

    exact_matches: list[str] = []
    same_digits_candidates: list[str] = []

    if pdf_available and raw_values:
        for val in raw_values:
            if val in pdf_text and val not in exact_matches:
                exact_matches.append(val)

        pdf_amounts = extract_amount_like_strings(pdf_text)
        raw_digit_sets = {amount_digits(v) for v in raw_values if amount_digits(v)}
        for cand in pdf_amounts:
            if amount_digits(cand) in raw_digit_sets and cand not in same_digits_candidates:
                same_digits_candidates.append(cand)

        # Fallback: if OCR spacing/punctuation breaks token extraction, use per-value digit regex
        if not same_digits_candidates:
            fallback_hits: list[str] = []
            for raw_val in raw_values:
                rx = build_same_digits_regex(raw_val)
                for m in rx.finditer(pdf_text):
                    hit = m.group(0).strip()
                    if hit and hit not in fallback_hits:
                        fallback_hits.append(hit)
            same_digits_candidates = fallback_hits

    return AmountValidationResult(
        ada=ada,
        field=field,
        raw_value=raw_value,
        raw_value_candidates=raw_values,
        pdf_available=pdf_available,
        exact_match_found=bool(exact_matches),
        exact_matches=exact_matches,
        same_digits_candidates=same_digits_candidates,
    )


def find_suspicious_direct_values(
    *,
    raw_df: pd.DataFrame | None = None,
    raw_csv: Path = DEFAULT_RAW_CSV,
    limit: int | None = None,
) -> pd.DataFrame:
    """
    Return a notebook-friendly subset of potentially problematic direct assignment values.
    """
    if raw_df is None:
        raw_df = load_raw_rows(raw_csv)

    df = raw_df.copy()
    if "direct_value" not in df.columns:
        return df.iloc[0:0]

    direct_num = (
        df["direct_value"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    df["_direct_num"] = pd.to_numeric(direct_num, errors="coerce")

    mask = (
        df["decisionType"].astype(str).str.startswith("ΑΝΑΘΕΣΗ", na=False)
        & df["subject"].astype(str).str.contains("απευθείας ανάθεση", case=False, na=False)
        & df["org_type"].astype(str).str.startswith("ΔΗΜ", na=False)
        & (df["_direct_num"].fillna(0) >= 300_000)
    )

    cols = ["ada", "issueDate", "org_type", "org_name_clean", "decisionType", "direct_value", "subject"]
    out = df.loc[mask, [c for c in cols if c in df.columns]].copy()
    if limit is not None:
        out = out.head(limit)
    return out


def _print_result(r: AmountValidationResult) -> None:
    print(f"ADA: {r.ada}")
    print(f"Field: {r.field}")
    print(f"Raw value(s): {r.raw_value_candidates if r.raw_value_candidates else '—'}")
    print(f"PDF available: {r.pdf_available}")
    print(f"Exact match found: {r.exact_match_found}")
    if r.exact_matches:
        print(f"Exact matches in PDF: {r.exact_matches}")
    if r.same_digits_candidates:
        print(f"Same-digits candidates in PDF: {r.same_digits_candidates}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate Diavgeia raw amount values against parsed PDF text")
    parser.add_argument("--ada", help="ADA to validate (e.g. ΨΣΣΞΩ9Ζ-5Η5)")
    parser.add_argument("--field", default="direct_value", choices=AMOUNT_FIELDS)
    parser.add_argument("--raw-csv", default=str(DEFAULT_RAW_CSV))
    parser.add_argument("--pdf-pages-dataset", default=str(DEFAULT_PDF_DATASET))
    parser.add_argument("--list-suspicious", action="store_true", help="List suspicious direct assignment values")
    parser.add_argument("--limit", type=int, default=20, help="Limit rows for --list-suspicious")
    args = parser.parse_args()

    raw_csv = Path(args.raw_csv)
    pdf_ds = Path(args.pdf_pages_dataset)

    if args.list_suspicious:
        df = find_suspicious_direct_values(raw_csv=raw_csv, limit=args.limit)
        if df.empty:
            print("No suspicious rows found.")
        else:
            print(df.to_string(index=False, max_colwidth=140))
        return

    if not args.ada:
        parser.error("Provide --ada or use --list-suspicious")

    raw_df = load_raw_rows(raw_csv)
    pdf_lookup = load_pdf_text_lookup(pdf_ds)
    result = validate_amount_for_ada(
        args.ada,
        field=args.field,
        raw_df=raw_df,
        pdf_text_by_ada=pdf_lookup,
    )
    _print_result(result)


if __name__ == "__main__":
    main()

