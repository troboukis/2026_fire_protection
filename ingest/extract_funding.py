"""
extract_funding.py
------------------
Extracts municipal fire protection funding allocations from PDF decisions
(Κατανομή ΚΑΠ Πυροπροστασίας) issued by the Ministry of Interior.

Produces: data/funding/municipal_funding.csv

Each row = one recipient (municipality or municipal association) per decision.

Output columns:
  year              fiscal year of allocation
  allocation_type   τακτική / συμπληρωματική / έκτακτη
  recipient_type    δήμος / σύνδεσμος
  recipient_raw     name as it appears in the document (uppercase)
  nomos             prefecture (uppercase)
  municipality_code ΚΩΔ. ΤΠΔ code (municipalities only, nullable)
  amount_eur        allocated amount in euros (float)
  source_file       source PDF filename
  source_ada        ADA identifier (from filename where available)
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pdfplumber

REPO_DIR = Path(__file__).resolve().parent.parent
FUNDING_DIR = REPO_DIR / "data" / "funding"
OUTPUT_PATH = FUNDING_DIR / "municipal_funding.csv"

# ---------------------------------------------------------------------------
# File manifest — metadata confirmed by reading first page of each PDF
# Duplicate: apof10427-05042018.pdf == 6ΖΜ9465ΧΘ7-8ΡΖ (same protocol 10427)
# ---------------------------------------------------------------------------

FILE_MANIFEST = [
    # 2016
    {
        "file": "apof15717-110516.pdf",
        "year": 2016,
        "allocation_type": "τακτική",
        "recipient_type": "δήμος",
        "ada": "7ΑΡ5465ΦΘΕ-ΡΘΖ",
    },
    {
        "file": "apof12471-130516.pdf",
        "year": 2016,
        "allocation_type": "τακτική",
        "recipient_type": "σύνδεσμος",
        "ada": "6ΜΔΕ465ΦΘΕ-ΝΩΡ",
    },
    # 2017
    {
        "file": "ΨΠΝ3465ΧΘ7-3ΡΥ.pdf",
        "year": 2017,
        "allocation_type": "τακτική",
        "recipient_type": "δήμος",
        "ada": "ΨΠΝ3465ΧΘ7-3ΡΥ",
    },
    {
        "file": "apof15124-15052017.pdf",
        "year": 2017,
        "allocation_type": "τακτική",
        "recipient_type": "σύνδεσμος",
        "ada": "",
    },
    {
        "file": "7ΚΧΨ465ΧΘ7-Ξ20.pdf",
        "year": 2017,
        "allocation_type": "έκτακτη",
        "recipient_type": "δήμος",
        "ada": "7ΚΧΨ465ΧΘ7-Ξ20",
    },
    # 2018 (apof10427-05042018.pdf is duplicate of 6ΖΜ9465ΧΘ7-8ΡΖ — skip it)
    {
        "file": "6ΖΜ9465ΧΘ7-8ΡΖ_τακτική_δήμοι_2018.pdf",
        "year": 2018,
        "allocation_type": "τακτική",
        "recipient_type": "δήμος",
        "ada": "6ΖΜ9465ΧΘ7-8ΡΖ",
    },
    {
        "file": "649Χ465ΧΘ7-ΡΝΒ_τακτική_σύνδεσμοι_ΟΤΑ_2018.pdf",
        "year": 2018,
        "allocation_type": "τακτική",
        "recipient_type": "σύνδεσμος",
        "ada": "649Χ465ΧΘ7-ΡΝΒ",
    },
    # 2019
    {
        "file": "6ΔΖΟ465ΧΘ7-368_τακτική_δήμοι_2019.pdf",
        "year": 2019,
        "allocation_type": "τακτική",
        "recipient_type": "δήμος",
        "ada": "6ΔΖΟ465ΧΘ7-368",
    },
    {
        "file": "Ψ0ΜΣ465ΧΘ7-Υ78_τακτική_σύνδεσμοι_ΟΤΑ_2019.pdf",
        "year": 2019,
        "allocation_type": "τακτική",
        "recipient_type": "σύνδεσμος",
        "ada": "Ψ0ΜΣ465ΧΘ7-Υ78",
    },
    # 2020
    {
        "file": "ΩΚΓΔ46ΜΤΛ6-ΝΣ2_τακτική_δήμοι_2020.pdf",
        "year": 2020,
        "allocation_type": "τακτική",
        "recipient_type": "δήμος",
        "ada": "ΩΚΓΔ46ΜΤΛ6-ΝΣ2",
    },
    {
        "file": "ΩΨΖΝ46ΜΤΛ6-Κ0Δ_τακτική_σύνδεσμοι_ΟΤΑ_2020.pdf",
        "year": 2020,
        "allocation_type": "τακτική",
        "recipient_type": "σύνδεσμος",
        "ada": "ΩΨΖΝ46ΜΤΛ6-Κ0Δ",
    },
    # 2021
    {
        "file": "9Γ6Ξ46ΜΤΛ6-Χ2Ι_τακτική_δήμοι_2021.pdf",
        "year": 2021,
        "allocation_type": "τακτική",
        "recipient_type": "δήμος",
        "ada": "9Γ6Ξ46ΜΤΛ6-Χ2Ι",
    },
    {
        "file": "617Ρ46ΜΤΛ6-ΝΧ5_τακτική_σύνδεσμοι_δήμων_2021.pdf",
        "year": 2021,
        "allocation_type": "τακτική",
        "recipient_type": "σύνδεσμος",
        "ada": "617Ρ46ΜΤΛ6-ΝΧ5",
    },
    # 2022
    {
        "file": "6ΠΝ946ΜΤΛ6-ΑΧ8_τακτική_δήμοι_2022.pdf",
        "year": 2022,
        "allocation_type": "τακτική",
        "recipient_type": "δήμος",
        "ada": "6ΠΝ946ΜΤΛ6-ΑΧ8",
    },
    {
        "file": "ΨΛΦ146ΜΤΛ6-4ΣΘ_συμπληρωματική_δήμοι_2022.pdf",
        "year": 2022,
        "allocation_type": "συμπληρωματική",
        "recipient_type": "δήμος",
        "ada": "ΨΛΦ146ΜΤΛ6-4ΣΘ",
    },
    {
        "file": "ΨΧ0Υ46ΜΤΛ6-89Σ_συμπληρωματική_δήμοι_2022.pdf",
        "year": 2022,
        "allocation_type": "συμπληρωματική",
        "recipient_type": "δήμος",
        "ada": "ΨΧ0Υ46ΜΤΛ6-89Σ",
    },
    # Ψ7146: scanned image-based PDF — no text or tables extractable without OCR
    # {
    #     "file": "Ψ7146ΜΤΛ6-ΖΥΡ_συμπληρωματική_δήμοι_σύνδεσμοι_δήμων_2022.pdf",
    #     "year": 2022,
    #     "allocation_type": "συμπληρωματική",
    #     "recipient_type": "δήμος",
    #     "ada": "Ψ7146ΜΤΛ6-ΖΥΡ",
    # },
    # 2023 — NOT IN COLLECTION (gap in documents)
    # 2024
    {
        "file": "apof59051-20240813.pdf",
        "year": 2024,
        "allocation_type": "έκτακτη",   # emergency post-fire (11 Aug 2024 fire)
        "recipient_type": "δήμος",
        "ada": "92ΑΞ46ΜΤΛ6-ΗΣΠ",
    },
    # 2025
    {
        "file": "apof12856-20250311.pdf",
        "year": 2025,
        "allocation_type": "τακτική",
        "recipient_type": "δήμος",
        "ada": "",
    },
]


# ---------------------------------------------------------------------------
# Amount parsing
# ---------------------------------------------------------------------------

def parse_greek_amount(value: str | None) -> float | None:
    """Parse Greek-formatted number: '27.000,00' -> 27000.0"""
    if not value:
        return None
    clean = str(value).strip().replace(".", "").replace(",", ".")
    clean = re.sub(r"[^\d.]", "", clean)
    try:
        return float(clean)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Table column detection
# ---------------------------------------------------------------------------

def find_col_index(header_row: list, keywords: list[str]) -> int | None:
    """Find column index by matching any of the keywords (case-insensitive)."""
    for i, cell in enumerate(header_row):
        if cell and any(kw.lower() in str(cell).lower() for kw in keywords):
            return i
    return None


def detect_data_col_offset(table: list[list]) -> int:
    """
    Some PDFs have a leading empty column in the header that offsets header
    labels by 1, but data is always anchored at position 0.
    Returns the offset to subtract from header-detected positions.
    """
    for row in table[:4]:
        if not row:
            continue
        cells = [str(c or "").strip() for c in row]
        # If the first non-empty cell is A/A at position > 0, we have an offset
        for i, cell in enumerate(cells):
            if re.match(r"^[AΑ]/[AΑ]$", cell):
                return i  # offset = i (subtract from all header positions)
    return 0


def extract_rows_from_table(
    table: list[list],
    recipient_type: str,
) -> list[dict]:
    """
    Extract structured rows from a pdfplumber table.

    Handles two layouts:
    - δήμος 15-col: A/A | ΚΩΔ.ΤΠΔ | ΔΗΜΟΣ | ΝΟΜΟΣ | ΠΟΣΟ
      Data always at positions 0, 3, 6, 9, 12 regardless of header offset.
    - σύνδεσμος 8-col: A/A | ΟΝΟΜΑ ΣΥΝΔΕΣΜΟΥ | ΝΟΜΟΣ | ΠΟΣΟ
      Uses keyword-based column detection.
    """
    if not table or len(table) < 2:
        return []

    # Locate header row (first row with ≥3 non-null cells)
    header = None
    data_start = 0
    for i, row in enumerate(table[:4]):
        non_null = [c for c in row if c and str(c).strip()]
        if len(non_null) >= 3:
            header = row
            data_start = i + 1
            break

    if header is None:
        return []

    ncols = len(header)

    # --- 15-column layout (δήμος standard format) ---
    # Data is pinned to positions 0, 3, 6, 9, 12 regardless of header offset.
    if ncols >= 14:
        rows = []
        for row in table[data_start:]:
            if not row or len(row) < 13:
                continue

            aa_val = str(row[0] or "").strip()
            # Skip blanks, header repeats, summary rows
            if not aa_val or aa_val.upper() in {"A/A", "Α/Α", "ΣΥΝΟΛΟ", "TOTAL"}:
                continue
            if not re.match(r"^\d+$", aa_val):
                continue

            code  = str(row[3] or "").strip()
            name  = str(row[6] or "").replace("\n", " ").strip()
            nomos = str(row[9] or "").replace("\n", " ").strip()
            amt   = parse_greek_amount(str(row[12] or ""))

            if not name or amt is None or amt <= 0:
                continue

            rows.append({
                "recipient_raw":     name.upper(),
                "nomos":             nomos.upper(),
                "municipality_code": code if recipient_type == "δήμος" else "",
                "amount_eur":        amt,
            })
        return rows

    # --- 13-column layout (2017 δήμος format) ---
    # Positions: 0=A/A, 1=ΚΩΔ.ΤΠΔ, 4=ΔΗΜΟΣ, 7=ΝΟΜΟΣ, 10=ΠΟΣΟ
    if ncols == 13:
        rows = []
        for row in table[data_start:]:
            if not row or len(row) < 11:
                continue

            aa_val = str(row[0] or "").strip()
            if not aa_val or aa_val.upper() in {"A/A", "Α/Α", "ΣΥΝΟΛΟ", "TOTAL"}:
                continue
            if not re.match(r"^\d+$", aa_val):
                continue

            code  = str(row[1] or "").strip()
            name  = str(row[4] or "").replace("\n", " ").strip()
            nomos = str(row[7] or "").replace("\n", " ").strip()
            amt   = parse_greek_amount(str(row[10] or ""))

            if not name or amt is None or amt <= 0:
                continue

            rows.append({
                "recipient_raw":     name.upper(),
                "nomos":             nomos.upper(),
                "municipality_code": code if recipient_type == "δήμος" else "",
                "amount_eur":        amt,
            })
        return rows

    # --- 8-column σύνδεσμοι layout ---
    # Fixed positions: 0=A/A, 1=name, 3=nomos, 5=amount
    # (2017/2018 headers mislabel amount at col 6; data is always at col 5)
    if ncols == 8:
        rows = []
        for row in table[data_start:]:
            if not row or len(row) < 6:
                continue
            aa_val = str(row[0] or "").strip()
            if not aa_val or aa_val.upper() in {"A/A", "Α/Α", "ΣΥΝΟΛΟ", "TOTAL"}:
                continue
            if not re.match(r"^\d+$", aa_val):
                continue
            name  = str(row[1] or "").replace("\n", " ").strip()
            nomos = str(row[3] or "").replace("\n", " ").strip()
            amt   = parse_greek_amount(str(row[5] or ""))
            if not name or amt is None or amt <= 0:
                continue
            rows.append({
                "recipient_raw":     name.upper(),
                "nomos":             nomos.upper(),
                "municipality_code": "",
                "amount_eur":        amt,
            })
        return rows

    # --- Narrower layout (σύνδεσμοι, smaller tables) ---
    offset = detect_data_col_offset(table)
    name_col  = find_col_index(header, ["ΔΗΜΟΣ", "ΟΝΟΜΑ", "ΣΥΝΔΕΣΜΟΥ"])
    nomos_col = find_col_index(header, ["ΝΟΜΟΣ", "ΝΟΜΑΡΧ"])
    amt_col   = find_col_index(header, ["ΠΟΣΟ", "ΚΑΤΑΝΟΜ", "ΠΡΟΤΑΣΗ", "ΕΠΙΧΟΡ"])

    if name_col is None or amt_col is None:
        return []

    # Adjust for header offset
    name_col  = max(0, name_col  - offset)
    nomos_col = max(0, nomos_col - offset) if nomos_col is not None else None
    amt_col   = max(0, amt_col   - offset)

    rows = []
    for row in table[data_start:]:
        if not row or len(row) <= amt_col:
            continue

        aa_val = str(row[0] or "").strip()
        if not aa_val or aa_val.upper() in {"A/A", "Α/Α", "ΣΥΝΟΛΟ", "TOTAL"}:
            continue
        if not re.match(r"^\d+$", aa_val):
            continue

        name  = str(row[name_col] or "").replace("\n", " ").strip()
        nomos = str(row[nomos_col] or "").replace("\n", " ").strip() if nomos_col is not None else ""
        amt   = parse_greek_amount(str(row[amt_col] or ""))

        if not name or amt is None or amt <= 0:
            continue

        rows.append({
            "recipient_raw":     name.upper(),
            "nomos":             nomos.upper(),
            "municipality_code": "",
            "amount_eur":        amt,
        })

    return rows


# ---------------------------------------------------------------------------
# Per-file extraction
# ---------------------------------------------------------------------------

def extract_file(meta: dict) -> list[dict]:
    path = FUNDING_DIR / meta["file"]
    if not path.exists():
        print(f"  [skip] file not found: {meta['file']}", flush=True)
        return []

    print(f"[{meta['year']}] {meta['file']}", flush=True)
    all_rows: list[dict] = []

    with pdfplumber.open(str(path)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                rows = extract_rows_from_table(table, meta["recipient_type"])
                all_rows.extend(rows)

        # Fallback for files with no tables: extract from raw text
        if not all_rows:
            print(f"  [warn] no tables found, trying text extraction", flush=True)
            full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
            for line in full_text.split("\n"):
                # Pattern: number  NAME  NOMOS  AMOUNT (tabular text layout)
                m = re.match(
                    r"^\d+\s+(.+?)\s{2,}(.+?)\s{2,}([\d.,]+)\s*$", line.strip()
                )
                if m:
                    amt = parse_greek_amount(m.group(3))
                    if amt and amt > 0:
                        all_rows.append({
                            "recipient_raw": m.group(1).upper().strip(),
                            "nomos": m.group(2).upper().strip(),
                            "municipality_code": "",
                            "amount_eur": amt,
                        })
            # Pattern for single-municipality inline decisions:
            # "ποσού X.XXX,XX € στον Δήμο MUNICIPALITY"
            # Note: some PDFs use ∆ (U+2206) for Δ and µ (U+00B5) for μ
            if not all_rows:
                m = re.search(
                    r"ποσο[υύ]\s+([\d.,]+)\s*€\s+στον\s+[Δ∆]ή[µμ]ο\s+(\S+)",
                    full_text,
                )
                if m:
                    amt = parse_greek_amount(m.group(1))
                    mname = m.group(2).rstrip(".,;")
                    if amt and amt > 0:
                        all_rows.append({
                            "recipient_raw": f"ΔΗΜΟΣ {mname.upper()}",
                            "nomos": "",
                            "municipality_code": "",
                            "amount_eur": amt,
                        })

    # Attach file-level metadata
    result = []
    for row in all_rows:
        result.append({
            "year":              meta["year"],
            "allocation_type":   meta["allocation_type"],
            "recipient_type":    meta["recipient_type"],
            "recipient_raw":     row["recipient_raw"],
            "nomos":             row["nomos"],
            "municipality_code": row["municipality_code"],
            "amount_eur":        row["amount_eur"],
            "source_file":       meta["file"],
            "source_ada":        meta["ada"],
        })

    print(f"  -> {len(result)} rows extracted", flush=True)
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    all_rows: list[dict] = []
    for meta in FILE_MANIFEST:
        all_rows.extend(extract_file(meta))

    df = pd.DataFrame(all_rows)

    # Drop exact duplicates (shouldn't happen but safe)
    df = df.drop_duplicates()

    df.to_csv(OUTPUT_PATH, index=False)

    print(f"\n[done] {len(df)} rows -> {OUTPUT_PATH}")
    print("\nBy year and allocation type:")
    summary = df.groupby(["year", "allocation_type", "recipient_type"]).agg(
        rows=("recipient_raw", "count"),
        total_eur=("amount_eur", "sum"),
    ).reset_index()
    print(summary.to_string(index=False))
    print(f"\nNotes:")
    print(f"  - 2023 allocation decisions are NOT in the collection (coverage gap).")
    print(f"  - Ψ7146ΜΤΛ6-ΖΥΡ (2022 συμπληρωματική) is a scanned image PDF — excluded (no OCR).")
    print(f"  - 2016 δήμοι (apof15717): only 49 rows — likely Athens-area partial allocation.")


if __name__ == "__main__":
    main()
