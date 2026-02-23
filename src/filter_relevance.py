"""
filter_relevance.py
-------------------
Local-only relevance filtering for Diavgeia records.

Reads:
- data/2026_diavgeia.csv (raw metadata dataset)
- data/pdf_pages_dataset.csv (parsed PDF text dataset, one row per PDF)

Writes:
- updates data/2026_diavgeia.csv with relevance columns
- writes data/2026_diavgeia_filtered.csv with only relevant rows

Matching strategy:
- normalize subject + PDF text (lowercase, remove accents, normalize separators)
- if any keyword matches either source -> keep row (`is_relevant=True`)

This script is intentionally local-only because it depends on local PDF downloads/parsing.
"""

from __future__ import annotations

import argparse
import os
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_DIR = SCRIPT_DIR.parent

DEFAULT_RAW_CSV = REPO_DIR / "data" / "2026_diavgeia.csv"
DEFAULT_PDF_PAGES_DATASET = REPO_DIR / "data" / "pdf_pages_dataset.csv"
DEFAULT_FILTERED_CSV = REPO_DIR / "data" / "2026_diavgeia_filtered.csv"
DEFAULT_RELEVANCE_LOG = REPO_DIR / "logs" / "relevance_filter_runs.csv"

RELEVANCE_KEYWORDS = [
    # "πυροπροστ",
    "αποψιλ",
    "δασοπροστ",
    # "αντιπυρ",
    "δασικων πυρκαγιων",
    "δασικη πυρκαγια",
    "δημιουργια περιμετρικων ζωνων",
    "δασικου οδικου δικτυου",
    "κλαδευσε",
    "αποκλαδευσ",
    "απόληψη δασικών προϊόντων",
    "εκτέλεση εργασιών πρόληψης και αντιμετώπισης εκτάκτων αναγκών από φυσικές – τεχνολογικές καταστροφές",
    "ΑΠΟΚΑΤΑΣΤΑΣΗ ΒΑΤΟΤΗΤΑΣ ΚΑΘΑΡΙΣΜΟΣ ΤΑΦΡΩΝ",
    "ΠΡΟΣΤΑΣΙΑΣ ΔΑΣΩΝ",
    "antiNERO",
    "αγροτικού οδικού δικτύου",
    "Προστασία και Αναβάθμιση Δασών",
    "δασικού παρατηρητηρίου",
    "πυροφυλακίου",
    "Ζώνες Δουλείας",
    "έκτακτης κάρπωσης",
    "καθαρισμός των υπαρχόντων δρόμων πρόσβασης",
    "Προϋπολογισμού",
    "ΠΥΡΑΣΦΑΛΕΙΑΣ",
    "ΠΕΡΙΜΕΤΡΙΚΩΝ ΖΩΝΩΝ",
    "ΠΡΟΣΤΑΣΙΑ & ΑΝΑΒΑΘΜΙΣΗ ΔΑΣΩΝ",
    "ΠΟΛΙΤΙΚΗΣ ΠΡΟΣΤΑΣΙΑΣ",
    "Έγκριση Επιχειρησιακού Σχεδιασμού",
    "δασικών πυρκαγιών",
    "Δημοτικού δάσους",
    "ΜΙΣΘΩΣΗ ΜΗΧΑΝΗΜΑΤΟΣ ΕΡΓΟΥ",
    "κλαδοτεμαχιστών",
    "Αγροτική Οδοποιία",
    "πυρκαγια",
    "ΚΑΤΑΣΒΕΣΗ ΔΑΣΙΚΗΣ ΠΥΡΚΑΓΙΑΣ",
    "ΠΥΡΟΜΕΤΕΩΡΟΛΟΓΙΚΟΥ",
    "σχεδίων Πολιτικής Προστασίας",
]

RELEVANCE_COLUMNS = [
    "subject_match",
    "pdf_match",
    "pdf_available_for_filter",
    "is_relevant",
    "matched_keywords_subject",
    "matched_keywords_pdf",
]


def ensure_local_only(allow_ci: bool = False) -> None:
    """Block execution in CI unless explicitly overridden."""
    in_ci = str(os.getenv("CI", "")).lower() in {"1", "true", "yes"} or bool(os.getenv("GITHUB_ACTIONS"))
    if in_ci and not allow_ci:
        raise RuntimeError(
            "filter_relevance.py is local-only and should not run in CI/GitHub Actions. "
            "Run it locally after fetch + PDF parsing."
        )


def normalize_text(value: Any) -> str:
    """Lowercase, remove accents, normalize final sigma, and collapse punctuation/whitespace."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""

    text = str(value).lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.replace("ς", "σ")
    text = re.sub(r"[^0-9a-zα-ω]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def collapse_list_for_csv(values: list[str]) -> Any:
    """Normalize match lists for CSV-friendly storage."""
    if not values:
        return pd.NA
    if len(values) == 1:
        return values[0]
    return values


def build_keyword_specs(keywords: list[str]) -> list[tuple[str, str]]:
    """
    Return [(original_keyword, normalized_keyword)].

    Deduplicates by normalized form while preserving first-seen original text for output.
    """
    specs: list[tuple[str, str]] = []
    seen_norm: set[str] = set()
    for kw in keywords:
        kw_norm = normalize_text(kw)
        if not kw_norm or kw_norm in seen_norm:
            continue
        specs.append((kw, kw_norm))
        seen_norm.add(kw_norm)
    return specs


def find_matching_keywords(text: Any, keyword_specs: list[tuple[str, str]]) -> list[str]:
    """Return original keywords whose normalized forms appear in normalized text."""
    text_norm = normalize_text(text)
    if not text_norm:
        return []
    return [orig for orig, norm in keyword_specs if norm in text_norm]


def build_pdf_text_lookup(pdf_pages_dataset: Path, chunksize: int = 5000) -> dict[str, str]:
    """
    Build {ada: text} lookup from parsed PDF dataset without joining into the raw dataframe.

    Reads only the required columns and processes in chunks to avoid memory spikes.
    If duplicate ADA rows appear, keeps the longer non-empty text.
    """
    if not pdf_pages_dataset.exists():
        print(f"[relevance][pdf] parsed PDF dataset not found: {pdf_pages_dataset}", flush=True)
        return {}

    lookup: dict[str, str] = {}
    rows_seen = 0
    try:
        for chunk in pd.read_csv(pdf_pages_dataset, usecols=["ada", "text"], chunksize=chunksize):
            rows_seen += len(chunk)
            for _, row in chunk.iterrows():
                ada = row.get("ada")
                if not isinstance(ada, str) or not ada.strip():
                    continue
                text = row.get("text")
                text_str = "" if pd.isna(text) else str(text)
                prev = lookup.get(ada)
                if prev is None or (len(text_str) > len(prev)):
                    lookup[ada] = text_str
            if rows_seen % (chunksize * 5) == 0:
                print(
                    f"[relevance][pdf] loaded rows={rows_seen} unique_adas={len(lookup)}",
                    flush=True,
                )
    except ValueError as exc:
        raise KeyError(
            f"Expected columns ['ada', 'text'] in {pdf_pages_dataset}. "
            "Rebuild the PDF dataset with src/pdf_pipeline.py."
        ) from exc

    print(f"[relevance][pdf] done rows={rows_seen} unique_adas={len(lookup)}", flush=True)
    return lookup


def ensure_relevance_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure the raw dataframe has the relevance columns."""
    df = df.copy()
    defaults = {
        "subject_match": False,
        "pdf_match": False,
        "pdf_available_for_filter": False,
        "is_relevant": False,
        "matched_keywords_subject": pd.NA,
        "matched_keywords_pdf": pd.NA,
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


def append_relevance_run_log(log_path: Path, row: dict[str, Any]) -> None:
    """Append one relevance-filter run record to CSV."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_df = pd.DataFrame([row])
    log_df.to_csv(log_path, mode="a", header=not log_path.exists(), index=False)
    print(f"[relevance][log] appended -> {log_path}", flush=True)


def apply_relevance_filter(
    df: pd.DataFrame,
    pdf_text_by_ada: dict[str, str],
    keyword_specs: list[tuple[str, str]],
    progress_every: int = 250,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Compute relevance columns using subject and PDF text lookup (by ADA) without joining datasets.
    """
    if "ada" not in df.columns:
        raise KeyError("Expected column 'ada' in raw dataframe")
    if "subject" not in df.columns:
        raise KeyError("Expected column 'subject' in raw dataframe")

    df = ensure_relevance_columns(df)
    total = len(df)
    progress_every = max(1, int(progress_every))

    print(
        f"[relevance] start rows={total} keywords={len(keyword_specs)} pdf_lookup_adas={len(pdf_text_by_ada)}",
        flush=True,
    )

    subject_yes = 0
    pdf_yes = 0
    relevant_yes = 0
    pdf_available_yes = 0

    for processed, idx in enumerate(df.index, start=1):
        ada = df.at[idx, "ada"]
        subject = df.at[idx, "subject"]

        subject_matches = find_matching_keywords(subject, keyword_specs)
        subject_match = len(subject_matches) > 0

        pdf_text = pdf_text_by_ada.get(ada, "") if isinstance(ada, str) else ""
        pdf_available = isinstance(pdf_text, str) and bool(pdf_text.strip())
        pdf_matches = find_matching_keywords(pdf_text, keyword_specs) if pdf_available else []
        pdf_match = len(pdf_matches) > 0

        is_relevant = bool(subject_match or pdf_match)

        df.at[idx, "subject_match"] = subject_match
        df.at[idx, "pdf_match"] = pdf_match
        df.at[idx, "pdf_available_for_filter"] = pdf_available
        df.at[idx, "is_relevant"] = is_relevant
        df.at[idx, "matched_keywords_subject"] = collapse_list_for_csv(subject_matches)
        df.at[idx, "matched_keywords_pdf"] = collapse_list_for_csv(pdf_matches)

        subject_yes += int(subject_match)
        pdf_yes += int(pdf_match)
        pdf_available_yes += int(pdf_available)
        relevant_yes += int(is_relevant)

        if processed % progress_every == 0 or processed == total:
            print(
                f"[relevance][progress] {processed}/{total} "
                f"relevant={relevant_yes} subject_match={subject_yes} "
                f"pdf_match={pdf_yes} pdf_available={pdf_available_yes}",
                flush=True,
            )

    stats = {
        "rows_total": total,
        "rows_relevant": relevant_yes,
        "rows_not_relevant": total - relevant_yes,
        "rows_subject_match": subject_yes,
        "rows_pdf_match": pdf_yes,
        "rows_pdf_available": pdf_available_yes,
    }
    print(f"[relevance] done {stats}", flush=True)
    return df, stats


def run_relevance_filter(
    input_csv: Path,
    pdf_pages_dataset: Path,
    filtered_output_csv: Path,
    log_csv: Path,
    progress_every: int = 250,
) -> dict[str, int]:
    """Load datasets, compute relevance columns, persist raw+filtered outputs."""
    run_started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    success = False
    error_message = "NONE"

    if not input_csv.exists():
        raise FileNotFoundError(f"Raw dataset not found: {input_csv}")

    stats: dict[str, int] = {
        "rows_total": 0,
        "rows_relevant": 0,
        "rows_not_relevant": 0,
        "rows_subject_match": 0,
        "rows_pdf_match": 0,
        "rows_pdf_available": 0,
    }
    filtered_rows = 0

    try:
        print(f"[relevance] loading raw dataset -> {input_csv}", flush=True)
        raw_df = pd.read_csv(input_csv)
        keyword_specs = build_keyword_specs(RELEVANCE_KEYWORDS)
        pdf_lookup = build_pdf_text_lookup(pdf_pages_dataset)

        enriched_df, stats = apply_relevance_filter(
            raw_df,
            pdf_text_by_ada=pdf_lookup,
            keyword_specs=keyword_specs,
            progress_every=progress_every,
        )

        # Persist raw dataset with new relevance columns.
        enriched_df.to_csv(input_csv, index=False)
        print(f"[relevance] updated raw dataset -> {input_csv}", flush=True)

        # Persist filtered dataset (DB input candidate).
        filtered_output_csv.parent.mkdir(parents=True, exist_ok=True)
        filtered_df = enriched_df[enriched_df["is_relevant"] == True].copy()  # noqa: E712
        filtered_rows = len(filtered_df)
        filtered_df.to_csv(filtered_output_csv, index=False)
        print(
            f"[relevance] wrote filtered dataset -> {filtered_output_csv} rows={filtered_rows}",
            flush=True,
        )
        success = True
        return stats
    except Exception as exc:
        error_message = f"{type(exc).__name__}: {exc}"
        print(f"[relevance][error] {error_message}", flush=True)
        raise
    finally:
        append_relevance_run_log(
            log_csv,
            {
                "run_started_at_local": run_started_at,
                "input_csv": str(input_csv),
                "pdf_pages_dataset": str(pdf_pages_dataset),
                "filtered_output_csv": str(filtered_output_csv),
                "keywords_count": len(build_keyword_specs(RELEVANCE_KEYWORDS)),
                "rows_total": stats.get("rows_total", 0),
                "rows_relevant": stats.get("rows_relevant", 0),
                "rows_not_relevant": stats.get("rows_not_relevant", 0),
                "rows_subject_match": stats.get("rows_subject_match", 0),
                "rows_pdf_match": stats.get("rows_pdf_match", 0),
                "rows_pdf_available": stats.get("rows_pdf_available", 0),
                "filtered_rows_written": filtered_rows,
                "success": success,
                "error_message": error_message,
            },
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Local-only relevance filter for Diavgeia raw + PDF datasets")
    parser.add_argument("--input-csv", default=str(DEFAULT_RAW_CSV), help="Raw Diavgeia CSV path")
    parser.add_argument(
        "--pdf-pages-dataset",
        default=str(DEFAULT_PDF_PAGES_DATASET),
        help="Parsed PDF text dataset CSV path (must include ada,text)",
    )
    parser.add_argument(
        "--filtered-output",
        default=str(DEFAULT_FILTERED_CSV),
        help="Output CSV path for relevant-only dataset",
    )
    parser.add_argument(
        "--log-csv",
        default=str(DEFAULT_RELEVANCE_LOG),
        help="Run log CSV path for relevance filter runs",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=250,
        help="Print progress every N raw rows",
    )
    parser.add_argument(
        "--allow-ci",
        action="store_true",
        help="Override local-only guard (not recommended)",
    )
    args = parser.parse_args()

    ensure_local_only(allow_ci=args.allow_ci)

    run_relevance_filter(
        input_csv=Path(args.input_csv),
        pdf_pages_dataset=Path(args.pdf_pages_dataset),
        filtered_output_csv=Path(args.filtered_output),
        log_csv=Path(args.log_csv),
        progress_every=args.progress_every,
    )


if __name__ == "__main__":
    main()
