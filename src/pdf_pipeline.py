"""
pdf_pipeline.py
---------------
Local PDF pipeline for Diavgeia records.

Features:
1) Download missing PDFs from documentUrl into local ./pdf directory.
2) Read local PDFs page-by-page and build a dataset for later extraction.
3) Append per-run metrics to logs/pdf_pipeline_runs.csv.
"""

from __future__ import annotations

import argparse
import csv
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, unquote
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from natural_pdf import PDF


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_DIR = SCRIPT_DIR.parent

DEFAULT_SOURCE_CSV = REPO_DIR / "data" / "2026_diavgeia.csv"
DEFAULT_PDF_DIR = REPO_DIR / "pdf"
DEFAULT_PAGES_DATASET = REPO_DIR / "data" / "pdf_pages_dataset.csv"
PDF_LOG_FILE = REPO_DIR / "logs" / "pdf_pipeline_runs.csv"

ATHENS_TZ = ZoneInfo("Europe/Athens")


def parse_pdf_file_to_row(pdf_path_str: str) -> tuple[dict[str, Any], int, str | None]:
    """
    Parse a single PDF and return one aggregated row (or an error string).

    Kept as a top-level function so it can be used by ProcessPoolExecutor.
    """
    pdf_path = Path(pdf_path_str)
    ada = pdf_path.stem
    try:
        pdf = PDF(str(pdf_path))
        page_texts: list[str] = []
        page_count = 0
        for idx, page in enumerate(pdf.pages, start=1):
            _ = idx  # preserve enumeration in case PDF backend depends on lazy iteration order
            text = page.extract_text(preserve_line_breaks=True) or ""
            page_texts.append(text)
            page_count += 1

        combined_text = "\n\n".join(page_texts)
        return (
            {
                "ada": ada,
                "file_name": pdf_path.name,
                "page_count": page_count,
                "text": combined_text,
                "text_length": len(combined_text),
                "parse_error": "",
            },
            page_count,
            None,
        )
    except Exception as exc:
        return (
            {
                "ada": ada,
                "file_name": pdf_path.name,
                "page_count": "",
                "text": "",
                "text_length": 0,
                "parse_error": "",
            },
            0,
            f"{type(exc).__name__}: {exc}",
        )


def extract_document_code(document_url: str) -> str:
    """
    Extract document code from Diavgeia document URL.

    Example:
    https://diavgeia.gov.gr/doc/9ΚΠΣΩ1Ε-ΕΑ0 -> 9ΚΠΣΩ1Ε-ΕΑ0
    """
    if not isinstance(document_url, str):
        return ""
    text = document_url.strip()
    if not text:
        return ""

    parsed = urlparse(text)
    path = unquote(parsed.path or "")

    # Accept '/doc/<code>' and '/doc/<code>/...'
    match = re.search(r"/doc/([^/?#]+)", path)
    if not match:
        return ""

    code = match.group(1).strip()
    # Minimal safety: prevent path traversal separators in filename.
    code = code.replace("/", "_").replace("\\", "_")
    return code


def append_pdf_run_log(run_stats: dict[str, Any]) -> None:
    """Append one run entry to logs/pdf_pipeline_runs.csv."""
    PDF_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    row = pd.DataFrame([run_stats])
    row.to_csv(PDF_LOG_FILE, mode="a", header=not PDF_LOG_FILE.exists(), index=False)
    print(f"[log] Appended PDF pipeline run -> {PDF_LOG_FILE}")


def load_source_records(csv_path: Path) -> pd.DataFrame:
    """Load source dataset and validate required columns."""
    if not csv_path.exists():
        raise FileNotFoundError(f"Source CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    required = {"ada", "documentUrl"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Missing required columns in {csv_path}: {sorted(missing)}")
    return df


def download_missing_pdfs(
    source_csv: Path,
    pdf_dir: Path,
    limit: int | None = None,
    timeout: int = 60,
) -> dict[str, int]:
    """
    Download missing PDFs by ADA from source_csv documentUrl.

    Returns counters for logging.
    """
    df = load_source_records(source_csv)
    pdf_dir.mkdir(parents=True, exist_ok=True)
    iter_df = df.head(limit) if limit is not None else df
    total_rows = len(iter_df)

    # Pre-calculate unique missing files for clearer progress reporting.
    pending_codes: set[str] = set()
    for _, row in iter_df.iterrows():
        url = str(row.get("documentUrl", "")).strip()
        if not url or url.lower() == "nan":
            continue
        code = extract_document_code(url)
        if not code:
            continue
        if not (pdf_dir / f"{code}.pdf").exists():
            pending_codes.add(code)
    total_to_download = len(pending_codes)

    print(
        f"[download] start source={source_csv} rows={total_rows} pdf_dir={pdf_dir} "
        f"limit={limit if limit is not None else 'ALL'} missing_files={total_to_download}",
        flush=True,
    )

    scanned = 0
    downloaded = 0
    skipped_existing = 0
    skipped_missing_url = 0
    failed = 0

    download_attempt_index = 0

    for _, row in iter_df.iterrows():
        if limit is not None and scanned >= limit:
            break
        scanned += 1
        if scanned % 25 == 0:
            print(
                f"[download][progress] scanned={scanned}/{total_rows} "
                f"downloaded={downloaded}/{total_to_download} "
                f"existing={skipped_existing} failed={failed}",
                flush=True,
            )

        url = str(row.get("documentUrl", "")).strip()
        document_code = extract_document_code(url)

        if not url or url.lower() == "nan":
            skipped_missing_url += 1
            continue
        if not document_code:
            failed += 1
            print(f"[download][error] Could not extract document code from URL: {url}", flush=True)
            continue

        pdf_path = pdf_dir / f"{document_code}.pdf"
        if pdf_path.exists():
            skipped_existing += 1
            continue

        tmp_path = pdf_path.with_suffix(".pdf.part")
        download_attempt_index += 1
        print(
            f"[download][start] downloading {download_attempt_index}/{total_to_download}: "
            f"{document_code}.pdf <- {url}",
            flush=True,
        )
        try:
            with requests.get(url, stream=True, timeout=(10, timeout)) as r:
                r.raise_for_status()
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 128):
                        if chunk:
                            f.write(chunk)
            tmp_path.replace(pdf_path)
            downloaded += 1
            print(
                f"[download][ok] {document_code}.pdf ({downloaded}/{total_to_download})",
                flush=True,
            )
        except Exception as exc:
            failed += 1
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            print(
                f"[download][error] doc_code={document_code} url={url} -> {type(exc).__name__}: {exc}",
                flush=True,
            )

    print(
        "[download] scanned=%s downloaded=%s skipped_existing=%s skipped_missing_url=%s failed=%s"
        % (scanned, downloaded, skipped_existing, skipped_missing_url, failed)
        ,
        flush=True,
    )
    return {
        "records_scanned": scanned,
        "downloaded": downloaded,
        "skipped_existing": skipped_existing,
        "skipped_missing_url": skipped_missing_url,
        "failed_downloads": failed,
    }


def build_pdf_pages_dataset(
    pdf_dir: Path,
    output_csv: Path,
    limit: int | None = None,
    workers: int = 1,
    append_missing: bool = False,
) -> dict[str, int]:
    """
    Build one-row-per-PDF dataset from local PDFs.

    Output columns:
    - ada
    - file_name
    - page_count
    - text
    - text_length
    - parse_error
    """
    pdf_files = sorted(pdf_dir.glob("*.pdf"))
    if limit is not None:
        pdf_files = pdf_files[:limit]

    existing_adas: set[str] = set()
    if append_missing and output_csv.exists():
        # Fail loudly if the existing dataset cannot be read; silent fallback would
        # cause a full re-parse, which defeats incremental mode.
        existing = pd.read_csv(output_csv, dtype=str)
        if "ada" not in existing.columns:
            raise KeyError(f"Column 'ada' not found in existing pages dataset: {output_csv}")
        existing_adas = {
            str(v).strip()
            for v in existing["ada"].tolist()
            if str(v).strip() and str(v).strip().lower() != "nan"
        }
        before = len(pdf_files)
        pdf_files = [p for p in pdf_files if p.stem not in existing_adas]
        print(
            f"[dataset][append-missing] existing_adas={len(existing_adas)} queued={len(pdf_files)} skipped_existing={before - len(pdf_files)}",
            flush=True,
        )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    workers = max(1, int(workers))
    print(
        f"[dataset] start pdf_dir={pdf_dir} files={len(pdf_files)} output={output_csv} "
        f"limit={limit if limit is not None else 'ALL'} workers={workers} "
        f"mode={'append-missing' if append_missing else 'rebuild'}",
        flush=True,
    )

    parsed_pdfs = 0
    parsed_pages = 0
    parse_errors = 0

    mode = "a" if append_missing else "w"
    write_header = (not append_missing) or (not output_csv.exists()) or output_csv.stat().st_size == 0
    with open(output_csv, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "ada",
                "file_name",
                "page_count",
                "text",
                "text_length",
                "parse_error",
            ],
        )
        if write_header:
            writer.writeheader()

        if workers == 1:
            for pdf_path in pdf_files:
                row, page_count, err = parse_pdf_file_to_row(str(pdf_path))
                if err is None:
                    writer.writerow(row)
                    parsed_pages += page_count
                    parsed_pdfs += 1
                    if parsed_pdfs % 50 == 0:
                        print(
                            f"[dataset][progress] parsed_pdfs={parsed_pdfs} parsed_pages={parsed_pages} "
                            f"errors={parse_errors}",
                            flush=True,
                        )
                else:
                    parse_errors += 1
                    row["parse_error"] = err
                    writer.writerow(row)
                    print(f"[parse][error] file={row['file_name']} -> {err}", flush=True)
        else:
            completed = 0
            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(parse_pdf_file_to_row, str(pdf_path)) for pdf_path in pdf_files]
                for future in as_completed(futures):
                    completed += 1
                    row, page_count, err = future.result()
                    if err is None:
                        writer.writerow(row)
                        parsed_pages += page_count
                        parsed_pdfs += 1
                    else:
                        parse_errors += 1
                        row["parse_error"] = err
                        writer.writerow(row)
                        print(f"[parse][error] file={row['file_name']} -> {err}", flush=True)

                    if completed % 25 == 0:
                        print(
                            f"[dataset][progress] completed={completed}/{len(pdf_files)} "
                            f"parsed_pdfs={parsed_pdfs} parsed_pages={parsed_pages} errors={parse_errors}",
                            flush=True,
                        )

    print(
        "[dataset] files_seen=%s parsed_pdfs=%s parsed_pages=%s parse_errors=%s output=%s"
        % (len(pdf_files), parsed_pdfs, parsed_pages, parse_errors, output_csv)
        ,
        flush=True,
    )
    return {
        "pdf_files_seen": len(pdf_files),
        "parsed_pdfs": parsed_pdfs,
        "parsed_pages": parsed_pages,
        "parse_errors": parse_errors,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Local Diavgeia PDF download and parsing pipeline")
    parser.add_argument("--source-csv", default=str(DEFAULT_SOURCE_CSV), help="Source CSV containing ada and documentUrl")
    parser.add_argument("--pdf-dir", default=str(DEFAULT_PDF_DIR), help="Local directory to store PDFs")
    parser.add_argument(
        "--pages-dataset",
        default=str(DEFAULT_PAGES_DATASET),
        help="Output CSV path for one-row-per-PDF dataset",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional limit for records/files in this run")
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes for PDF parsing (build step only)",
    )
    parser.add_argument("--download-only", action="store_true", help="Run only missing PDF download step")
    parser.add_argument("--build-only", action="store_true", help="Run only page dataset build step")
    parser.add_argument(
        "--append-missing",
        action="store_true",
        help="Build step: append only PDFs missing from pages dataset (incremental, no full rebuild)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="HTTP read timeout in seconds for PDF download (connect timeout fixed at 10s)",
    )
    args = parser.parse_args()

    if args.download_only and args.build_only:
        raise ValueError("Use only one of --download-only or --build-only")

    run_started_at = datetime.now(ATHENS_TZ).strftime("%Y-%m-%d %H:%M:%S %z")
    print(f"[main] PDF pipeline started at {run_started_at}", flush=True)
    run_stats: dict[str, Any] = {
        "run_started_at_athens": run_started_at,
        "records_scanned": 0,
        "downloaded": 0,
        "skipped_existing": 0,
        "skipped_missing_url": 0,
        "failed_downloads": 0,
        "pdf_files_seen": 0,
        "parsed_pdfs": 0,
        "parsed_pages": 0,
        "parse_errors": 0,
        "success": False,
        "error_message": "NONE",
    }

    try:
        source_csv = Path(args.source_csv)
        pdf_dir = Path(args.pdf_dir)
        pages_dataset = Path(args.pages_dataset)

        if not args.build_only:
            run_stats.update(
                download_missing_pdfs(
                    source_csv=source_csv,
                    pdf_dir=pdf_dir,
                    limit=args.limit,
                    timeout=args.timeout,
                )
            )

        if not args.download_only:
            run_stats.update(
                build_pdf_pages_dataset(
                    pdf_dir=pdf_dir,
                    output_csv=pages_dataset,
                    limit=args.limit,
                    workers=args.workers,
                    append_missing=args.append_missing,
                )
            )

        run_stats["success"] = True
        print("[main] PDF pipeline completed successfully.", flush=True)
    except Exception as exc:
        run_stats["error_message"] = f"{type(exc).__name__}: {exc}"
        print(f"[error] {run_stats['error_message']}", flush=True)
        raise
    finally:
        append_pdf_run_log(run_stats)


if __name__ == "__main__":
    main()
