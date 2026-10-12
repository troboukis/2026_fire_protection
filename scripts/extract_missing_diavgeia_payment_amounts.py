"""Download missing payment PDFs and keep page text for an auditable backfill.

This step does not write to the database. It can select missing amounts directly
from the database after normal ingestion and persist PDF evidence in the mapping.
"""
from __future__ import annotations

import argparse
import csv
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import sys

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.diavgeia_pdf_amounts import extract_payment_total
from src.diavgeia_amounts import PDF_PAYMENT_AMOUNTS_PATH, load_pdf_payment_amounts

MAPPING_FIELDS = ['ada', 'amount', 'amount_kind', 'page', 'reason',
                  'arithmetic_confirmed', 'schedule_rows', 'pdf_sha256', 'document_url', 'evidence']


def missing_payment_inventory() -> list[dict]:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from src.map_copernicus_to_municipalities import resolve_database_url
    from scripts.backfill_diavgeia_pdf_amounts import MISSING, TYPE

    connection = psycopg2.connect(resolve_database_url(None), connect_timeout=15)
    connection.set_session(readonly=True)
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SET LOCAL statement_timeout = '30s'")
            cursor.execute(f'''SELECT ada, document_url, subject, spending_contractors_value
                FROM public.diavgeia
                WHERE diavgeia_document_type_decision_uid=%s AND {MISSING}
                ORDER BY ada''', (TYPE,))
            return [dict(row) for row in cursor.fetchall()]
    finally:
        connection.rollback()
        connection.close()


def save_amount_mapping(results: list[dict], path: Path = PDF_PAYMENT_AMOUNTS_PATH) -> int:
    existing = {}
    if path.exists():
        with path.open(encoding='utf-8', newline='') as handle:
            existing = {row['ada']: row for row in csv.DictReader(handle)}
    extracted = [row for row in results if row['status'] == 'extracted']
    for row in extracted:
        value = Decimal(row['amount'])
        if not value.is_finite() or value < 0 or not row.get('evidence'):
            raise ValueError(f"Invalid extraction for {row['ada']}")
        if row['ada'] in existing and existing[row['ada']]['amount'] != row['amount']:
            raise ValueError(f"Conflicting saved amount for {row['ada']}")
        existing[row['ada']] = row
    if extracted or not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix('.csv.tmp')
        with temporary.open('w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=MAPPING_FIELDS, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(existing[ada] for ada in sorted(existing))
        temporary.replace(path)
        load_pdf_payment_amounts.cache_clear()
    return len(extracted)


def collect(row: dict, output: Path, *, extract_amount: bool = True, session=None, on_progress=None) -> dict:
    ada = row['ada']
    pdf = ROOT / 'pdf' / f'{ada}.pdf'
    text_path = output / 'text' / f'{ada}.txt'
    result = dict(row)
    progress = on_progress or (lambda message: None)
    try:
        if not pdf.exists():
            progress('Αίτημα λήψης PDF')
            response = (session or requests).get(row['document_url'], timeout=(10, 60))
            response.raise_for_status()
            if not response.content.startswith(b'%PDF'):
                raise ValueError('Response is not a PDF')
            temporary_pdf = pdf.with_suffix('.pdf.part')
            temporary_pdf.write_bytes(response.content)
            temporary_pdf.replace(pdf)
            progress(f'Αποθηκεύτηκαν {len(response.content):,} bytes')
        result['pdf_sha256'] = hashlib.sha256(pdf.read_bytes()).hexdigest()
        hash_path = text_path.with_suffix('.sha256')
        if text_path.exists() and hash_path.exists() and hash_path.read_text() == result['pdf_sha256']:
            text = text_path.read_text()
        else:
            progress('Εξαγωγή κειμένου από PDF')
            text = subprocess.run(['pdftotext', '-layout', str(pdf), '-'],
                                  check=True, capture_output=True, text=True, timeout=90).stdout
            pages = text.split('\f')
            if pages and not pages[-1].strip():
                pages.pop()
            ocr_pages = []
            for index, page in enumerate(pages, 1):
                # English signature watermarks must not hide a scanned Greek body.
                if sum('\u0370' <= c <= '\u03ff' for c in page) >= 80:
                    continue
                with tempfile.TemporaryDirectory() as directory:
                    progress(f'OCR σελίδας {index}/{len(pages)}')
                    prefix = str(Path(directory) / 'page')
                    subprocess.run(['pdftoppm', '-f', str(index), '-l', str(index),
                                    '-r', '220', '-png', '-singlefile', str(pdf), prefix],
                                   check=True, capture_output=True, timeout=180)
                    pages[index - 1] = subprocess.run(
                        ['tesseract', prefix + '.png', 'stdout', '-l', 'ell+eng'],
                        check=True, capture_output=True, text=True, timeout=180).stdout
                    ocr_pages.append(index)
            text = '\f'.join(pages)
            text_path.write_text(text)
            hash_path.write_text(result['pdf_sha256'])
            result['ocr_pages'] = ocr_pages
        result.update(status='read', text_path=str(text_path), page_count=len(text.split('\f')))
        if extract_amount:
            result.update(extract_payment_total(text))
    except Exception as error:
        result.update(status='error', error=f'{type(error).__name__}: {error}')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--inventory', type=Path)
    source.add_argument('--from-database', action='store_true')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--save-mapping', action='store_true', help='Persist extracted amounts for subsequent database ingestion/backfill.')
    parser.add_argument('--workers', type=int, default=6)
    args = parser.parse_args()
    (ROOT / 'pdf').mkdir(exist_ok=True)
    (args.output / 'text').mkdir(parents=True, exist_ok=True)
    rows = missing_payment_inventory() if args.from_database else json.loads(args.inventory.read_text())
    (args.output / 'inventory.json').write_text(json.dumps(rows, ensure_ascii=False, indent=2))
    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        tasks = [pool.submit(collect, row, args.output) for row in rows]
        for task in as_completed(tasks):
            result = task.result()
            results.append(result)
            (args.output / 'collection.json').write_text(json.dumps(results, ensure_ascii=False, indent=2))
            if len(results) % 25 == 0 or result['status'] == 'error':
                print(f"PDF {len(results)}/{len(rows)}; errors={sum(r['status']=='error' for r in results)}", flush=True)
    results.sort(key=lambda row: row['ada'])
    (args.output / 'extraction.json').write_text(json.dumps(results, ensure_ascii=False, indent=2))
    fields = ['ada', 'amount', 'status', 'reason', 'amount_kind', 'page', 'evidence',
              'arithmetic_confirmed', 'schedule_rows', 'pdf_sha256', 'document_url']
    with (args.output / 'amounts.csv').open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(results)
    print(f"Extracted {sum(r['status']=='extracted' for r in results)}/{len(rows)} amounts; database unchanged.")
    unresolved = sum(row['status'] != 'extracted' for row in results)
    if args.save_mapping:
        saved = save_amount_mapping(results)
        print(f'Αποθηκεύτηκαν {saved} τεκμηριωμένα ποσά στην πηγή εισαγωγής. Χωρίς σαφές ποσό: {unresolved}.')
    if unresolved:
        print(f'::warning::{unresolved} payment PDFs need review; their amounts remain empty.')


if __name__ == '__main__':
    main()
