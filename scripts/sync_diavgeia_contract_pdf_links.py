"""Link contracts and all Diavgeia document types by exact PDF references.

Contract PDFs: ADA -> Diavgeia document. Diavgeia PDFs: SYMV ADAM -> contract.
Default is a preview; --apply persists successful scans on their source rows
and inserts links in the same transaction. Runtime state is never stored in Git.
Already parsed PDFs are reused; --refresh rereads them. Unavailable PDFs are
retried on the next run. References to absent records are retained and rematched
on every run, so later database imports can complete their links.
"""
from __future__ import annotations

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
import uuid
import threading
import time

from tqdm import tqdm

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.extract_missing_diavgeia_payment_amounts import collect
from scripts.research_diavgeia_kimdis_payment_links import contract_references
from src.map_copernicus_to_municipalities import resolve_database_url

STATE = ROOT / 'state' / 'diavgeia_contract_pdf_links.json'  # Legacy migration input only.
EXTRACTOR_VERSION = 2
ADA = re.compile(r'(?<![\w-])([A-ZΑ-Ω0-9]{4,12})\s*-\s*([A-ZΑ-Ω0-9]{3})(?!\w|-\S)')
STOP = threading.Event()
VERBOSE = False
REQUEST_DELAY = 1.0


def report(message: str):
    tqdm.write(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")


class ServiceGate:
    """Pace document starts; HTTP and retries belong to the existing downloaders."""
    def __init__(self):
        self.lock = threading.Lock()
        self.next_request = 0.0

    def wait(self):
        with self.lock:
            while self.next_request > time.monotonic():
                if STOP.wait(min(self.next_request-time.monotonic(), 1)):
                    raise InterruptedError('Διακοπή από τον χρήστη')
            if STOP.is_set():
                raise InterruptedError('Διακοπή από τον χρήστη')
            self.next_request = time.monotonic() + REQUEST_DELAY


GATES = {'contract': ServiceGate(), 'diavgeia': ServiceGate()}
SERVICE_NAMES = {'contract': 'ΚΗΜΔΗΣ', 'diavgeia': 'Διαύγεια'}


def download_document(document: dict, pdf_path: Path):
    GATES[document['source_type']].wait()
    if VERBOSE:
        report(f'ΛΗΨΗ PDF {SERVICE_NAMES[document["source_type"]]}: {document["source_id"]}')
    if document['source_type'] == 'diavgeia':
        from src.fetch_diavgeia import _download_pdf_to_path
        _download_pdf_to_path(document['document_url'], pdf_path)
    else:
        from src.locate_work import Document
        contract = Document(document['source_id'], debug=VERBOSE)
        contract.downloadContract()
        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = pdf_path.with_suffix('.pdf.part')
        try:
            contract.saveLocalCopy(str(temporary))
            temporary.replace(pdf_path)
        finally:
            temporary.unlink(missing_ok=True)


def extract_references(text: str, source_type: str) -> list[dict]:
    found = {}
    for page_number, page in enumerate(text.split('\f'), 1):
        if source_type == 'contract':
            matches = [(m[1] + '-' + m[2], m.start(), m.end()) for m in ADA.finditer(page.upper())
                       if re.search(r'[A-ZΑ-Ω]', m[1])]
        else:
            matches = contract_references(page)
        for reference, start, end in matches:
            found.setdefault(reference, {'reference': reference, 'page': page_number,
                                        'evidence': page[max(0, start-120):end+160].strip()})
    return list(found.values())


def fingerprint(document: dict) -> str:
    fields = ['source_type', 'source_id', 'document_url', 'subject', 'status', 'version_comment']
    values = {k: document.get(k) for k in fields}
    values['document_url'] = document.get('identity_document_url', document.get('document_url'))
    return hashlib.sha256(json.dumps(values, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def needs_pdf_read(document: dict, previous: dict, refresh: bool = False) -> bool:
    # A successful negative result is complete too; it need not be reread.
    return (refresh or previous.get('status') != 'read'
            or previous.get('fingerprint') != fingerprint(document)
            or previous.get('extractor_version') != EXTRACTOR_VERSION)


def scan_payload(record: dict) -> dict:
    """Only successful public-PDF evidence belongs in the source row."""
    if record.get('status') != 'read':
        raise ValueError('Only successful PDF scans can be persisted')
    return {key: record[key] for key in
            ('fingerprint', 'extractor_version', 'checked_at', 'pdf_sha256', 'references')}


def contract_document(row: dict) -> dict:
    return {'id': row['id'], 'source_type': 'contract', 'source_id': row['reference_number'],
            'subject': row['title'], 'status': str(row['cancelled']),
            'document_url': 'https://cerpp.eprocurement.gov.gr/khmdhs-opendata/contract/attachment/'
                            + row['reference_number']}


def decision_document(row: dict) -> dict:
    return {**{key: row[key] for key in ('id', 'ada', 'document_url', 'subject', 'status', 'version_comment')},
            'source_type': 'diavgeia', 'source_id': row['ada']}


def load_documents(connection) -> tuple[dict, dict, list[dict], dict]:
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute('SELECT id,reference_number,title,cancelled,pdf_link_scan FROM public.procurement '
                       'WHERE reference_number IS NOT NULL')
        contracts = {row['reference_number']: dict(row) for row in cursor.fetchall()}
        cursor.execute('SELECT id,ada,document_url,subject,status,version_comment,pdf_link_scan '
                       'FROM public.diavgeia WHERE ada IS NOT NULL')
        decisions = {row['ada']: dict(row) for row in cursor.fetchall()}
    documents = [decision_document(row) for row in decisions.values()]
    documents += [contract_document(row) for row in contracts.values()]
    records = {}
    for source_type, mapping in [('contract', contracts), ('diavgeia', decisions)]:
        for source_id, row in mapping.items():
            if row.get('pdf_link_scan') is not None:
                records[source_type + ':' + source_id] = {
                    **row['pdf_link_scan'], 'status': 'read',
                    'source_type': source_type, 'source_id': source_id}
    return contracts, decisions, documents, records


def valid_cached_records(records: dict, documents: list[dict]) -> dict:
    """Stale scans remain stored for evidence but must not create new links."""
    valid = {}
    for document in documents:
        key = document['source_type'] + ':' + document['source_id']
        record = records.get(key, {})
        if not needs_pdf_read(document, record):
            valid[key] = record
    return valid


def read_document(document: dict, output: Path, refresh: bool = False) -> dict:
    # Collector stores PDFs under their exact ADA/ADAM and handles native text/OCR.
    if refresh:
        (ROOT / 'pdf' / (document['source_id'] + '.pdf')).unlink(missing_ok=True)
    if STOP.is_set():
        return {'status': 'cancelled'}
    if VERBOSE and (ROOT / 'pdf' / (document['source_id']+'.pdf')).exists():
        report(f'ΤΟΠΙΚΟ PDF {SERVICE_NAMES[document["source_type"]]}: {document["source_id"]}')
    result = collect({'ada': document['source_id'], 'document_url': document['document_url']}, output,
                     extract_amount=False, download_pdf=lambda url, path: download_document(document, path),
                     on_progress=(lambda message: report(f'{document["source_id"]}: {message}')) if VERBOSE else None)
    record = {'source_type': document['source_type'], 'source_id': document['source_id'],
              'fingerprint': fingerprint(document), 'checked_at': datetime.now(timezone.utc).isoformat(),
              'extractor_version': EXTRACTOR_VERSION}
    if not result.get('text_path') or result.get('status') == 'error':
        if STOP.is_set():
            return {'status': 'cancelled'}
        return {**record, 'status': 'error', 'error': result.get('error', 'PDF text unavailable')}
    text = Path(result['text_path']).read_text()
    if not text.strip():
        return {**record, 'status': 'error', 'error': 'Δεν εξήχθη αναγνώσιμο κείμενο από το PDF'}
    return {**record, 'status': 'read', 'pdf_sha256': result['pdf_sha256'],
            'references': extract_references(text, document['source_type'])}


def match_links(records: dict, contracts: dict, decisions: dict) -> list[dict]:
    links = []
    for record in records.values():
        if record['status'] != 'read':
            continue
        source = record['source_id']
        if record['source_type'] == 'contract' and source not in contracts:
            continue
        if record['source_type'] == 'diavgeia' and source not in decisions:
            continue
        for ref in record['references']:
            if record['source_type'] == 'contract':
                contract, ada = source, ref['reference']
            else:
                contract, ada = ref['reference'], source
            if contract not in contracts or ada not in decisions:
                continue
            links.append({'diavgeia_id': decisions[ada]['id'], 'procurement_id': contracts[contract]['id'],
                          'ada': ada, 'contract_reference': contract,
                          'source_type': record['source_type'], 'source_id': source,
                          'page': ref['page'], 'evidence': ref['evidence'],
                          'pdf_sha256': record['pdf_sha256']})
    return links


def save_state(records: dict, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.json.tmp')
    temporary.write_text(json.dumps({'version': 1, 'documents': records}, ensure_ascii=False, indent=2))
    temporary.replace(path)


def backup_links(connection, backup: Path):
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute('SELECT * FROM public.diavgeia_procurement ORDER BY diavgeia_id,procurement_id')
        before = cursor.fetchall()
    backup.parent.mkdir(parents=True, exist_ok=True)
    with backup.open('x', encoding='utf-8') as handle:
        json.dump(before, handle, ensure_ascii=False, indent=2, default=str)
    connection.rollback()


class SourceChangedError(RuntimeError):
    """Retry if the source changed while its PDF was being read."""


def persist_scan_and_links(connection, document: dict, record: dict,
                           contracts: dict, decisions: dict) -> set:
    payload = scan_payload(record)
    if needs_pdf_read(document, record):
        raise ValueError('Cannot persist an obsolete scan as current')
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            if document['source_type'] == 'contract':
                cursor.execute('SELECT id,reference_number,title,cancelled FROM public.procurement '
                               'WHERE id=%s FOR UPDATE', (document['id'],))
                row = cursor.fetchone()
                current = contract_document(row) if row else None
                table = 'procurement'
            else:
                cursor.execute('SELECT id,ada,document_url,subject,status,version_comment '
                               'FROM public.diavgeia WHERE id=%s FOR UPDATE', (document['id'],))
                row = cursor.fetchone()
                current = decision_document(row) if row else None
                table = 'diavgeia'
            if current is None or fingerprint(current) != record['fingerprint']:
                raise SourceChangedError('Το έγγραφο άλλαξε ή αφαιρέθηκε κατά την ανάγνωση· θα ελεγχθεί ξανά.')
            cursor.execute(f'UPDATE public.{table} SET pdf_link_scan=%s '
                           'WHERE id=%s AND pdf_link_scan IS DISTINCT FROM %s::jsonb',
                           (Json(payload), document['id'], Json(payload)))
            links = match_links({'current': record}, contracts, decisions)
            pairs = sorted({(r['diavgeia_id'], r['procurement_id']) for r in links})
            added = execute_values(cursor, '''INSERT INTO public.diavgeia_procurement
                (diavgeia_id,procurement_id) VALUES %s ON CONFLICT DO NOTHING
                RETURNING diavgeia_id,procurement_id''', pairs, page_size=1000, fetch=True) if pairs else []
        connection.commit()
        return {(pair['diavgeia_id'], pair['procurement_id']) if isinstance(pair, dict) else tuple(pair)
                for pair in added}
    except BaseException:
        connection.rollback()
        raise


def commit_links(connection, pairs: set) -> set:
    """Commit and verify each discovered document's links immediately."""
    if not pairs:
        return set()
    ordered = sorted(pairs)
    try:
        with connection.cursor() as cursor:
            added = execute_values(cursor, '''INSERT INTO public.diavgeia_procurement (diavgeia_id,procurement_id)
                VALUES %s ON CONFLICT DO NOTHING RETURNING diavgeia_id,procurement_id''', ordered, page_size=1000, fetch=True)
        connection.commit()
        with connection.cursor() as cursor:
            cursor.execute('''SELECT count(*) FROM unnest(%s::bigint[], %s::bigint[]) AS wanted(d_id,p_id)
                LEFT JOIN public.diavgeia_procurement b ON b.diavgeia_id=wanted.d_id AND b.procurement_id=wanted.p_id
                WHERE b.diavgeia_id IS NULL''', ([r[0] for r in ordered], [r[1] for r in ordered]))
            if cursor.fetchone()[0]:
                raise RuntimeError('Post-commit link verification failed')
        connection.rollback()  # End verification transaction before the next PDF.
        return set(added)
    except BaseException:
        connection.rollback()
        raise


def commit_cached_scans(connection, records: dict, documents: list[dict],
                        contracts: dict, decisions: dict) -> set:
    """Recheck cached source metadata under locks before creating any new pair."""
    candidates = match_links(valid_cached_records(records, documents), contracts, decisions)
    source_keys = {(r['source_type'], r['source_id']) for r in candidates}
    current_records = {}
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            for source_type in ('contract', 'diavgeia'):
                ids = sorted(d['id'] for d in documents
                             if (source_type, d['source_id']) in source_keys
                             and d['source_type'] == source_type)
                if not ids:
                    continue
                if source_type == 'contract':
                    cursor.execute('SELECT id,reference_number,title,cancelled,pdf_link_scan '
                                   'FROM public.procurement WHERE id=ANY(%s) ORDER BY id FOR UPDATE', (ids,))
                else:
                    cursor.execute('SELECT id,ada,document_url,subject,status,version_comment,pdf_link_scan '
                                   'FROM public.diavgeia WHERE id=ANY(%s) ORDER BY id FOR UPDATE', (ids,))
                for row in cursor.fetchall():
                    if row['pdf_link_scan'] is None:
                        continue
                    document = contract_document(row) if source_type == 'contract' else decision_document(row)
                    record = {**row['pdf_link_scan'], 'status': 'read',
                              'source_type': source_type, 'source_id': document['source_id']}
                    if not needs_pdf_read(document, record):
                        current_records[source_type + ':' + document['source_id']] = record
        links = match_links(current_records, contracts, decisions)
        pairs = {(r['diavgeia_id'], r['procurement_id']) for r in links}
        if not pairs:
            connection.rollback()  # Release source locks even when all cached results became stale.
            return set()
        return commit_links(connection, pairs)
    except BaseException:
        connection.rollback()
        raise


def main():
    global VERBOSE, REQUEST_DELAY
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--backup', type=Path)
    parser.add_argument('--workers', type=int, default=1, help='Workers per service; the two services always run concurrently.')
    parser.add_argument('--request-delay', type=float, default=1.0, help='Minimum seconds between download starts per service.')
    parser.add_argument('--verbose', action='store_true', help='Print download, parsing, OCR and result messages.')
    parser.add_argument('--refresh', action='store_true')
    parser.add_argument('--max-new-pdfs', type=int, default=0, help='Optional per-run batch size; 0 reads all pending PDFs.')
    parser.add_argument('--cached-only', action='store_true', help='Match previously read PDF references without new downloads.')
    args = parser.parse_args()
    run_id = datetime.now().strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:6]
    args.output = args.output or ROOT / 'exports' / ('diavgeia-contract-pdf-links-' + run_id)
    args.backup = args.backup or ROOT / 'backupDB' / ('diavgeia-contract-pdf-links-' + run_id + '-before.json')
    if args.workers < 1 or args.request_delay < 1:
        parser.error('workers >= 1, request-delay >= 1')
    VERBOSE, REQUEST_DELAY = args.verbose, args.request_delay
    STOP.clear()
    for source in GATES:
        GATES[source] = ServiceGate()
    if args.max_new_pdfs < 0:
        parser.error('--max-new-pdfs must be nonnegative')
    if args.cached_only and args.refresh:
        parser.error('--cached-only cannot be combined with --refresh')
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / 'text').mkdir(exist_ok=True)
    connection = psycopg2.connect(resolve_database_url(None), connect_timeout=15)
    connection.set_session(readonly=True)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout='40s'")
        contracts, decisions, documents, records = load_documents(connection)
        connection.rollback()
        connection.close()
        pending = []
        for document in documents:
            key = document['source_type'] + ':' + document['source_id']
            old = records.get(key, {})
            if needs_pdf_read(document, old, args.refresh):
                pending.append(document)
        total_pending = len(pending)
        if args.cached_only:
            pending = []
        elif args.max_new_pdfs:
            pending = pending[:args.max_new_pdfs]
        print(f"Έγγραφα Διαύγειας: {len(decisions)}. Συμβάσεις: {len(contracts)}. PDF προς ανάγνωση: {len(pending)}.", flush=True)
        report(f'Αρχεία αποτελεσμάτων: {args.output}')
        added = 0
        if args.apply:
            connection = psycopg2.connect(resolve_database_url(None), connect_timeout=15)
            with connection.cursor() as cursor:
                cursor.execute("SET statement_timeout='40s'")
            connection.commit()
            backup_links(connection, args.backup)
            report(f'Αντίγραφο: {args.backup}. Κάθε νέα σύνδεση γράφεται αμέσως στη βάση.')
            added += len(commit_cached_scans(connection, records, documents, contracts, decisions))
            report(f'Νέες συνδέσεις από αποθηκευμένα PDF: {added}')
        pools = {source: ThreadPoolExecutor(max_workers=args.workers) for source in SERVICE_NAMES}
        bars = {source: tqdm(total=sum(d['source_type'] == source for d in pending),
                             desc=SERVICE_NAMES[source], unit='PDF', position=index, dynamic_ncols=True)
                for index, source in enumerate(SERVICE_NAMES)}
        futures = {}
        processed, failures, deferred = 0, 0, 0
        service_counts = {source: {'αποτυχίες': 0, 'αναβολές': 0} for source in SERVICE_NAMES}
        heartbeat = time.monotonic()
        queues = {source: iter([d for d in pending if d['source_type'] == source]) for source in SERVICE_NAMES}

        def submit_next(source):
            doc = next(queues[source], None)
            if doc is None or STOP.is_set():
                return
            old = records.get(doc['source_type']+':'+doc['source_id'], {})
            refresh = args.refresh or (old.get('status') == 'read' and old.get('fingerprint') != fingerprint(doc))
            futures[pools[source].submit(read_document, doc, args.output, refresh)] = doc

        try:
            for source in SERVICE_NAMES:
                for _ in range(args.workers):
                    submit_next(source)
            while futures:
                done, _ = wait(futures, timeout=1, return_when=FIRST_COMPLETED)
                for future in done:
                    doc = futures.pop(future)
                    if future.cancelled():
                        continue
                    record = future.result()
                    if record['status'] == 'cancelled':
                        continue
                    if args.apply and record['status'] == 'read':
                        found_links = match_links({'current': record}, contracts, decisions)
                        try:
                            committed = persist_scan_and_links(connection, doc, record, contracts, decisions)
                        except SourceChangedError as error:
                            record = {**record, 'status': 'deferred', 'error': str(error)}
                        else:
                            added += len(committed)
                            for link in found_links:
                                if (link['diavgeia_id'], link['procurement_id']) in committed:
                                    report(f'ΓΡΑΦΤΗΚΕ ΣΤΗ ΒΑΣΗ: {link["ada"]} ↔ {link["contract_reference"]}')
                                    committed.remove((link['diavgeia_id'], link['procurement_id']))
                    if record['status'] == 'read':
                        records[record['source_type']+':'+record['source_id']] = record
                    else:
                        # Keep the last successful result; errors are operational logs only.
                        with (args.output / 'errors.jsonl').open('a', encoding='utf-8') as handle:
                            handle.write(json.dumps(record, ensure_ascii=False) + '\n')
                    processed += 1
                    failures += record['status'] == 'error'
                    deferred += record['status'] == 'deferred'
                    service_counts[doc['source_type']]['αποτυχίες'] += record['status'] == 'error'
                    service_counts[doc['source_type']]['αναβολές'] += record['status'] == 'deferred'
                    bars[doc['source_type']].update(1)
                    bars[doc['source_type']].set_postfix(service_counts[doc['source_type']])
                    if record['status'] != 'read':
                        report(f'{SERVICE_NAMES[doc["source_type"]]} {doc["source_id"]}: {record.get("error")}')
                    elif VERBOSE:
                        report(f'OK {doc["source_id"]}: {len(record["references"])} αναφορές')
                    submit_next(doc['source_type'])
                if not done and time.monotonic()-heartbeat >= 30:
                    active = ', '.join(sorted({SERVICE_NAMES[d['source_type']] for d in futures.values()}))
                    report(f'Ενεργό πρόγραμμα — εξετάστηκαν {processed}/{len(pending)} PDF. Σε εξέλιξη: {active}.')
                    heartbeat = time.monotonic()
        except KeyboardInterrupt:
            STOP.set()
            if not args.apply:
                save_state(records, args.output / 'preview-state.json')
            report('Διακόπηκε. Αποθηκεύτηκε η πρόοδος. Οι συνδέσεις που γράφτηκαν ήδη παραμένουν στη βάση.')
            return
        except BaseException:
            STOP.set()
            if not args.apply:
                save_state(records, args.output / 'preview-state.json')
            raise
        finally:
            for pool in pools.values():
                pool.shutdown(wait=not STOP.is_set(), cancel_futures=True)
            for bar in bars.values():
                bar.close()
        if not args.apply:
            save_state(records, args.output / 'preview-state.json')
        links = match_links(valid_cached_records(records, documents), contracts, decisions)
        (args.output / 'links.json').write_text(json.dumps(links, ensure_ascii=False, indent=2))
        with (args.output / 'links.csv').open('w', encoding='utf-8-sig', newline='') as handle:
            fields = ['ada', 'contract_reference', 'source_type', 'source_id', 'page', 'evidence']
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(links)
        pairs = {(r['diavgeia_id'], r['procurement_id']) for r in links}
        summary = {'diavgeia_documents': len(decisions), 'contracts': len(contracts),
                   'pdfs_processed_this_run': processed,
                   'pdf_errors': failures,
                   'remaining_unprocessed': total_pending-processed+failures+deferred,
                   'deferred_this_run': deferred,
                   'matched_pairs': len(pairs), 'added_links': added,
                   'database_changed': bool(args.apply and (added or processed-failures-deferred))}
        (args.output / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2))
        print(f'Τεκμηριωμένες συνδέσεις PDF: {len(pairs)}. Νέες συνδέσεις στη βάση: {added}.', flush=True)
        if summary['pdf_errors']:
            print(f"::warning::{summary['pdf_errors']} PDFs unavailable; they will be retried next run.")
    finally:
        if not connection.closed:
            connection.rollback()
            connection.close()


if __name__ == '__main__':
    main()
