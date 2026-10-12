"""Link contracts and all Diavgeia document types by exact PDF references.

Contract PDFs: ADA -> Diavgeia document. Diavgeia PDFs: SYMV ADAM -> contract.
Default is a preview; --apply persists the evidence cache and inserts links only.
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
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import requests
from tqdm import tqdm

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.extract_missing_diavgeia_payment_amounts import collect
from scripts.research_diavgeia_kimdis_payment_links import contract_references
from src.map_copernicus_to_municipalities import resolve_database_url

STATE = ROOT / 'state' / 'diavgeia_contract_pdf_links.json'
ADA = re.compile(r'(?<![A-ZΑ-Ω0-9])[A-ZΑ-Ω0-9]+-[A-ZΑ-Ω0-9]+(?![A-ZΑ-Ω0-9])')
CLIENT = threading.local()
STOP = threading.Event()
VERBOSE = False
REQUEST_DELAY = 1.0
MAX_RATE_WAIT = 60.0


def report(message: str):
    tqdm.write(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")


class ServiceGate:
    def __init__(self):
        self.lock = threading.Lock()
        self.next_request = 0.0
        self.retry_until = 0.0


GATES = {'contract': ServiceGate(), 'diavgeia': ServiceGate()}
SERVICE_NAMES = {'contract': 'ΚΗΜΔΗΣ', 'diavgeia': 'Διαύγεια'}


class RateLimitPause(RuntimeError):
    pass


class ServiceSession:
    """Independent start intervals for the two services, without locking I/O."""
    def __init__(self, client, source_type):
        self.client = client
        self.source_type = source_type
        self.gate = GATES[source_type]

    def get(self, url, **kwargs):
        gate = self.gate
        with gate.lock:
            remaining = gate.next_request - time.monotonic()
            if remaining > MAX_RATE_WAIT:
                raise RateLimitPause(f'{SERVICE_NAMES[self.source_type]}: αναμονή υπηρεσίας {remaining:.0f}s, αναβολή για επόμενη εκτέλεση')
            announced = False
            while gate.next_request > time.monotonic():
                if STOP.is_set():
                    raise InterruptedError('Διακοπή από τον χρήστη')
                remaining = gate.next_request - time.monotonic()
                if remaining > 2 and not announced:
                    report(f'ΑΝΑΜΟΝΗ {SERVICE_NAMES[self.source_type]}: {remaining:.0f}s πριν από το επόμενο αίτημα')
                    announced = True
                time.sleep(min(remaining, 1))
            if STOP.is_set():
                raise InterruptedError('Διακοπή από τον χρήστη')
            gate.next_request = time.monotonic() + REQUEST_DELAY
        # Network work for one service must never hold up the other service.
        response = self.client.get(url, **kwargs)
        if response.status_code == 429:
            delay = 60.0
            retry_after = response.headers.get('Retry-After', '').strip()
            try:
                delay = max(1, float(retry_after))
            except ValueError:
                try:
                    delay = max(1, (parsedate_to_datetime(retry_after)-datetime.now(timezone.utc)).total_seconds())
                except (ValueError, TypeError):
                    pass
            with gate.lock:
                gate.next_request = max(gate.next_request, time.monotonic()+delay)
                gate.retry_until = max(gate.retry_until, time.time()+delay)
            report(f'HTTP 429 — {SERVICE_NAMES[self.source_type]} ζητά αναμονή {delay:.0f}s. Δεν παρακάμπτεται το όριο.')
        return response


def extract_references(text: str, source_type: str) -> list[dict]:
    found = {}
    for page_number, page in enumerate(text.split('\f'), 1):
        if source_type == 'contract':
            matches = [(m[0], m.start(), m.end()) for m in ADA.finditer(page.upper())]
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
    return refresh or previous.get('status') != 'read' or previous.get('fingerprint') != fingerprint(document)


def read_document(document: dict, output: Path, refresh: bool = False) -> dict:
    # Collector stores PDFs under their exact ADA/ADAM and handles native text/OCR.
    if refresh:
        (ROOT / 'pdf' / (document['source_id'] + '.pdf')).unlink(missing_ok=True)
    if STOP.is_set():
        return {'status': 'cancelled'}
    if not hasattr(CLIENT, 'session'):
        CLIENT.session = requests.Session()
    client = ServiceSession(CLIENT.session, document['source_type'])
    if VERBOSE:
        action = 'ΤΟΠΙΚΟ PDF' if (ROOT / 'pdf' / (document['source_id']+'.pdf')).exists() else 'ΛΗΨΗ PDF'
        report(f'{action} {SERVICE_NAMES[document["source_type"]]}: {document["source_id"]}')
    result = collect({'ada': document['source_id'], 'document_url': document['document_url']}, output,
                     extract_amount=False, session=client,
                     on_progress=(lambda message: report(f'{document["source_id"]}: {message}')) if VERBOSE else None)
    record = {'source_type': document['source_type'], 'source_id': document['source_id'],
              'fingerprint': fingerprint(document), 'checked_at': datetime.now(timezone.utc).isoformat()}
    if not result.get('text_path') or result.get('status') == 'error':
        if STOP.is_set():
            return {'status': 'cancelled'}
        if '429 ' in result.get('error', '') or 'RateLimitPause:' in result.get('error', ''):
            return {**record, 'status': 'deferred', 'error': result.get('error'),
                    'retry_at': datetime.fromtimestamp(client.gate.retry_until, timezone.utc).isoformat()}
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


def main():
    global VERBOSE, REQUEST_DELAY, MAX_RATE_WAIT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--backup', type=Path)
    parser.add_argument('--state', type=Path, default=STATE)
    parser.add_argument('--workers', type=int, default=1, help='Workers per service; the two services always run concurrently.')
    parser.add_argument('--request-delay', type=float, default=1.0, help='Minimum seconds between download starts per service.')
    parser.add_argument('--max-rate-wait', type=float, default=60.0, help='Defer a service if it requests a longer wait.')
    parser.add_argument('--verbose', action='store_true', help='Print download, parsing, OCR and result messages.')
    parser.add_argument('--refresh', action='store_true')
    parser.add_argument('--max-new-pdfs', type=int, default=0, help='Optional per-run batch size; 0 reads all pending PDFs.')
    parser.add_argument('--cached-only', action='store_true', help='Match previously read PDF references without new downloads.')
    args = parser.parse_args()
    run_id = datetime.now().strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:6]
    args.output = args.output or ROOT / 'exports' / ('diavgeia-contract-pdf-links-' + run_id)
    args.backup = args.backup or ROOT / 'backupDB' / ('diavgeia-contract-pdf-links-' + run_id + '-before.json')
    if args.workers < 1 or args.request_delay < 1 or args.max_rate_wait < 0:
        parser.error('workers >= 1, request-delay >= 1, max-rate-wait >= 0')
    VERBOSE, REQUEST_DELAY, MAX_RATE_WAIT = args.verbose, args.request_delay, args.max_rate_wait
    STOP.clear()
    if args.max_new_pdfs < 0:
        parser.error('--max-new-pdfs must be nonnegative')
    if args.cached_only and args.refresh:
        parser.error('--cached-only cannot be combined with --refresh')
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / 'text').mkdir(exist_ok=True)
    connection = psycopg2.connect(resolve_database_url(None), connect_timeout=15)
    connection.set_session(readonly=True)
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SET LOCAL statement_timeout='40s'")
            cursor.execute('SELECT id,reference_number,title,cancelled FROM public.procurement WHERE reference_number IS NOT NULL')
            contracts = {r['reference_number']: dict(r) for r in cursor.fetchall()}
            cursor.execute('SELECT id,ada,document_url,subject,status,version_comment FROM public.diavgeia')
            decisions = {r['ada']: dict(r) for r in cursor.fetchall()}
        connection.rollback()
        connection.close()
        documents = [dict(r, source_type='diavgeia', source_id=ada) for ada, r in decisions.items()]
        for document in documents:
            if urlparse(document.get('document_url') or '').hostname == 'static.old.diavgeia.gov.gr':
                document['identity_document_url'] = document['document_url']
                document['document_url'] = 'https://diavgeia.gov.gr/doc/' + document['source_id']
        documents += [{'source_type': 'contract', 'source_id': ref, 'subject': r['title'],
                       'status': str(r['cancelled']),
                       'document_url': 'https://cerpp.eprocurement.gov.gr/khmdhs-opendata/contract/attachment/'+ref}
                      for ref, r in contracts.items()]
        records = json.loads(args.state.read_text())['documents'] if args.state.exists() else {}
        for record in records.values():
            if record.get('retry_at'):
                remaining = datetime.fromisoformat(record['retry_at']).timestamp()-time.time()
                if remaining > 0:
                    gate = GATES[record['source_type']]
                    gate.next_request = max(gate.next_request, time.monotonic()+remaining)
                    gate.retry_until = max(gate.retry_until, time.time()+remaining)
        pending = []
        for document in documents:
            key = document['source_type'] + ':' + document['source_id']
            old = records.get(key, {})
            if needs_pdf_read(document, old, args.refresh):
                pending.append(document)
        total_pending = len(pending)
        paused = {source for source, gate in GATES.items() if gate.next_request-time.monotonic() > MAX_RATE_WAIT}
        for source in paused:
            report(f'{SERVICE_NAMES[source]}: αποθηκευμένη αναμονή {GATES[source].next_request-time.monotonic():.0f}s. Οι λήψεις αναβάλλονται.')
        pending = [document for document in pending if document['source_type'] not in paused]
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
            cached_links = match_links(records, contracts, decisions)
            cached_pairs = {(r['diavgeia_id'], r['procurement_id']) for r in cached_links}
            added += len(commit_links(connection, cached_pairs))
            report(f'Νέες συνδέσεις από αποθηκευμένα PDF: {added}')
        state_path = args.state if args.apply else args.output / 'preview-state.json'
        pools = {source: ThreadPoolExecutor(max_workers=args.workers) for source in SERVICE_NAMES}
        bars = {source: tqdm(total=sum(d['source_type'] == source for d in pending),
                             desc=SERVICE_NAMES[source], unit='PDF', position=index, dynamic_ncols=True)
                for index, source in enumerate(SERVICE_NAMES)}
        futures = {}
        processed, failures, deferred, dirty = 0, 0, 0, 0
        service_counts = {source: {'αποτυχίες': 0, 'αναβολές': 0} for source in SERVICE_NAMES}
        checkpoint = heartbeat = time.monotonic()
        try:
            for doc in pending:
                old = records.get(doc['source_type']+':'+doc['source_id'], {})
                refresh = args.refresh or (old.get('status') == 'read' and old.get('fingerprint') != fingerprint(doc))
                future = pools[doc['source_type']].submit(read_document, doc, args.output, refresh)
                futures[future] = doc
            while futures:
                done, _ = wait(futures, timeout=1, return_when=FIRST_COMPLETED)
                for future in done:
                    doc = futures.pop(future)
                    if future.cancelled():
                        continue
                    record = future.result()
                    if record['status'] == 'cancelled':
                        continue
                    records[record['source_type']+':'+record['source_id']] = record
                    if args.apply and record['status'] == 'read':
                        found_links = match_links({'current': record}, contracts, decisions)
                        found_pairs = {(r['diavgeia_id'], r['procurement_id']) for r in found_links}
                        committed = commit_links(connection, found_pairs)
                        added += len(committed)
                        for link in found_links:
                            if (link['diavgeia_id'], link['procurement_id']) in committed:
                                report(f'ΓΡΑΦΤΗΚΕ ΣΤΗ ΒΑΣΗ: {link["ada"]} ↔ {link["contract_reference"]}')
                                committed.remove((link['diavgeia_id'], link['procurement_id']))
                    processed += 1
                    dirty += 1
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
                    if record['status'] == 'deferred' and GATES[doc['source_type']].next_request-time.monotonic() > MAX_RATE_WAIT:
                        report(f'Αναβάλλονται οι υπόλοιπες λήψεις {SERVICE_NAMES[doc["source_type"]]}. Η άλλη υπηρεσία συνεχίζει.')
                        for queued, queued_doc in list(futures.items()):
                            if queued_doc['source_type'] == doc['source_type']:
                                queued.cancel()
                if dirty and (dirty >= 25 or time.monotonic()-checkpoint >= 15):
                    save_state(records, state_path)
                    dirty = 0
                    checkpoint = time.monotonic()
                if not done and time.monotonic()-heartbeat >= 30:
                    waits = ', '.join(f'{SERVICE_NAMES[s]}: {max(0, g.next_request-time.monotonic()):.0f}s' for s, g in GATES.items())
                    report(f'Ενεργό πρόγραμμα — ολοκληρώθηκαν {processed}/{len(pending)} PDF. Αναμονές: {waits}')
                    heartbeat = time.monotonic()
        except KeyboardInterrupt:
            STOP.set()
            save_state(records, state_path)
            report('Διακόπηκε. Αποθηκεύτηκε η πρόοδος. Οι συνδέσεις που γράφτηκαν ήδη παραμένουν στη βάση.')
            return
        except BaseException:
            STOP.set()
            save_state(records, state_path)
            raise
        finally:
            for pool in pools.values():
                pool.shutdown(wait=not STOP.is_set(), cancel_futures=True)
            for bar in bars.values():
                bar.close()
        if args.apply:
            save_state(records, args.state)
        else:
            save_state(records, args.output / 'preview-state.json')
        links = match_links(records, contracts, decisions)
        (args.output / 'links.json').write_text(json.dumps(links, ensure_ascii=False, indent=2))
        with (args.output / 'links.csv').open('w', encoding='utf-8-sig', newline='') as handle:
            fields = ['ada', 'contract_reference', 'source_type', 'source_id', 'page', 'evidence']
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(links)
        pairs = {(r['diavgeia_id'], r['procurement_id']) for r in links}
        summary = {'diavgeia_documents': len(decisions), 'contracts': len(contracts),
                   'pdfs_processed_this_run': processed,
                   'pdf_errors': sum(records.get(d['source_type']+':'+d['source_id'], {}).get('status') == 'error' for d in documents),
                   'remaining_unprocessed': total_pending-processed,
                   'rate_limited_this_run': deferred,
                   'matched_pairs': len(pairs), 'added_links': added, 'database_changed': bool(added)}
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
