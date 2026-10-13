"""Preview or import the legacy PDF-link cache into existing document rows.

No source documents or bridge links are created, deleted, or updated. Apply
requires empty target scan columns and archives the exact input file together
with the current scan columns and bridge before changing any rows.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil
import sys
import uuid

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import sync_diavgeia_contract_pdf_links as sync


def write_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf-8') as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, default=str)


def text_index(exports: Path) -> dict[str, list[Path]]:
    indexed = defaultdict(list)
    for path in sorted(exports.glob('*/text/*.txt'), reverse=True):
        indexed[path.stem].append(path)
    return dict(indexed)


def saved_text(record: dict, indexed: dict, output: Path) -> tuple[str | None, str]:
    """Reuse only extraction text bound to the legacy PDF's recorded hash."""
    expected_hash = record.get('pdf_sha256')
    if not expected_hash:
        return None, 'missing_pdf_hash'
    for path in indexed.get(record['source_id'], []):
        sidecar = path.with_suffix('.sha256')
        if sidecar.exists() and sidecar.read_text().strip() == expected_hash:
            text = path.read_text()
            if text.strip():
                return text, 'saved_text'
    pdf = ROOT / 'pdf' / (record['source_id'] + '.pdf')
    if not pdf.exists():
        return None, 'missing_local_pdf_or_matching_text'
    with pdf.open('rb') as handle:
        actual_hash = hashlib.file_digest(handle, 'sha256').hexdigest()
    if actual_hash != expected_hash:
        return None, 'local_pdf_hash_mismatch'

    def no_download(*args, **kwargs):
        raise RuntimeError('Migration never downloads PDFs')

    (output / 'local-pdf-text' / 'text').mkdir(parents=True, exist_ok=True)
    collected = sync.collect(
        {'ada': record['source_id'], 'document_url': ''}, output / 'local-pdf-text',
        extract_amount=False, download_pdf=no_download,
    )
    if collected.get('status') != 'read' or collected.get('pdf_sha256') != expected_hash:
        return None, 'local_pdf_extraction_failed'
    text = Path(collected['text_path']).read_text()
    return (text, 'local_pdf') if text.strip() else (None, 'empty_local_pdf_text')


def invalid_scan_fields(record: dict) -> list[str]:
    invalid = []
    for field in ['fingerprint', 'pdf_sha256']:
        value = record.get(field)
        if not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{64}', value):
            invalid.append(field)
    value = record.get('checked_at')
    try:
        parsed = datetime.fromisoformat(value) if isinstance(value, str) else None
        if parsed is None or parsed.tzinfo is None or parsed.utcoffset() is None:
            invalid.append('checked_at')
    except ValueError:
        invalid.append('checked_at')
    references = record.get('references')
    if not isinstance(references, list) or any(
        not isinstance(ref, dict)
        or not isinstance(ref.get('reference'), str)
        or not ref['reference']
        or not isinstance(ref.get('page'), int)
        or isinstance(ref['page'], bool)
        or ref['page'] < 1
        or not isinstance(ref.get('evidence'), str)
        for ref in references
    ):
        invalid.append('references')
    return invalid


def resolved_pairs(records: dict, contracts: dict, decisions: dict) -> set[tuple[int, int]]:
    """Compare literal legacy/new references independently of scan versions."""
    pairs = set()
    for record in records.values():
        if record.get('status') != 'read':
            continue
        source = record['source_id']
        references = record.get('references', [])
        if not isinstance(references, list):
            continue
        for ref in references:
            if not isinstance(ref, dict) or not isinstance(ref.get('reference'), str):
                continue
            if record['source_type'] == 'contract':
                adam, ada = source, ref['reference']
            else:
                adam, ada = ref['reference'], source
            if adam in contracts and ada in decisions:
                pairs.add((decisions[ada]['id'], contracts[adam]['id']))
    return pairs


def prepare_import(records: dict, documents: list[dict], contracts: dict, decisions: dict,
                   indexed: dict, output: Path) -> tuple[dict, Counter, list[dict]]:
    documents_by_key = {d['source_type'] + ':' + d['source_id']: d for d in documents}
    prepared, coverage, issues = {}, Counter(), []
    checked_at = datetime.now(timezone.utc).isoformat()
    for key, old in records.items():
        expected_key = old['source_type'] + ':' + old['source_id']
        if key != expected_key:
            raise ValueError(f'Invalid cache identity: {key}')
        if key not in documents_by_key:
            coverage['archived_missing_' + old['source_type']] += 1
            continue
        if old.get('status') != 'read':
            coverage['archived_unsuccessful_' + old['source_type']] += 1
            continue
        invalid_fields = invalid_scan_fields(old)
        if invalid_fields:
            coverage['archived_invalid_' + old['source_type']] += 1
            issues.append({'source_type': old['source_type'], 'source_id': old['source_id'],
                           'reason': 'invalid_legacy_fields', 'invalid_fields': invalid_fields})
            continue
        document = documents_by_key[key]
        text, method = saved_text(old, indexed, output)
        if text is not None:
            record = {**old, 'references': sync.extract_references(text, old['source_type']),
                      'extractor_version': sync.EXTRACTOR_VERSION, 'checked_at': checked_at}
        else:
            # Shape filtering cannot recover references the old detector missed.
            # Such records keep version 1 and are scheduled for a genuine reread.
            target_map = decisions if old['source_type'] == 'contract' else contracts
            references = []
            for ref in old.get('references', []):
                identifier = ref['reference']
                detected = sync.extract_references(identifier, old['source_type'])
                if identifier in target_map or any(r['reference'] == identifier for r in detected):
                    references.append(ref)
            record = {**old, 'references': references, 'extractor_version': 1}
            issues.append({'source_type': old['source_type'], 'source_id': old['source_id'],
                           'reason': method, 'extractor_version': 1})
        payload = sync.scan_payload(record)
        prepared[key] = {'document': document, 'record': record, 'payload': payload}
        coverage['import_' + old['source_type']] += 1
        coverage['extractor_version_' + str(record['extractor_version'])] += 1
        coverage[method] += 1
        if record['fingerprint'] != sync.fingerprint(document):
            coverage['stale_source_fingerprint'] += 1
        coverage['references_before'] += len(old.get('references', []))
        coverage['references_after'] += len(record['references'])
    return prepared, coverage, issues


def database_snapshot(connection) -> dict:
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute('SELECT id,ada,pdf_link_scan FROM public.diavgeia ORDER BY id')
        decisions = [dict(row) for row in cursor.fetchall()]
        cursor.execute('SELECT id,reference_number,pdf_link_scan FROM public.procurement ORDER BY id')
        contracts = [dict(row) for row in cursor.fetchall()]
        cursor.execute('SELECT * FROM public.diavgeia_procurement ORDER BY diavgeia_id,procurement_id')
        links = [dict(row) for row in cursor.fetchall()]
    return {'diavgeia': decisions, 'procurement': contracts, 'diavgeia_procurement': links}


def bridge_pairs(snapshot: dict) -> set[tuple[int, int]]:
    return {(r['diavgeia_id'], r['procurement_id']) for r in snapshot['diavgeia_procurement']}


def require_empty_targets(prepared: dict, snapshot: dict):
    existing = {('diavgeia', r['id']): r for r in snapshot['diavgeia']}
    existing.update({('contract', r['id']): r for r in snapshot['procurement']})
    occupied = []
    for key, item in prepared.items():
        document = item['document']
        row = existing.get((document['source_type'], document['id']))
        identity = 'ada' if document['source_type'] == 'diavgeia' else 'reference_number'
        if row is None or row[identity] != document['source_id']:
            raise RuntimeError(f'Source identity changed during migration: {key}')
        if row['pdf_link_scan'] is not None:
            occupied.append(key)
    if occupied:
        raise RuntimeError(f'Refusing to overwrite {len(occupied)} existing scans; first: {occupied[0]}')


def apply_import(connection, prepared: dict, state: Path, expected_state_hash: str,
                 backup: Path, batch_size: int) -> dict:
    """Lock, archive, update only empty scans, verify, and commit once."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout='90s'")
            # Match runtime writes: lock source rows before acquiring the bridge.
            for source_type, table in [('contract', 'procurement'), ('diavgeia', 'diavgeia')]:
                ids = sorted(item['document']['id'] for item in prepared.values()
                             if item['document']['source_type'] == source_type)
                if ids:
                    cursor.execute(f'SELECT id FROM public.{table} WHERE id=ANY(%s) ORDER BY id FOR UPDATE', (ids,))
                    cursor.fetchall()
            # Preserve a stable bridge snapshot while this one-time import runs.
            cursor.execute('LOCK TABLE public.diavgeia_procurement IN SHARE MODE')
        before = database_snapshot(connection)
        require_empty_targets(prepared, before)
        with state.open('rb') as handle:
            if hashlib.file_digest(handle, 'sha256').hexdigest() != expected_state_hash:
                raise RuntimeError('Legacy cache changed after preview; rerun migration')
        backup.mkdir(parents=True, exist_ok=False)
        shutil.copyfile(state, backup / 'legacy-state.json')
        with (backup / 'legacy-state.json').open('rb') as handle:
            if hashlib.file_digest(handle, 'sha256').hexdigest() != expected_state_hash:
                raise RuntimeError('Legacy cache changed while archiving; no scans were written')
        write_json(backup / 'database-before.json', before)
        counts = Counter()
        for source_type, table, identity in [('diavgeia', 'diavgeia', 'ada'),
                                              ('contract', 'procurement', 'reference_number')]:
            items = [item for item in prepared.values() if item['document']['source_type'] == source_type]
            values = [(item['document']['id'], item['document']['source_id'], Json(item['payload']))
                      for item in items]
            if not values:
                continue
            with connection.cursor() as cursor:
                written = execute_values(cursor, f'''UPDATE public.{table} AS target
                    SET pdf_link_scan = incoming.scan::jsonb
                    FROM (VALUES %s) AS incoming(id,source_id,scan)
                    WHERE target.id=incoming.id AND target.{identity}=incoming.source_id
                      AND target.pdf_link_scan IS NULL
                    RETURNING target.id,target.pdf_link_scan''', values,
                    template='(%s::bigint,%s::text,%s::jsonb)', page_size=batch_size, fetch=True)
            expected = {item['document']['id']: item['payload'] for item in items}
            actual = dict(written)
            if actual != expected:
                raise RuntimeError(f'Imported scan verification failed for {table}')
            counts[source_type] = len(written)
        after = database_snapshot(connection)
        if after['diavgeia_procurement'] != before['diavgeia_procurement']:
            raise RuntimeError('Bridge changed during scan import')
        expected_scans = {(item['document']['source_type'], item['document']['id']): item['payload']
                          for item in prepared.values()}
        for source_type, table in [('diavgeia', 'diavgeia'), ('contract', 'procurement')]:
            before_rows = {row['id']: row for row in before[table]}
            if set(before_rows) != {row['id'] for row in after[table]}:
                raise RuntimeError(f'Source row set changed during import: {table}')
            for row in after[table]:
                wanted = expected_scans.get((source_type, row['id']), before_rows[row['id']]['pdf_link_scan'])
                if row['pdf_link_scan'] != wanted:
                    raise RuntimeError(f'Persisted scan mismatch: {table} row {row["id"]}')
        connection.commit()
        return {'imported': dict(counts), 'bridge_pairs_preserved': len(bridge_pairs(before)),
                'backup_directory': str(backup)}
    except BaseException:
        connection.rollback()
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state', type=Path, default=ROOT / 'state' / 'diavgeia_contract_pdf_links.json')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--backup', type=Path)
    parser.add_argument('--batch-size', type=int, default=500)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error('--batch-size must be positive')
    run_id = datetime.now().strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:6]
    output = args.output or ROOT / 'exports' / ('pdf-link-scan-migration-' + run_id)
    backup = args.backup or ROOT / 'backupDB' / ('pdf-link-scan-migration-' + run_id)
    for name in ['migration-summary.json', 'reextraction-issues.json', 'pair-differences.json']:
        if (output / name).exists():
            parser.error(f'Output already exists: {output / name}; choose a fresh --output')
    raw = args.state.read_bytes()
    state_hash = hashlib.sha256(raw).hexdigest()
    state = json.loads(raw)
    if state.get('version') != 1 or not isinstance(state.get('documents'), dict):
        parser.error('Expected a version 1 legacy documents cache')
    records = state['documents']
    connection = psycopg2.connect(sync.resolve_database_url(None), connect_timeout=15)
    try:
        connection.set_session(readonly=True)
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout='60s'")
        contracts, decisions, documents, _ = sync.load_documents(connection)
        snapshot = database_snapshot(connection)
        connection.rollback()
        prepared, coverage, issues = prepare_import(
            records, documents, contracts, decisions, text_index(ROOT / 'exports'), output,
        )
        require_empty_targets(prepared, snapshot)
        old_pairs = resolved_pairs(records, contracts, decisions)
        new_records = {key: item['record'] for key, item in prepared.items()}
        new_pairs = resolved_pairs(new_records, contracts, decisions)
        preserved_pairs = bridge_pairs(snapshot)
        if not old_pairs <= preserved_pairs:
            raise RuntimeError('Legacy resolved pairs are missing from the bridge; resolve before migration')
        summary = {'mode': 'apply' if args.apply else 'preview', 'legacy_records': len(records),
                   'input_sha256': state_hash, 'coverage': dict(coverage),
                   'legacy_resolved_pairs': len(old_pairs), 'reextracted_resolved_pairs': len(new_pairs),
                   'legacy_pairs_absent_from_reextracted_results': len(old_pairs - new_pairs),
                   'new_pairs_absent_from_legacy_results': len(new_pairs - old_pairs),
                   'existing_bridge_pairs': len(preserved_pairs),
                   'bridge_will_be_unchanged': True, 'database_changed': False,
                   'payload_bytes': sum(len(json.dumps(item['payload'], ensure_ascii=False).encode())
                                        for item in prepared.values())}
        print(f"Εγγραφές προς μεταφορά: {len(prepared)}. Υπάρχουσες συνδέσεις: {len(preserved_pairs)}.", flush=True)
        print(f"Νέα εξαγωγή από αποθηκευμένο κείμενο/PDF: {coverage['saved_text'] + coverage['local_pdf']}. "
              f"Παλιές εξαγωγές που χρειάζονται επανάληψη: {coverage['extractor_version_1']}.", flush=True)
        if args.apply:
            connection.set_session(readonly=False)
            summary['verification'] = apply_import(connection, prepared, args.state, state_hash, backup, args.batch_size)
            summary['database_changed'] = bool(prepared)
            print(f'Η μεταφορά επαληθεύτηκε. Αντίγραφα: {backup}', flush=True)
        else:
            print('Προεπισκόπηση: δεν άλλαξε η βάση. Για εφαρμογή χρησιμοποιείται --apply.', flush=True)
        write_json(output / 'migration-summary.json', summary)
        write_json(output / 'reextraction-issues.json', issues)
        write_json(output / 'pair-differences.json', {
            'legacy_pairs_absent_from_reextracted_results': sorted(old_pairs - new_pairs),
            'new_pairs_absent_from_legacy_results': sorted(new_pairs - old_pairs),
        })
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    finally:
        connection.rollback()
        connection.close()


if __name__ == '__main__':
    main()
