#!/usr/bin/env python3
"""Batch classification: submit, status, collect [--apply].

submit sends only documents not resolved by keywords; no database writes.
collect previews by default; --apply saves validated keyword/model results.
One local state file preserves source snapshots and prevents duplicate submissions.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
from types import SimpleNamespace
import psycopg2
from dotenv import load_dotenv
from openai import OpenAI

if __package__:
    from . import classify_fire_prevention as core
else:
    import classify_fire_prevention as core

STATE = core.ROOT / 'logs' / 'prevention-batch-state.json'
AUDIT = core.ROOT / 'logs' / 'prevention-classification.jsonl'
TERMINAL = {'completed', 'failed', 'expired', 'cancelled'}


def save_state(path, state):
    temp = path.with_suffix('.tmp')
    with temp.open('w', encoding='utf-8') as out:
        json.dump(state, out, ensure_ascii=False)
        out.flush()
        os.fsync(out.fileno())
    temp.replace(path)
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def recover_submission(client, path, state):
    """Reattach an existing remote job; never create a job during recovery."""
    if state.get('batch_id') or state.get('phase') == 'local_only':
        return state
    input_file_id = state.get('input_file_id')
    if not input_file_id:
        raise ValueError('Δεν υπάρχει αποθηκευμένο input_file_id για ανάκτηση. Δεν έγινε νέα υποβολή.')
    if client is None:
        client = OpenAI(timeout=90, max_retries=2)
    # SDK iteration follows all pages, including older jobs.
    matches = [job for job in client.batches.list(limit=100)
               if job.input_file_id == input_file_id and job.endpoint == '/v1/responses']
    if not matches:
        raise ValueError('Δεν εντοπίστηκε ακόμη το batch. Δοκίμασε ξανά recover αργότερα. '
                         'Δεν έγινε νέα υποβολή.')
    if len(matches) != 1:
        raise ValueError('Βρέθηκαν πολλαπλά batches για το ίδιο αρχείο. '
                         'Απαιτείται έλεγχος πριν από την επιλογή. Δεν έγινε νέα υποβολή.')
    recovered = {**state, 'batch_id': matches[0].id, 'phase': 'submitted'}
    save_state(path, recovered)
    state.update(recovered)
    print('Ανακτήθηκε το υπάρχον batch. Δεν δημιουργήθηκε νέο.')
    return state


def make_request(document, model):
    text = {k: document[k] for k in ('title', 'short_descriptions', 'subject') if document.get(k)}
    return {'custom_id': f"{document['source']}:{document['id']}",
            'method': 'POST', 'url': '/v1/responses',
            'body': {'model': model, 'store': False, 'instructions': core.PROMPT,
                     'input': json.dumps(text, ensure_ascii=False),
                     'text': {'format': {'type': 'json_schema', 'name': 'fire_prevention',
                                         'strict': True, 'schema': core.SCHEMA}}}}


def prepare(documents, model):
    entries, requests = {}, []
    for document in documents:
        audit = {}
        result = core.classify_keywords(document, audit)
        if result is None and not any((document.get(k) or '').strip()
                                      for k in ('title', 'short_descriptions', 'subject')):
            result = {'prevention': 2, 'reason': 'Δεν υπάρχει διαθέσιμο κείμενο.',
                      'evidence': '', 'evidence_field': None}
            audit['classification_method'] = 'empty_text'
        cid = f"{document['source']}:{document['id']}"
        entries[cid] = {'document': document, 'result': result, 'audit': audit}
        if result is None:
            requests.append(make_request(document, model))
    return entries, requests


def parse_batch_row(row, document, audit):
    if row.get('error'):
        raise ValueError(json.dumps(row['error'], ensure_ascii=False))
    response = row.get('response') or {}
    if response.get('status_code') != 200:
        raise ValueError(f"HTTP {response.get('status_code')}: {response.get('body')}")
    body = response['body']
    audit.update(response_id=body.get('id'), actual_model=body.get('model'),
                 token_usage=body.get('usage'), classification_method='model')
    if body.get('status') != 'completed':
        raise ValueError('Model response was not completed')
    parts = [content['text'] for item in body.get('output', [])
             if item.get('type') == 'message' for content in item.get('content', [])
             if content.get('type') == 'output_text']
    candidate = json.loads(''.join(parts))
    audit['model_candidate'] = candidate
    return core.validate_classification(candidate, document)


def submit(client, args):
    args.state.parent.mkdir(parents=True, exist_ok=True)
    # Reserve before any network call. An interrupted submission must be inspected,
    # never silently resubmitted (which would incur duplicate charges).
    with args.state.open('x', encoding='utf-8') as out:
        json.dump({'phase': 'preparing'}, out)
    conn = psycopg2.connect(os.environ['DATABASE_URL'], connect_timeout=15)
    try:
        conn.set_session(readonly=True)
        documents = core.select_documents(conn, SimpleNamespace(
            source=args.source, reference_number=args.reference_number, ada=args.ada,
            limit_per_source=None))
    finally:
        conn.close()
    entries, requests = prepare(documents, args.model)
    state = {'phase': 'prepared', 'model': args.model, 'prompt_version': core.PROMPT_VERSION,
             'entries': entries, 'batch_id': None}
    save_state(args.state, state)
    print(f'Σύνολο: {len(entries)}. Τοπικά αποτελέσματα: {len(entries)-len(requests)}. OpenAI: {len(requests)}.')
    if requests:
        payload = ('\n'.join(json.dumps(r, ensure_ascii=False) for r in requests)+'\n').encode('utf-8')
        if len(requests) > 50000 or len(payload) > 200_000_000:
            raise ValueError('Batch exceeds API limits; select a single source or identifier')
        if client is None:
            client = OpenAI(timeout=90, max_retries=2)
        uploaded = client.files.create(file=('prevention-batch.jsonl', payload), purpose='batch')
        state.update(input_file_id=uploaded.id, phase='submitting')
        save_state(args.state, state)
        # A timeout can follow a successful remote create. Retrying create could
        # duplicate the charged job: recover by input_file_id instead.
        batch = client.with_options(max_retries=0).batches.create(
            input_file_id=uploaded.id, endpoint='/v1/responses', completion_window='24h')
        state.update(batch_id=batch.id, phase='submitted')
    else:
        state['phase'] = 'local_only'
    save_state(args.state, state)
    print('Η υποβολή ολοκληρώθηκε. Καμία εγγραφή στη βάση. Έλεγχος με την εντολή status.')


def collect(client, args, state):
    if not state.get('batch_id') and state.get('phase') != 'local_only':
        recover_submission(client, args.state, state)
    batch_id = state.get('batch_id')
    output_rows = {}
    if batch_id:
        if client is None:
            client = OpenAI(timeout=90, max_retries=2)
        batch = client.batches.retrieve(batch_id)
        print(f'Κατάσταση batch: {batch.status}')
        if batch.status not in TERMINAL:
            print('Δεν έχει ολοκληρωθεί ακόμη. Δοκίμασε ξανά αργότερα.')
            return 0
        for file_id in (batch.output_file_id, batch.error_file_id):
            if file_id:
                for line in client.files.content(file_id).text.splitlines():
                    row = json.loads(line)
                    cid = row['custom_id']
                    if cid not in state['entries'] or cid in output_rows:
                        raise ValueError('Unknown or duplicate batch result identifier')
                    output_rows[cid] = row
    elif state.get('phase') != 'local_only':
        raise ValueError('Submission did not complete. Inspect local state before retrying.')
    conn = psycopg2.connect(os.environ['DATABASE_URL'], connect_timeout=15) if args.apply else None
    errors = 0
    try:
        AUDIT.parent.mkdir(parents=True, exist_ok=True)
        with AUDIT.open('a', encoding='utf-8') as report:
            for cid, entry in state['entries'].items():
                document = entry['document']
                record = {'source': document['source'], 'id': document['id'],
                          'identifier': document.get('reference_number', document.get('ada')),
                          'requested_model': state['model'], 'model': state['model'],
                          'prompt_version': state['prompt_version'], 'batch_id': batch_id,
                          'text_source': 'stored_metadata', **entry['audit']}
                try:
                    result = entry['result']
                    if result is None:
                        if cid not in output_rows:
                            raise ValueError('No batch response returned for this document')
                        result = parse_batch_row(output_rows[cid], document, record)
                    else:
                        result = core.validate_classification(result, document)
                    record.update(result)
                    record['status'] = core.apply_result(conn, document, result) if args.apply else 'preview'
                except Exception as exc:
                    if conn:
                        conn.rollback()
                    record.update(status='error', error_type=type(exc).__name__, error=core.safe_error(exc))
                    errors += 1
                print(core.format_terminal_result(record)+'\n', flush=True)
                report.write(json.dumps(record, ensure_ascii=False)+'\n')
                report.flush()
    finally:
        if conn:
            conn.close()
    # Keep snapshots after collection; repeated collect safely skips non-NULL rows.
    if args.apply:
        state['phase'] = 'collected'
        save_state(args.state, state)
    print(f'Αρχείο καταγραφής: {AUDIT}')
    return 1 if errors else 0


def main():
    load_dotenv(core.ROOT / '.env')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('submit', 'status', 'collect', 'recover'))
    parser.add_argument('--state', type=Path, default=STATE, help='Local snapshot/state file')
    parser.add_argument('--source', choices=('procurement', 'diavgeia', 'both'), default='both')
    parser.add_argument('--reference-number')
    parser.add_argument('--ada')
    parser.add_argument('--model', default=os.getenv('OPENAI_PREVENTION_MODEL', 'gpt-5.6-luna'))
    parser.add_argument('--apply', action='store_true', help='Save results on collect only')
    args = parser.parse_args()
    if args.apply and args.command != 'collect':
        parser.error('--apply is supported only with collect')
    if args.reference_number or args.ada:
        args.source = 'both' if args.reference_number and args.ada else ('procurement' if args.reference_number else 'diavgeia')
    if args.command == 'submit':
        if args.state.exists():
            parser.error('Υπάρχει ήδη batch. Χρησιμοποίησε status/collect ή άλλο --state για νέο batch.')
        submit(None, args)
        return 0
    state = json.loads(args.state.read_text(encoding='utf-8'))
    if args.command == 'recover':
        recover_submission(None, args.state, state)
        return 0
    if args.command == 'status':
        if not state.get('batch_id') and state.get('phase') != 'local_only':
            recover_submission(None, args.state, state)
        client = OpenAI(timeout=90, max_retries=2) if state.get('batch_id') else None
        print(f"Κατάσταση: {client.batches.retrieve(state['batch_id']).status}" if state.get('batch_id')
              else f"Κατάσταση: {state['phase']}")
        return 0
    return collect(None, args, state)


if __name__ == '__main__':
    raise SystemExit(main())
