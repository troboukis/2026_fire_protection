#!/usr/bin/env python3
"""Preview, back up and remove records issued by hospital organizations.

Preview is read-only. --apply requires the exact preview manifest and aborts if
the matched records or their dependencies changed. Organization lookup rows and
unrelated payments are preserved; only their references to deleted decisions are
cleared where required by foreign keys.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.delete_procurements_by_keywords import load_database_url
from src.organization_exclusions import is_hospital_organization


def serialized(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def digest(value) -> str:
    return hashlib.sha256(serialized(value).encode()).hexdigest()


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x', encoding='utf-8') as handle:
        handle.write(json.dumps(value, ensure_ascii=False, indent=2, default=str) + '\n')
        handle.flush()
        import os
        os.fsync(handle.fileno())


def read_rows(cur, query, params=()) -> list[dict]:
    cur.execute(query, params)
    return [dict(row) for row in cur.fetchall()]


def hospital_records(cur) -> tuple[list[dict], list[dict]]:
    organizations = read_rows(cur, '''
        SELECT o.*, n.organization_normalized_name AS normalized_alias
        FROM public.organization o
        LEFT JOIN public.organization_normalized_name n
          ON n.id = o.organization_normalized_name_id ORDER BY o.id
    ''')
    hospital_keys = {o['organization_key'] for o in organizations
                     if is_hospital_organization(o['organization_value'],
                                                 o['organization_normalized_value'],
                                                 o['normalized_alias'])}
    procurements = read_rows(cur, 'SELECT id, organization_key FROM public.procurement ORDER BY id')
    decisions = read_rows(cur, 'SELECT id, organization_key, org_type, org_name_clean FROM public.diavgeia ORDER BY id')
    pids = [p['id'] for p in procurements if p['organization_key'] in hospital_keys]
    dids = [d['id'] for d in decisions if d['organization_key'] in hospital_keys
            or is_hospital_organization(d['org_name_clean'], org_type=d['org_type'])]
    return (read_rows(cur, 'SELECT * FROM public.procurement WHERE id = ANY(%s) ORDER BY id', (pids,)),
            read_rows(cur, 'SELECT * FROM public.diavgeia WHERE id = ANY(%s) ORDER BY id', (dids,)))


def capture(cur, record_selector=hospital_records) -> dict:
    procurements, decisions = record_selector(cur)
    pids = [p['id'] for p in procurements]
    dids = [d['id'] for d in decisions]
    refs = [p['reference_number'] for p in procurements if p['reference_number']]
    pointed_payments = [p['payment_id'] for p in procurements if p['payment_id'] is not None]
    payments = read_rows(cur, '''
        SELECT * FROM public.payment WHERE procurement_id = ANY(%s)
           OR diavgeia_id = ANY(%s) OR id = ANY(%s) ORDER BY id
    ''', (pids, dids, pointed_payments))
    deleted_payments = [p['id'] for p in payments if p['procurement_id'] in pids]
    cross_links = read_rows(cur, '''
        SELECT id, reference_number, payment_id FROM public.procurement
        WHERE payment_id = ANY(%s) AND NOT (id = ANY(%s)) ORDER BY id
    ''', (deleted_payments, pids))
    if cross_links:
        raise RuntimeError('Unrelated procurements reference payments selected for deletion: '
                           + serialized(cross_links))
    rows = {
        'procurement': procurements,
        'diavgeia': decisions,
        'payment': payments,
        'cpv': read_rows(cur, 'SELECT * FROM public.cpv WHERE procurement_id = ANY(%s) ORDER BY id', (pids,)),
        'works': read_rows(cur, 'SELECT * FROM public.works WHERE reference_number = ANY(%s) ORDER BY id', (refs,)),
        'diavgeia_beneficiary': read_rows(cur, 'SELECT * FROM public.diavgeia_beneficiary WHERE diavgeia_id = ANY(%s) ORDER BY diavgeia_id, beneficiary_vat_number', (dids,)),
        'diavgeia_procurement': read_rows(cur, '''
            SELECT * FROM public.diavgeia_procurement
            WHERE procurement_id = ANY(%s) OR diavgeia_id = ANY(%s)
            ORDER BY diavgeia_id, procurement_id
        ''', (pids, dids)),
        'payment_beneficiary': read_rows(cur, 'SELECT * FROM public.payment_beneficiary WHERE payment_id = ANY(%s) ORDER BY payment_id, beneficiary_vat_number', (deleted_payments,)),
        'organization': read_rows(cur, 'SELECT * FROM public.organization WHERE diavgeia_id = ANY(%s) ORDER BY id', (dids,)),
    }
    fks = read_rows(cur, '''
        SELECT c.conname, c.conrelid::regclass::text AS child,
               c.confrelid::regclass::text AS parent, pg_get_constraintdef(c.oid) AS definition
        FROM pg_constraint c WHERE c.contype = 'f' AND c.confrelid IN
            ('public.procurement'::regclass, 'public.diavgeia'::regclass,
             'public.payment'::regclass, 'public.works'::regclass)
        ORDER BY child, c.conname
    ''')
    expected = {
        ('cpv', 'procurement'), ('diavgeia_beneficiary', 'diavgeia'),
        ('diavgeia_procurement', 'diavgeia'), ('diavgeia_procurement', 'procurement'),
        ('organization', 'diavgeia'), ('payment', 'diavgeia'),
        ('payment', 'procurement'), ('payment_beneficiary', 'payment'),
        ('procurement', 'payment'), ('works', 'procurement'),
    }
    if {(r['child'], r['parent']) for r in fks} != expected:
        raise RuntimeError('Foreign-key dependency graph changed; review backup coverage before deleting.')
    return {'rows': rows, 'foreign_keys': fks,
            'counts_before': {name: read_rows(cur, sql.SQL('SELECT count(*) AS n FROM public.{}').format(sql.Identifier(name)))[0]['n'] for name in rows},
            'deleted_payment_ids': deleted_payments}


def manifest(captured: dict) -> dict:
    return {
        'rule_sha256': hashlib.sha256((ROOT / 'src/organization_exclusions.py').read_bytes()).hexdigest(),
        'data_sha256': digest(captured),
        'procurement_ids': [p['id'] for p in captured['rows']['procurement']],
        'diavgeia_ids': [d['id'] for d in captured['rows']['diavgeia']],
        'affected_rows': {k: len(v) for k, v in captured['rows'].items()},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--preview-dir', type=Path, required=True)
    parser.add_argument('--backup-dir', type=Path)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if args.apply and not args.backup_dir:
        parser.error('--apply requires --backup-dir')
    conn = psycopg2.connect(load_database_url(None), connect_timeout=15)
    conn.set_session(readonly=not args.apply, isolation_level='REPEATABLE READ')
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET LOCAL statement_timeout = '60s'")
            if args.apply:
                # Keep the preview comparison, backup and deletes in one consistent
                # transaction while preventing concurrent ingestion into these tables.
                cur.execute('''LOCK TABLE public.procurement, public.diavgeia, public.payment,
                    public.cpv, public.works, public.diavgeia_beneficiary,
                    public.diavgeia_procurement, public.payment_beneficiary,
                    public.organization, public.organization_normalized_name
                    IN SHARE ROW EXCLUSIVE MODE''')
            captured = capture(cur)
            current = manifest(captured)
            if not args.apply:
                write_json(args.preview_dir / 'preview.json', current)
                write_json(args.preview_dir / 'affected-records.json', captured)
                with (args.preview_dir / 'references.csv').open('x', encoding='utf-8', newline='') as handle:
                    writer = csv.writer(handle)
                    writer.writerow(['table', 'id', 'reference', 'organization', 'subject'])
                    for p in captured['rows']['procurement']:
                        writer.writerow(['procurement', p['id'], p['reference_number'], p['organization_key'], p['title']])
                    for d in captured['rows']['diavgeia']:
                        writer.writerow(['diavgeia', d['id'], d['ada'], d['org_name_clean'], d['subject']])
                conn.rollback()
                print(serialized(current))
                return
            approved = json.loads((args.preview_dir / 'preview.json').read_text())
            if current != approved:
                raise RuntimeError('Preview no longer matches current data. Generate and review a new preview.')
            write_json(args.backup_dir / 'backup.json', captured)
            write_json(args.backup_dir / 'manifest.json', current)
            # Read the backup back from disk and confirm every saved record.
            if digest(json.loads((args.backup_dir / 'backup.json').read_text())) != digest(json.loads(serialized(captured))):
                raise RuntimeError('Backup verification failed; no deletion performed.')
            pids, dids = current['procurement_ids'], current['diavgeia_ids']
            cur.execute('UPDATE public.organization SET diavgeia_id = NULL WHERE diavgeia_id = ANY(%s)', (dids,))
            detached_orgs = cur.rowcount
            cur.execute('''UPDATE public.payment SET diavgeia_id = NULL
                WHERE diavgeia_id = ANY(%s) AND NOT (id = ANY(%s))''',
                        (dids, captured['deleted_payment_ids']))
            detached_payments = cur.rowcount
            cur.execute('DELETE FROM public.procurement WHERE id = ANY(%s) RETURNING id', (pids,))
            deleted_p = sorted(r['id'] for r in cur.fetchall())
            cur.execute('DELETE FROM public.diavgeia WHERE id = ANY(%s) RETURNING id', (dids,))
            deleted_d = sorted(r['id'] for r in cur.fetchall())
            if deleted_p != pids or deleted_d != dids:
                raise RuntimeError('Deleted IDs differ from the reviewed preview.')
            cur.execute('SET CONSTRAINTS ALL IMMEDIATE')
            after = capture(cur)
            if after['rows']['procurement'] or after['rows']['diavgeia']:
                raise RuntimeError('Hospital records remain; rolling back.')
            deleted_counts = {name: captured['counts_before'][name] - after['counts_before'][name]
                              for name in captured['rows']}
            expected_counts = {name: len(rows) for name, rows in captured['rows'].items()}
            expected_counts['payment'] = len(captured['deleted_payment_ids'])
            expected_counts['organization'] = 0
            if deleted_counts != expected_counts:
                raise RuntimeError('Unexpected table count changes: ' + serialized(deleted_counts))
            result = {'applied_at': datetime.now(timezone.utc), 'deleted': deleted_counts,
                      'detached_organization_references': detached_orgs,
                      'detached_payment_references': detached_payments,
                      'remaining': {'procurement': 0, 'diavgeia': 0},
                      'backup': str(args.backup_dir / 'backup.json')}
            conn.commit()
            write_json(args.backup_dir / 'result.json', result)
            print(serialized(result))
    finally:
        conn.rollback()
        conn.close()


if __name__ == '__main__':
    main()
