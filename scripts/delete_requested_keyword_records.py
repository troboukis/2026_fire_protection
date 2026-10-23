#!/usr/bin/env python3
"""Delete only the keyword matches in an explicitly reviewed record catalog.

The default mode creates a fresh read-only dependency preview. Applying requires
that exact preview and a verified full backup; it never broadens the selection
to the rest of the exclusion dictionary.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.delete_hospital_organization_records import capture, digest, read_rows, serialized, write_json
from scripts.delete_procurements_by_keywords import load_database_url
from src.keyword_exclusions import DEFAULT_EXCLUDE_KEYWORDS, _tokens_match_keyword_prefix, normalize_keyword_tokens


REQUESTED_KEYWORDS = (
    'γυμνασ', 'λυκει', 'νηπιαγ', 'δημοτικ σχολει', 'θεατρ', 'πανεπιστημ',
    'πυροσβεστηρ', 'γνθ', 'γ ν θ', 'ανακαιν', 'νοσοκομ', 'αλατοδιανομ', 'εκχιονισμ',
)


def selected_records(cur) -> tuple[list[dict], list[dict]]:
    patterns = [(normalize_keyword_tokens(k), DEFAULT_EXCLUDE_KEYWORDS[k]) for k in REQUESTED_KEYWORDS]

    def matches(*values) -> bool:
        fields = [normalize_keyword_tokens(v) for v in values]
        return any(_tokens_match_keyword_prefix(tokens, roots, gap)
                   for roots, gap in patterns for tokens in fields)

    organizations = read_rows(cur, '''SELECT organization_key, organization_value,
        organization_normalized_value FROM public.organization ORDER BY id''')
    names = defaultdict(set)
    for org in organizations:
        for field in ('organization_value', 'organization_normalized_value'):
            if org[field]:
                names[org['organization_key']].add(org[field])
    procurements = read_rows(cur, '''SELECT id, title, short_descriptions,
        organization_key FROM public.procurement ORDER BY id''')
    decisions = read_rows(cur, 'SELECT id, subject, org_name_clean FROM public.diavgeia ORDER BY id')
    pids = [p['id'] for p in procurements
            if matches(p['title'], p['short_descriptions'], *sorted(names[p['organization_key']]))]
    dids = [d['id'] for d in decisions if matches(d['subject'], d['org_name_clean'])]
    return (read_rows(cur, 'SELECT * FROM public.procurement WHERE id = ANY(%s) ORDER BY id', (pids,)),
            read_rows(cur, 'SELECT * FROM public.diavgeia WHERE id = ANY(%s) ORDER BY id', (dids,)))


def validate_catalog(captured: dict, approved: dict) -> None:
    """The authorization covers these identities, matching fields and amounts."""
    for table, key, fields in (
        ('procurement', 'procurements', ('reference_number', 'title', 'short_descriptions', 'organization_key',
                                       'contract_signed_date', 'cancelled', 'next_ref_no', 'prev_reference_no')),
        ('diavgeia', 'diavgeia', ('ada', 'subject', 'org_type', 'org_name_clean', 'organization_key',
                                'spending_contractors_value', 'diavgeia_document_type_decision_uid')),
    ):
        expected = {r['id']: r for r in approved[key]}
        actual = {r['id']: r for r in json.loads(serialized(captured['rows'][table]))}
        if set(actual) != set(expected):
            raise RuntimeError(f'{table}: IDs changed since the reviewed catalog; no deletion performed.')
        for rid, row in actual.items():
            if any(row[field] != expected[rid][field] for field in fields):
                raise RuntimeError(f'{table} {rid}: reviewed identity or matching fields changed.')
    amounts = defaultdict(lambda: Decimal(0))
    for payment in captured['rows']['payment']:
        if payment['procurement_id'] is not None:
            amounts[payment['procurement_id']] += Decimal(str(payment['amount_without_vat'] or 0))
    for row in approved['procurements']:
        if amounts[row['id']] != Decimal(row['amount']):
            raise RuntimeError(f"Procurement {row['id']}: amount changed since the reviewed catalog.")


def make_manifest(captured: dict, approved: dict) -> dict:
    return {
        'rule_sha256': digest({'script': Path(__file__).read_text(),
                               'capture_script': (ROOT / 'scripts/delete_hospital_organization_records.py').read_text(),
                               'policy': (ROOT / 'src/keyword_exclusions.py').read_text()}),
        'catalog_sha256': digest(approved), 'data_sha256': digest(captured),
        'procurement_ids': [r['id'] for r in captured['rows']['procurement']],
        'diavgeia_ids': [r['id'] for r in captured['rows']['diavgeia']],
        'affected_rows': {k: len(v) for k, v in captured['rows'].items()},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--catalog-dir', type=Path, required=True)
    parser.add_argument('--preview-dir', type=Path, required=True)
    parser.add_argument('--backup-dir', type=Path)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if args.apply and not args.backup_dir:
        parser.error('--apply requires --backup-dir')
    approved = json.loads((args.catalog_dir / 'matched-records.json').read_text())
    summary = json.loads((args.catalog_dir / 'preview-summary.json').read_text())
    if summary['requested_keywords'] != list(REQUESTED_KEYWORDS):
        raise RuntimeError('Catalog keywords differ from the reviewed selection.')
    conn = psycopg2.connect(load_database_url(None), connect_timeout=15)
    conn.set_session(readonly=not args.apply, isolation_level='REPEATABLE READ')
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET LOCAL statement_timeout = '60s'")
            if args.apply:
                cur.execute('''LOCK TABLE public.procurement, public.diavgeia, public.payment,
                    public.cpv, public.works, public.diavgeia_beneficiary, public.diavgeia_procurement,
                    public.payment_beneficiary, public.organization, public.organization_normalized_name
                    IN SHARE ROW EXCLUSIVE MODE''')
            before = capture(cur, selected_records)
            validate_catalog(before, approved)
            manifest = make_manifest(before, approved)
            if not args.apply:
                write_json(args.preview_dir / 'preview.json', manifest)
                write_json(args.preview_dir / 'affected-records.json', before)
                conn.rollback()
                print(serialized({'affected_rows': manifest['affected_rows'],
                                  'preview': str(args.preview_dir / 'preview.json')}))
                return
            if manifest != json.loads((args.preview_dir / 'preview.json').read_text()):
                raise RuntimeError('Dependency preview changed; no deletion performed.')
            write_json(args.backup_dir / 'backup.json', before)
            write_json(args.backup_dir / 'manifest.json', manifest)
            if digest(json.loads((args.backup_dir / 'backup.json').read_text())) != digest(json.loads(serialized(before))):
                raise RuntimeError('Backup read-back verification failed; no deletion performed.')
            pids, dids = manifest['procurement_ids'], manifest['diavgeia_ids']
            cur.execute('UPDATE public.organization SET diavgeia_id = NULL WHERE diavgeia_id = ANY(%s)', (dids,))
            detached_orgs = cur.rowcount
            cur.execute('''UPDATE public.payment SET diavgeia_id = NULL
                WHERE diavgeia_id = ANY(%s) AND NOT (id = ANY(%s))''', (dids, before['deleted_payment_ids']))
            detached_payments = cur.rowcount
            cur.execute('DELETE FROM public.procurement WHERE id = ANY(%s) RETURNING id', (pids,))
            deleted_p = sorted(r['id'] for r in cur.fetchall())
            cur.execute('DELETE FROM public.diavgeia WHERE id = ANY(%s) RETURNING id', (dids,))
            deleted_d = sorted(r['id'] for r in cur.fetchall())
            if deleted_p != pids or deleted_d != dids:
                raise RuntimeError('Deleted IDs differ from the reviewed catalog.')
            cur.execute('SET CONSTRAINTS ALL IMMEDIATE')
            after = capture(cur, selected_records)
            if after['rows']['procurement'] or after['rows']['diavgeia']:
                raise RuntimeError('Requested keyword matches remain; rolling back.')
            deleted = {t: before['counts_before'][t] - after['counts_before'][t] for t in before['rows']}
            expected = {t: len(rows) for t, rows in before['rows'].items()}
            expected.update(payment=len(before['deleted_payment_ids']), organization=0)
            if deleted != expected:
                raise RuntimeError('Unexpected table changes: ' + serialized(deleted))
            result = {'applied_at': datetime.now(timezone.utc), 'deleted': deleted,
                      'remaining': {'procurement': 0, 'diavgeia': 0},
                      'detached_organization_references': detached_orgs,
                      'detached_payment_references': detached_payments,
                      'backup': str(args.backup_dir / 'backup.json')}
            conn.commit()
            write_json(args.backup_dir / 'result.json', result)
            print(serialized(result))
    finally:
        conn.rollback()
        conn.close()


if __name__ == '__main__':
    main()
