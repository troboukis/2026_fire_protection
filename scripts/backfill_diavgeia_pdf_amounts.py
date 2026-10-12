"""Fill only missing payment-finalization amounts from reviewed PDF evidence.

Default is read-only. --apply backs up complete eligible rows before updating.
The CSV mapping is also consumed by the standard CSV-to-database loader.
"""
from __future__ import annotations

import argparse
from decimal import Decimal
import json
from pathlib import Path
import sys

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.diavgeia_amounts import load_pdf_payment_amounts
from src.map_copernicus_to_municipalities import resolve_database_url

MISSING = "(spending_contractors_value IS NULL OR lower(btrim(spending_contractors_value)) IN ('','nan','none','null','[]','<na>'))"
TYPE = 'ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--backup', type=Path)
    args = parser.parse_args()
    if args.apply and args.backup is None:
        parser.error('--apply requires --backup')
    amounts = load_pdf_payment_amounts()
    if not amounts:
        raise ValueError('No reviewed PDF amounts')
    for amount in amounts.values():
        value = Decimal(amount)
        if not value.is_finite() or value < 0:
            raise ValueError('Invalid amount')
    connection = psycopg2.connect(resolve_database_url(None), connect_timeout=15)
    connection.set_session(readonly=not args.apply)
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SET LOCAL statement_timeout = '30s'")
            cursor.execute(
                f'SELECT * FROM public.diavgeia WHERE ada=ANY(%s) AND diavgeia_document_type_decision_uid=%s AND {MISSING}'
                + (' FOR UPDATE' if args.apply else ''),
                (list(amounts), TYPE))
            before = cursor.fetchall()
        print(f'Αποφάσεις με τεκμηριωμένο ποσό: {len(amounts)}. Κενές εγγραφές προς ενημέρωση: {len(before)}.', flush=True)
        if not args.apply:
            connection.rollback()
            print('Προεπισκόπηση: η βάση δεν άλλαξε.')
            return
        if args.backup.exists():
            raise FileExistsError('Backup exists; use a new path')
        args.backup.parent.mkdir(parents=True, exist_ok=True)
        args.backup.write_text(json.dumps(before, ensure_ascii=False, indent=2, default=str))
        if before:
            with connection.cursor() as cursor:
                updated = execute_values(cursor, f'''
                    UPDATE public.diavgeia AS d
                    SET spending_contractors_value = source.amount, updated_at = NOW()
                    FROM (VALUES %s) AS source(ada, amount)
                    WHERE d.ada = source.ada
                      AND d.diavgeia_document_type_decision_uid = '{TYPE}'
                      AND {MISSING}
                    RETURNING d.ada, d.spending_contractors_value
                ''', [(row['ada'], amounts[row['ada']]) for row in before], fetch=True)
                if dict(updated) != {row['ada']: amounts[row['ada']] for row in before}:
                    raise RuntimeError('Update did not match eligible rows')
        else:
            updated = []
        connection.commit()
        with connection.cursor() as cursor:
            cursor.execute('SELECT ada,spending_contractors_value FROM public.diavgeia WHERE ada=ANY(%s)',
                           ([row['ada'] for row in before],))
            if dict(cursor.fetchall()) != {row['ada']: amounts[row['ada']] for row in before}:
                raise RuntimeError('Post-commit verification failed')
            cursor.execute(f'SELECT count(*) FROM public.diavgeia WHERE diavgeia_document_type_decision_uid=%s AND {MISSING}', (TYPE,))
            remaining = cursor.fetchone()[0]
        print(f'Ενημερώθηκαν και επαληθεύτηκαν {len(updated)} ποσά. Αποφάσεις χωρίς ποσό: {remaining}.')
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


if __name__ == '__main__':
    main()
