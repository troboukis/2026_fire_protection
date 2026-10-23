"""Regression coverage for report calculations, without a database."""

import csv
import importlib.util
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch


MODULE_PATH = Path(__file__).with_name('build_yearly_report.py')
SPEC = importlib.util.spec_from_file_location('build_yearly_report', MODULE_PATH)
builder = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(builder)


def contract(identifier, year, *, cancelled=False):
    return {
        'id': identifier,
        'reference_number': f'{str(year)[2:]}SYMV{identifier:09d}',
        'title': 'Καθαρισμοί πυροπροστασίας',
        'contract_signed_date': date(year, 3, 1),
        'cancelled': cancelled,
        'next_ref_no': None,
        'prev_reference_no': None,
        'diavgeia_ada': None,
        'contract_number': None,
        'contract_type': 'Υπηρεσίες',
        'procedure_type_value': 'Απευθείας ανάθεση',
        'organization_key': 'municipal-org',
        'municipality_key': 'municipality',
        'region_key': None,
        'canonical_owner_scope': 'municipality',
        'prevention': 1,
        'amount_without_vat': 1000 * identifier,
        'amount_with_vat': 1240 * identifier,
        'beneficiary_vat_number': '123456789',
        'beneficiary_name': 'Ανάδοχος',
    }


def payment(identifier, amount, *, year=2026, org_type='ΔΗΜΟΣ', subject=None, ada=None):
    return {
        'id': identifier,
        'ada': ada or f'TEST{identifier}-ABC',
        'publish_date': date(year, 6, 1),
        'subject': subject or f'Καθαρισμοί πυροπροστασίας {identifier}',
        'org_type': org_type,
        'org_name_clean': 'ΔΗΜΟΣ ΔΟΚΙΜΗΣ' if org_type == 'ΔΗΜΟΣ' else 'ΠΕΡΙΦΕΡΕΙΑ ΔΟΚΙΜΗΣ',
        'organization_key': 'municipal-org',
        'municipality_key': 'municipality',
        'region_key': None,
        'amount_text': str(amount),
        'prevention': 1,
        'document_url': f'https://example.test/{identifier}',
    }


class ReviewedPaymentCategoryTest(unittest.TestCase):
    def test_category_overrides_are_ada_specific_and_cancellations_still_win(self):
        subject = 'Πληρωμή Παραστατικού: [ΕΝΤ0305234]'
        self.assertEqual(builder.categorize(subject, '9ΥΡ77Λ7-ΥΚ9'), 'vegetation')
        self.assertEqual(builder.categorize(subject, 'OTHER-ADA'), builder.categorize(subject))
        self.assertEqual(builder.categorize('Υποστηρικτικές υπηρεσίες μελετών', 'Ψ86Ε46Ψ844-ΓΥ4'), 'other')
        self.assertEqual(builder.categorize('Ακύρωση πληρωμής', '9ΥΡ77Λ7-ΥΚ9'), 'cancellation')


class AuthorityTimingRegressionTest(unittest.TestCase):
    def test_authority_scopes_use_signature_date_and_keep_may_first_out(self):
        snapshot = datetime(2026, 10, 23, tzinfo=timezone.utc)
        data = {key: [] for key in builder.SQL}
        data['_meta'] = [{'db_now': snapshot}]
        scopes = {
            'regional': ('region', 'ΠΕΡΙΦΕΡΕΙΑ ΔΟΚΙΜΗΣ'),
            'environment_ministry': ('ministry', 'ΥΠΟΥΡΓΕΙΟ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΕΝΕΡΓΕΙΑΣ'),
            'civil_protection_ministry': ('ministry', 'ΥΠΟΥΡΓΕΙΟ ΚΛΙΜΑΤΙΚΗΣ ΚΡΙΣΗΣ ΚΑΙ ΠΟΛΙΤΙΚΗΣ ΠΡΟΣΤΑΣΙΑΣ'),
        }
        for scope, (owner, name) in scopes.items():
            data['organization'].append({
                'organization_key': scope, 'organization_normalized_value': name,
                'organization_value': name, 'authority_scope': owner,
            })
            for signed in (date(2026, 4, 30), date(2026, 5, 1)):
                row = contract(len(data['procurement']) + 1, 2026)
                row.update(organization_key=scope, canonical_owner_scope=owner,
                           municipality_key=None, contract_signed_date=signed)
                data['procurement'].append(row)
        with TemporaryDirectory() as directory, patch.object(builder, 'load_all', return_value=data):
            report = builder.build(snapshot, Path(directory))
        for scope in scopes:
            with self.subTest(scope=scope):
                row = next(r for r in report['timing'] if r['year'] == 2026 and r['scope'] == scope)
                self.assertEqual(row['contracts'], 2)
                self.assertEqual(row['before_may_contracts'], 1)
                self.assertEqual(row['may_to_august_contracts'], 1)


class MunicipalSpendingRegressionTest(unittest.TestCase):
    def test_only_current_year_unlinked_municipal_payments_are_added(self):
        snapshot = datetime(2026, 10, 22, tzinfo=timezone.utc)
        data = {key: [] for key in builder.SQL}
        data['_meta'] = [{'db_now': snapshot}]
        data['procurement'] = [
            contract(1, 2026),
            contract(2, 2025),
            contract(3, 2026, cancelled=True),
        ]
        data['payments'] = [
            payment(1, 124),  # 100 without VAT
            payment(2, 80, subject='Μισθοδοσία προσωπικού πυροπροστασίας'),  # no VAT
            payment(3, 248, org_type='ΠΕΡΙΦΕΡΕΙΑ'),
            payment(4, 372),  # linked to a current-year contract
            payment(5, 496),  # linked to a previous-year contract
            payment(6, 620),  # linked to two contracts, still one excluded payment
            payment(7, 744, year=2025),
            payment(8, 868),  # linked to a cancelled contract
            payment(9, 992, ada='ORIGINAL-ABC'),
            payment(10, 992, subject='Ακύρωση πληρωμής ORIGINAL-ABC'),
        ]
        data['bridge'] = [
            {'diavgeia_id': 4, 'procurement_id': 1},
            {'diavgeia_id': 5, 'procurement_id': 2},
            {'diavgeia_id': 6, 'procurement_id': 1},
            {'diavgeia_id': 6, 'procurement_id': 2},
            {'diavgeia_id': 8, 'procurement_id': 3},
        ]

        with TemporaryDirectory() as directory, patch.object(builder, 'load_all', return_value=data):
            root = Path(directory)
            report = builder.build(snapshot, root)
            with (root / builder.REPORT_DIR / 'payments_2026.csv').open(encoding='utf-8', newline='') as handle:
                exported = {row['ada']: row for row in csv.DictReader(handle)}

        payments = report['payments']
        self.assertEqual(payments['municipal']['payments'], 2)
        self.assertEqual(payments['municipal']['amount'], 180)
        self.assertEqual(payments['municipal']['payroll'], 1)
        self.assertEqual(payments['municipal']['by_month'][5], {'month': 6, 'payments': 2, 'amount': 180})
        self.assertEqual(payments['counted'], 3)
        self.assertEqual(payments['amount'], 380)
        self.assertEqual(payments['linked'], 4)
        self.assertEqual(payments['linked_amount'], 1900)
        self.assertEqual(payments['cancellations'], 1)
        self.assertEqual(len(payments['cancelled_originals']), 1)
        self.assertEqual(report['contracts']['amount'], 1000)
        self.assertEqual(report['total']['amount'], 1380)
        self.assertNotIn('TEST7-ABC', exported)
        for identifier in (4, 5, 6, 8):
            with self.subTest(linked_payment=identifier):
                self.assertEqual(exported[f'TEST{identifier}-ABC']['counted_in_total'], 'no')
        self.assertEqual(exported['TEST5-ABC']['linked_contract_years'], '2025')
        self.assertEqual(exported['TEST6-ABC']['linked_contract_years'], '2025 | 2026')
        self.assertEqual(exported['ORIGINAL-ABC']['excluded'], 'cancelled_original')
        self.assertEqual(exported['TEST10-ABC']['excluded'], 'cancellation')


class ReportPayrollFilterTest(unittest.TestCase):
    def test_requested_groups_and_normalization(self):
        for salary in ('Μισθοδοσίας', 'ΥΠΕΡΩΡΙΕΣ', 'Εισφορών', 'ΑΠΟΔΟΧΕΣ', 'αποδοχων'):
            for context in ('πυροφυλάκων', 'ΔΑΣΟΠΡΟΣΤΑΣΙΑΣ', 'πυρασφάλειας',
                            'αντιπυρικής περιόδου', 'Πολιτικής Προστασίας', 'εκτάκτων αναγκών'):
                with self.subTest(salary=salary, context=context):
                    self.assertIsNone(builder.payroll_report_exclusion(salary, 'payroll', context))
            self.assertEqual(builder.payroll_report_exclusion(salary, 'payroll'),
                             'payroll_missing_fire_context')
        self.assertIsNone(builder.payroll_report_exclusion(
            'ΧΕΠ_ΜΙΣΘΟΔΟΣΙΑ προσωπικού ΠΥΡΟΠΡΟΣΤΑΣΙΑΣ', 'payroll'))
        self.assertEqual(builder.payroll_report_exclusion(
            'Μισθοδοσία πολιτικής', 'payroll', 'προστασίας'), 'payroll_missing_fire_context')
        self.assertEqual(builder.payroll_report_exclusion(
            'Μισθοδοσία', 'payroll', 'πολιτικής και προστασίας'), 'payroll_missing_fire_context')
        self.assertEqual(builder.payroll_report_exclusion(
            'Πληρωμή προσωπικού ΙΔΟΧ πυροπροστασίας', 'payroll'), 'payroll_missing_keywords')

    def test_acceptance_and_other_nonpayroll_remain(self):
        for subject in ('Αποδοχή χρηματοδότησης', 'ΑΠΟΔΟΧΗΣ ΔΩΡΕΑΣ',
                        'Αποδοχή χρηματοδότησης πυροπροστασίας'):
            with self.subTest(subject=subject):
                category = builder.categorize(subject)
                self.assertNotEqual(category, 'payroll')
                self.assertIsNone(builder.payroll_report_exclusion(subject, category))
        self.assertIsNone(builder.payroll_report_exclusion('Καθαρισμός δασικού δρόμου', 'vegetation'))
        fee = 'Ετήσια χρηματική εισφορά στο δίκτυο πολιτικής προστασίας'
        self.assertEqual(builder.categorize(fee), 'other')
        self.assertIsNone(builder.payroll_report_exclusion(fee, 'other'))

    def test_exclusions_apply_to_every_report_total_and_keep_audit_rows(self):
        snapshot = datetime(2026, 10, 23, tzinfo=timezone.utc)
        data = {key: [] for key in builder.SQL}
        data['_meta'] = [{'db_now': snapshot}]
        data['procurement'] = [contract(1, 2026)]
        data['payments'] = [
            payment(1, 100, subject='Μισθοδοσία προσωπικού πυροπροστασίας'),
            payment(2, 200, subject='Μισθοδοσία διοικητικού προσωπικού'),
            payment(3, 300, subject='Πληρωμή προσωπικού ΙΔΟΧ πυροπροστασίας'),
            payment(4, 400, subject='Καταβολή αποδοχών πυροφυλάκων'),
            payment(5, 3500, subject='Ετήσια χρηματική εισφορά στο δίκτυο πολιτικής προστασίας'),
            payment(6, 124, subject='Αποδοχή δωρεάς για πυροπροστασία'),
            payment(7, 124, subject='Προμήθεια εξοπλισμού πυροπροστασίας'),
            payment(8, 50, subject='ΧΕΠ_ΜΙΣΘΟΔΟΣΙΑ προσωπικού ΙΔΟΧ ΠΥΡΟΠΡΟΣΤΑΣΙΑΣ'),
            payment(9, 80, subject='Μισθοδοσία διοίκησης', ada='ORIGINAL-ABC'),
            payment(10, 80, subject='Ακύρωση πληρωμής ORIGINAL-ABC'),
            payment(11, 100, subject='Μισθοδοσία προσωπικού'),
        ]
        # Issuer names cannot provide the missing context.
        data['payments'][1]['org_name_clean'] = 'ΤΜΗΜΑ ΠΟΛΙΤΙΚΗΣ ΠΡΟΣΤΑΣΙΑΣ'
        data['bridge'] = [{'diavgeia_id': 11, 'procurement_id': 1}]
        with TemporaryDirectory() as directory, patch.object(builder, 'load_all', return_value=data):
            root = Path(directory)
            report = builder.build(snapshot, root)
            builder.check(report)
            with (root / builder.REPORT_DIR / 'payments_2026.csv').open(encoding='utf-8', newline='') as handle:
                exported = {row['ada']: row for row in csv.DictReader(handle)}

        summary = report['payments']
        payroll = next(row for row in summary['by_category'] if row['category'] == 'payroll')
        self.assertEqual((payroll['payments'], payroll['amount']), (3, 550))
        self.assertEqual(summary['records'], 11)
        self.assertEqual((summary['valid'], summary['counted'], summary['amount']), (6, 6, 4250))
        self.assertEqual(summary['report_excluded_payroll'], {'payments': 3, 'amount': 600})
        self.assertEqual(summary['municipal']['payments'], 6)
        self.assertEqual(summary['municipal']['amount'], 4250)
        self.assertEqual(summary['municipal']['payroll'], 3)
        self.assertEqual(summary['payroll_by_month'][5], {'month': 6, 'payments': 3})
        self.assertEqual(report['municipalities']['payments_amount'], 4250)
        self.assertEqual(report['facts']['payroll_org_types'], {'ΔΗΜΟΣ': 3})
        self.assertEqual(report['total']['amount'], 5250)
        self.assertEqual(summary['report_year_contracts_with_linked_payment'], 0)
        self.assertEqual(len(exported), 11)
        self.assertEqual(exported['TEST2-ABC']['excluded'], 'payroll_missing_fire_context')
        self.assertEqual(exported['TEST3-ABC']['excluded'], 'payroll_missing_keywords')
        self.assertEqual(exported['TEST11-ABC']['excluded'], 'payroll_missing_fire_context')
        self.assertEqual(exported['ORIGINAL-ABC']['excluded'], 'cancelled_original')
        self.assertEqual(exported['TEST10-ABC']['excluded'], 'cancellation')
        self.assertEqual(exported['TEST5-ABC']['category'], 'other')
        for identifier in (2, 3, 9, 10, 11):
            ada = 'ORIGINAL-ABC' if identifier == 9 else f'TEST{identifier}-ABC'
            self.assertEqual(exported[ada]['counted_in_total'], 'no')


if __name__ == '__main__':
    unittest.main()
