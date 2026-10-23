#!/usr/bin/env python3
"""Ετήσια αναφορά 2026: οι δαπάνες για την πυροπροστασία.

Read-only builder for the /analysis/yearly_report page. The workflow
.github/workflows/yearly-report-refresh.yml runs it every day until the end of 2026.

* DATABASE_URL comes from the environment or the repository .env and is never printed.
* All queries run in one REPEATABLE READ, READ ONLY transaction that is rolled back:
  the builder never writes to the database.
* Snapshot: the moment of the run, or --snapshot. Rows created later are ignored.
  The period runs from 1/1/2026 to the snapshot date, capped at 31/12/2026.
* Amounts are without VAT. Diavgeia payment amounts include VAT and are divided by 1.24,
  except payroll, contributions, withholdings, grants and fees, which carry no VAT.
* Total without double counting = 2026 contracts + 2026 payments that are not linked to
  any contract of the dataset.
* The extra payroll keyword/context filter applies only to this report. It never
  changes collection, import rules or database records.

Outputs (overwritten on each run):
  data/reports/yearly-report-2026/report_data.json        figures, rules and evidence lists
  data/reports/yearly-report-2026/contracts_2026.csv      every 2026 contract
  data/reports/yearly-report-2026/payments_2026.csv       every 2026 payment finalization with its status
  data/reports/yearly-report-2026/municipalities_2026.csv funding vs. recorded activity
  app/src/features/analysis/yearly-report/data/yearlyReport2026.generated.json

Run from the repository root:
  .fireprotection/bin/python scripts/reports/yearly-report-2026/build_yearly_report.py
"""
from __future__ import annotations

import argparse
import ast
import csv
import json
import os
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import ROUND_CEILING, ROUND_HALF_UP, Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras

ROOT = Path(__file__).resolve().parents[3]

REPORT_DIR = Path('data/reports/yearly-report-2026')
APP_JSON = Path('app/src/features/analysis/yearly-report/data/yearlyReport2026.generated.json')

REPORT_YEAR = 2026
FIRST_YEAR = 2024
PERIOD_START = date(REPORT_YEAR, 1, 1)
YEAR_LAST_DAY = date(REPORT_YEAR, 12, 31)
TZ = ZoneInfo('Europe/Athens')
PAYMENT_TYPE = 'ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ'
DIRECT_AWARD_LIMIT = Decimal('30000')
DIRECT_AWARD_BIN = Decimal('5000')
DIRECT_AWARD_BINS_TOP = Decimal('60000')  # 5.000 € bins up to here, then one open bin
VAT_RATE = Decimal('0.24')
CENT = Decimal('0.01')

# Reviewed exclusions apply after version deduplication, so older versions cannot
# reappear in the report when a terminal contract is outside its scope.
# ADMIE DEA 42569: online condition monitoring of transformers/reactors in
# substations and extra-high-voltage centres; no documented forest-fire object.
# https://www.admie.gr/anakoinoseis/promitheies/dea-42569-meleti-promitheia-egkatastasi-parametropoiisi-kai-syntirisi
EXCLUDED_CONTRACT_REFERENCES = {
    '26SYMV019427117': 'OLMS σε αυτομετασχηματιστές/αυτεπαγωγές σε ΚΥΤ: δεν τεκμηριώνεται αντικείμενο δασικής πυροπροστασίας.',
    '26SYMV019431551': 'OLMS σε αυτεπαγωγές σε υποσταθμούς: δεν τεκμηριώνεται αντικείμενο δασικής πυροπροστασίας.',
}


# ── Database ───────────────────────────────────────────────────────────

def database_url() -> str:
    url = os.environ.get('DATABASE_URL', '').strip()
    if url:
        return url
    env = ROOT / '.env'
    if env.exists():
        for line in env.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if line.startswith('DATABASE_URL='):
                return line.split('=', 1)[1].strip().strip('"').strip("'")
    raise SystemExit('DATABASE_URL is missing from the environment and from .env')


SQL = {
    'procurement': """
        SELECT p.id, p.reference_number, p.title, p.short_descriptions, p.contract_signed_date,
               p.start_date, p.end_date, p.no_end_date, p.cancelled, p.next_ref_no, p.prev_reference_no,
               p.diavgeia_ada, p.contract_number, p.contract_type, p.procedure_type_value,
               p.organization_key, p.municipality_key, p.region_key, p.canonical_owner_scope, p.prevention,
               fp.amount_without_vat, fp.amount_with_vat, fp.beneficiary_vat_number, fp.beneficiary_name
        FROM public.procurement p
        LEFT JOIN LATERAL (
            SELECT py.amount_without_vat, py.amount_with_vat, py.beneficiary_vat_number, py.beneficiary_name
            FROM public.payment py
            WHERE py.procurement_id = p.id AND py.created_at <= %(snap)s
            ORDER BY py.id LIMIT 1
        ) fp ON TRUE
        WHERE p.created_at <= %(snap)s
    """,
    'organization': """
        SELECT DISTINCT ON (organization_key) organization_key, organization_normalized_value, organization_value, authority_scope
        FROM public.organization ORDER BY organization_key, id
    """,
    'municipality': """
        SELECT DISTINCT ON (municipality_key) municipality_key, municipality_normalized_value, municipality_value
        FROM public.municipality ORDER BY municipality_key, id
    """,
    'region': """
        SELECT DISTINCT ON (region_key) region_key, region_normalized_value, region_value
        FROM public.region ORDER BY region_key, id
    """,
    'cpv': """
        SELECT procurement_id, cpv_key, cpv_value FROM public.cpv WHERE created_at <= %(snap)s
    """,
    'payments': """
        SELECT d.id, d.ada, (d.publish_timestamp AT TIME ZONE %(tz)s)::date AS publish_date, d.subject,
               d.org_type, d.org_name_clean, d.organization_key, d.municipality_key, d.region_key,
               d.spending_contractors_value AS amount_text, d.prevention, d.document_url
        FROM public.diavgeia d
        WHERE d.diavgeia_document_type_decision_uid = %(ptype)s AND d.created_at <= %(snap)s
    """,
    'bridge': """
        SELECT dp.diavgeia_id, dp.procurement_id FROM public.diavgeia_procurement dp WHERE dp.created_at <= %(snap)s
    """,
    'payment_beneficiaries': """
        SELECT db.diavgeia_id, db.beneficiary_vat_number
        FROM public.diavgeia_beneficiary db JOIN public.diavgeia d ON d.id = db.diavgeia_id
        WHERE d.diavgeia_document_type_decision_uid = %(ptype)s AND db.created_at <= %(snap)s
    """,
    'diavgeia_years': """
        SELECT d.diavgeia_document_type_decision_uid AS decision_type,
               EXTRACT(YEAR FROM d.publish_timestamp AT TIME ZONE %(tz)s)::int AS year, COUNT(*) AS n
        FROM public.diavgeia d WHERE d.created_at <= %(snap)s GROUP BY 1, 2
    """,
    'fund': """
        SELECT year, allocation_type, recipient_type, recipient_raw, nomos, municipality_key, organization_key, amount_eur, source_ada
        FROM public.fund WHERE created_at <= %(snap)s
    """,
    'municipality_canonical': """
        SELECT municipality_key, municipality_normalized_value FROM public.municipality_normalized_name
    """,
    'copernicus': """
        SELECT copernicus_id, (firedate AT TIME ZONE %(tz)s)::date AS fire_date, area_ha
        FROM public.copernicus WHERE created_at <= %(snap)s AND firedate IS NOT NULL
    """,
    'copernicus_municipality': """
        SELECT cm.copernicus_id, cm.municipality_key, cm.overlap_area_ha
        FROM public.copernicus_municipality cm WHERE cm.created_at <= %(snap)s
    """,
}


def load_all(snapshot: datetime) -> dict[str, list[dict]]:
    params = {'snap': snapshot.isoformat(), 'tz': 'Europe/Athens', 'ptype': PAYMENT_TYPE}
    conn = psycopg2.connect(database_url())
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            cur.execute("SET LOCAL statement_timeout = '300s'")
            cur.execute("SELECT current_setting('transaction_read_only') AS ro, now() AS db_now")
            meta = cur.fetchone()
            if meta['ro'] != 'on':
                raise SystemExit('Refusing to continue: transaction is not read-only')
            data = {'_meta': [dict(meta)]}
            for name, sql in SQL.items():
                cur.execute(sql, params)
                data[name] = [dict(row) for row in cur.fetchall()]
        conn.rollback()
    finally:
        conn.close()
    return data


# ── Text helpers ───────────────────────────────────────────────────────

def norm(text: str | None) -> str:
    text = unicodedata.normalize('NFD', text or '')
    text = ''.join(ch for ch in text if unicodedata.category(ch) != 'Mn').casefold().replace('ς', 'σ')
    text = re.sub(r'[^\w\s]', ' ', text)
    return ' '.join(text.split())


def find(text: str, roots) -> str | None:
    """First root that occurs at a word start of the normalized text."""
    for root in roots:
        if re.search(r'(?<!\w)' + root, text):
            return root
    return None


def has(text: str, *roots: str) -> bool:
    return find(text, roots) is not None


def btrim(value) -> str | None:
    if value is None:
        return None
    value = str(value).strip(' ')
    return value or None


def money(value) -> float:
    return float(round(Decimal(value or 0), 2))


def pct(part, whole) -> float | None:
    whole = Decimal(whole or 0)
    if whole == 0:
        return None
    return float(round(Decimal(part or 0) * 100 / whole, 2))


def total(items, key='amt') -> Decimal:
    return sum((item[key] or Decimal(0) for item in items), Decimal(0))


# ── Amount parsing (Diavgeia payment finalizations) ────────────────────

GREEK_AMOUNT = re.compile(r'^-?\d{1,3}(\.\d{3})*(,\d+)?$|^-?\d+(,\d+)?$')


def parse_amount(text: str | None, ada: str) -> tuple[Decimal | None, int]:
    """Return (sum of beneficiary amounts, number of lines).

    The DB keeps the raw text. Greek format uses '.' for thousands and ',' for
    decimals; lists hold one amount per beneficiary. Amounts that came from the
    reviewed PDF fallback use a dot decimal (e.g. '11169.84'). A dot followed by
    one or two digits cannot be Greek thousands grouping (always three digits), so
    such tokens are read as dot decimals; anything else raises an error.
    """
    if text is None or not str(text).strip():
        return None, 0
    raw = str(text).strip()
    tokens = ast.literal_eval(raw) if raw.startswith('[') else [raw]
    amount = Decimal(0)
    for token in tokens:
        token = str(token).strip()
        if re.fullmatch(r'-?\d+\.\d{1,2}', token):
            amount += Decimal(token)
        elif GREEK_AMOUNT.match(token):
            amount += Decimal(token.replace('.', '').replace(',', '.'))
        else:
            raise ValueError(f'Unparsed amount {token!r} for {ada}')
    return amount, len(tokens)


# ── Payment subject categories (transparent keyword rules) ─────────────

CATEGORY_LABELS = {
    'cancellation': 'Ακυρωτικές πράξεις',
    'payroll': 'Μισθοδοσία προσωπικού',
    'fuel': 'Καύσιμα και λιπαντικά',
    'rrf_ypen': 'Έργα του Ταμείου Ανάκαμψης (ΥΠΕΝ)',
    'vegetation': 'Αντιπυρικές ζώνες, καθαρισμοί, κλαδέματα',
    'machinery': 'Μισθώσεις μηχανημάτων και οχημάτων',
    'equipment': 'Εξοπλισμός, οχήματα και συντήρηση μέσων',
    'unclear': 'Το θέμα δεν περιγράφει τη δαπάνη',
    'other': 'Λοιπές δαπάνες πυροπροστασίας',
}
CATEGORY_RULES = {
    'cancellation': 'Το θέμα περιέχει «ακύρωση/ακυρωτικό» (όχι «σε αντικατάσταση του ακυρωμένου») ή το ποσό είναι αρνητικό (αντιλογισμός).',
    'payroll': 'Μισθοδοσία, υπερωρίες ή εισφορές, ή οι λέξεις «αποδοχές/αποδοχών», μαζί με αναφορά σε πυροπροστασία, δασοπροστασία, πυρασφάλεια, αντιπυρική περίοδο, πολιτική προστασία ή έκτακτες ανάγκες.',
    'fuel': 'Καύσιμα, πετρέλαιο, βενζίνη, diesel και biodiesel, λιπαντικά. Όχι η «καύσιμη ύλη» του δάσους.',
    'rrf_ypen': 'Εντολές πληρωμής της ΣΑ ΤΑ075 «Έργα χρηματοδοτούμενα από το Ταμείο Ανάκαμψης».',
    'vegetation': 'Αντιπυρικές/περιμετρικές ζώνες, αποψιλώσεις, καθαρισμοί, κοπή χόρτων, κλαδέματα, καύσιμη ύλη, δασικοί/αγροτικοί δρόμοι, οικόπεδα.',
    'machinery': 'Μίσθωση ή ενοικίαση μηχανημάτων, οχημάτων, φορτηγών, υδροφόρων (και «μισθ.» ακολουθούμενο από μηχανήματα ή οχήματα).',
    'equipment': 'Προμήθειες, εξοπλισμός, ανταλλακτικά, επισκευές, συντήρηση μέσων, δεξαμενές.',
    'unclear': 'Το θέμα περιέχει μόνο αριθμό εντάλματος/εντολής χωρίς περιγραφή.',
    'other': 'Όλα τα υπόλοιπα.',
}
FILLER = re.compile(r'[0-9a-zα-ω]{0,2}|\d[\d ]*|χεπ|χε|ενταλμα\w*|πληρωμ\w*|εντολ\w*|χρηματικ\w*|σειρα|εκδοση|αλε|καταστασ\w*|'
                    r'οριστικοποιησ\w*|τησ|του|με|αριθμο|αριθ|για|και|στο|την|των|μηνοσ|ετουσ|δαπανη|παραστατικου')
PAYROLL_TERMS = ('μισθοδοσ', 'μισθοδοτ', r'αποδοχεσ\b', r'αποδοχων\b', 'υπερωρ', r'υπερ\w* απασχολ', 'ιδοχ', 'ι δ ο χ', 'ιδαχ', 'ανθυγιειν',
                 'εργοδοτ', 'ημερομισθ', 'δεδουλευμ', 'αμοιβεσ προσωπικ', 'πληρωμη προσωπικ', 'μισθ ')
REPORT_PAYROLL_ROOTS = ('μισθοδ', 'υπερω', 'εισφορ')
REPORT_PAYROLL_SALARY_WORDS = frozenset({'αποδοχεσ', 'αποδοχων'})
REPORT_PAYROLL_CONTEXT = ('πυρο', 'δασο', 'πυρασφ', 'αντιπυ', r'πολιτικ\w* προστασι', r'εκτακτ\w* αναγκ')
# Payments that carry no VAT, besides payroll.
NO_VAT_TERMS = (r'εισφορ', r'κρατησ', r'αποδοσ\w* (?:\w+ ){0,2}φορ', r'επιχορηγ', r'παραβολ', r'ανταποδοτικ\w* τελ', r'οδοιπορ',
                r'ημερησι\w* αποζημιωσ')


def payroll_report_exclusion(subject: str | None, category: str, description: str | None = None) -> str | None:
    """Apply the user's two required keyword groups only to the report's rows.

    The existing broader payroll category identifies legacy salary titles too;
    those also need both requested groups. The two groups may occur in different
    text fields, while a context phrase must remain within one field. Issuer
    names and PDF text are deliberately absent from these inputs.
    """
    if category == 'cancellation':
        return None
    texts = [' '.join(norm(text).replace('_', ' ').split()) for text in (subject, description)]
    salary_words = any(has(text, *REPORT_PAYROLL_ROOTS)
                       or bool(REPORT_PAYROLL_SALARY_WORDS.intersection(text.split())) for text in texts)
    if category != 'payroll' and not salary_words:
        return None
    if not salary_words:
        return 'payroll_missing_keywords'
    if not any(has(text, *REPORT_PAYROLL_CONTEXT) for text in texts):
        return 'payroll_missing_fire_context'
    return None


PAYMENT_CATEGORY_OVERRIDES = {
    'Ψ86Ε46Ψ844-ΓΥ4': 'other',  # Reviewed supporting services for forest-fire studies.
    '9ΥΡ77Λ7-ΥΚ9': 'vegetation',  # PDF line 59: clearing vegetation for fire prevention.
}


def categorize(subject: str | None, ada: str | None = None) -> str:
    s = norm(subject)
    if has(s, 'ακυρωσ', 'ακυρωτικ') and not has(s, 'αντικατασταση του ακυρωμεν'):
        return 'cancellation'
    if ada in PAYMENT_CATEGORY_OVERRIDES:
        return PAYMENT_CATEGORY_OVERRIDES[ada]
    machine_rental = has(s, r'μισθ\w* (?:\w+ )?(?:μηχαν|οχημ|φορτηγ|jcb|εκσκαφ)')
    if has(s, *PAYROLL_TERMS) and not machine_rental:
        return 'payroll'
    # «Καύσιμη ύλη» is the combustible vegetation of the forest, not fuel.
    if has(s, r'καυσιμ(?!\w* υλ)', 'πετρελαι', 'βενζιν', r'(?:bio)?diesel', 'λιπαντικ'):
        return 'fuel'
    if 'ταμειο ανακαμψησ' in s or has(s, 'τα075'):
        return 'rrf_ypen'
    if has(s, r'αντιπυρικ\w* ζων', 'περιμετρικ', 'ζωνων πυρασφαλ', 'αποψιλ', 'καθαρισ', 'κοπη', 'κοπησ', 'χορτ', 'κλαδ',
           'εκχορτ', 'αγριοχορτ', 'βλαστησ', 'αποκλαδ', 'θρυμματ', r'δασικ\w* δρομ', 'αγροτικ', 'οικοπεδ', 'υλοτομ',
           'ζωνεσ', r'καυσιμ\w* υλ'):
        return 'vegetation'
    if machine_rental or (has(s, 'μισθωσ', 'ενοικιασ') and has(s, 'μηχαν', 'οχημ', 'φορτηγ', 'jcb', 'εκσκαφ', 'ισοπεδ', 'φορτωτ', 'υδροφορ', 'βυτιοφορ')):
        return 'machinery'
    if has(s, 'προμηθ', 'εξοπλισμ', 'ανταλλακτ', 'επισκευ', 'συντηρησ', 'οχημ', 'δεξαμεν', 'αλυσοπριον', 'χορτοκοπτ', 'υδροφορ', 'μεταφορ'):
        return 'equipment'
    words = [w for w in s.split() if not FILLER.fullmatch(w)]
    return 'unclear' if len(words) <= 1 else 'other'


def carries_vat(subject: str | None, category: str) -> bool:
    if category == 'payroll':
        return False
    s = norm(subject)
    term = find(s, NO_VAT_TERMS)
    if term == r'επιχορηγ' and has(s, 'προμηθ'):
        return True  # a purchase booked against a grant still carries VAT
    return term is None


def without_vat(amount: Decimal | None, vat: bool) -> Decimal | None:
    if amount is None:
        return None
    if not vat:
        return amount
    return (amount / (1 + VAT_RATE)).quantize(CENT, rounding=ROUND_HALF_UP)


# ── Contracts (same business rules as sql/044_contract_analysis_rpc.sql) ──

ACCENT_UP = str.maketrans('ΆΈΉΊΪΌΎΫΏ', 'ΑΕΗΙΙΟΥΥΩ')


def contract_type_label(value: str | None) -> str:
    s = (value or '').upper().translate(ACCENT_UP)
    if 'ΥΠΗΡΕΣ' in s:
        return 'Υπηρεσίες'
    if 'ΠΡΟΜΗΘΕΙ' in s:
        return 'Προμήθειες'
    if 'ΕΡΓ' in s:
        return 'Έργα'
    return 'Λοιπές'


def procedure_label(value: str | None) -> str:
    s = (value or '').lower()
    if 'απευθείας ανάθεση' in s:
        return 'Απευθείας Ανάθεση'
    if 'ανοιχτή' in s:
        return 'Ανοιχτή Διαδικασία'
    if 'διαπραγ' in s:
        return 'Διαπραγμάτευση'
    return 'Άλλη'


def authority_label(row: dict, orgs: dict, munis: dict, regions: dict) -> str:
    o = orgs.get(row['organization_key'])
    m = munis.get(row['municipality_key'])
    r = regions.get(row['region_key'])
    o_name = (o['organization_normalized_value'] or o['organization_value']) if o else None
    m_name = btrim((m['municipality_normalized_value'] or m['municipality_value'])) if m else None
    r_name = btrim((r['region_normalized_value'] or r['region_value'])) if r else None
    scope = row['canonical_owner_scope']
    if scope == 'municipality':
        return (f'ΔΗΜΟΣ {m_name}' if m_name else None) or o_name or 'Δήμος —'
    if scope == 'region':
        return (f'ΠΕΡΙΦΕΡΕΙΑ {r_name}' if r_name else None) or (f'ΔΗΜΟΣ {m_name}' if m_name else None) or o_name or 'Περιφέρεια —'
    if o and o['authority_scope'] == 'municipality' and m_name:
        return f'ΔΗΜΟΣ {m_name}'
    if o and o['authority_scope'] in ('region', 'decentralized') and r_name:
        return f'ΠΕΡΙΦΕΡΕΙΑ {r_name}'
    return o_name or (f'ΔΗΜΟΣ {m_name}' if m_name else None) or (f'ΠΕΡΙΦΕΡΕΙΑ {r_name}' if r_name else None) or '—'


def authority_group(row: dict, orgs: dict) -> str:
    if row['canonical_owner_scope'] == 'municipality':
        return 'Δήμοι'
    if row['canonical_owner_scope'] == 'region':
        return 'Περιφέρειες'
    o = orgs.get(row['organization_key'])
    name = (o['organization_normalized_value'] if o else '') or ''
    if name.startswith('ΥΠΟΥΡΓΕΙΟ'):
        return 'Υπουργεία'
    return 'Λοιποί φορείς'


def dedupe_contracts(rows: list[dict]) -> list[dict]:
    """Active, terminal contracts, one per business key (highest id), as in the RPC."""
    successors = {btrim(r['prev_reference_no']) for r in rows if btrim(r['prev_reference_no'])}
    kept = {}
    for r in rows:
        if r['cancelled']:
            continue
        if btrim(r['next_ref_no']):
            continue
        if r['reference_number'] is not None and r['reference_number'] in successors:
            continue
        key = (btrim(r['reference_number']) or btrim(r['diavgeia_ada']) or btrim(r['contract_number'])
               or '|'.join([r['organization_key'] or '', r['title'] or '', r['contract_signed_date'].isoformat() if r['contract_signed_date'] else '']))
        if key not in kept or r['id'] > kept[key]['id']:
            kept[key] = r
    return list(kept.values())


TYPE_ORDER = ['Υπηρεσίες', 'Προμήθειες', 'Έργα', 'Λοιπές']
PROCEDURE_ORDER = ['Απευθείας Ανάθεση', 'Ανοιχτή Διαδικασία', 'Διαπραγμάτευση', 'Άλλη']
GROUPS = ['Υπουργεία', 'Δήμοι', 'Περιφέρειες', 'Λοιποί φορείς']
PREVENTION_LABELS = {1: 'Πρόληψη', 0: 'Καταστολή', 2: 'Χωρίς σαφή κατάταξη', None: 'Χωρίς ταξινόμηση ακόμη'}


def direct_award_bin(amount: Decimal) -> int:
    """5.000 € bins that include their upper bound («έως»): 25.000–30.000 € is 25.000 < x ≤ 30.000."""
    if amount > DIRECT_AWARD_BINS_TOP:
        return int(DIRECT_AWARD_BINS_TOP // DIRECT_AWARD_BIN)
    if amount <= 0:
        return 0
    return int((amount / DIRECT_AWARD_BIN).to_integral_value(rounding=ROUND_CEILING)) - 1


def direct_award_stats(rows: list[dict]) -> dict:
    direct = sorted(r['amt'] for r in rows if r['procedure'] == 'Απευθείας Ανάθεση')
    median = None
    if direct:
        mid = len(direct) // 2
        median = direct[mid] if len(direct) % 2 else (direct[mid - 1] + direct[mid]) / 2
    counts = Counter(direct_award_bin(a) for a in direct)
    top = int(DIRECT_AWARD_BINS_TOP // DIRECT_AWARD_BIN)
    bins = [{'lo': int(i * DIRECT_AWARD_BIN), 'hi': int((i + 1) * DIRECT_AWARD_BIN) if i < top else None, 'count': counts.get(i, 0)}
            for i in range(top + 1)]
    return {
        'count': len(direct),
        'amount': money(sum(direct, Decimal(0))),
        'median': money(median) if median is not None else None,
        'up_to_30k': sum(1 for a in direct if a <= DIRECT_AWARD_LIMIT),
        'from_25k_to_30k': sum(1 for a in direct if Decimal('25000') < a <= DIRECT_AWARD_LIMIT),
        'from_20k_to_25k': sum(1 for a in direct if Decimal('20000') < a <= Decimal('25000')),
        'above_30k': sum(1 for a in direct if a > DIRECT_AWARD_LIMIT),
        'bins': bins,
    }


def contract_stats(rows: list[dict]) -> dict:
    amount = total(rows)

    def bars(field: str, order: list[str]) -> list[dict]:
        acc = defaultdict(lambda: [0, Decimal(0)])
        for r in rows:
            acc[r[field]][0] += 1
            acc[r[field]][1] += r['amt']
        return [{'label': k, 'count': acc[k][0], 'amount': money(acc[k][1]),
                 'share_amount_pct': pct(acc[k][1], amount), 'share_count_pct': pct(acc[k][0], len(rows))} for k in order]

    authorities = defaultdict(lambda: [0, Decimal(0)])
    cpvs = Counter()
    for r in rows:
        authorities[r['authority_label']][0] += 1
        authorities[r['authority_label']][1] += r['amt']
        cpvs.update(r['cpv_labels'])
    top_authorities = sorted(authorities.items(), key=lambda kv: (-kv[1][1], -kv[1][0], kv[0]))[:8]
    top_cpvs = sorted(cpvs.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
    groups = {g: [0, Decimal(0)] for g in GROUPS}
    for r in rows:
        groups[r['authority_group']][0] += 1
        groups[r['authority_group']][1] += r['amt']
    return {
        'contracts': len(rows),
        'amount': money(amount),
        'contract_types': bars('type_label', TYPE_ORDER),
        'procedures': bars('procedure', PROCEDURE_ORDER),
        'top_authorities': [{'authority': k, 'contracts': v[0], 'amount': money(v[1]), 'share_pct': pct(v[1], amount)}
                            for k, v in top_authorities],
        'top_cpvs': [{'cpv': k, 'contracts': n, 'share_contracts_pct': pct(n, len(rows))} for k, n in top_cpvs],
        'by_group': [{'group': g, 'contracts': v[0], 'amount': money(v[1])} for g, v in groups.items()],
        'direct_awards': direct_award_stats(rows),
    }


def same_day(year: int, month: int, day: int) -> date:
    try:
        return date(year, month, day)
    except ValueError:  # 29 February in a year that is not a leap year
        return date(year, month, day - 1)


# ── Main analysis ──────────────────────────────────────────────────────

def build(snapshot: datetime, out_root: Path) -> dict:
    data = load_all(snapshot)
    snapshot_day = snapshot.astimezone(TZ).date()
    period_end = min(snapshot_day, YEAR_LAST_DAY)
    orgs = {r['organization_key']: r for r in data['organization']}
    munis = {r['municipality_key']: r for r in data['municipality']}
    regions = {r['region_key']: r for r in data['region']}
    canonical_munis = {r['municipality_key']: r['municipality_normalized_value'] for r in data['municipality_canonical']}

    cpv_by_proc = defaultdict(set)
    for c in data['cpv']:
        label = btrim(c['cpv_value']) or btrim(c['cpv_key'])
        if label:
            cpv_by_proc[c['procurement_id']].add(label)

    # ── Contracts ──
    all_proc = data['procurement']
    by_id = {r['id']: r for r in all_proc}
    by_ref = {r['reference_number']: r for r in all_proc if r['reference_number']}
    deduplicated = dedupe_contracts(all_proc)
    deduplicated_ids = {r['id'] for r in deduplicated}
    scope_excluded = [r for r in deduplicated if btrim(r['reference_number']) in EXCLUDED_CONTRACT_REFERENCES]
    counted = [r for r in deduplicated if btrim(r['reference_number']) not in EXCLUDED_CONTRACT_REFERENCES]
    for r in counted:
        org = orgs.get(r['organization_key'])
        r['org_name'] = (org['organization_normalized_value'] or org['organization_value']) if org else ''
        r['amt'] = Decimal(r['amount_without_vat'] or 0)
        r['amt_w'] = Decimal(r['amount_with_vat']) if r['amount_with_vat'] is not None else None
        r['type_label'] = contract_type_label(r['contract_type'])
        r['procedure'] = procedure_label(r['procedure_type_value'])
        r['authority_label'] = authority_label(r, orgs, munis, regions)
        r['authority_group'] = authority_group(r, orgs)
        r['cpv_labels'] = sorted(cpv_by_proc.get(r['id'], ()))
    counted_ids = {r['id'] for r in counted}
    signed_contracts = [r for r in counted if r['contract_signed_date']]

    def signed_between(rows, start, end):
        return [r for r in rows if start <= r['contract_signed_date'] <= end]

    c26 = signed_between(signed_contracts, PERIOD_START, period_end)
    if not c26:
        raise SystemExit('No 2026 contracts in the period; refusing to publish an empty report')
    latest_contract = max(r['contract_signed_date'] for r in c26)
    stats = contract_stats(c26)
    full_years = {str(y): contract_stats(signed_between(signed_contracts, date(y, 1, 1), date(y, 12, 31)))
                  for y in range(FIRST_YEAR, REPORT_YEAR)}

    # Same calendar window in every year: 1 January to the last day of the period.
    ytd_rows, ytd_sets = [], {}
    for y in range(FIRST_YEAR, REPORT_YEAR + 1):
        sel = signed_between(signed_contracts, date(y, 1, 1), same_day(y, period_end.month, period_end.day))
        ytd_sets[y] = sel
        prevention = defaultdict(lambda: [0, Decimal(0)])
        for r in sel:
            prevention[r['prevention']][0] += 1
            prevention[r['prevention']][1] += r['amt']
        ytd_rows.append({
            'year': y,
            'contracts': len(sel),
            'amount': money(total(sel)),
            'by_group': contract_stats(sel)['by_group'] if sel else [{'group': g, 'contracts': 0, 'amount': 0.0} for g in GROUPS],
            'by_prevention': [{'class': PREVENTION_LABELS[k], 'code': k, 'contracts': v[0], 'amount': money(v[1])}
                              for k, v in sorted(prevention.items(), key=lambda kv: (kv[0] is None, kv[0] if kv[0] is not None else 9))],
        })

    def by_authority(rows):
        acc = defaultdict(lambda: [0, Decimal(0)])
        for r in rows:
            acc[r['authority_label']][0] += 1
            acc[r['authority_label']][1] += r['amt']
        return acc

    prev_auth, cur_auth = by_authority(ytd_sets[REPORT_YEAR - 1]), by_authority(ytd_sets[REPORT_YEAR])
    changes = []
    for name in set(prev_auth) | set(cur_auth):
        before, after = prev_auth.get(name, [0, Decimal(0)]), cur_auth.get(name, [0, Decimal(0)])
        changes.append({'authority': name, 'contracts_previous': before[0], 'contracts_current': after[0],
                        'amount_previous': money(before[1]), 'amount_current': money(after[1]),
                        'change': money(after[1] - before[1])})
    changes.sort(key=lambda x: (x['change'], x['authority']))
    authority_changes = {'largest_decreases': [c for c in changes if c['change'] < 0][:5],
                         'largest_increases': [c for c in changes[::-1] if c['change'] > 0][:5]}

    # Forest-fire classification of the 2026 contracts (stored by the classifier).
    relevance = defaultdict(lambda: [0, Decimal(0)])
    for r in c26:
        relevance[r['prevention']][0] += 1
        relevance[r['prevention']][1] += r['amt']
    relevance_rows = [{'class': PREVENTION_LABELS[k], 'code': k, 'contracts': v[0], 'amount': money(v[1]),
                       'share_amount_pct': pct(v[1], total(c26))}
                      for k, v in sorted(relevance.items(), key=lambda kv: (kv[0] is None, kv[0] if kv[0] is not None else 9))]

    # Timing of the contracts relative to 1 May, the start of the fire season.
    timing = []
    for y in range(FIRST_YEAR, REPORT_YEAR + 1):
        year_rows = c26 if y == REPORT_YEAR else signed_between(signed_contracts, date(y, 1, 1), date(y, 12, 31))
        for scope_name, flt in (
            ('all', lambda r: True),
            ('municipal', lambda r: r['canonical_owner_scope'] == 'municipality'),
            ('regional', lambda r: r['canonical_owner_scope'] == 'region'),
            ('environment_ministry', lambda r: r['org_name'] == 'ΥΠΟΥΡΓΕΙΟ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΕΝΕΡΓΕΙΑΣ'),
            ('civil_protection_ministry', lambda r: r['org_name'] == 'ΥΠΟΥΡΓΕΙΟ ΚΛΙΜΑΤΙΚΗΣ ΚΡΙΣΗΣ ΚΑΙ ΠΟΛΙΤΙΚΗΣ ΠΡΟΣΤΑΣΙΑΣ'),
        ):
            rows_y = [r for r in year_rows if flt(r)]
            by_month = Counter(r['contract_signed_date'].month for r in rows_y)
            before = [r for r in rows_y if r['contract_signed_date'] < date(y, 5, 1)]
            may_aug = [r for r in rows_y if date(y, 5, 1) <= r['contract_signed_date'] <= date(y, 8, 31)]
            timing.append({
                'year': y, 'scope': scope_name, 'contracts': len(rows_y), 'amount': money(total(rows_y)),
                'before_may_contracts': len(before), 'before_may_amount': money(total(before)),
                'may_to_august_contracts': len(may_aug),
                'by_month': [{'month': m, 'contracts': by_month.get(m, 0)} for m in range(1, 13)],
            })

    # Contractors, grouped by VAT number (a contract without one counts on its own).
    contractor = defaultdict(lambda: {'contracts': 0, 'amount': Decimal(0), 'names': Counter(),
                                      'authorities': defaultdict(lambda: [0, Decimal(0)])})
    for r in c26:
        e = contractor[btrim(r['beneficiary_vat_number']) or f'?{r["id"]}']
        e['contracts'] += 1
        e['amount'] += r['amt']
        e['names'][' '.join((r['beneficiary_name'] or '—').split())] += 1
        e['authorities'][r['authority_label']][0] += 1
        e['authorities'][r['authority_label']][1] += r['amt']
    top_contractors = sorted(contractor.values(), key=lambda e: (-e['amount'], -e['contracts']))[:10]
    contractors = {
        'distinct': len(contractor),
        'top10_share_pct': pct(sum((e['amount'] for e in top_contractors), Decimal(0)), total(c26)),
        # The most frequent spelling of the name under each VAT number; VAT numbers are not exported.
        'top10': [{'name': max(e['names'].items(), key=lambda kv: (kv[1], kv[0]))[0], 'contracts': e['contracts'],
                   'amount': money(e['amount']), 'share_pct': pct(e['amount'], total(c26)),
                   'authorities': [{'authority': k, 'contracts': v[0], 'amount': money(v[1])}
                                   for k, v in sorted(e['authorities'].items(), key=lambda kv: (-kv[1][1], kv[0]))]}
                  for e in top_contractors],
    }

    # ── Payments (Diavgeia payment finalizations) ──
    bridge = defaultdict(set)
    for b in data['bridge']:
        bridge[b['diavgeia_id']].add(b['procurement_id'])
    pay_ben = defaultdict(set)
    for b in data['payment_beneficiaries']:
        if btrim(b['beneficiary_vat_number']):
            pay_ben[b['diavgeia_id']].add(btrim(b['beneficiary_vat_number']))
    successors = defaultdict(list)
    for r in all_proc:
        prev = btrim(r['prev_reference_no'])
        if prev and prev in by_ref:
            successors[prev].append(r)

    def terminal(r, seen=None):
        seen = seen or set()
        if r['id'] in seen:
            return r
        seen.add(r['id'])
        nxt = sorted(successors.get(r['reference_number'], []), key=lambda x: (x['contract_signed_date'] or date.min, x['id']))
        if nxt:
            return terminal(nxt[-1], seen)
        ref = btrim(r['next_ref_no'])
        if ref and ref in by_ref:
            return terminal(by_ref[ref], seen)
        return r

    payments = data['payments']
    for p in payments:
        amount, lines = parse_amount(p['amount_text'], p['ada'])
        p['amount'] = amount
        p['lines'] = lines
        p['category'] = 'cancellation' if amount is not None and amount < 0 else categorize(p['subject'], p['ada'])
        p['payroll_report_exclusion'] = payroll_report_exclusion(p['subject'], p['category'], p.get('description'))
        p['vat'] = carries_vat(p['subject'], p['category'])
        p['amt'] = without_vat(amount, p['vat'])

    # Cancellations are excluded; so is the original payment they cancel, when present.
    cancelled_originals = {}
    payment_by_ada = {p['ada']: p for p in payments if p['ada']}
    ada_pattern = re.compile(r'[0-9Α-ΩA-Z]{4,}-[0-9Α-ΩA-Z]{3}')
    for c in payments:
        if c['category'] != 'cancellation':
            continue
        cited = [payment_by_ada[a] for a in ada_pattern.findall((c['subject'] or '').upper())
                 if a in payment_by_ada and a != c['ada'] and payment_by_ada[a]['category'] != 'cancellation']
        if cited:
            for original in cited:
                cancelled_originals.setdefault(original['ada'], c['ada'])
            continue
        if c['amount'] is None:
            continue
        candidates = [p for p in payments if p['category'] != 'cancellation' and p['org_name_clean'] == c['org_name_clean']
                      and p['amount'] is not None and abs(p['amount']) == abs(c['amount'])
                      and p['publish_date'] and c['publish_date'] and p['publish_date'] <= c['publish_date']
                      and p['ada'] not in cancelled_originals]
        if candidates:
            original = max(candidates, key=lambda p: p['publish_date'])
            cancelled_originals[original['ada']] = c['ada']

    # Only links recorded in the database count, as on the application's Diavgeia page.
    # Contract references in the subject are kept only to report the ones missing from the data.
    contract_ref_pattern = re.compile(r'\d{2}SYMV\d{9}')
    for p in payments:
        p['subject_refs'] = set(contract_ref_pattern.findall(p['subject'] or ''))
        p['explicit'] = {i for i in bridge.get(p['id'], ()) if i in by_id}

    p26 = [p for p in payments if p['publish_date'] and PERIOD_START <= p['publish_date'] <= period_end]
    latest_payment = max((p['publish_date'] for p in p26), default=None)
    for p in p26:
        p['excluded'] = ('cancellation' if p['category'] == 'cancellation'
                         else 'cancelled_original' if p['ada'] in cancelled_originals
                         else p['payroll_report_exclusion'])
        p['subject_refs_not_in_db'] = sorted(s for s in p['subject_refs'] if s not in by_ref)
        p['link_method'] = 'database' if p['explicit'] else None
        terminals = {terminal(by_id[i])['id'] for i in p['explicit']}
        p['linked_refs'] = sorted(by_id[t]['reference_number'] or str(t) for t in terminals)
        p['linked_years'] = sorted({by_id[t]['contract_signed_date'].year for t in terminals if by_id[t]['contract_signed_date']})
        p['linked_counted'] = [by_id[t] for t in terminals if t in counted_ids]
        p['valid'] = p['excluded'] is None
        p['counted'] = p['valid'] and p['link_method'] is None

    valid = [p for p in p26 if p['valid']]
    linked = [p for p in valid if not p['counted']]
    counted_payments = [p for p in valid if p['counted']]

    cat_rows = []
    for cat, label in CATEGORY_LABELS.items():
        if cat == 'cancellation':
            continue
        items = [p for p in counted_payments if p['category'] == cat]
        cat_rows.append({'category': cat, 'label': label, 'rule': CATEGORY_RULES[cat], 'payments': len(items),
                         'amount': money(total(items)), 'bodies': len({p['org_name_clean'] for p in items})})
    by_org_type = defaultdict(lambda: [0, Decimal(0)])
    for p in counted_payments:
        by_org_type[p['org_type'] or '—'][0] += 1
        by_org_type[p['org_type'] or '—'][1] += p['amt'] or 0
    payroll = [p for p in counted_payments if p['category'] == 'payroll']
    payroll_by_month = Counter(p['publish_date'].month for p in payroll)
    # Municipal payments without a linked contract, by month of publication (2026 only:
    # earlier years hold far fewer Diavgeia payments, so they are not compared).
    municipal_payments = [p for p in counted_payments if p['org_type'] == 'ΔΗΜΟΣ']
    municipal_by_month = defaultdict(lambda: [0, Decimal(0)])
    for p in municipal_payments:
        municipal_by_month[p['publish_date'].month][0] += 1
        municipal_by_month[p['publish_date'].month][1] += p['amt'] or 0

    # Why VAT is removed: payments linked to exactly one contract, against its value.
    vat_evidence = Counter()
    for p in valid:
        if p['link_method'] != 'database' or len(p['linked_counted']) != 1 or p['amount'] is None:
            continue
        r = p['linked_counted'][0]
        if r['amt_w'] is not None and abs(p['amount'] - r['amt_w']) <= CENT:
            vat_evidence['equals_with_vat'] += 1
        elif r['amount_without_vat'] is not None and abs(p['amount'] - Decimal(r['amount_without_vat'])) <= CENT:
            vat_evidence['equals_without_vat'] += 1
        else:
            vat_evidence['other'] += 1

    duplicates = defaultdict(list)
    for p in counted_payments:
        duplicates[(p['org_name_clean'], p['amount'], ' '.join((p['subject'] or '').split()).upper())].append(p)
    duplicate_groups = [v for v in duplicates.values() if len(v) > 1]

    paid_contracts = {terminal(by_id[i])['id'] for p in payments
                      if p['category'] != 'cancellation' and p['ada'] not in cancelled_originals
                      and p['payroll_report_exclusion'] is None for i in p['explicit']}
    c26_paid = [r for r in c26 if r['id'] in paid_contracts]

    # ── Total without double counting ──
    contracts_amount, payments_amount = total(c26), total(counted_payments)
    footprint = {
        'contracts': len(c26),
        'contracts_amount': money(contracts_amount),
        'payments': len(counted_payments),
        'payments_amount': money(payments_amount),
        'amount': money(contracts_amount + payments_amount),
    }

    # ── Municipalities: central funding vs. recorded activity ──
    fund_years = defaultdict(lambda: defaultdict(lambda: [0, Decimal(0), set()]))
    for f in data['fund']:
        e = fund_years[f['year']][(f['allocation_type'], f['recipient_type'])]
        e[0] += 1
        e[1] += Decimal(f['amount_eur'] or 0)
        e[2].add(f['source_ada'])
    funding = [{'year': y, 'allocation_type': k[0], 'recipient_type': k[1], 'rows': v[0], 'amount': money(v[1]), 'source_adas': sorted(v[2])}
               for y in sorted(fund_years) for k, v in sorted(fund_years[y].items()) if y >= 2019]
    fund_muni = defaultdict(Decimal)
    fund_nomos = defaultdict(set)
    for f in data['fund']:
        if f['year'] == REPORT_YEAR and f['recipient_type'] == 'δήμος' and f['municipality_key']:
            fund_muni[f['municipality_key']] += Decimal(f['amount_eur'] or 0)
            if f['nomos']:
                fund_nomos[f['municipality_key']].add(f['nomos'])
    muni_contracts = defaultdict(lambda: [0, Decimal(0)])
    for r in c26:
        if r['canonical_owner_scope'] == 'municipality' and r['municipality_key']:
            muni_contracts[r['municipality_key']][0] += 1
            muni_contracts[r['municipality_key']][1] += r['amt']
    muni_payments = defaultdict(lambda: [0, Decimal(0)])
    for p in valid:
        if p['org_type'] == 'ΔΗΜΟΣ' and p['municipality_key']:
            muni_payments[p['municipality_key']][0] += 1
            muni_payments[p['municipality_key']][1] += p['amt'] or 0
    muni_rows = []
    for key in sorted(set(fund_muni) | set(muni_contracts) | set(muni_payments)):
        muni_rows.append({'municipality_key': key, 'municipality': canonical_munis.get(key, key),
                          'nomos': ', '.join(sorted(fund_nomos.get(key, ()))),
                          'funding_2026': money(fund_muni.get(key, 0)),
                          'contracts': muni_contracts[key][0] if key in muni_contracts else 0,
                          'contracts_amount': money(muni_contracts[key][1]) if key in muni_contracts else 0.0,
                          'payments': muni_payments[key][0] if key in muni_payments else 0,
                          'payments_amount': money(muni_payments[key][1]) if key in muni_payments else 0.0})
    funded = [m for m in muni_rows if m['funding_2026'] > 0]
    no_trace = [m for m in funded if m['contracts'] == 0 and m['payments'] == 0]
    municipalities = {
        'funded': len(funded),
        'funding_total': money(sum(fund_muni.values(), Decimal(0))),
        'with_contracts': len(muni_contracts),
        'contracts': sum(v[0] for v in muni_contracts.values()),
        'contracts_amount': money(sum((v[1] for v in muni_contracts.values()), Decimal(0))),
        'with_payments': len(muni_payments),
        'payments': sum(v[0] for v in muni_payments.values()),
        'payments_amount': money(sum((v[1] for v in muni_payments.values()), Decimal(0))),
        'funded_without_contract': sum(1 for m in funded if m['contracts'] == 0),
        'funded_without_contract_or_payment': len(no_trace),
        'funding_of_no_trace': money(sum(Decimal(str(m['funding_2026'])) for m in no_trace)),
        'no_trace_list': [{'municipality': m['municipality'], 'nomos': m['nomos'], 'funding_2026': m['funding_2026']}
                          for m in sorted(no_trace, key=lambda m: (m['municipality'], m['nomos']))],
    }

    # ── Fires (EFFIS / Copernicus) ──
    fires_by_year = defaultdict(lambda: [0, Decimal(0)])
    fire_area = {}
    for f in data['copernicus']:
        fires_by_year[f['fire_date'].year][0] += 1
        fires_by_year[f['fire_date'].year][1] += Decimal(f['area_ha'] or 0)
        fire_area[f['copernicus_id']] = f
    report_fires = [f for f in data['copernicus'] if f['fire_date'].year == REPORT_YEAR]
    large = [f for f in report_fires if Decimal(f['area_ha'] or 0) >= 1000]
    burned_by_muni = defaultdict(lambda: [set(), Decimal(0)])
    for cm in data['copernicus_municipality']:
        f = fire_area.get(cm['copernicus_id'])
        if f and f['fire_date'].year == REPORT_YEAR:
            e = burned_by_muni[cm['municipality_key']]
            e[0].add(cm['copernicus_id'])
            e[1] += Decimal(cm['overlap_area_ha'] or 0)
    top_burned = sorted(burned_by_muni.items(), key=lambda kv: -kv[1][1])[:5]
    fires = {
        'by_year': [{'year': y, 'fires': v[0], 'area_ha': money(v[1])} for y, v in sorted(fires_by_year.items())],
        'report_year_last_fire_date': max(f['fire_date'] for f in report_fires).isoformat() if report_fires else None,
        'large_fires': len(large),
        'large_fires_area_ha': money(sum((Decimal(f['area_ha'] or 0) for f in large), Decimal(0))),
        'top_municipalities': [{'municipality': canonical_munis.get(k, k), 'fires': len(v[0]), 'area_ha': money(v[1]),
                                'municipal_contracts_2026': muni_contracts[k][0] if k in muni_contracts else 0,
                                'municipal_contracts_2026_amount': money(muni_contracts[k][1]) if k in muni_contracts else 0.0,
                                'municipal_payments_2026': muni_payments[k][0] if k in muni_payments else 0,
                                'municipal_payments_2026_amount': money(muni_payments[k][1]) if k in muni_payments else 0.0}
                               for k, v in top_burned],
    }

    # ── Facts quoted in the narrative ──
    ypen = 'ΥΠΟΥΡΓΕΙΟ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΕΝΕΡΓΕΙΑΣ'
    before_may = [r for r in c26 if r['contract_signed_date'] < date(REPORT_YEAR, 5, 1)]
    municipal = [r for r in c26 if r['canonical_owner_scope'] == 'municipality']
    facts = {
        'before_may': {'contracts': len(before_may), 'amount': money(total(before_may)),
                       'ypen_amount': money(total([r for r in before_may if r['authority_label'] == ypen]))},
        'municipal_timing': {'contracts': len(municipal),
                             'from_may_1': sum(1 for r in municipal if r['contract_signed_date'] >= date(REPORT_YEAR, 5, 1))},
        'first_half': {str(y): money(total(signed_between(signed_contracts, date(y, 1, 1), date(y, 6, 30)))) for y in range(FIRST_YEAR, REPORT_YEAR + 1)},
        'first_half_complete': period_end >= date(REPORT_YEAR, 6, 30),
        'west_attica_ypen_contract': next(({'reference_number': r['reference_number'], 'title': r['title'],
                                            'signed': r['contract_signed_date'].isoformat(), 'amount': money(r['amt'])}
                                           for r in c26 if r['reference_number'] == '26SYMV018599431'), None),
        'payroll_org_types': dict(Counter(p['org_type'] for p in valid if p['category'] == 'payroll')),
        'civil_protection_ministry_payments': sum(1 for p in valid if 'ΚΛΙΜΑΤΙΚΗΣ' in (p['org_name_clean'] or '')),
    }
    diav_years = defaultdict(dict)
    for row in data['diavgeia_years']:
        diav_years[row['decision_type']][row['year']] = row['n']

    payments_summary = {
        'records': len(p26),
        'valid': len(valid),
        'valid_amount': money(total(valid)),
        'counted': len(counted_payments),
        'amount': money(payments_amount),
        'linked': len(linked),
        'linked_amount': money(total(linked)),
        'linked_by_method': {'database': {'payments': len(linked), 'amount': money(total(linked))}},
        'linked_to_report_year': sum(1 for p in linked if REPORT_YEAR in p['linked_years']),
        'vat_removed': sum(1 for p in counted_payments if p['vat']),
        'vat_free': sum(1 for p in counted_payments if not p['vat']),
        'cancellations': len([p for p in p26 if p['excluded'] == 'cancellation']),
        'positive_cancellations_by_org': dict(Counter((f"{p['org_type']} {p['org_name_clean']}" if p['org_type'] in ('ΔΗΜΟΣ', 'ΠΕΡΙΦΕΡΕΙΑ') else p['org_name_clean'])
                                                      for p in p26 if p['excluded'] == 'cancellation' and (p['amount'] or 0) > 0)),
        'cancelled_originals': [{'ada': p['ada'], 'amount': money(p['amount']), 'cancelled_by': cancelled_originals[p['ada']]}
                                for p in p26 if p['excluded'] == 'cancelled_original'],
        'report_excluded_payroll': {
            'payments': sum(1 for p in p26 if (p['excluded'] or '').startswith('payroll_')),
            'amount': money(total([p for p in p26 if (p['excluded'] or '').startswith('payroll_')])),
        },
        'by_category': cat_rows,
        'by_org_type': [{'org_type': k, 'payments': v[0], 'amount': money(v[1])} for k, v in sorted(by_org_type.items(), key=lambda kv: -kv[1][1])],
        'payroll_bodies': len({p['org_name_clean'] for p in payroll}),
        'municipal': {
            'payments': len(municipal_payments),
            'amount': money(total(municipal_payments)),
            'from_may_1': sum(1 for p in municipal_payments if p['publish_date'] >= date(REPORT_YEAR, 5, 1)),
            'payroll': sum(1 for p in municipal_payments if p['category'] == 'payroll'),
            'by_month': [{'month': mo, 'payments': municipal_by_month[mo][0], 'amount': money(municipal_by_month[mo][1])}
                         for mo in range(1, 13)],
        },
        'payroll_by_month': [{'month': m, 'payments': payroll_by_month.get(m, 0)} for m in range(1, 13)],
        'vat_evidence': {k: vat_evidence.get(k, 0) for k in ('equals_with_vat', 'equals_without_vat', 'other')},
        'subject_refs_not_in_db': sum(1 for p in counted_payments if p['subject_refs_not_in_db']),
        'without_beneficiary_vat': sum(1 for p in counted_payments if not pay_ben.get(p['id'])),
        'exact_duplicate_groups': len(duplicate_groups),
        'exact_duplicates_extra_amount': money(sum((g[0]['amt'] or 0) * (len(g) - 1) for g in duplicate_groups)),
        'report_year_contracts_with_linked_payment': len(c26_paid),
        'report_year_contracts_with_linked_payment_value_pct': pct(total(c26_paid), contracts_amount),
        'finalizations_by_year': {str(k): v for k, v in sorted(diav_years.get(PAYMENT_TYPE, {}).items())},
    }

    snapshot_local = snapshot.astimezone(TZ)
    meta = {
        'built_at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'database_now': data['_meta'][0]['db_now'].isoformat(),
        'snapshot_at': snapshot_local.isoformat(timespec='seconds'),
        'snapshot_date': snapshot_local.strftime('%Y-%m-%d'),
        'snapshot_time': snapshot_local.strftime('%H:%M'),
        'report_year': REPORT_YEAR,
        'period_start': PERIOD_START.isoformat(),
        'period_end': period_end.isoformat(),
        'year_complete': snapshot_day > YEAR_LAST_DAY,
        'latest_contract_signed': latest_contract.isoformat(),
        'latest_payment_published': latest_payment.isoformat() if latest_payment else None,
        'vat_rate': float(VAT_RATE),
    }
    report = {
        'meta': meta,
        'total': footprint,
        'contracts': stats,
        'years': full_years,
        'ytd': ytd_rows,
        'authorityChanges': authority_changes,
        'relevance': relevance_rows,
        'timing': timing,
        'contractors': contractors,
        'payments': payments_summary,
        'municipalities': municipalities,
        'funding': funding,
        'fires': fires,
        'facts': facts,
        'dataQuality': {
            'superseded_or_duplicate_report_year_contract_rows': sum(
                1 for r in all_proc if r['contract_signed_date'] and PERIOD_START <= r['contract_signed_date'] <= period_end
                and not r['cancelled'] and r['id'] not in deduplicated_ids),
            'scope_excluded_contracts': [
                {'reference_number': r['reference_number'],
                 'reason': EXCLUDED_CONTRACT_REFERENCES[btrim(r['reference_number'])],
                 'amount_without_vat': money(r['amount_without_vat'] or 0)}
                for r in scope_excluded
            ],
        },
    }
    check(report)

    out_dir = out_root / REPORT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / 'report_data.json').write_text(json.dumps(report, ensure_ascii=False, indent=1, default=str) + '\n', encoding='utf-8')

    with (out_dir / 'contracts_2026.csv').open('w', encoding='utf-8', newline='') as handle:
        w = csv.writer(handle)
        w.writerow(['reference_number', 'signed', 'authority', 'authority_group', 'title', 'contract_type', 'procedure',
                    'prevention', 'amount_without_vat', 'amount_with_vat', 'beneficiary', 'cpvs', 'linked_payment'])
        for r in sorted(c26, key=lambda r: (r['contract_signed_date'], r['id'])):
            w.writerow([r['reference_number'], r['contract_signed_date'].isoformat(), r['authority_label'], r['authority_group'],
                        ' '.join((r['title'] or '').split()), r['type_label'], r['procedure'], r['prevention'],
                        money(r['amt']), money(r['amt_w']) if r['amt_w'] is not None else '', r['beneficiary_name'] or '',
                        ' | '.join(r['cpv_labels']), 'yes' if r['id'] in paid_contracts else 'no'])

    with (out_dir / 'payments_2026.csv').open('w', encoding='utf-8', newline='') as handle:
        w = csv.writer(handle)
        w.writerow(['ada', 'published', 'org_type', 'organization', 'subject', 'amount_as_recorded', 'amount_without_vat',
                    'vat_removed', 'amount_lines', 'category',
                    'excluded', 'link_method', 'linked_contracts', 'linked_contract_years', 'counted_in_total',
                    'contract_refs_in_subject_not_in_db', 'prevention', 'url'])
        for p in sorted(p26, key=lambda p: (p['publish_date'], p['ada'])):
            w.writerow([p['ada'], p['publish_date'].isoformat(), p['org_type'], p['org_name_clean'], ' '.join((p['subject'] or '').split()),
                        money(p['amount']) if p['amount'] is not None else '', money(p['amt']) if p['amt'] is not None else '',
                        'yes' if p['vat'] else 'no', p['lines'], p['category'],
                        p['excluded'] or '', p['link_method'] or '', ' | '.join(p['linked_refs']), ' | '.join(map(str, p['linked_years'])),
                        'yes' if p['counted'] else 'no', ' | '.join(p['subject_refs_not_in_db']), p['prevention'], p['document_url']])

    with (out_dir / 'municipalities_2026.csv').open('w', encoding='utf-8', newline='') as handle:
        w = csv.writer(handle)
        w.writerow(['municipality_key', 'municipality', 'nomos', 'funding_2026', 'contracts', 'contracts_without_vat',
                    'payments', 'payments_without_vat'])
        for m in muni_rows:
            w.writerow([m['municipality_key'], m['municipality'], m['nomos'], m['funding_2026'], m['contracts'], m['contracts_amount'],
                        m['payments'], m['payments_amount']])

    # Audience-facing export: aggregates only, no tax identifiers or personal payees.
    app = report
    app_path = out_root / APP_JSON
    app_path.parent.mkdir(parents=True, exist_ok=True)
    app_path.write_text(json.dumps(app, ensure_ascii=False, indent=1, default=str) + '\n', encoding='utf-8')
    return report


def check(report: dict) -> None:
    """Arithmetic checks; a failed check stops the run before anything is written."""
    def close(a, b, tolerance=0.05):
        return abs(float(a) - float(b)) <= tolerance

    t, c, p = report['total'], report['contracts'], report['payments']
    problems = []
    if not close(t['amount'], t['contracts_amount'] + t['payments_amount']):
        problems.append('total != contracts + payments')
    if not close(t['contracts_amount'], c['amount']) or t['contracts'] != c['contracts']:
        problems.append('contracts total mismatch')
    for field in ('procedures', 'contract_types'):
        if not close(sum(row['amount'] for row in c[field]), c['amount'], 1) or sum(row['count'] for row in c[field]) != c['contracts']:
            problems.append(f'{field} do not add up')
    if not close(sum(row['amount'] for row in p['by_category']), p['amount'], 1) or sum(row['payments'] for row in p['by_category']) != p['counted']:
        problems.append('payment categories do not add up')
    if p['counted'] + p['linked'] != p['valid'] or not close(p['amount'] + p['linked_amount'], p['valid_amount'], 1):
        problems.append('payments: counted + linked != valid')
    if p['records'] != (p['valid'] + p['cancellations'] + len(p['cancelled_originals'])
                        + p['report_excluded_payroll']['payments']):
        problems.append('payments: valid and excluded rows do not match all source records')
    if not close(sum(row['amount'] for row in report['relevance']), c['amount'], 1):
        problems.append('classification does not add up')
    da = c['direct_awards']
    band = next((row['count'] for row in da['bins'] if row['lo'] == 25000), None)
    if sum(row['count'] for row in da['bins']) != da['count'] or band != da['from_25k_to_30k']:
        problems.append('direct-award bins do not match the counts in the text')
    current = next(row for row in report['ytd'] if row['year'] == report['meta']['report_year'])
    if current['contracts'] != c['contracts'] or not close(current['amount'], c['amount']):
        problems.append('year-to-date row differs from the contracts of the period')
    if problems:
        raise SystemExit('Checks failed: ' + '; '.join(problems))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--snapshot', help='ISO timestamp with timezone; rows created later are ignored (default: now)')
    parser.add_argument('--out-root', default=str(ROOT), help='write outputs under this directory instead of the repository')
    args = parser.parse_args()
    snapshot = datetime.fromisoformat(args.snapshot) if args.snapshot else datetime.now(TZ).replace(microsecond=0)
    if snapshot.tzinfo is None:
        raise SystemExit('--snapshot needs a timezone, e.g. "2026-10-19 21:00:00+03:00"')
    report = build(snapshot, Path(args.out_root))
    print(json.dumps({'snapshot_at': report['meta']['snapshot_at'], 'period_end': report['meta']['period_end'],
                      'contracts': report['total']['contracts'], 'contracts_amount': report['total']['contracts_amount'],
                      'payments': report['total']['payments'], 'payments_amount': report['total']['payments_amount'],
                      'total': report['total']['amount']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
