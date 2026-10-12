"""Conservative, evidence-preserving total amount extraction from payment PDFs."""
from __future__ import annotations

from decimal import Decimal
import re
import unicodedata

MONEY = re.compile(r'(?<![\d.,])(?:\d{1,3}(?:\.\d{3})+|\d+),\d{2}(?!\d)')
TOTAL_LABEL = re.compile(
    r'συνολικ[οη]\s+(?:ποσο|αξια)|συνολ\.?\s*αξια|'
    r'συνολο\s+ενταλματοσ|συν\s+εντ/τοσ?|συν\.?\s*ποσο|'
    r'συνολο\s+εντελλομενων\s+ποσων|συνολο\s*χ\.?\s*ε\.?|'
    r'εντελλομενο\s+ποσο(?!\s+στο\s+δικαιουχο)|εντελλομεθα\s+με\s+το\s+παρον|'
    r'ακαθαριστ[οε][σ]?\s+(?:ποσο|αποδοχε[σ])'
)
GENERIC_TOTAL = re.compile(r'γενικο\s+συνολο|συνολο\s*\(ολογραφωσ\)|συνολο\b')
NET_LABEL = re.compile(r'πληρωτεο(?:\s+ποσο|\s+στο\s+δικαιουχο)?|καθαρο\s+στο\s+δικαιουχο|καθαρ\.?\s+δικαιουχ|καθαρο\s+ποσο|εντελλομενο\s+ποσο\s+στο\s+δικαιουχο')
DEDUCTIONS_LABEL = re.compile(r'συνολο\s+κρατησεων|συνολ\.?\s*κρατησεων|συν\.\s*κρατησει|κρατησεισ\s*(?:\(-\)|€)|κρατ\.\s*\(-\)')


def normalize(text: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn').casefold().replace('ς', 'σ')


def money_value(value: str) -> Decimal:
    return Decimal(value.replace('.', '').replace(',', '.'))


def extract_payment_total(text: str) -> dict:
    """Return a total only if the strongest labels identify one unique amount.

    Generic table totals require confirmation by gross = net + deductions.
    Conflicting amounts across pages are intentionally left for review.
    """
    candidates = []
    nets, deductions = set(), set()
    for page_number, page in enumerate(text.split('\f'), 1):
        lines = page.splitlines()
        for index, line in enumerate(lines):
            normalized = normalize(line)
            for regex, target in ((NET_LABEL, nets), (DEDUCTIONS_LABEL, deductions)):
                match = regex.search(normalized)
                if match:
                    values = MONEY.findall(line[match.end():])
                    if not values and index+1 < len(lines) and len(MONEY.findall(lines[index+1])) == 1:
                        values = MONEY.findall(lines[index+1])
                    if values:
                        target.add(money_value(values[0]))
            label = TOTAL_LABEL.search(normalized)
            tier = 'explicit'
            if not label:
                label = GENERIC_TOTAL.search(normalized)
                tier = 'generic'
            # Some templates wrap "ΣΥΝΟΛΙΚΟ ΠΟΣΟ" into two lines.
            if not label and 'συνολικο' in normalized and index+1 < len(lines) and re.match(r'^\s*ποσο\s*:', normalize(lines[index+1])):
                label = re.search(r'συνολικο', normalized)
                tier = 'explicit'
            if not label:
                continue
            tail = line[label.end():]
            if tier == 'generic' and re.match(r'\s*(?:κρατησ|κρατησε|ενταλθεν|δεσμευ|φ\.?\s*π\.?\s*α)', normalize(tail)):
                continue
            if 'στο δικαιουχο' in normalize(tail) and 'εντελλομενο' in label.group():
                continue
            evidence = line.strip()
            if 'α/α' in normalized and ('αρ. τιμ' in normalized or 'ημ/νια' in normalized):
                continue
            # A prose sentence may put its numeric amount on the next two lines.
            if tier == 'explicit' and not MONEY.search(tail) and 'συνολικο ποσο' in normalized and 'που ειναι' in normalized:
                tier = 'prose'
                tail += '\n' + '\n'.join(lines[index+1:index+3])
                evidence += '\n' + '\n'.join(lines[index+1:index+3])
            # Adjacent deduction columns must not be included in total evidence.
            stop = re.search(r'κρατησ|πληρωτε|καθαρο', normalize(tail))
            if stop:
                tail = tail[:stop.start()]
            values = MONEY.findall(tail)
            if not values:
                continue
            # An explicit label with multiple amounts is ambiguous.
            if len(set(values)) != 1:
                continue
            candidates.append({'amount': format(money_value(values[0]), '.2f'),
                               'raw_amount': values[0], 'page': page_number,
                               'evidence': evidence, 'kind': tier})
    explicit = [c for c in candidates if c['kind'] == 'explicit']
    generic = [c for c in candidates if c['kind'] == 'generic']
    prose = [c for c in candidates if c['kind'] == 'prose']
    # Prose payment instructions sometimes describe the net payable as "total".
    pool = explicit or generic or prose
    values = {c['amount'] for c in pool}
    result = {'amount': None, 'status': 'review', 'candidates': candidates,
              'net_amounts': sorted(str(v) for v in nets),
              'deduction_amounts': sorted(str(v) for v in deductions)}
    if len(values) != 1:
        result['reason'] = 'conflicting_totals' if values else 'no_total_label'
        return result
    amount = Decimal(next(iter(values)))
    arithmetic = any(amount == n+d for n in nets for d in deductions)
    # Payment schedules have only a payable total: verify every numbered row.
    schedule_rows = []
    if 'κατασταση οριστικοποιησησ πληρωμων' in normalize(text):
        for line in text.splitlines():
            row_number = re.match(r'^\s*(\d+)\s+\S', line)
            row_money = MONEY.findall(line)
            if row_number and row_money:
                schedule_rows.append((int(row_number[1]), money_value(row_money[0])))
    schedule_confirmed = bool(schedule_rows) and [n for n, _ in schedule_rows] == list(range(1, len(schedule_rows)+1)) and sum((v for _, v in schedule_rows), Decimal(0)) == amount
    prose_confirmed = any(c['amount'] == format(amount, '.2f') for c in prose)
    if not explicit and not arithmetic and not (generic and prose_confirmed) and not schedule_confirmed:
        result['reason'] = 'generic_total_without_arithmetic_confirmation'
        return result
    result.update(amount=format(amount, '.2f'), status='extracted',
                  evidence=pool[0]['evidence'], page=pool[0]['page'],
                  arithmetic_confirmed=arithmetic,
                  amount_kind='payment_schedule_payable_total' if schedule_confirmed else 'gross_warrant_total',
                  schedule_rows=len(schedule_rows) if schedule_confirmed else None,
                  reason='schedule_sum_verified' if schedule_confirmed else 'explicit_total' if explicit else 'gross_equals_net_plus_deductions' if arithmetic else 'table_and_prose_total_agree')
    return result
