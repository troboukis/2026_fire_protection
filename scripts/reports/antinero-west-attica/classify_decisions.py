#!/usr/bin/env python3
"""Deterministic Greek document classification, with auditable evidence and abstention.

Classify the administrative action, never a mentioned parent contract or an earlier
act in the recital. Unknown/conflicting actions remain grey. No network/LLM calls.
"""
import hashlib
import re
import unicodedata

VERSION = '1.0.0'
CATEGORIES = [
    {'id': 'studies', 'label': 'Μελέτες', 'color': '#2864a0'},
    {'id': 'changes', 'label': 'Τροποποιήσεις', 'color': '#ad661b'},
    {'id': 'acceptance', 'label': 'Παραλαβή', 'color': '#277b64'},
    {'id': 'payments', 'label': 'Πληρωμές', 'color': '#8555a3'},
    {'id': 'other', 'label': 'Λοιπά', 'color': '#777777'},
]


def normalize(value):
    value = ''.join(c for c in unicodedata.normalize('NFD', value.lower()) if not unicodedata.combining(c))
    return re.sub(r'\s+', ' ', value.replace('ς', 'σ').replace('_', ' ')).strip()


# Order is only for stable reporting. Cross-category matches abstain, not first-win.
RULES = [
    ('studies', 'study', r'(?:εγκρισ\w*|εγκριν\w*|θεωρ\w*).{0,65}(?:μελετ\w*|τεχνικ\w* εκθεσ\w*)'),
    ('studies', 'schedule', r'(?:εγκρισ\w*|εγκριν\w*).{0,40}(?:χρονοδιαγραμμα\w*|οργανογραμμα\w*)'),
    ('studies', 'supervision', r'(?:ορισμ\w*|οριζ\w*).{0,45}επιβλε\w*'),
    ('studies', 'committee', r'(?:συγκροτ\w*|ορισμ\w*|οριζ\w*).{0,60}επιτροπ\w*'),
    ('studies', 'start', r'(?:πρωτοκολλ\w* εγκαταστασ\w*|εντολ\w* εναρξ\w*|εγγραφ\w* εντολ\w*|εγκρισ\w*.{0,30}εναρξ\w*)'),
    ('changes', 'extension', r'παρατασ\w*'),
    ('changes', 'suspension', r'(?:διακοπ\w*|αναστολ\w*|επανεναρξ\w*)'),
    ('changes', 'amendment', r'(?:τροποποι\w*|επικαιροποι\w*|συμπληρωματικ\w* συμβασ\w*)'),
    ('changes', 'work_table', r'(?:\bα\s*\.?\s*π\s*\.?\s*ε\b|\bπ\.?\s*κ\.?\s*τ\.?\s*μ\.?\s*ν\.?\s*ε\b|ανακεφαλαιωτικ\w* πινακ\w*|κανονισμ\w* τιμ\w*|δαπαν\w* αναθεωρησ\w*|πινακ\w* αναθεωρησ\w*)'),
    ('acceptance', 'deliverable_check', r'ελεγχ\w* πληροτητ\w*.{0,50}παραδοτε\w*'),
    ('acceptance', 'measurement', r'επιμετρ\w*'),
    ('acceptance', 'acceptance_protocol', r'(?:πρωτοκολλ\w*|οριστικ\w*|τμηματικ\w*|ποιοτικ\w*|ποσοτικ\w*).{0,50}παραλαβ\w*'),
    ('acceptance', 'completion', r'βεβαιωσ\w*.{0,40}(?:περατωσ\w*|περαιωσ\w*|ολοκληρωσ\w*)'),
    ('acceptance', 'recorded_work', r'(?:απολογιστικ\w*|ημερολογι\w*|πινακ\w* ημερησι\w* εργασι\w*)'),
    ('payments', 'account', r'λογαριασμ\w*|ειδικ\w* απολογισμ\w*'),
    ('payments', 'payment', r'πληρωμ\w*|εκκαθαρισ\w*|καταβολ\w*|αποζημιωσ\w*|\bπριμ\b'),
]


def action_prefix(value):
    # The quoted project title often contains misleading words (e.g. "μελέτη").
    value = re.split(r'[«“"]', value.lstrip('«“" :'), maxsplit=1)[0]
    return re.split(r'\b(?:του|το|στο|για το) (?:ειδικου |ειδικο )?(?:δασοτεχνικου |δασοτεχνικο )?εργο\w*\b|\bμε τιτλο\b', value, maxsplit=1)[0][:650]


def matches(value):
    found = [(category, rule, m.group()) for category, rule, pattern in RULES if (m := re.search(r'\b(?:' + pattern + ')', value))]
    # A committee is organisational, even when named "Επιτροπή Παραλαβής".
    if any(rule == 'committee' for _, rule, _ in found):
        found = [x for x in found if x[1] != 'acceptance_protocol']
    # An explicitly updated schedule/study belongs to amendments.
    if any(rule in ('amendment', 'extension', 'suspension') for _, rule, _ in found):
        found = [x for x in found if x[0] != 'studies']
    # A combined account/measurement approval is primarily an account decision.
    if any(rule == 'account' for _, rule, _ in found):
        found = [x for x in found if x[0] not in ('acceptance', 'studies')]
    return found


def classify_document(title, text=''):
    normalized = normalize(title)
    prefix = action_prefix(normalized)
    evidence_source = 'title'
    found = matches(prefix)
    guarantee_only = bool(re.search(r'μειωσ\w* εγγυησ\w*', prefix)) and not any(c == 'payments' for c, _, _ in found)
    if guarantee_only:
        found = []
    if not found and not guarantee_only:
        # Inspect only the operative section, never the legal/historical recital.
        normalized_text = normalize(text)
        markers = list(re.finditer(r'α\s*π\s*ο\s*φ\s*α\s*σ\s*ι\s*ζ\s*(?:ο\s*υ\s*μ\s*ε|ε\s*ι)\b', normalized_text))
        if markers:
            prefix = action_prefix(normalized_text[markers[-1].end():].lstrip(' :.-'))
            found = matches(prefix)
            evidence_source = 'operative_text'
    categories = {x[0] for x in found}
    category = next(iter(categories)) if len(categories) == 1 else 'other'
    return {'category_id': category, 'method': 'rules', 'version': VERSION,
            'basis': evidence_source if found else 'unmatched',
            'reason': 'conflicting_actions' if len(categories) > 1 else 'matched_action' if found else 'no_clear_action',
            'matches': [{'category_id': c, 'rule': r, 'evidence': e} for c, r, e in found],
            'title_sha256': hashlib.sha256(title.encode()).hexdigest(),
            'text_sha256': hashlib.sha256(text.encode()).hexdigest() if evidence_source == 'operative_text' else None}
