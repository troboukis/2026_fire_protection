"""Keyword rules for forest-fire classification, independent of API/database access.

classify_keywords(document, audit=None) returns a classification for a single
matching category, or None when no rules match or categories conflict.
"""
import re
import unicodedata

KEYWORD_RULES_VERSION = '2026-10-08-v1'
MAX_INTERVENING_WORDS = 2
PREVENTION_BLOCK_ROOTS = ('εκτακτ', 'επειγουσ')
# Ordered word roots; each root must start a complete token.
KEYWORD_RULES = {
    1: (
        ('προμηθ', 'δεξαμεν'), ('αποψιλ',), ('_αποψιλ'), ('αποψηλ',), ('_αποψηλ',),
        ('συντηρ', 'πρασιν'), ('συντηρ', 'πυροσβεστηρ'),
        ('προμηθ', 'πυροσβεστηρ'), ('εργασ', 'προληψ'),
        ('εργασ', 'καθαρισ'), ('αντιπυρ', 'ζων'),
        ('προληψ', 'δασικ', 'πυρκαγι'), ('καθαρισ', 'δρομ'),
        ('διαπλατυν', 'αντιπυρ', 'ζων'),
        ('συντηρ', 'ζων', 'αντιπυρ', 'προστασ'),
        ('αποκαταστασ', 'αγροτικ', 'οδ'), ('υπηρεσ', 'προληψ'),
        ('καθαρισ', 'δασικ', 'δρομ'), ('συντηρ', 'δασικ', 'δρομ'),
        ('καθαρισ', 'αγροτικ', 'δρομ'), ('συντηρ', 'αγροτικ', 'δρομ'),
        ('κοπ', 'χορτ'), ('καθαρισ', 'πραν'), ('προληπτικ', 'καθαρισ'),
        ('δρασ', 'πυροπροστασ'), ('καθαρισ', 'πρασιν'),
        ('καθαρισ', 'απ', 'σκουπιδ'), ('καθαρισμ', 'χορτ'), ('καθαρισμ', 'απ', 'χορτ'), ('καθαρισ', 'σκουπιδ'), ('κλαδεμ', 'δενδρ'), ('αυτεπαγγελτ', 'καθαρισ'), ('εργασ', 'αντιπυρικ', 'προστασ'), ('θερμικ', 'καμερ'), ('επινωτ', 'πυροσβεστ'), ('εγκαταστ', 'πυροπροστασ'), ('αναβασθμισ', 'συστηματ', 'πυροσβεσ'), ('απομακρυνσ', 'βλαστησ'), ('συντηρησ', 'προαυλι', 'χωρ'), ('συντηρησ', 'δικτυ', 'πυρασφαλ'), ('αναβαθμισ', 'εκσυγχρονισμ', 'πυροσβεστικ', 'σημει'), ('βοτανισμ',), ('διανοιξ', 'ζων'), ('εργασ', 'υλοτομ'), ('εργ', 'αντιπυρικ', 'προστασ'), ('αποχλοασ',), ('παροδι', 'καθαρισμ'), ('εργασ', 'αποχωματ'), ('βελτιωσ', 'δασικ', 'δρομ'), ('εκτελεσ', 'εργασ', 'δασοπυροσβ'), ('καθαρισμ', 'ερεισματ'), ('προμηθ', 'τοποθετησ', 'πυροσβεστικ', 'φωλε'), ('καθαρισμ', 'ρεματ'), ('καθαρισμ', 'ταφρο'), ('καθαρισμ', 'γεφυρ'), ('καθαρισμ', 'τμηματ', 'ποταμ'), ('προστασ', 'αναδειξ', 'δημοσ', 'δασο'), ('καθαρισμ', 'περιβαλ', 'χωρ'), ('αποκλαδωσ',), ('διανοιξ', 'ταφρ'), ('καθαρισμ', 'ιδιωτικ', 'οικοπεδ'), ('διαχειρισ', 'πρασιν'), ('βατοτητ', 'αγροτοδασικ', 'δικτυ'), ('καθαρισμ', 'κοινοχρηστ', 'χωρ'), ('υπηρεσ', 'πρασιν', 'κλαδεμ'), ('καθαρισμ', 'αυλει', 'χωρ'), ('πυροπροστασ', 'Δ.Ε'), ('αποκαταστ', 'βατοτητ'), ('συντηρησ', 'μονοπατ'), ('καθαρισμ', 'ιδιωτικ', 'οικοπεδ'), ('καθαρισμ', 'ποταμ'), ('ξυλευσ', 'δεντρ'), ('καθαρισμ', 'παροδ'), ('καθαρισμ', 'κοινοχρησ', 'χωρ')
    ),
    0: (
        ('εργασ', 'κατασβεσ'), ('κατεπειγουσ', 'μισθ'),
        ('εκτακτ', 'εργασ'), ('μισθω', 'μηχανημ', 'αντιμετωπισ', 'εκτακτ'), ('μισθω', 'μηχανημ', 'εργ', 'οχημ'), ('αεροπυροσβ',), ('επειγουσ', 'χωματουργ', 'εργασ')
    ),
}


def normalize_keyword_token(text):
    text = unicodedata.normalize('NFD', text)
    return ''.join(c for c in text if unicodedata.category(c) != 'Mn').casefold().replace('ς', 'σ')


def matching_spans(tokens, roots, max_intervening_words=MAX_INTERVENING_WORDS):
    """Match ordered roots with a bounded gap between each pair of roots."""
    for start, token in enumerate(tokens):
        if not token.startswith(roots[0]):
            continue
        positions = {start}
        for root in roots[1:]:
            positions = {
                next_position
                for position in positions
                for next_position in range(position + 1,
                                           min(len(tokens), position + max_intervening_words + 2))
                if tokens[next_position].startswith(root)
            }
            if not positions:
                break
        if positions:
            yield start, min(positions)


def classify_keywords(document, audit=None):
    matches = []
    blockers = []
    for field in ('title', 'short_descriptions', 'subject'):
        text = document.get(field) or ''
        tokens = list(re.finditer(r'\w+', text, flags=re.UNICODE))
        normalized = [normalize_keyword_token(t.group()) for t in tokens]
        for token, normalized_token in zip(tokens, normalized):
            if normalized_token.startswith(PREVENTION_BLOCK_ROOTS):
                blockers.append({'field': field, 'word': token.group()})
        for value, rules in KEYWORD_RULES.items():
            for roots in rules:
                for start, end in matching_spans(normalized, roots):
                    matches.append({'prevention': value, 'roots': list(roots),
                                    'evidence_field': field,
                                    'evidence': text[tokens[start].start():tokens[end].end()]})
    if audit is not None:
        audit.update(keyword_rules_version=KEYWORD_RULES_VERSION, keyword_matches=matches)
        if blockers:
            audit['prevention_blockers'] = blockers
    if blockers:
        matches = [match for match in matches if match['prevention'] == 0]
    if len({m['prevention'] for m in matches}) != 1:
        return None  # Missing or conflicting rules: defer to the model.
    match = matches[0]
    if audit is not None:
        audit['classification_method'] = 'keywords'
    label = 'πρόληψης' if match['prevention'] == 1 else 'καταστολής'
    return {'prevention': match['prevention'],
            'reason': f'Αντιστοιχία με κανόνα λέξεων κλειδιών {label}.',
            'evidence_field': match['evidence_field'], 'evidence': match['evidence']}
