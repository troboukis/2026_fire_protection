#!/usr/bin/env python3
"""Export audience-facing report data; raw research notes stay outside the app."""
from classify_decisions import CATEGORIES
import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
FOLDER = ROOT / 'data/reports/antinero-west-attica'
OUTPUT = ROOT / 'app/src/features/analysis/antinero/data/antineroReport.generated.json'

FORESTRY_BY_CONTRACT = {
    '22SYMV011593395': 'Δασαρχεία Αιγάλεω και Πεντέλης',
    '23SYMV012820428': 'Δασαρχεία Μεγάρων, Πάρνηθας, Πόρου και Πεντέλης',
    '23SYMV012963827': 'Δασαρχείο Αιγάλεω και Διεύθυνση Δασών Αθηνών',
    '23SYMV012964096': 'Δασαρχείο Αιγάλεω',
    '23SYMV012992150': 'Δασαρχεία Αιγάλεω, Καπανδριτίου και Λαυρίου',
    '23SYMV013154974': 'Δασαρχεία Μεγάρων, Πάρνηθας, Πόρου και Πεντέλης',
    '23SYMV013156865': 'Δασαρχείο Αιγάλεω',
    '24SYMV014192335': 'Δασαρχείο Αιγάλεω',
    '24SYMV014217833': 'Δασαρχείο Μεγάρων',
    '24SYMV014223991': 'Δασαρχεία Αιγάλεω και Μεγάρων',
    '24SYMV014447969': 'Διεύθυνση Δασών Αργολίδας και Δασαρχεία Αγρινίου, Μεγάρων, Άμφισσας και Λιδωρικίου',
    '24SYMV014662191': 'Δασαρχεία Καπανδριτίου, Πεντέλης και Αιγάλεω',
    '24SYMV014662244': 'Δασαρχεία Πάρνηθας και Μεγάρων',
    '24SYMV015170089': 'Δασαρχεία Πεντέλης, Καπανδριτίου, Λαυρίου, Πάρνηθας, Μεγάρων, Πειραιώς και Πόρου',
    '24SYMV016017961': 'Δασαρχείο Αιγάλεω',
    '25SYMV016570021': 'Δασαρχεία Αιγάλεω, Μεγάρων, Πειραιά, Λαυρίου και Πόρου',
    '25SYMV017345053': 'Δασικές υπηρεσίες Αττικής και όμορων Περιφερειακών Ενοτήτων',
    '25SYMV017471439': 'Δασαρχεία Αιγάλεω και Μεγάρων',
    '26SYMV018599431': 'Δασαρχεία Αιγάλεω, Μεγάρων, Πειραιά και Πεντέλης',
    '26SYMV018936694': 'Δασαρχεία Αιγάλεω και Μεγάρων',
    '26SYMV018978343': 'Δασαρχείο Χαλκίδας',
}

AREA_BY_CONTRACT = {
    '22SYMV011593395': 'Αιγάλεω και Πεντέλη',
    '23SYMV012820428': 'Μέγαρα, Πάρνηθα, Πόρος και Πεντέλη',
    '23SYMV012963827': 'Αιγάλεω και Αθήνα',
    '23SYMV012964096': 'Αιγάλεω',
    '23SYMV012992150': 'Αιγάλεω, Καπανδρίτι και Λαύριο',
    '23SYMV013154974': 'Μέγαρα, Πάρνηθα, Πόρος και Πεντέλη',
    '23SYMV013156865': 'Αιγάλεω',
    '24SYMV014192335': 'Δήμος Μάνδρας–Ειδυλλίας',
    '24SYMV014217833': 'Περιφερειακή Ενότητα Δυτικής Αττικής',
    '24SYMV014223991': 'Αιγάλεω και Μέγαρα',
    '24SYMV014447969': 'Αργολίδα, Αγρίνιο, Μέγαρα, Άμφισσα και Λιδωρίκι',
    '24SYMV014662191': 'Καπανδρίτι, Πεντέλη και Αιγάλεω',
    '24SYMV014662244': 'Πάρνηθα και Μέγαρα',
    '24SYMV015170089': 'Πεντέλη, Καπανδρίτι, Λαύριο, Πάρνηθα, Μέγαρα, Πειραιάς και Πόρος',
    '24SYMV016017961': 'Αιγάλεω',
    '25SYMV016570021': 'Αιγάλεω, Μέγαρα, Πειραιάς, Λαύριο και Πόρος',
    '25SYMV017345053': 'Αττική και όμορες Περιφερειακές Ενότητες',
    '25SYMV017471439': 'Αιγάλεω και Μέγαρα',
    '26SYMV018599431': 'Αιγάλεω, Μέγαρα, Πειραιάς και Πεντέλη',
    '26SYMV018936694': 'Αιγάλεω και Μέγαρα',
    '26SYMV018978343': 'Χαλκίδα, Εύβοια',
}

DOCUMENT_DATE_OVERRIDES = {
    '23SYMV013154974': ('2023-07-24', 'Ημερομηνία υπογραφής τροποποίησης'),
    '23SYMV013156865': ('2023-07-24', 'Ημερομηνία υπογραφής τροποποίησης'),
    '26SYMV018936694': ('2026-04-30', 'Ημερομηνία έγκρισης συμπληρωματικής πράξης'),
    '26SYMV018978343': ('2026-05-07', 'Ημερομηνία έγκρισης συμπληρωματικής πράξης'),
}


def beneficiary_key(contractor):
    identity = contractor.get('vat_number') or re.sub(r'\W+', '', contractor['name'].upper())
    return f'beneficiary-{hashlib.sha256(identity.encode()).hexdigest()[:12]}'


def build():
    data = json.loads((FOLDER / 'dataset.json').read_text())
    editorial = json.loads((FOLDER / 'publication.json').read_text())
    decisions = {d['id']: d for d in data['decisions']}
    findings = {f['id']: f for f in data['findings']}
    published_findings = []
    for item in editorial['findings']:
        finding = findings[item['id']]
        digest = hashlib.sha256(json.dumps(finding['review'], ensure_ascii=False, sort_keys=True).encode()).hexdigest()
        assert digest == item['source_review_sha256'], item['id']
        assert item['contract_ids'] == finding['contract_ids']
        published_findings.append({k: item[k] for k in ('id', 'contract_ids', 'title', 'text', 'qualification')} | {
            'sources': [{'id': e['document_id'], 'url': e['url']} for e in finding['review']['evidence'] if e['url']]})
    for did, item in editorial['decisions'].items():
        assert did in decisions
        evidence = item['evidence']
        assert evidence['path'] == decisions[did]['text_path']
        assert hashlib.sha256((ROOT / evidence['path']).read_bytes()).hexdigest() == evidence['sha256'], did
    beneficiary_names = {}
    for contract in data['contracts']:
        for contractor in contract['contractors']:
            key = beneficiary_key(contractor)
            beneficiary_names.setdefault(key, set()).add(contractor['name'])
    beneficiary_labels = {key: min(names, key=lambda name: (len(name), name)) for key, names in beneficiary_names.items()}
    public = {'cutoff': data['research_cutoff'], 'categories': CATEGORIES, 'contracts': [], 'decisions': [], 'findings': published_findings}
    for c in data['contracts']:
        # Registry phase and the funding programme named in the attachment can differ.
        programme_text = (ROOT / c['text_path']).read_text().upper().replace('Ι', 'I')
        attachment_match = re.search(r'ANTI[ -]?NERO\s+(IV|III|II|I)\b', programme_text)
        title_match = re.search(r'ANTI[ -]?NERO\s+(IV|III|II|I)\b', c['title'].upper().replace('Ι', 'I'))
        programme_match = title_match or attachment_match
        programme = f'AntiNERO {programme_match.group(1)}' if programme_match else 'AntiNERO'
        programme_note = ('Η ονομασία προέρχεται από τον τίτλο της καταχώρισης ΚΗΜΔΗΣ.' if title_match else
            'Η ονομασία προέρχεται από το κείμενο του διαθέσιμου εγγράφου.' if attachment_match else
            'Δεν προσδιορίζεται αριθμημένη φάση στα στοιχεία που χρησιμοποιήθηκαν για αυτή την ετικέτα.')
        if title_match and attachment_match and title_match.group(1) != attachment_match.group(1):
            programme_note += f' Το έγγραφο αναφέρει επίσης AntiNERO {attachment_match.group(1)} στο χρηματοδοτικό πλαίσιο· οι δύο ονομασίες διατηρούνται χωριστά.'
        document_date, date_kind = DOCUMENT_DATE_OVERRIDES.get(
            c['id'], (c['signed_on_metadata'], 'Ημερομηνία υπογραφής'))
        node_context = {
            'programme': programme,
            'programmeNote': programme_note,
            'forestry': FORESTRY_BY_CONTRACT[c['id']],
            'area': AREA_BY_CONTRACT[c['id']],
            'scopeNote': c['scope']['geographic_caveat'],
        }
        public['contracts'].append({
            'nodeContext': node_context,
            'id': c['id'], 'parentId': c['parent_contract_id'], 'kind': c['record_kind'],
            'label': 'Μικτές αντιπυρικές ζώνες' if c['id'] == '24SYMV014192335' else c['report_label'] or c['title'],
            'description': '\n'.join(filter(None, c['scope']['full_scope_descriptions'])),
            'authority': c['authority']['value'], 'contractors': [x['name'] for x in c['contractors']],
            'beneficiaries': [{
                'id': beneficiary_key(x),
                'name': beneficiary_labels[beneficiary_key(x)],
            } for x in c['contractors']],
            'documentDate': document_date, 'dateKind': date_kind,
            'signedOn': c['signed_on_metadata'], 'offices': c['scope']['matched_research_offices'],
            'withoutVat': c['amount']['without_vat'], 'withVat': c['amount']['with_vat'],
            'amountBasis': c['amount']['basis'], 'url': c['url'], 'aliases': c['reference_aliases'],
            'decisionIds': c['decision_ids'],
        })
    for d in decisions.values():
        edit = editorial['decisions'].get(d['id'])
        public['decisions'].append({'id': d['id'], 'date': d['issued_on'], 'subject': d['subject'],
            'url': d['url'], 'contractIds': d['contract_ids'], 'reviewed': edit is not None,
            'label': edit['label'] if edit else d['subject'],
            'categoryId': d['classification']['category_id'],
            'category': next(c['label'] for c in CATEGORIES if c['id'] == d['classification']['category_id']),
            'summary': edit['summary'] if edit else None})
    assert len(public['contracts']) == 21 and len(public['decisions']) == 426
    assert set(FORESTRY_BY_CONTRACT) == {c['id'] for c in data['contracts']}
    assert set(AREA_BY_CONTRACT) == {c['id'] for c in data['contracts']}
    aigaleo = next(c for c in public['contracts'] if c['id'] == '24SYMV014192335')
    assert set(aigaleo['decisionIds']) == set(editorial['decisions'])
    OUTPUT.write_text(json.dumps(public, ensure_ascii=False, indent=2)+'\n')
    print(f'Exported {len(public["contracts"])} contracts, {len(public["decisions"])} decisions, {len(published_findings)} publication findings')


if __name__ == '__main__':
    build()
