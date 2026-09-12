#!/usr/bin/env python3
"""Build an offline research snapshot. No network or database access; stdlib only."""
from classify_decisions import classify_document
import csv
import hashlib
import json
import re
import zipfile
from collections import Counter
from datetime import datetime
from decimal import Decimal
from pathlib import Path
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / 'antinero_west_attica'
OUTPUT = ROOT / 'data/reports/antinero-west-attica/dataset.json'
NS = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
REF = re.compile(r'\d{2}SYMV\d+')
MONEY = re.compile(r'\d{1,3}(?:\.\d{3})*,\d{2}')
AMENDMENTS = {
    '23SYMV013154974': ('23SYMV012820428', 'correction_no_price_change'),
    '23SYMV013156865': ('23SYMV012964096', 'correction_no_price_change'),
    '26SYMV018978343': ('25SYMV017345053', 'supplement'),
    '26SYMV018936694': ('25SYMV017471439', 'supplement'),
}

def source_ref(path, **locator):
    return {'path': str(path.relative_to(ROOT)), **locator}

def text(element):
    return ''.join(t.text or '' for t in element.findall('.//w:t', NS))

def euros(value):
    return format(Decimal(value.replace('.', '').replace(',', '.')), '.2f')

def build():
    metadata_path = SOURCE / 'khmdhs_2022_plus/relevant_contracts.json'
    records = json.loads(metadata_path.read_text())
    inventory_path = SOURCE / 'diavgeia_inventory.tsv'
    with inventory_path.open() as handle:
        inventory = list(csv.DictReader(handle, delimiter='\t'))
    with (SOURCE / 'khmdhs_2022_plus/relevant_contracts.tsv').open() as handle:
        summaries = {r['reference_number']: r for r in csv.DictReader(handle, delimiter='\t')}
    report_path = SOURCE / 'Antinero_West_Attica_Report.docx'
    with zipfile.ZipFile(report_path) as archive:
        body = ET.fromstring(archive.read('word/document.xml')).find('w:body', NS)
    profiles, findings = {}, []
    section = subsection = current = ''
    valid = {r['referenceNumber'] for r in records}
    for index, block in enumerate(body):
        content = text(block)
        style = block.find('w:pPr/w:pStyle', NS)
        style = style.get('{'+NS['w']+'}val') if style is not None else ''
        refs = [r for r in REF.findall(content) if r in valid]
        if style == 'Heading1':
            section, subsection, current = content, '', ''
        elif style in ('Heading2', 'Heading3'):
            if style == 'Heading2':
                subsection = content
            current = refs[0] if refs else ''
        elif block.tag.endswith('}tbl') and current and subsection in ('Συμβάσεις, συμβαλλόμενοι και ποσά', 'Αναλυτικά στοιχεία συμβάσεων'):
            fields = {}
            for row in block.findall('w:tr', NS):
                cells = [text(c) for c in row.findall('w:tc', NS)]
                if len(cells) == 2:
                    fields[cells[0]] = cells[1]
            profiles[current] = {'fields': fields, 'source': source_ref(report_path, body_block_index=index, section=section)}
        elif content and (subsection in ('Έναρξη, ολοκλήρωση και συντήρηση', 'Τροποποιήσεις του 2023') or section.startswith('Μέρος Γ') and current or content.startswith('Για τη 24SYMV015170089,')):
            related = sorted(set(([current] if current else []) + refs))
            if 'Οι μελέτες ΔΜ-01 και ΔΜ-02' in content:
                related = ['24SYMV014662191', '24SYMV014662244']
            if not related:
                raise ValueError(f'Unassigned research paragraph: {content}')
            findings.append({'id': f'report-block-{index}', 'contract_ids': related,
                'text': content, 'kind': 'reported_research',
                'source': source_ref(report_path, body_block_index=index, section=section, subsection=subsection),
                'primary_citation_status': 'not_resolved', 'explicit_decision_ids': []})
    decisions, relationships = {}, []
    for line, row in enumerate(inventory, 2):
        ada = row['ada']
        day = datetime.strptime(row['issue_date'], '%d/%m/%Y %H:%M:%S').date().isoformat()
        decision = {'id': ada, 'node_type': 'decision', 'subject': row['subject'],
            'issued_on': day, 'issued_at_raw': row['issue_date'], 'url': row['document_url'],
            'pdf_path': source_ref(SOURCE / 'diavgeia' / f'{ada}.pdf')['path'],
            'text_path': source_ref(SOURCE / 'text/diavgeia' / f'{ada}.txt')['path'],
            'classification': classify_document(row['subject'], (SOURCE / 'text/diavgeia' / f'{ada}.txt').read_text()), 'summary': None,
            'database_presence_at_snapshot': row['in_local_db'] == 'true', 'contract_ids': [], 'sources': []}
        existing = decisions.setdefault(ada, decision)
        for key in ('subject', 'issued_on', 'url', 'database_presence_at_snapshot'):
            assert existing[key] == decision[key], (ada, key)
        existing['contract_ids'].append(row['contract_reference'])
        existing['sources'].append(source_ref(inventory_path, row=line))
        relationships.append({'id': f"reference:{row['contract_reference']}:{ada}",
            'type': 'references_contract', 'source': ada, 'target': row['contract_reference'],
            'evidence': source_ref(inventory_path, row=line),
            'meaning': 'Association in the archived contract-reference search; not proof of execution or payment.'})
    for finding in findings:
        finding['explicit_decision_ids'] = sorted(ada for ada in decisions if ada in finding['text'])
        if finding['explicit_decision_ids']:
            finding['primary_citation_status'] = 'explicit_references_in_report_not_reverified'
    review_path = OUTPUT.parent / 'finding_reviews.json'
    review_bundle = json.loads(review_path.read_text())
    reviews = {r['finding_id']: r for r in review_bundle['reviews']}
    assert len(reviews) == len(review_bundle['reviews']) == len(findings)
    assert set(reviews) == {f['id'] for f in findings}
    for finding in findings:
        review = reviews[finding['id']]
        assert review['original_text_sha256'] == hashlib.sha256(finding['text'].encode()).hexdigest(), finding['id']
        assert review['status'] in {'supported', 'qualified', 'corrected', 'unresolved'}
        assert review['evidence'] and review['notes']
        for evidence in review['evidence']:
            assert hashlib.sha256((ROOT / evidence['path']).read_bytes()).hexdigest() == evidence['sha256'], evidence['path']
        finding['review'] = review
        finding['primary_citation_status'] = 'reviewed_against_archived_sources'
    contracts = []
    for raw in records:
        cid = raw['referenceNumber']
        profile = profiles[cid]
        fields = profile['fields']
        pair = MONEY.findall(fields.get('Ποσό χωρίς / με ΦΠΑ', ''))
        if 'Ποσό χωρίς ΦΠΑ' in fields:
            pair = [MONEY.search(fields['Ποσό χωρίς ΦΠΑ']).group(), MONEY.search(fields['Ποσό με ΦΠΑ']).group()]
        amount = {'currency': 'EUR', 'without_vat': euros(pair[0]) if pair else None,
            'with_vat': euros(pair[1]) if pair else None,
            'basis': 'additional_amount' if cid in AMENDMENTS and AMENDMENTS[cid][1] == 'supplement' else 'unchanged_by_amendment' if cid in AMENDMENTS else 'original_contract_amount',
            'source': profile['source'], 'verification': 'transcribed_from_research_report'}
        members = raw.get('contractingDataDetails') or {}
        linked = sorted((d['id'] for d in decisions.values() if cid in d['contract_ids']), key=lambda i: (decisions[i]['issued_on'], i))
        contracts.append({'id': cid, 'node_type': 'contract',
            'record_kind': AMENDMENTS[cid][1] if cid in AMENDMENTS else 'original',
            'parent_contract_id': AMENDMENTS[cid][0] if cid in AMENDMENTS else None,
            'title': raw['title'], 'report_label': fields.get('Αντικείμενο'),
            'authority': raw['organization'], 'contractors': [
                {'name': m['name'], 'vat_number': m.get('vatNumber')} for m in members.get('contractingMembersDataList', [])],
            'signed_on_metadata': raw.get('contractSignedDate'),
            'published_at_metadata': raw.get('submissionDate'),
            'signature_and_publication_report': fields.get('Υπογραφή / ανάρτηση'),
            'signer_metadata': members.get('signers'),
            'amount': amount,
            'amount_metadata': {'without_vat': str(raw.get('totalCostWithoutVAT')), 'with_vat': str(raw.get('totalCostWithVAT')), 'source': source_ref(metadata_path, reference_number=cid)},
            'scope': {'matched_research_offices': [office for office in ('Αιγάλεω', 'Μεγάρων') if office in summaries[cid]['forest_offices']],
                'selection_note_raw': summaries[cid]['forest_offices'],
                'geographic_caveat': 'This supplement concerns Chalkida, retained for the parent chain.' if cid == '26SYMV018978343' else None,
                'full_scope_descriptions': [o.get('shortDescription') for o in raw.get('objectDetailsList', [])],
                'west_attica_allocated_amount': None,
                'note': 'Matched offices are a selection aid, not the full geographic scope. Do not attribute the full amount to West Attica.'},
            'reference_aliases': ['24SYMV0142117833'] if cid == '24SYMV014217833' else [],
            'metadata_chain': {'previous': raw.get('prevReferenceNo'), 'next': raw.get('nextRefNo')},
            'report_profile': profile,
            'finding_ids': [f['id'] for f in findings if cid in f['contract_ids']],
            'decision_ids': linked,
            'url': f'https://cerpp.eprocurement.gov.gr/khmdhs-opendata/contract/attachment/{cid}',
            'pdf_path': source_ref(SOURCE / 'contracts' / f'{cid}.pdf')['path'],
            'text_path': source_ref(SOURCE / 'contracts_text' / f'{cid}.txt')['path'],
            'metadata_source': source_ref(metadata_path, reference_number=cid)})
    for child, (parent, kind) in AMENDMENTS.items():
        relationships.append({'id': f'amendment:{child}:{parent}', 'type': 'amends_contract',
            'source': child, 'target': parent, 'amendment_kind': kind, 'evidence': profiles[child]['source']})
    sources = [ROOT / 'scripts/reports/antinero-west-attica/classify_decisions.py', metadata_path, inventory_path, report_path, SOURCE/'README.md', SOURCE/'KHMDHS_2022_PLUS.md', SOURCE/'khmdhs_2022_plus/relevant_contracts.tsv']
    data = {'schema_version': '1.1.0', 'id': 'antinero-west-attica', 'research_cutoff': '2026-08-12',
        'search_start': '2022-01-01', 'language': 'el',
        'editorial_status': 'primary_source_review_complete_editorial_integration_pending',
        'finding_review': {'date': review_bundle['review_date'], 'method': review_bundle['method'],
            'status_counts': dict(Counter(r['status'] for r in reviews.values()))},
        'sources': [{**source_ref(p), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in [*sources, review_path]],
        'limitations': [
            'Frozen local research snapshot, not a live database inventory.',
            '21 contract records include amendments; they are not 21 independent projects.',
            'Report findings retain narrative dates and scope; no unverified event dates are inferred.',
            'Decision issue dates are not necessarily execution, installation or payment dates.',
            'Missing decision matches do not establish absence of administrative activity.',
            'Original report text is preserved, not approved publication copy. Apply review notes and corrections before display.',
            'Review uses archived extracted texts with whole-document citations, not visual PDF or live registry validation.',
            'Contract profile fields and all 426 decisions have not each received a comprehensive field-level audit.',
            'The report shorthand for acceptance of 24SYMV014192335 must be read with decision ΡΩ5Π4653Π8-Ο4Χ: partial acceptance excluding planting.',
            'Supplement 26SYMV018978343 concerns Chalkida; it is retained to preserve the parent contract chain.',
        ], 'contracts': sorted(contracts, key=lambda c: c['id']),
        'decisions': sorted(decisions.values(), key=lambda d: (d['issued_on'], d['id'])),
        'relationships': sorted(relationships, key=lambda e: e['id']), 'findings': findings}
    validate(data)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(data, ensure_ascii=False, indent=2)+'\n')
    return data


def validate(data):
    contracts = {c['id']: c for c in data['contracts']}
    decisions = {d['id']: d for d in data['decisions']}
    assert len(contracts) == len(data['contracts']) == 21
    assert len(decisions) == len(data['decisions']) == 426
    assert len(data['relationships']) == len({e['id'] for e in data['relationships']}) == 433
    assert Counter(e['type'] for e in data['relationships']) == {'references_contract': 429, 'amends_contract': 4}
    assert contracts['24SYMV015170089']['amount']['without_vat'] == '799483.29'
    assert contracts['26SYMV018978343']['amount']['basis'] == 'additional_amount'
    assert contracts['23SYMV013154974']['amount']['without_vat'] is None
    assert '24SYMV0142117833' not in contracts
    assert sum(len(d['contract_ids']) for d in decisions.values()) == 429
    for edge in data['relationships']:
        assert edge['target'] in contracts
        assert edge['source'] in (decisions if edge['type'] == 'references_contract' else contracts)
    for node in [*contracts.values(), *decisions.values()]:
        for field in ('pdf_path', 'text_path'):
            assert (ROOT / node[field]).is_file(), node[field]
    for contract in contracts.values():
        assert contract['finding_ids'], contract['id']
        assert all(cid in decisions for cid in contract['decision_ids'])
    for finding in data['findings']:
        assert finding['contract_ids'] and all(cid in contracts for cid in finding['contract_ids'])
        assert all(ada in decisions for ada in finding['explicit_decision_ids'])

if __name__ == '__main__':
    dataset = build()
    print(json.dumps({'contracts': len(dataset['contracts']), 'decisions': len(dataset['decisions']),
        'relationships': len(dataset['relationships']), 'findings': len(dataset['findings']),
        'citation_status': dict(Counter(f['primary_citation_status'] for f in dataset['findings']))}, indent=2))
