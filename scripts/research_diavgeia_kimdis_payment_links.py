"""Research explicit payment-to-contract references without changing the database.

Input snapshots: decisions.json, procurements.json, raw_payment_metadata.json,
and pdf-read/extraction.json from the PDF collector. Outputs preserve evidence
and distinguish exact contract references from related-decision candidates.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import json
from pathlib import Path
import re

CONTRACT_REF = re.compile(r'(?<![A-ZΑ-Ω0-9])\d{2}\s*S\s*[YΥ]\s*[MΜ]\s*V\s*\d(?:\s*\d){8}(?!\d)', re.IGNORECASE)
ADA_REF = re.compile(r'(?<![A-ZΑ-Ω0-9])[A-ZΑ-Ω0-9]{4,12}-[A-ZΑ-Ω0-9]{2,5}(?![A-ZΑ-Ω0-9])')


def contract_references(text):
    for match in CONTRACT_REF.finditer(text):
        ref = re.sub(r'\s+', '', match[0]).upper().replace('Υ', 'Y').replace('Μ', 'M')
        yield ref, match.start(), match.end()


def build_report(directory: Path):
    decisions = json.loads((directory / 'decisions.json').read_text())
    procurements = json.loads((directory / 'procurements.json').read_text())
    metadata = json.loads((directory / 'raw_payment_metadata.json').read_text())
    pdf_rows = json.loads((directory / 'pdf-read' / 'extraction.json').read_text())
    contracts = {row['reference_number']: row for row in procurements}
    by_ada = defaultdict(set)
    for contract in procurements:
        for field in ('diavgeia_ada', 'decision_related_ada', 'contract_related_ada'):
            for ada in ADA_REF.findall(contract.get(field) or ''):
                by_ada[ada].add((contract['reference_number'], field))
    pdfs = {row['ada']: row for row in pdf_rows}
    direct, candidates, coverage, shared = [], [], [], []
    for decision in decisions:
        ada = decision['ada']
        found, related = defaultdict(list), defaultdict(list)
        raw = metadata.get(ada, {})
        for field in ('subject', 'payment_related_commitment_or_spending', 'payment_see_also'):
            text = str(raw.get(field) or '')
            for ref, start, end in contract_references(text):
                found[ref].append({'source': field, 'page': None, 'evidence': text[max(0,start-180):end+250]})
            if field != 'subject':
                for related_ada in set(ADA_REF.findall(text)) - {ada}:
                    related[related_ada].append({'source': field, 'page': None, 'evidence': text})
        pdf = pdfs.get(ada, {})
        text = ''
        if pdf.get('text_path'):
            text = Path(pdf['text_path']).read_text()
            for number, page in enumerate(text.split('\f'), 1):
                for ref, start, end in contract_references(page):
                    found[ref].append({'source': 'pdf', 'page': number, 'evidence': page[max(0,start-180):end+250].strip()})
                for match in ADA_REF.finditer(page):
                    if match[0] != ada and match[0] in by_ada:
                        related[match[0]].append({'source': 'pdf', 'page': number, 'evidence': page[max(0,match.start()-180):match.end()+250].strip()})
        common = {'payment_ada': ada, 'payment_subject': decision['subject'],
                  'payment_organization': decision['org'],
                  'payment_document_url': decision['document_url'],
                  'decision_total_amount': decision['spending_contractors_value'],
                  'payment_publish_timestamp': decision['publish_timestamp'],
                  'pdf_sha256': pdf.get('pdf_sha256'),
                  'multiple_contract_references': len(found) > 1,
                  'aggregate_payment_schedule': 'ΚΑΤΑΣΤΑΣΗ ΟΡΙΣΤΙΚΟΠΟΙΗΣΗΣ ΠΛΗΡΩΜΩΝ' in text.upper()}
        for ref, evidence in sorted(found.items()):
            contract = contracts.get(ref)
            best = next((e for e in evidence if e['source']=='pdf'), evidence[0])
            direct.append({**common, 'contract_reference': ref,
                           'contract_in_database': contract is not None,
                           'contract_title': contract['title'] if contract else None,
                           'contract_budget': contract['contract_budget'] if contract else None,
                           'contract_cancelled': contract['cancelled'] if contract else None,
                           'contract_municipality_key': contract['municipality_key'] if contract else None,
                           'evidence_source': best['source'], 'page': best['page'],
                           'evidence': best['evidence'], 'all_evidence': evidence})
        for related_ada, evidence in sorted(related.items()):
            sharing = len({r for r, _ in by_ada[related_ada]})
            metadata_reference = any(e['source'] != 'pdf' for e in evidence)
            if sharing > 1 and not metadata_reference:
                shared.append({**common, 'related_ada': related_ada,
                               'contracts_sharing_related_ada': sharing,
                               'contract_references': sorted({r for r, _ in by_ada[related_ada]}),
                               'evidence': evidence[0]['evidence'],
                               'reason': 'Shared decision is insufficient to identify a specific contract'})
                continue
            for ref, contract_field in sorted(by_ada.get(related_ada, ())):
                if ref in found:
                    continue
                contract = contracts[ref]
                candidates.append({**common, 'contract_reference': ref, 'contract_title': contract['title'],
                                   'related_ada': related_ada, 'contract_ada_field': contract_field,
                                   'contracts_sharing_related_ada': sharing,
                                   'evidence_source': evidence[0]['source'], 'page': evidence[0]['page'],
                                   'evidence': evidence[0]['evidence']})
        coverage.append({'payment_ada': ada, 'payment_subject': decision['subject'],
                         'pdf_read': bool(pdf.get('text_path')), 'pdf_error': pdf.get('error'),
                         'exact_contract_references': len(found),
                         'known_contract_references': sum(ref in contracts for ref in found),
                         'related_contract_decision_candidates': sum(len(by_ada[a]) for a in related)})
    def save(name, rows):
        (directory / (name + '.json')).write_text(json.dumps(rows, ensure_ascii=False, indent=2))
        if rows:
            fields = [key for key in rows[0] if key != 'all_evidence']
            with (directory / (name+'.csv')).open('w',encoding='utf-8-sig',newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=fields, extrasaction='ignore')
                writer.writeheader(); writer.writerows(rows)
    save('exact_contract_references', direct)
    save('related_decision_candidates', candidates)
    save('shared_decision_references', shared)
    save('coverage', coverage)
    known = [row for row in direct if row['contract_in_database']]
    summary = {'payment_decisions_examined': len(decisions),
               'pdfs_read': sum(r['pdf_read'] for r in coverage),
               'exact_reference_payment_decisions': len({r['payment_ada'] for r in direct}),
               'exact_reference_pairs': len(direct),
               'known_contract_payment_decisions': len({r['payment_ada'] for r in known}),
               'known_contract_pairs': len(known),
               'known_contracts': len({r['contract_reference'] for r in known}),
               'external_contracts': len({r['contract_reference'] for r in direct if not r['contract_in_database']}),
               'related_decision_candidate_payments': len({r['payment_ada'] for r in candidates}),
               'related_decision_candidate_pairs': len({(r['payment_ada'],r['contract_reference']) for r in candidates}),
               'candidate_only_payments': len({r['payment_ada'] for r in candidates}-{r['payment_ada'] for r in direct}),
               'shared_decision_only_evidence_payments': len({r['payment_ada'] for r in shared}-{r['payment_ada'] for r in direct+candidates}),
               'payments_without_identifier_match': len(set(r['ada'] for r in decisions)-{r['payment_ada'] for r in direct+candidates}),
               'database_changed': False}
    (directory / 'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2))
    print(json.dumps(summary,ensure_ascii=False,indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory',type=Path,required=True)
    build_report(parser.parse_args().directory)
