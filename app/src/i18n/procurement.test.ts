import { createElement, type ReactNode } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { act, create, type ReactTestRenderer } from 'react-test-renderer'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import I18nProvider from './I18nProvider'
import { useProcurementI18n } from './useProcurementI18n'
import { createFormatters } from './format'
import type { AppLanguage } from './languages'
import greek from './catalogs/procurement.el.json'
import english from './catalogs/procurement.en.json'
import ContractsPage from '../pages/ContractsPage'
import DiavgeiaPage from '../pages/DiavgeiaPage'
import ContractModal, { type ContractModalContract } from '../components/ContractModal'
import LatestContractCard from '../components/LatestContractCard'
import DiavgeiaDecisionCard from '../components/DiavgeiaDecisionCard'
import { buildDiavgeiaDecisionCardView } from '../lib/diavgeiaDecision'

vi.mock('../lib/supabase', () => ({ supabase: {} }))
vi.mock('../lib/devView', () => ({ DEV_VIEW_AVAILABLE: false, useDevViewEnabled: () => [false], useDevView: () => ({ enabled: false }) }))

const title = 'Πρωτότυπος τίτλος: «Πυροπροστασία 2026»'
const raw = { submittedAt: '2026-05-01', signedAt: '2026-05-02', startDate: '2026-06-01', endDate: null, withoutVat: 1234, withVat: 1530.16 }
const contract = {
  id: '1', what: title, who: 'Ελληνικός φορέας', beneficiary: 'Ελληνικός ανάδοχος',
  beneficiaryGemi: '123456', contractType: 'Απευθείας ανάθεση', contractKind: 'Υπηρεσίες',
  referenceNumber: '26SYMV012345678', contractRelatedAda: 'Ψ123ΩΑ-ΒΓΔ', sourceValues: raw,
  when: '01 Μαΐ 2026', signedAt: '02 Μαΐ 2026', startDate: '01 Ιουν 2026', endDate: '—',
  withoutVatAmount: '1.234 €', withVatAmount: '1.530 €', howMuch: '1.234 €', why: 'Πρωτότυπη περιγραφή',
  cpv: 'Πρωτότυπη κατηγορία', cpvCode: '12345678-9',
  contractNumber: '1', organizationVat: '123456789', beneficiaryVat: '987654321',
  signers: 'Υπογράφων', assignCriteria: '—', awardProcedure: 'Απευθείας ανάθεση',
  unitsOperator: 'Τμήμα πηγής', fundingCofund: '—', fundingSelf: '—', fundingEspa: '—', fundingRegular: '—',
  auctionRefNo: '123', paymentRefNo: '456', shortDescription: 'Πρωτότυπη περιγραφή', rawBudget: '—', contractBudget: '—',
  previousReferenceNumber: '—', nextReferenceNumber: '—', documentUrl: null,
} satisfies ContractModalContract

function page(language: AppLanguage, children: ReactNode) {
  return createElement(I18nProvider, { language, children: createElement(MemoryRouter, {
    initialEntries: ['/contracts?q=%CF%80%CF%85%CF%81&dateFrom=2026-01-01&dateTo=2026-11-17&minAmount=1234.50'],
  }, children) })
}
function render(language: AppLanguage, children: ReactNode) {
  return renderToStaticMarkup(page(language, children))
}

describe('reviewed procurement resources', () => {
  it('covers every Greek message and preserves interpolation tokens and numeric references', () => {
    expect(Object.keys(english).sort()).toEqual(Object.keys(greek).sort())
    for (const key of Object.keys(greek) as Array<keyof typeof greek>) {
      expect(english[key].match(/\{\{[^}]+\}\}/g)?.sort() ?? [], key).toEqual(greek[key].match(/\{\{[^}]+\}\}/g)?.sort() ?? [])
      expect(english[key].replace(/\{\{[^}]+\}\}/g, '').match(/\d+/g) ?? [], key).toEqual(greek[key].replace(/\{\{[^}]+\}\}/g, '').match(/\d+/g) ?? [])
      expect(english[key], key).not.toMatch(/[Α-Ωα-ω]/)
    }
  })

  it.each(['el', 'en'] as const)('keeps the original search and numeric filter values in %s', language => {
    const html = render(language, createElement(ContractsPage))
    expect(html).toContain('value="πυρ"')
    expect(html).toContain('value="1234.50"')
    expect(html).toContain('value="2026-01-01"')
    expect(html).toContain(language === 'en' ? 'All contracts' : 'Όλες οι Συμβάσεις')
    expect(html).toContain(language === 'en' ? 'Minimum amount (excluding VAT)' : 'Ελάχιστο ποσό (χωρίς ΦΠΑ)')
  })

  it('translates the default decision label while retaining the Greek selected value', () => {
    const html = render('en', createElement(DiavgeiaPage))
    expect(html).toContain('value="ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ" selected="">Payment finalisation</option>')
    expect(html).toContain('Exclude amounts already counted in contracts')
    expect(html).not.toMatch(/procurement:|undefined|NaN/)
  })
})

describe('original titles, identifiers and financial meaning', () => {
  it.each(['el', 'en'] as const)('retains the exact Greek contract title and identifiers in the %s modal', language => {
    const html = render(language, createElement(ContractModal, { contract, onClose: () => {} }))
    expect(html).toContain(`<h2 lang="el">${title}</h2>`)
    expect(html).toContain(contract.referenceNumber)
    expect(html).toContain(contract.contractRelatedAda)
    expect(html).toContain(contract.cpvCode)
    const format = createFormatters(language)
    expect(html).toContain(`<span class="contract-modal__amount">${format.currency(raw.withoutVat, { maximumFractionDigits: 0 })}</span>`)
    expect(html).toContain(format.currency(raw.withVat, { maximumFractionDigits: 0 }))
    expect(html).toContain(format.date(raw.signedAt))
  })

  it('translates card actions and procedure while preserving its Greek title, beneficiary and GEMI URL', () => {
    const html = render('en', createElement(LatestContractCard, { item: { ...contract, documentUrl: 'https://example.test/source.pdf' } }))
    expect(html).toContain(`lang="el" title="${title}">${title}</h2>`)
    expect(html).toContain('Open document')
    expect(html).toContain('Direct award')
    expect(html).toContain('Signed: 02 May 2026')
    expect(html).toContain('€1,234')
    expect(html).toContain('https://publicity.businessportal.gr/company/123456')
    expect(html).toContain('Open GEMI for Ελληνικός ανάδοχος')
    expect(html).toContain('https://example.test/source.pdf')
  })

  it('keeps a decision subject/ADA untouched and uses the same euro amount in the English card', () => {
    const item = buildDiavgeiaDecisionCardView({
      diavgeia_id: 1, subject: title, ada: 'Ψ123ΩΑ-ΒΓΔ', org_type: 'ΔΗΜΟΣ', org_name_clean: 'Δοκιμής',
      decision_date: '2026-05-02', diavgeia_document_type_decision_uid: 'ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ',
      spending_contractors_value: '1234.00', document_url: null,
    })
    const html = render('en', createElement(DiavgeiaDecisionCard, { item }))
    expect(html).toContain(`lang="el" title="${title}">${title}</h2>`)
    expect(html).toContain(item.ada)
    expect(html).toContain('€1,234')
    expect(html).toContain('02 May 2026')
    expect(html).toContain('Payment finalisation')
    expect(html).toContain('Open decision')
    expect(item.subject).toBe(title)
    expect(item.amountEur).toBe(1234)
  })
})

describe('language changes within a mounted modal', () => {
  let tree: ReactTestRenderer | undefined
  afterEach(() => { if (tree) act(() => tree!.unmount()); tree = undefined })

  it('reformats supplied source values without refetching or modifying the selected record', () => {
    const original = structuredClone(contract)
    const modal = () => createElement(ContractModal, { contract, onClose: () => {} })
    act(() => { tree = create(page('el', modal())) })
    act(() => tree!.update(page('en', modal())))
    expect(tree!.root.findByType('h2').children).toEqual([title])
    expect(tree!.root.findByType('h2').props.lang).toBe('el')
    expect(JSON.stringify(tree!.toJSON())).toContain('€1,234')
    expect(JSON.stringify(tree!.toJSON())).toContain('02 May 2026')
    act(() => tree!.update(page('el', modal())))
    expect(JSON.stringify(tree!.toJSON())).toContain('1.234')
    expect(contract).toEqual(original)
  })

  it('updates CPV/category presentation in an open modal while retaining the selected source record', () => {
    const selected = { ...contract, who: 'ΔΗΜΟΣ ΜΑΡΑΘΩΝΟΣ', why: 'Εξοπλισμός πυρόσβεσης',
      cpv: 'Εξοπλισμός πυρόσβεσης', cpvCode: '35110000-8', awardProcedure: 'Άγονη διαδικασία ανάθεσης σύμβασης' }
    const original = structuredClone(selected)
    const modal = () => createElement(ContractModal, { contract: selected, onClose: () => {} })
    act(() => { tree = create(page('el', modal())) })
    act(() => tree!.update(page('en', modal())))
    const english = JSON.stringify(tree!.toJSON())
    expect(english).toContain('Firefighting, rescue and safety equipment (35110000-8)')
    expect(english).toContain('Fire-fighting equipment')
    expect(english).toContain('Unsuccessful contract award procedure')
    expect(english).toContain('Municipality of Marathon')
    expect(tree!.root.findByType('h2').children).toEqual([title])
    act(() => tree!.update(page('el', modal())))
    expect(JSON.stringify(tree!.toJSON())).toContain('Εξοπλισμός πυρόσβεσης (35110000-8)')
    expect(selected).toEqual(original)
  })

  it('maps only reviewed categories and preserves unknown source labels', () => {
    function Probe() {
      const { category, periodLabel } = useProcurementI18n()
      return createElement('p', null, [category('ΑΠΕΥΘΕΙΑΣ ΑΝΑΘΕΣΗ'), category('Άγνωστη διαδικασία'), periodLabel('2026-05-02', '')].join(' | '))
    }
    expect(render('en', createElement(Probe))).toContain('Direct award | Άγνωστη διαδικασία | From 02 May 2026')
    expect(render('el', createElement(Probe))).toContain('ΑΠΕΥΘΕΙΑΣ ΑΝΑΘΕΣΗ | Άγνωστη διαδικασία | Από 02 Μαΐ 2026')
  })
})
