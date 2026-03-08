import { describe, expect, it } from 'vitest'

import { buildCpvHtml, buildPdfTemplate, escapeHtml, type ContractPdfData } from './contractPdf'

const baseContract: ContractPdfData = {
  id: '1',
  who: 'ΔΗΜΟΣ ΔΟΚΙΜΗΣ',
  what: 'Καθαρισμοί <χώρων>',
  when: '2026',
  why: 'Πρόληψη & προστασία',
  beneficiary: 'Ανάδοχος',
  contractType: 'Υπηρεσία',
  withoutVatAmount: '1.000 EUR',
  withVatAmount: '1.240 EUR',
  referenceNumber: '26SYMV000000001',
  contractNumber: 'C-1',
  cpv: 'Υπηρεσίες πρόληψης πυρκαγιών',
  cpvCode: '75251110-4',
  signedAt: '2026-03-08',
  startDate: '2026-03-09',
  endDate: '2026-03-31',
  organizationVat: '123456789',
  beneficiaryVat: '987654321',
  shortDescription: 'Αποψίλωση & καθαρισμός',
}

describe('contractPdf helpers', () => {
  it('escapes html-sensitive characters', () => {
    expect(escapeHtml('<b>"hi"&\'</b>')).toBe('&lt;b&gt;&quot;hi&quot;&amp;&#39;&lt;/b&gt;')
  })

  it('renders cpv items when available', () => {
    expect(buildCpvHtml({
      ...baseContract,
      cpvItems: [
        { code: '75251110-4', label: 'Υπηρεσίες πρόληψης πυρκαγιών' },
        { code: '77314000-4', label: 'Υπηρεσίες συντήρησης οικοπέδων' },
      ],
    })).toContain('<br/>')
  })

  it('builds escaped printable html template', () => {
    const html = buildPdfTemplate(baseContract)
    expect(html).toContain('Project ΠΥΡ')
    expect(html).toContain('&lt;χώρων&gt;')
    expect(html).toContain('Πρόληψη &amp; προστασία')
    expect(html).toContain('75251110-4')
  })
})
