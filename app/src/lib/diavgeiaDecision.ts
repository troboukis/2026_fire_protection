import type { DiavgeiaDecisionCardView } from '../components/DiavgeiaDecisionCard'

export type MunicipalityDiavgeiaDecisionRpcRow = {
  diavgeia_id: number | string
  org_type: string | null
  org_name_clean: string | null
  subject: string | null
  decision_date: string | null
  ada: string | null
  diavgeia_document_type_decision_uid: string | null
  spending_contractors_value: string | null
  document_url: string | null
}

function cleanText(value: unknown): string | null {
  if (value == null) return null
  const text = String(value).trim()
  return text ? text : null
}

export function formatDiavgeiaDecisionDate(value: string | null | undefined): string {
  const text = cleanText(value)
  if (!text) return '—'
  const dt = new Date(text)
  if (Number.isNaN(dt.getTime())) return '—'
  return dt.toLocaleDateString('el-GR', { day: '2-digit', month: 'short', year: 'numeric' })
}

function parseGreekAmount(value: string | null | undefined): number | null {
  const text = cleanText(value)
  if (!text) return null
  const match = text.match(/\d[\d.,]*/)
  if (!match) return null
  const normalized = match[0].includes(',')
    ? match[0].replace(/\./g, '').replace(',', '.')
    : match[0]
  const amount = Number(normalized)
  return Number.isFinite(amount) && amount > 0 ? amount : null
}

function formatEur(value: string | null | undefined): string | null {
  const amount = parseGreekAmount(value)
  if (amount == null) return null
  return amount.toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 })
}

export function buildDiavgeiaDecisionCardView(row: MunicipalityDiavgeiaDecisionRpcRow): DiavgeiaDecisionCardView {
  const ada = cleanText(row.ada)
  return {
    id: `diavgeia-${row.diavgeia_id}`,
    orgType: cleanText(row.org_type) ?? '—',
    orgNameClean: cleanText(row.org_name_clean) ?? '—',
    subject: cleanText(row.subject) ?? '—',
    when: formatDiavgeiaDecisionDate(row.decision_date),
    ada: ada ?? '—',
    decisionTypeUid: cleanText(row.diavgeia_document_type_decision_uid) ?? '—',
    amount: formatEur(row.spending_contractors_value),
    documentUrl: cleanText(row.document_url) ?? (ada ? `https://diavgeia.gov.gr/doc/${ada}` : null),
    sortDate: cleanText(row.decision_date),
  }
}
