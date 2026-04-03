export type PaymentSummaryRow = {
  beneficiary_name?: string | null
  beneficiary_vat_number?: string | null
  signers?: string | null
  payment_ref_no?: string | null
  amount_without_vat?: number | string | null
  amount_with_vat?: number | string | null
}

function cleanText(value: unknown): string | null {
  if (value == null) return null
  const text = String(value).trim()
  if (!text || text.toLowerCase() === 'nan' || text.toLowerCase() === 'none') return null
  return text
}

function toFiniteNumber(value: unknown): number | null {
  if (value == null || value === '') return null
  const numberValue = Number(value)
  return Number.isFinite(numberValue) ? numberValue : null
}

function splitPipeDelimited(value: unknown): string[] {
  const text = cleanText(value)
  if (!text) return []
  return text
    .split('|')
    .map((part) => cleanText(part))
    .filter((part): part is string => Boolean(part))
}

function joinDistinctValues(rows: PaymentSummaryRow[], pick: (row: PaymentSummaryRow) => unknown): string | null {
  const values: string[] = []
  const seen = new Set<string>()
  for (const row of rows) {
    for (const value of splitPipeDelimited(pick(row))) {
      if (seen.has(value)) continue
      seen.add(value)
      values.push(value)
    }
  }
  return values.join(' | ') || null
}

function sumValues(rows: PaymentSummaryRow[], pick: (row: PaymentSummaryRow) => unknown): number | null {
  let total: number | null = null
  for (const row of rows) {
    const value = toFiniteNumber(pick(row))
    if (value == null) continue
    total = (total ?? 0) + value
  }
  return total
}

export function summarizePaymentRows(rows: PaymentSummaryRow[]) {
  return {
    beneficiary_name: joinDistinctValues(rows, (row) => row.beneficiary_name),
    beneficiary_vat_number: joinDistinctValues(rows, (row) => row.beneficiary_vat_number),
    signers: joinDistinctValues(rows, (row) => row.signers),
    payment_ref_no: joinDistinctValues(rows, (row) => row.payment_ref_no),
    amount_without_vat: sumValues(rows, (row) => row.amount_without_vat),
    amount_with_vat: sumValues(rows, (row) => row.amount_with_vat),
  }
}
