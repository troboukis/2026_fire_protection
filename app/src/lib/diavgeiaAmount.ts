const MISSING_AMOUNT_TEXT = new Set(['', 'nan', 'none', 'null', 'nat', '<na>'])

function parseScalarAmountCents(value: string | number | null | undefined): number | null {
  if (value == null) return null
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return null
    const cents = Math.sign(value) * Math.round((Math.abs(value) + Number.EPSILON) * 100)
    return Number.isSafeInteger(cents) ? cents : null
  }

  const text = value.trim()
  if (MISSING_AMOUNT_TEXT.has(text.toLowerCase())) return null

  // Greek amounts may group thousands with dots; PDF fallback amounts use a decimal dot.
  const match = text.match(/^([+-]?)(\d{1,3}(?:\.\d{3})+|\d+)(?:,(\d{1,2}))?$/)
    ?? text.match(/^([+-]?)(\d+)\.(\d{1,2})$/)
  if (!match) return null

  const whole = Number(match[2].replace(/\./g, ''))
  const fraction = Number((match[3] ?? '').padEnd(2, '0'))
  const cents = (match[1] === '-' ? -1 : 1) * (whole * 100 + fraction)
  return Number.isSafeInteger(cents) ? cents : null
}

export function parseDiavgeiaAmountCents(value: string | number | null | undefined): number | null {
  if (typeof value !== 'string' || !value.trim().startsWith('[')) {
    return parseScalarAmountCents(value)
  }

  // Multiple beneficiaries are stored as a Python list representation or a JSON array.
  let items: unknown
  try {
    items = JSON.parse(value.replace(/'/g, '"'))
  } catch {
    return null
  }
  if (!Array.isArray(items) || items.length === 0) return null

  let totalCents = 0
  for (const item of items) {
    if (typeof item !== 'string' && typeof item !== 'number') return null
    const cents = parseScalarAmountCents(item)
    if (cents == null) return null
    totalCents += cents
    if (!Number.isSafeInteger(totalCents)) return null
  }
  return totalCents
}

export function summarizeDiavgeiaAmounts(
  rows: readonly { spending_contractors_value: string | null }[],
): { totalCents: number; missingCount: number } {
  let totalCents = 0
  let missingCount = 0
  for (const row of rows) {
    const cents = parseDiavgeiaAmountCents(row.spending_contractors_value)
    if (cents == null) missingCount += 1
    else totalCents += cents
  }
  return { totalCents, missingCount }
}
