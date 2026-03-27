export type ContractWindowFields = {
  contract_signed_date?: string | null
  start_date?: string | null
  end_date?: string | null
  no_end_date?: boolean | null
}

function cleanDateString(value: unknown): string | null {
  if (value == null) return null
  const text = String(value).trim()
  if (!text) return null
  const isoMatch = text.match(/^(\d{4}-\d{2}-\d{2})/)
  if (isoMatch) return isoMatch[1]
  const parsed = new Date(text)
  if (Number.isNaN(parsed.getTime())) return null
  return parsed.toISOString().slice(0, 10)
}

export function getContractEffectiveWindow(contract: ContractWindowFields): {
  signedDate: string | null
  startDate: string | null
  endDate: string | null
  effectiveStart: string | null
  effectiveEnd: string | null
} {
  const signedDate = cleanDateString(contract.contract_signed_date)
  const startDate = cleanDateString(contract.start_date) ?? signedDate
  const endDate = cleanDateString(contract.end_date)
  const effectiveStart = startDate ?? signedDate
  if (!effectiveStart) {
    return {
      signedDate,
      startDate,
      endDate,
      effectiveStart: null,
      effectiveEnd: null,
    }
  }
  return {
    signedDate,
    startDate,
    endDate,
    effectiveStart,
    effectiveEnd: contract.no_end_date ? '9999-12-31' : (endDate ?? effectiveStart),
  }
}

export function isContractActiveInYear(contract: ContractWindowFields, year: number): boolean {
  const yearStart = `${year}-01-01`
  const yearEnd = `${year}-12-31`
  const { effectiveStart, endDate } = getContractEffectiveWindow(contract)
  if (!effectiveStart || !endDate) return false
  return effectiveStart <= yearEnd && endDate >= yearStart
}

export function getContractYearAnchorDate(contract: ContractWindowFields, year: number): string | null {
  const yearStart = `${year}-01-01`
  const yearEnd = `${year}-12-31`
  const { signedDate, effectiveStart, effectiveEnd } = getContractEffectiveWindow(contract)
  if (!effectiveStart || !effectiveEnd) return null
  if (effectiveStart > yearEnd || effectiveEnd < yearStart) return null
  if (signedDate && signedDate >= yearStart && signedDate <= yearEnd) return signedDate
  return effectiveStart < yearStart ? yearStart : effectiveStart
}

export function isContractActiveOnDate(contract: ContractWindowFields, date: string): boolean {
  const cleanDate = cleanDateString(date)
  const { effectiveStart, endDate } = getContractEffectiveWindow(contract)
  if (!cleanDate || !effectiveStart || !endDate) return false
  return effectiveStart <= cleanDate && endDate >= cleanDate
}
