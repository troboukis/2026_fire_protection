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

function yearBounds(year: number): { yearStart: string; yearEnd: string } {
  return {
    yearStart: `${year}-01-01`,
    yearEnd: `${year}-12-31`,
  }
}

function getContractYearDefinitionFields(contract: ContractWindowFields): {
  signedDate: string | null
  endDate: string | null
  hasExplicitEndDate: boolean
} {
  const signedDate = cleanDateString(contract.contract_signed_date)
  const endDate = cleanDateString(contract.end_date)
  const hasExplicitEndDate = !contract.no_end_date && Boolean(endDate)
  return {
    signedDate,
    endDate,
    hasExplicitEndDate,
  }
}

export function isContractActiveInYear(contract: ContractWindowFields, year: number): boolean {
  const { yearStart, yearEnd } = yearBounds(year)
  const { signedDate, endDate, hasExplicitEndDate } = getContractYearDefinitionFields(contract)
  if (!signedDate) return false
  if (signedDate >= yearStart && signedDate <= yearEnd) return true
  return signedDate < yearStart && hasExplicitEndDate && Boolean(endDate) && (endDate as string) >= yearStart
}

export function getContractYearAnchorDate(contract: ContractWindowFields, year: number): string | null {
  const { yearStart, yearEnd } = yearBounds(year)
  const { signedDate, endDate, hasExplicitEndDate } = getContractYearDefinitionFields(contract)
  if (!signedDate) return null
  if (signedDate >= yearStart && signedDate <= yearEnd) return signedDate
  if (signedDate < yearStart && hasExplicitEndDate && Boolean(endDate) && (endDate as string) >= yearStart) {
    return yearStart
  }
  return null
}

export function isContractActiveOnDate(contract: ContractWindowFields, date: string): boolean {
  const cleanDate = cleanDateString(date)
  if (!cleanDate) return false
  const year = Number(cleanDate.slice(0, 4))
  if (!Number.isFinite(year)) return false
  return isContractActiveInYear(contract, year)
}
