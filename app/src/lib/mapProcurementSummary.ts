import { isContractActiveInYear } from './contractWindow'

export type MapAuthorityScope = 'municipality' | 'region' | 'decentralized' | 'national' | 'other'

export type ProcurementMapRow = {
  municipality_key: string | null
  contract_signed_date?: string | null
  start_date?: string | null
  end_date?: string | null
  no_end_date?: boolean | null
  organization_key?: string | null
  canonical_owner_scope?: 'municipality' | 'region' | 'organization' | null
  cancelled?: boolean | null
  next_ref_no?: string | null
  prev_reference_no?: string | null
  reference_number?: string | null
}

function cleanText(value: unknown): string | null {
  if (value == null) return null
  const text = String(value).trim()
  return text ? text : null
}

function normalizeMunicipalityId(input: unknown): string {
  const s = String(input ?? '').trim()
  if (!s) return ''
  const noDecimal = s.replace(/\.0+$/, '')
  if (/^\d+$/.test(noDecimal)) return String(Number(noDecimal))
  return noDecimal
}

function getAuthorityScope(
  row: ProcurementMapRow,
  orgScopeByKey: Map<string, MapAuthorityScope>,
): MapAuthorityScope {
  if (row.canonical_owner_scope === 'municipality') return 'municipality'
  const orgKey = cleanText(row.organization_key)
  if (!orgKey) return 'other'
  return orgScopeByKey.get(orgKey) ?? 'other'
}

export function buildMunicipalityMapSummary(
  rows: ProcurementMapRow[],
  mapYear: number,
  orgScopeByKey: Map<string, MapAuthorityScope>,
): {
  countByMunicipality: Map<string, number>
  procurementRowsForYear: number
  nationalTotalCount: number
} {
  const supersededReferenceNumbers = new Set<string>()
  for (const row of rows) {
    const previousReference = cleanText(row.prev_reference_no)
    if (previousReference) supersededReferenceNumbers.add(previousReference)
  }

  const countByMunicipality = new Map<string, number>()
  let procurementRowsForYear = 0

  for (const row of rows) {
    if (row.cancelled) continue
    if (cleanText(row.next_ref_no)) continue

    const referenceNumber = cleanText(row.reference_number)
    if (referenceNumber && supersededReferenceNumbers.has(referenceNumber)) continue
    if (!isContractActiveInYear(row, mapYear)) continue
    if (getAuthorityScope(row, orgScopeByKey) !== 'municipality') continue

    const municipalityId = normalizeMunicipalityId(row.municipality_key)
    if (!municipalityId) continue

    procurementRowsForYear += 1
    countByMunicipality.set(municipalityId, (countByMunicipality.get(municipalityId) ?? 0) + 1)
  }

  const nationalTotalCount = Array.from(countByMunicipality.values()).reduce((sum, count) => sum + count, 0)
  return {
    countByMunicipality,
    procurementRowsForYear,
    nationalTotalCount,
  }
}
