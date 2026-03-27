export type ContractAuthorityScope = 'municipality' | 'region' | 'decentralized' | 'national' | 'other'

function cleanText(value: unknown): string | null {
  if (value == null) return null
  const text = String(value).trim()
  return text ? text : null
}

function ensurePrefix(value: string | null, prefix: string): string | null {
  const text = cleanText(value)
  if (!text) return null
  return text.startsWith(prefix) ? text : `${prefix} ${text}`
}

export function formatMunicipalityAuthorityLabel(value: string | null | undefined): string | null {
  return ensurePrefix(cleanText(value) ?? null, 'ΔΗΜΟΣ')
}

export function formatRegionAuthorityLabel(value: string | null | undefined): string | null {
  return ensurePrefix(cleanText(value) ?? null, 'ΠΕΡΙΦΕΡΕΙΑ')
}

export function buildContractAuthorityLabel(args: {
  canonicalOwnerScope: string | null | undefined
  organizationScope: string | null | undefined
  organizationName: string | null | undefined
  municipalityLabel: string | null | undefined
  regionLabel: string | null | undefined
}): string {
  const canonical = String(args.canonicalOwnerScope ?? '').trim().toLowerCase()
  const orgScope = String(args.organizationScope ?? '').trim().toLowerCase()
  const organizationName = cleanText(args.organizationName)
  const municipalityLabel = formatMunicipalityAuthorityLabel(args.municipalityLabel)
  const regionLabel = formatRegionAuthorityLabel(args.regionLabel)

  if (canonical === 'municipality') return municipalityLabel ?? organizationName ?? 'Δήμος —'
  if (canonical === 'region') return regionLabel ?? municipalityLabel ?? organizationName ?? 'Περιφέρεια —'
  if (orgScope === 'municipality') return municipalityLabel ?? organizationName ?? 'Δήμος —'
  if (orgScope === 'region' || orgScope === 'decentralized') {
    return regionLabel ?? municipalityLabel ?? organizationName ?? 'Περιφέρεια —'
  }
  return organizationName ?? municipalityLabel ?? regionLabel ?? '—'
}
