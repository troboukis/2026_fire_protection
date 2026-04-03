const CONTRACTS_PAGE_YEAR_START = 2024

function isoTodayLocal(): string {
  const now = new Date()
  const tzOffsetMs = now.getTimezoneOffset() * 60_000
  return new Date(now.getTime() - tzOffsetMs).toISOString().slice(0, 10)
}

export function buildContractsPageHref({
  organizationKeys,
  regionKey,
  municipalityKey,
}: {
  organizationKeys?: string[]
  regionKey?: string | null
  municipalityKey?: string | null
}): string {
  const params = new URLSearchParams()
  params.set('dateFrom', `${CONTRACTS_PAGE_YEAR_START}-01-01`)
  params.set('dateTo', isoTodayLocal())

  for (const organizationKey of Array.from(new Set((organizationKeys ?? []).map((key) => key.trim()).filter(Boolean)))) {
    params.append('organizationKey', organizationKey)
  }

  if (regionKey?.trim()) params.set('regionKey', regionKey.trim())
  if (municipalityKey?.trim()) params.set('municipalityKey', municipalityKey.trim())

  return `/contracts?${params.toString()}`
}
