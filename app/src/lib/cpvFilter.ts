export type CpvFilterRow = {
  cpv_key: string
  cpv_value: string | null
  procurement_id: number
}

export type CpvFilterIndex = Map<string, { label: string; procurementIds: Set<number> }>

export function buildCpvFilterIndex(rows: readonly CpvFilterRow[]): CpvFilterIndex {
  const index: CpvFilterIndex = new Map()
  for (const row of rows) {
    const code = row.cpv_key.trim()
    if (!code) continue
    const description = row.cpv_value?.trim() ?? ''
    const entry = index.get(code) ?? { label: code, procurementIds: new Set<number>() }
    if (description && entry.label === code) entry.label = `${code} · ${description}`
    entry.procurementIds.add(row.procurement_id)
    index.set(code, entry)
  }
  return index
}

export function matchesContractCpv(procurementId: number, code: string, index: CpvFilterIndex): boolean {
  return !code || (index.get(code)?.procurementIds.has(procurementId) ?? false)
}
