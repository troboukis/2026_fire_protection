// ── Τύποι ────────────────────────────────────────────────────────────
export type BarItem = {
  label: string
  value: number
  total_m: number
  pct: number
  tone: 'high' | 'mid' | 'low' | 'faint'
}

export interface MonthlyPoint {
  month: string    // '2024-01'
  label: string    // "Ιαν '24"
  count: number
  total_k: number  // €K χωρίς ΦΠΑ
}

export type TopOrgItem = {
  name: string
  contracts: number
  total_m: number
}

export type TopCpvItem = {
  cpv: string
  desc: string
  count: number
  mainProcedure: string
  mainProcedurePct: number
}

export type SunburstDatum = {
  name: string
  value?: number
  children?: SunburstDatum[]
}

export type SectionRow = {
  signedDate: string | null
  effectiveStart: string
  effectiveEnd: string
  orgName: string
  authorityLabel: string
  contractType: string
  procedure: string
  amount: number
  cpvs: string[]
}

export type AnalysisData = {
  monthly: MonthlyPoint[]
  contractTypeData: BarItem[]
  procedureData: BarItem[]
  topOrgs: TopOrgItem[]
  topCpv: TopCpvItem[]
  sectionRows: SectionRow[]
  totalContracts: number
  totalAmount: number
  avgAmount: number
  medianAmount: number
  peakContractsMonthLabel: string
  peakContractsMonthCount: number
  peakSpendMonthLabel: string
  peakSpendMonthAmount: number
  directAwardPct: number
}

export type ContractAnalysisRpcPayload = {
  sectionRows?: unknown
}

export const ANALYSIS_START = '2024-01-01'
export const CURRENT_YEAR = new Date().getFullYear()
export const ANALYSIS_END = `${CURRENT_YEAR}-12-31`

export const BREAKPOINT_XS = 360
export const BREAKPOINT_SM = 440
export const BREAKPOINT_MD = 640

export const MONTH_NAMES_SHORT = ['Ιαν', 'Φεβ', 'Μαρ', 'Απρ', 'Μαϊ', 'Ιουν', 'Ιουλ', 'Αυγ', 'Σεπ', 'Οκτ', 'Νοε', 'Δεκ']

function monthLabelFromMonthKey(month: string): string {
  const [y, m] = month.split('-')
  const mi = Number(m) - 1
  const yy = y.slice(2)
  return `${MONTH_NAMES_SHORT[mi] ?? m} '${yy}`
}

export function toneForPct(pct: number): BarItem['tone'] {
  if (pct >= 50) return 'high'
  if (pct >= 20) return 'mid'
  if (pct >= 5) return 'low'
  return 'faint'
}

function buildRangeMonths(startYm: string, endYm: string): string[] {
  const out: string[] = []
  let [y, m] = startYm.split('-').map(Number)
  const [ey, em] = endYm.split('-').map(Number)
  while (y < ey || (y === ey && m <= em)) {
    out.push(`${y}-${String(m).padStart(2, '0')}`)
    m += 1
    if (m > 12) {
      m = 1
      y += 1
    }
  }
  return out
}

function cleanDateString(v: string | null | undefined): string | null {
  const s = String(v ?? '').trim()
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null
  return s
}

// ── Live aggregates loaded from Supabase ─────────────────────────────

export function buildProcedureAuthoritySunburstData(rows: SectionRow[]): SunburstDatum | null {
  const procedureToAuthorities = new Map<string, Map<string, number>>()

  for (const row of rows) {
    if (!procedureToAuthorities.has(row.procedure)) procedureToAuthorities.set(row.procedure, new Map())
    const authorityMap = procedureToAuthorities.get(row.procedure)!
    authorityMap.set(row.authorityLabel, (authorityMap.get(row.authorityLabel) ?? 0) + 1)
  }

  const children = [...procedureToAuthorities.entries()]
    .map(([procedure, authorityMap]) => ({
      name: procedure,
      children: [...authorityMap.entries()]
        .map(([authority, count]) => ({ name: authority, value: count }))
        .sort((a, b) => (b.value ?? 0) - (a.value ?? 0) || a.name.localeCompare(b.name, 'el')),
    }))
    .sort((a, b) => {
      const aTotal = a.children.reduce((sum, item) => sum + (item.value ?? 0), 0)
      const bTotal = b.children.reduce((sum, item) => sum + (item.value ?? 0), 0)
      return bTotal - aTotal || a.name.localeCompare(b.name, 'el')
    })

  if (children.length === 0) return null

  return {
    name: 'Συμβάσεις',
    children,
  }
}

export function buildTopCpvList(
  cpvAggMap: Map<string, { count: number; procedures: Map<string, number> }>,
  limit = 8,
): TopCpvItem[] {
  return [...cpvAggMap.entries()]
    .map(([cpv, v]) => {
      const main = [...v.procedures.entries()].sort((a, b) => b[1] - a[1])[0]
      const mainCount = main?.[1] ?? 0
      return {
        cpv,
        desc: cpv,
        count: v.count,
        mainProcedure: main?.[0] ?? '—',
        mainProcedurePct: Number(((mainCount / Math.max(v.count, 1)) * 100).toFixed(1)),
      }
    })
    .sort((a, b) => b.count - a.count)
    .slice(0, limit)
}

export function toSectionRow(value: unknown): SectionRow | null {
  if (!value || typeof value !== 'object') return null
  const row = value as Record<string, unknown>
  const signedDate = cleanDateString(String(row.signedDate ?? '')) ?? null
  const effectiveStart = cleanDateString(String(row.effectiveStart ?? '')) ?? signedDate
  const effectiveEnd = cleanDateString(String(row.effectiveEnd ?? '')) ?? effectiveStart
  if (!effectiveStart || !effectiveEnd) return null

  const cpvs = Array.isArray(row.cpvs)
    ? row.cpvs.map((item) => String(item ?? '').trim()).filter(Boolean)
    : []

  return {
    signedDate,
    effectiveStart,
    effectiveEnd,
    orgName: String(row.orgName ?? '—').trim() || '—',
    authorityLabel: String(row.authorityLabel ?? '—').trim() || '—',
    contractType: String(row.contractType ?? 'Λοιπές').trim() || 'Λοιπές',
    procedure: String(row.procedure ?? 'Άλλη').trim() || 'Άλλη',
    amount: Number(row.amount ?? 0) || 0,
    cpvs,
  }
}

export function buildAnalysisDataFromRows(sectionRows: SectionRow[]): AnalysisData {
  const periodStart = ANALYSIS_START
  const periodEnd = ANALYSIS_END

  const monthlyMap = new Map<string, { count: number; total_k: number }>()
  const contractTypeMap = new Map<string, { count: number; total: number }>()
  const procedureMap = new Map<string, { count: number; total: number }>()
  const cpvAggMap = new Map<string, { count: number; procedures: Map<string, number> }>()
  const amounts: number[] = []
  const monthKeys: string[] = []

  let totalContracts = 0
  let totalAmount = 0
  let signedWindowContracts = 0
  let signedWindowDirectAwards = 0

  for (const row of sectionRows) {
    const baseDateForMonth = row.signedDate ?? row.effectiveStart
    const monthAnchorDate = baseDateForMonth < periodStart ? periodStart : baseDateForMonth
    const month = monthAnchorDate.slice(0, 7)
    if (!/^\d{4}-\d{2}$/.test(month)) continue
    if (month < periodStart.slice(0, 7) || month > periodEnd.slice(0, 7)) continue

    monthKeys.push(month)
    totalContracts += 1
    totalAmount += row.amount
    if (row.amount > 0) amounts.push(row.amount)

    if (row.signedDate && row.signedDate >= periodStart && row.signedDate <= periodEnd) {
      signedWindowContracts += 1
      if (row.procedure === 'Απευθείας Ανάθεση') signedWindowDirectAwards += 1
    }

    const monthly = monthlyMap.get(month) ?? { count: 0, total_k: 0 }
    monthly.count += 1
    monthly.total_k += row.amount / 1000
    monthlyMap.set(month, monthly)

    const ct = contractTypeMap.get(row.contractType) ?? { count: 0, total: 0 }
    ct.count += 1
    ct.total += row.amount
    contractTypeMap.set(row.contractType, ct)

    const pr = procedureMap.get(row.procedure) ?? { count: 0, total: 0 }
    pr.count += 1
    pr.total += row.amount
    procedureMap.set(row.procedure, pr)

    for (const cpv of row.cpvs) {
      const ca = cpvAggMap.get(cpv) ?? { count: 0, procedures: new Map<string, number>() }
      ca.count += 1
      ca.procedures.set(row.procedure, (ca.procedures.get(row.procedure) ?? 0) + 1)
      cpvAggMap.set(cpv, ca)
    }
  }

  const fallbackStartMonth = periodStart.slice(0, 7)
  const fallbackEndMonth = periodEnd.slice(0, 7)
  const sortedMonthKeys = [...monthKeys].sort()
  const minMonth = sortedMonthKeys[0] ?? fallbackStartMonth
  const maxMonth = sortedMonthKeys[sortedMonthKeys.length - 1] ?? fallbackEndMonth
  const monthly: MonthlyPoint[] = buildRangeMonths(minMonth, maxMonth).map((month) => {
    const data = monthlyMap.get(month) ?? { count: 0, total_k: 0 }
    return {
      month,
      label: monthLabelFromMonthKey(month),
      count: data.count,
      total_k: Number(data.total_k.toFixed(1)),
    }
  })

  const toBarData = (map: Map<string, { count: number; total: number }>, labels: string[]): BarItem[] =>
    labels.map((label) => {
      const v = map.get(label) ?? { count: 0, total: 0 }
      const pct = totalAmount > 0 ? (v.total / totalAmount) * 100 : 0
      return {
        label,
        value: v.count,
        total_m: v.total / 1_000_000,
        pct: Number(pct.toFixed(1)),
        tone: toneForPct(pct),
      }
    }).sort((a, b) => b.pct - a.pct || b.total_m - a.total_m || a.label.localeCompare(b.label, 'el'))

  const sortedAmounts = [...amounts].sort((a, b) => a - b)
  const medianAmount = sortedAmounts.length === 0
    ? 0
    : sortedAmounts.length % 2 === 1
      ? sortedAmounts[(sortedAmounts.length - 1) / 2]
      : (sortedAmounts[sortedAmounts.length / 2 - 1] + sortedAmounts[sortedAmounts.length / 2]) / 2

  const peakCountMonth = monthly.reduce((acc, cur) => (cur.count > acc.count ? cur : acc), monthly[0] ?? { month: '2024-01', label: "Ιαν '24", count: 0, total_k: 0 })
  const peakSpendMonth = monthly.reduce((acc, cur) => (cur.total_k > acc.total_k ? cur : acc), monthly[0] ?? { month: '2024-01', label: "Ιαν '24", count: 0, total_k: 0 })

  return {
    monthly,
    contractTypeData: toBarData(contractTypeMap, ['Υπηρεσίες', 'Προμήθειες', 'Έργα', 'Λοιπές']),
    procedureData: toBarData(procedureMap, ['Απευθείας Ανάθεση', 'Ανοιχτή Διαδικασία', 'Διαπραγμάτευση', 'Άλλη']),
    topOrgs: [],
    topCpv: buildTopCpvList(cpvAggMap),
    sectionRows,
    totalContracts,
    totalAmount,
    avgAmount: totalContracts > 0 ? totalAmount / totalContracts : 0,
    medianAmount,
    peakContractsMonthLabel: peakCountMonth.label,
    peakContractsMonthCount: peakCountMonth.count,
    peakSpendMonthLabel: peakSpendMonth.label,
    peakSpendMonthAmount: peakSpendMonth.total_k * 1000,
    directAwardPct: signedWindowContracts > 0 ? (signedWindowDirectAwards / signedWindowContracts) * 100 : 0,
  }
}
