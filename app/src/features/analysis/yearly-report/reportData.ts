import snapshot from './data/yearlyReport2026.generated.json'

// The snapshot is rebuilt every day, so values (and empty lists or nulls) change between
// builds. An explicit type keeps the page compiling whatever the current values are.

type Bar = { label: string; count: number; amount: number; share_amount_pct: number | null; share_count_pct: number | null }
type GroupRow = { group: string; contracts: number; amount: number }
/** 5.000 € bins; `hi` is null for the open bin above 60.000 €. */
type AwardBin = { lo: number; hi: number | null; count: number }

export type ContractStats = {
  contracts: number
  amount: number
  contract_types: Bar[]
  procedures: Bar[]
  top_authorities: { authority: string; contracts: number; amount: number; share_pct: number | null }[]
  top_cpvs: { cpv: string; contracts: number; share_contracts_pct: number | null }[]
  by_group: GroupRow[]
  direct_awards: {
    count: number
    amount: number
    median: number | null
    up_to_30k: number
    from_25k_to_30k: number
    from_20k_to_25k: number
    above_30k: number
    bins: AwardBin[]
  }
}

export type AuthorityChange = {
  authority: string
  contracts_previous: number
  contracts_current: number
  amount_previous: number
  amount_current: number
  change: number
}

type CategoryRow = { category: string; label: string; rule: string; payments: number; amount: number; bodies: number }

export type YearlyReportData = {
  meta: {
    built_at: string
    snapshot_at: string
    snapshot_date: string
    snapshot_time: string
    report_year: number
    period_start: string
    period_end: string
    year_complete: boolean
    latest_contract_signed: string
    latest_payment_published: string | null
    vat_rate: number
  }
  total: { contracts: number; contracts_amount: number; payments: number; payments_amount: number; amount: number }
  contracts: ContractStats
  years: Record<string, ContractStats>
  ytd: {
    year: number
    contracts: number
    amount: number
    by_group: GroupRow[]
    by_prevention: { class: string; code: number | null; contracts: number; amount: number }[]
  }[]
  authorityChanges: { largest_decreases: AuthorityChange[]; largest_increases: AuthorityChange[] }
  relevance: { class: string; code: number | null; contracts: number; amount: number; share_amount_pct: number | null }[]
  timing: {
    year: number
    scope: string
    contracts: number
    amount: number
    before_may_contracts: number
    before_may_amount: number
    may_to_august_contracts: number
    by_month: { month: number; contracts: number }[]
  }[]
  contractors: {
    distinct: number
    top10_share_pct: number | null
    top10: {
      name: string
      contracts: number
      amount: number
      share_pct: number | null
      authorities: { authority: string; contracts: number; amount: number }[]
    }[]
  }
  payments: {
    records: number
    valid: number
    valid_amount: number
    counted: number
    amount: number
    linked: number
    linked_amount: number
    linked_by_method: Record<string, { payments: number; amount: number }>
    linked_to_report_year: number
    vat_removed: number
    vat_free: number
    cancellations: number
    positive_cancellations_by_org: Record<string, number>
    cancelled_originals: { ada: string; amount: number; cancelled_by: string }[]
    report_excluded_payroll: { payments: number; amount: number }
    by_category: CategoryRow[]
    by_org_type: { org_type: string; payments: number; amount: number }[]
    payroll_bodies: number
    /** Municipal payments without a linked contract, by month of publication. */
    municipal: {
      payments: number
      amount: number
      from_may_1: number
      payroll: number
      by_month: { month: number; payments: number; amount: number }[]
    }
    payroll_by_month: { month: number; payments: number }[]
    vat_evidence: { equals_with_vat: number; equals_without_vat: number; other: number }
    subject_refs_not_in_db: number
    without_beneficiary_vat: number
    exact_duplicate_groups: number
    exact_duplicates_extra_amount: number
    report_year_contracts_with_linked_payment: number
    report_year_contracts_with_linked_payment_value_pct: number | null
    finalizations_by_year: Record<string, number>
  }
  municipalities: {
    funded: number
    funding_total: number
    with_contracts: number
    contracts: number
    contracts_amount: number
    with_payments: number
    payments: number
    payments_amount: number
    funded_without_contract: number
    funded_without_contract_or_payment: number
    funding_of_no_trace: number
    no_trace_list: { municipality: string; nomos: string; funding_2026: number }[]
  }
  funding: { year: number; allocation_type: string; recipient_type: string; rows: number; amount: number; source_adas: string[] }[]
  fires: {
    by_year: { year: number; fires: number; area_ha: number }[]
    report_year_last_fire_date: string | null
    large_fires: number
    large_fires_area_ha: number
    top_municipalities: {
      municipality: string
      fires: number
      area_ha: number
      municipal_contracts_2026: number
      municipal_contracts_2026_amount: number
      municipal_payments_2026: number
      municipal_payments_2026_amount: number
    }[]
  }
  facts: {
    before_may: { contracts: number; amount: number; ypen_amount: number }
    municipal_timing: { contracts: number; from_may_1: number }
    first_half: Record<string, number>
    first_half_complete: boolean
    west_attica_ypen_contract: { reference_number: string; title: string; signed: string; amount: number } | null
    payroll_org_types: Record<string, number>
    civil_protection_ministry_payments: number
  }
  dataQuality: { superseded_or_duplicate_report_year_contract_rows: number }
}

export const report: YearlyReportData = snapshot
export const REPORT_YEAR = report.meta.report_year
export const contracts = report.contracts

const numberFormat = new Intl.NumberFormat('el-GR')
const decimalFormat = new Intl.NumberFormat('el-GR', { minimumFractionDigits: 1, maximumFractionDigits: 1 })

/** 2132 → «2.132» */
export function num(value: number): string {
  return numberFormat.format(value)
}

/** All measured values use one decimal place, including a trailing zero. */
export function decimal(value: number): string {
  return decimalFormat.format(value)
}

/** 173140768.42 → «173,1 εκατ. €» */
export function mil(value: number): string {
  return `${decimal(value / 1_000_000)} εκατ. €`
}

/** Millions in running text: «21,6 εκατ. ευρώ», «3,0 εκατ. ευρώ». */
export function milText(value: number): string {
  return `${decimal(value / 1_000_000)} εκατ. ευρώ`
}

/** Millions from 1 εκατ. € up, thousands below: «1,8 εκατ. €», «738,4 χιλ. €». */
export function amount(value: number): string {
  if (Math.abs(value) >= 1_000_000) return mil(value)
  if (Math.abs(value) >= 1_000) return `${decimal(value / 1_000)} χιλ. €`
  return eur(value)
}

/** Millions rounded down to one decimal so that «τουλάχιστον» stays true. */
export function minimumMillions(value: number): string {
  return decimal(Math.floor(value / 100_000) / 10)
}

/** 12000 → «12.000,0 €» */
export function eur(value: number): string {
  return `${decimal(value)} €`
}

/** 49.51 → «49,5%», −41.47 → «−41,5%» (typographic minus) */
export function pct(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—'
  const formatted = decimal(Math.abs(value))
  return `${value < 0 && formatted.replace(/[0,]/g, '') !== '' ? '−' : ''}${formatted}%`
}

/** Percentage in running text: «12,0%», «12,7%». */
export function pctText(value: number | null | undefined): string {
  return pct(value)
}

/** Change with an explicit sign: «+12,0%», «−53,4%». */
export function signedPct(value: number): string {
  const text = pct(value)
  return value > 0 && text.replace(/[0,%]/g, '') !== '' ? `+${text}` : text
}

/** Small counts as words in running text: 2 → «δύο». */
export function count(value: number, feminine = false): string {
  const words: Record<number, string> = { 1: feminine ? 'μία' : 'ένα', 2: 'δύο', 3: feminine ? 'τρεις' : 'τρία', 4: feminine ? 'τέσσερις' : 'τέσσερα', 5: 'πέντε', 6: 'έξι', 7: 'επτά', 8: 'οκτώ', 9: 'εννέα', 10: 'δέκα' }
  return words[value] ?? numberFormat.format(value)
}

export function share(part: number, whole: number): number {
  return whole === 0 ? 0 : (part / whole) * 100
}

/** «2026-10-02» → «2 Οκτωβρίου 2026» */
export function longDate(iso: string): string {
  return new Date(`${iso}T12:00:00`).toLocaleDateString('el-GR', { day: 'numeric', month: 'long', year: 'numeric' })
}

/** «2026-10-02» → «2/10/2026» */
export function shortDate(iso: string): string {
  return new Date(`${iso}T12:00:00`).toLocaleDateString('el-GR')
}

/** «2026-10-02» → «2/10» */
export function dayMonth(iso: string): string {
  const day = new Date(`${iso}T12:00:00`)
  return `${day.getDate()}/${day.getMonth() + 1}`
}

/** «α, β και γ» */
export function listJoin(items: string[]): string {
  return items.length <= 1 ? items.join('') : `${items.slice(0, -1).join(', ')} και ${items[items.length - 1]}`
}

export const MONTHS = ['Ιαν', 'Φεβ', 'Μαρ', 'Απρ', 'Μαΐ', 'Ιουν', 'Ιουλ', 'Αυγ', 'Σεπ', 'Οκτ', 'Νοε', 'Δεκ']

const ZERO_BAR: Bar = { label: '', count: 0, amount: 0, share_amount_pct: 0, share_count_pct: 0 }

export function procedure(label: string): Bar {
  return contracts.procedures.find(item => item.label === label) ?? { ...ZERO_BAR, label }
}

export function contractType(label: string): Bar {
  return contracts.contract_types.find(item => item.label === label) ?? { ...ZERO_BAR, label }
}

export function ytd(year: number) {
  return report.ytd.find(item => item.year === year) ?? { year, contracts: 0, amount: 0, by_group: [], by_prevention: [] }
}

/** Contract-value change against the same period of an earlier year. */
export function contractValueChangePct(comparisonYear: number): number {
  const previousAmount = ytd(comparisonYear).amount
  return share(ytd(REPORT_YEAR).amount - previousAmount, previousAmount)
}

export function ytdGroup(year: number, group: string) {
  return ytd(year).by_group.find(item => item.group === group) ?? { group, contracts: 0, amount: 0 }
}

export function ytdPrevention(year: number, code: number | null) {
  return ytd(year).by_prevention.find(item => item.code === code) ?? { class: '', code, contracts: 0, amount: 0 }
}

export function fullYear(year: number): ContractStats | undefined {
  return report.years[String(year)]
}

export function timing(year: number, scope: 'all' | 'municipal' | 'regional' | 'environment_ministry' | 'civil_protection_ministry') {
  return report.timing.find(item => item.year === year && item.scope === scope)
    ?? { year, scope, contracts: 0, amount: 0, before_may_contracts: 0, before_may_amount: 0, may_to_august_contracts: 0, by_month: MONTHS.map((_, i) => ({ month: i + 1, contracts: 0 })) }
}

export function relevanceClass(code: number | null) {
  return report.relevance.find(item => item.code === code) ?? { class: '', code, contracts: 0, amount: 0, share_amount_pct: 0 }
}

export function paymentCategory(category: string): CategoryRow {
  return report.payments.by_category.find(item => item.category === category)
    ?? { category, label: category, rule: '', payments: 0, amount: 0, bodies: 0 }
}

/** Municipal fire-protection allocations per year (regular plus supplementary or emergency). */
export function municipalFunding() {
  const byYear = new Map<number, { regular: number; extra: number }>()
  for (const row of report.funding) {
    if (row.recipient_type !== 'δήμος') continue
    const entry = byYear.get(row.year) ?? { regular: 0, extra: 0 }
    if (row.allocation_type === 'τακτική') entry.regular += row.amount
    else entry.extra += row.amount
    byYear.set(row.year, entry)
  }
  return [...byYear.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([year, value]) => ({ year, regular: value.regular, extra: value.extra, total: value.regular + value.extra }))
}

export const derived = (() => {
  const current = ytd(REPORT_YEAR)
  const previous = ytd(REPORT_YEAR - 1)
  const municipalTiming = report.facts.municipal_timing
  const direct = contracts.direct_awards
  const payroll = report.payments.payroll_by_month
  return {
    ytdChangePct: contractValueChangePct(REPORT_YEAR - 1),
    ytdCountChangePct: share(current.contracts - previous.contracts, previous.contracts),
    municipalAfterMayPct: share(municipalTiming.from_may_1, municipalTiming.contracts),
    directUpTo30kPct: share(direct.up_to_30k, direct.count),
    directBandRatio: direct.from_20k_to_25k === 0 ? 0 : direct.from_25k_to_30k / direct.from_20k_to_25k,
    largeFiresSharePct: share(report.fires.large_fires_area_ha, report.fires.by_year.find(item => item.year === REPORT_YEAR)?.area_ha ?? 0),
    payrollAugSep: payroll.filter(item => item.month === 8 || item.month === 9).reduce((sum, item) => sum + item.payments, 0),
    payrollTotal: payroll.reduce((sum, item) => sum + item.payments, 0),
  }
})()
