import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import ContractAnalysis from './components/ContractAnalysis'
import ComponentTag from './components/ComponentTag'
import ErrorBoundary from './components/ErrorBoundary'
import ContractModal, { type ContractModalContract } from './components/ContractModal'
import LatestContractCardItem, { type LatestContractCardView } from './components/LatestContractCard'
import OrganizationSection, { type OrganizationSectionData } from './components/OrganizationSection'
import { openContractPdfPrintView } from './lib/contractPdf'
import { supabase } from './lib/supabase'

type BeneficiaryInsightRow = {
  beneficiary: string
  organization: string
  totalAmount: number
  contractCount: number
  cpv: string
  startDate: string
  endDate: string
  duration: string
  progressPct: number | null
  signer: string
  relevantContracts: LatestContractCard[]
}

function formatDateTimeEl(iso: string): string {
  const dt = new Date(iso)
  if (Number.isNaN(dt.getTime())) return '—'
  return new Intl.DateTimeFormat('el-GR', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(dt)
}

type LatestContractCard = LatestContractCardView & ContractModalContract & {
  id: string
  who: string
  what: string
  when: string
  why: string
  beneficiary: string
  contractType: string
  howMuch: string
  withoutVatAmount: string
  withVatAmount: string
  referenceNumber: string
  contractNumber: string
  cpv: string
  cpvCode: string
  signedAt: string
  startDate: string
  endDate: string
  organizationVat: string
  beneficiaryVat: string
  signers: string
  assignCriteria: string
  contractKind: string
  unitsOperator: string
  fundingCofund: string
  fundingSelf: string
  fundingEspa: string
  fundingRegular: string
  auctionRefNo: string
  paymentRefNo: string
  shortDescription: string
  rawBudget: string
  contractBudget: string
  documentUrl: string | null
}

type HeroStats = {
  periodMainStart: string
  periodMainEnd: string
  totalMain: number
  totalPrev1: number
  totalPrev2: number
  totalVsPrev1Pct: number | null
  topContractType: string
  topContractTypeCount: number
  topContractTypePrevCount: number
  topContractTypeVsPrev1Pct: number | null
  topCpvText: string
  topCpvCount: number
  topCpvPrevCount: number
  topCpvVsPrev1Pct: number | null
}

type HeroCurvePoint = {
  year: number
  dayOfYear: number
  yearDays: number
  value: number
}

type HeroSectionRpcPoint = {
  series_year: number | string
  day_of_year: number | string
  year_days: number | string
  cumulative_amount: number | string | null
}

type HeroSectionRpcResponse = {
  period_main_start: string | null
  period_main_end: string | null
  total_main: number | string | null
  total_prev1: number | string | null
  total_prev2: number | string | null
  top_contract_type: string | null
  top_contract_type_count: number | string | null
  top_contract_type_prev1_count: number | string | null
  top_cpv_text: string | null
  top_cpv_count: number | string | null
  top_cpv_prev1_count: number | string | null
  curve_points: HeroSectionRpcPoint[] | null
}

function cleanText(v: unknown): string | null {
  if (v == null) return null
  const s = String(v).trim()
  if (!s || s.toLowerCase() === 'nan' || s.toLowerCase() === 'none') return null
  return s
}

function firstPipePart(v: unknown): string | null {
  const s = cleanText(v)
  if (!s) return null
  return s.split('|').map(x => x.trim()).filter(Boolean)[0] ?? null
}

function formatDateEl(iso: string | null): string {
  if (!iso) return '—'
  const dt = new Date(iso)
  if (Number.isNaN(dt.getTime())) return '—'
  return new Intl.DateTimeFormat('el-GR', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
  }).format(dt)
}

function formatEur(n: number | null): string {
  if (n == null || Number.isNaN(n)) return '—'
  return n.toLocaleString('el-GR', {
    style: 'currency',
    currency: 'EUR',
    maximumFractionDigits: 0,
  })
}

function toLowerEl(v: string | null): string {
  if (!v) return '—'
  return v.toLocaleLowerCase('el-GR')
}

function toSentenceCaseEl(v: string | null): string {
  const lower = toLowerEl(v)
  if (!lower || lower === '—') return '—'
  return lower.charAt(0).toLocaleUpperCase('el-GR') + lower.slice(1)
}

function toUpperEl(v: string | null): string {
  if (!v) return '—'
  return v.toLocaleUpperCase('el-GR')
}

function formatEurCompact(n: number): string {
  if (Number.isNaN(n)) return '—'
  const abs = Math.abs(n)
  if (abs >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B €`
  if (abs >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M €`
  if (abs >= 1_000) return `${(n / 1_000).toFixed(1)}K €`
  return formatEur(n)
}

function formatDateLabel(iso: string | null): string {
  if (!iso) return '—'
  const dt = new Date(iso)
  if (Number.isNaN(dt.getTime())) return '—'
  return new Intl.DateTimeFormat('el-GR', { day: '2-digit', month: '2-digit', year: 'numeric' }).format(dt)
}

function formatPct(value: number | null): string {
  if (value == null || Number.isNaN(value)) return '—'
  const sign = value > 0 ? '+' : ''
  return `${sign}${value.toFixed(1)}%`
}

function resolvePct(raw: number | null, current: number, previous: number): number {
  if (raw != null && !Number.isNaN(raw)) return raw
  if (previous === 0) return current > 0 ? 100 : 0
  return ((current - previous) / previous) * 100
}

function pctColor(value: number | null): string {
  if (value == null || Number.isNaN(value)) return 'var(--ink-faint)'
  if (value > 0) return '#1f8f4d'
  if (value < 0) return 'var(--accent)'
  return 'var(--ink-faint)'
}

function dayFraction(dayOfYear: number, yearDays: number): number {
  const denom = Math.max(1, yearDays - 1)
  return Math.min(1, Math.max(0, (dayOfYear - 1) / denom))
}

function DebugComponentLabel({ name }: { name: string }) {
  return (
    <div
      style={{
        background: '#ffeb3b',
        color: '#000',
        fontFamily: 'monospace',
        fontSize: '12px',
        padding: '2px 6px',
        border: '1px dashed #000',
        margin: '6px 0',
        display: 'inline-block',
        zIndex: 1000,
      }}
    >
      COMPONENT: {name}
    </div>
  )
}

const YEAR_START = 2024

const CHART_YEAR_STYLES = [
  { stroke: '#111111', opacity: 1,    strokeWidth: 3.8 },
  { stroke: '#d9a095', opacity: 0.95, strokeWidth: 2.4 },
  { stroke: '#dadada', opacity: 0.62, strokeWidth: 2.4 },
  { stroke: '#dadada', opacity: 0.45, strokeWidth: 1.8 },
]

export default function App() {
  const buildTimeLabel = formatDateTimeEl(__LAST_COMMIT_ISO__)
  const [lastDbUpdateLabel, setLastDbUpdateLabel] = useState(buildTimeLabel)
  const [latestContracts, setLatestContracts] = useState<LatestContractCard[]>([])
  const [latestContractsLoading, setLatestContractsLoading] = useState(true)
  const [selectedContract, setSelectedContract] = useState<LatestContractCard | null>(null)
  const [heroStatsLoading, setHeroStatsLoading] = useState(true)
  const [featuredBeneficiaries, setFeaturedBeneficiaries] = useState<BeneficiaryInsightRow[]>([])
  const [featuredBeneficiariesLoading, setFeaturedBeneficiariesLoading] = useState(true)
  const [organizationSection, setOrganizationSection] = useState<OrganizationSectionData>({
    name: 'ΑΔΜΗΕ',
    yearLabel: String(new Date().getFullYear()),
    totalSpend: 0,
    totalSpendNote: 'Συμβάσεις φορέα από το σύνολο των διαθέσιμων εγγραφών.',
    cpvCodes: [],
    topCpvValue: null,
    contractCount: 0,
    beneficiaryCount: 0,
    latestSignedAt: null,
    timeline: [],
  })
  const [organizationSectionLoading, setOrganizationSectionLoading] = useState(true)
  const [heroCurvePoints, setHeroCurvePoints] = useState<HeroCurvePoint[]>([])
  const [heroStats, setHeroStats] = useState<HeroStats>({
    periodMainStart: '',
    periodMainEnd: '',
    totalMain: 0,
    totalPrev1: 0,
    totalPrev2: 0,
    totalVsPrev1Pct: null,
    topContractType: '—',
    topContractTypeCount: 0,
    topContractTypePrevCount: 0,
    topContractTypeVsPrev1Pct: null,
    topCpvText: '—',
    topCpvCount: 0,
    topCpvPrevCount: 0,
    topCpvVsPrev1Pct: null,
  })
  const currentYear = new Date().getFullYear()
  const totalVsPrev1Pct = resolvePct(heroStats.totalVsPrev1Pct, heroStats.totalMain, heroStats.totalPrev1)
  const totalVsPrev2Pct = resolvePct(null, heroStats.totalMain, heroStats.totalPrev2)
  const topTypeVsPrev1Pct = resolvePct(
    heroStats.topContractTypeVsPrev1Pct,
    heroStats.topContractTypeCount,
    heroStats.topContractTypePrevCount,
  )
  const topCpvVsPrev1Pct = resolvePct(
    heroStats.topCpvVsPrev1Pct,
    heroStats.topCpvCount,
    heroStats.topCpvPrevCount,
  )
  const chartYears = Array.from({ length: currentYear - YEAR_START + 1 }, (_, i) => YEAR_START + i)
  const chartByYear = useMemo(() => {
    const grouped = new Map<number, HeroCurvePoint[]>()
    for (const y of chartYears) grouped.set(y, [])
    for (const p of heroCurvePoints) {
      const arr = grouped.get(p.year)
      if (arr) arr.push(p)
    }
    for (const [, arr] of grouped) arr.sort((a, b) => a.dayOfYear - b.dayOfYear)
    return grouped
  }, [heroCurvePoints])
  const chartTicks = [
    { label: '01 Ιαν', month: 1, day: 1 },
    { label: '01 Μαϊ', month: 5, day: 1 },
    { label: '01 Αυγ', month: 8, day: 1 },
    { label: '31 Δεκ', month: 12, day: 31 },
  ]
  const chartMax = useMemo(() => {
    const vals = heroCurvePoints.map(p => p.value).filter(v => Number.isFinite(v))
    return Math.max(1, ...vals)
  }, [heroCurvePoints])

  useEffect(() => {
    let cancelled = false

    const loadLastDbUpdate = async () => {
      const tables = ['procurement', 'payment', 'diavgeia', 'fund', 'forest_fire']
      const results = await Promise.all(
        tables.map(async (table) => {
          const { data, error } = await supabase
            .from(table)
            .select('updated_at')
            .order('updated_at', { ascending: false })
            .limit(1)
          if (error) return null
          const row = (data?.[0] ?? null) as { updated_at?: string | null } | null
          return cleanText(row?.updated_at ?? null)
        }),
      )
      const latestIso = results
        .filter((v): v is string => Boolean(v))
        .sort((a, b) => new Date(b).getTime() - new Date(a).getTime())[0]
      if (!cancelled && latestIso) setLastDbUpdateLabel(formatDateTimeEl(latestIso))
    }

    loadLastDbUpdate()
    return () => { cancelled = true }
  }, [])

  useEffect(() => {
    if (!selectedContract) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setSelectedContract(null)
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [selectedContract])

  useEffect(() => {
    let cancelled = false
    setLatestContractsLoading(true)

    const loadLatestContracts = async () => {
      try {
        const chunk = <T,>(arr: T[], size: number): T[][] => {
          const out: T[][] = []
          for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
          return out
        }

        const { data, error } = await supabase
          .from('procurement')
          .select(`
            id,
            organization_key,
            title,
            submission_at,
            contract_signed_date,
            short_descriptions,
            procedure_type_value,
            reference_number,
            contract_number,
            contract_budget,
            budget,
            assign_criteria,
            contract_type,
            units_operator,
            funding_details_cofund,
            funding_details_self_fund,
            funding_details_espa,
            funding_details_regular_budget,
            auction_ref_no,
            organization_vat_number,
            start_date,
            end_date,
            diavgeia_ada
          `)
          .not('submission_at', 'is', null)
          .order('submission_at', { ascending: false })
          .limit(80)

        if (cancelled || error) return

        const ids = ((data ?? []) as Array<{ id: number }>).map((r) => r.id)
        const orgKeys = Array.from(new Set((data ?? []).map((r) => cleanText((r as { organization_key?: string | null }).organization_key)).filter(Boolean))) as string[]

        const paymentByProcId = new Map<number, { beneficiary_name: string | null; beneficiary_vat_number: string | null; signers: string | null; payment_ref_no: string | null; amount_without_vat: number | null; amount_with_vat: number | null }>()
        for (const c of chunk(ids, 200)) {
          const { data: pData } = await supabase
            .from('payment')
            .select('procurement_id, beneficiary_name, beneficiary_vat_number, signers, payment_ref_no, amount_without_vat, amount_with_vat')
            .in('procurement_id', c)
          for (const p of (pData ?? []) as Array<{ procurement_id: number; beneficiary_name: string | null; beneficiary_vat_number: string | null; signers: string | null; payment_ref_no: string | null; amount_without_vat: number | null; amount_with_vat: number | null }>) {
            if (!paymentByProcId.has(p.procurement_id)) paymentByProcId.set(p.procurement_id, p)
          }
        }

        const cpvByProcId = new Map<number, Array<{ code: string; label: string }>>()
        for (const c of chunk(ids, 200)) {
          const { data: cpvData } = await supabase
            .from('cpv')
            .select('procurement_id, cpv_key, cpv_value')
            .in('procurement_id', c)
          for (const cpv of (cpvData ?? []) as Array<{ procurement_id: number; cpv_key: string | null; cpv_value: string | null }>) {
            const code = cleanText(cpv.cpv_key) ?? '—'
            const label = cleanText(cpv.cpv_value) ?? '—'
            if (!cpvByProcId.has(cpv.procurement_id)) cpvByProcId.set(cpv.procurement_id, [])
            const items = cpvByProcId.get(cpv.procurement_id)!
            if (!items.find((x) => x.code === code && x.label === label)) items.push({ code, label })
          }
        }

        const orgNameByKey = new Map<string, string>()
        for (const c of chunk(orgKeys, 200)) {
          const { data: oData } = await supabase
            .from('organization')
            .select('organization_key, organization_normalized_value, organization_value')
            .in('organization_key', c)
          for (const o of (oData ?? []) as Array<{ organization_key: string; organization_normalized_value: string | null; organization_value: string | null }>) {
            if (!orgNameByKey.has(o.organization_key)) {
              orgNameByKey.set(o.organization_key, cleanText(o.organization_normalized_value) ?? cleanText(o.organization_value) ?? o.organization_key)
            }
          }
        }

        const cards: LatestContractCard[] = (data ?? []).map((row) => {
          const r = row as {
            id: number
            organization_key: string | null
            title: string | null
            submission_at: string | null
            contract_signed_date: string | null
            short_descriptions: string | null
            procedure_type_value: string | null
            reference_number: string | null
            contract_number: string | null
            contract_budget: number | null
            budget: number | null
            assign_criteria: string | null
            contract_type: string | null
            units_operator: string | null
            funding_details_cofund: string | null
            funding_details_self_fund: string | null
            funding_details_espa: string | null
            funding_details_regular_budget: string | null
            auction_ref_no: string | null
            organization_vat_number: string | null
            start_date: string | null
            end_date: string | null
            diavgeia_ada: string | null
          }
          const p = paymentByProcId.get(r.id)
          const cpvItems = cpvByProcId.get(r.id) ?? []
          const c = cpvItems[0] ?? null
          const amountWithoutVat = p?.amount_without_vat ?? null

          const why =
            c?.label ??
            firstPipePart(r.short_descriptions) ??
            '—'
          const diavgeiaAda = cleanText(r.diavgeia_ada)
          const orgKey = cleanText(r.organization_key)

          return {
            id: String(r.id),
            who: (orgKey ? orgNameByKey.get(orgKey) : null) ?? '—',
            what: cleanText(r.title) ?? '—',
            when: formatDateEl(cleanText(r.submission_at)),
            why: toSentenceCaseEl(why),
            beneficiary: toUpperEl(cleanText(p?.beneficiary_name)),
            contractType: cleanText(r.procedure_type_value) ?? '—',
            howMuch: formatEur(amountWithoutVat),
            withoutVatAmount: formatEur(amountWithoutVat),
            withVatAmount: formatEur(p?.amount_with_vat ?? null),
            referenceNumber: cleanText(r.reference_number) ?? '—',
            contractNumber: cleanText(r.contract_number) ?? '—',
            cpv: c?.label ?? '—',
            cpvCode: c?.code ?? '—',
            cpvItems,
            signedAt: formatDateEl(cleanText(r.contract_signed_date)),
            startDate: formatDateEl(cleanText(r.start_date)),
            endDate: formatDateEl(cleanText(r.end_date)),
            organizationVat: cleanText(r.organization_vat_number) ?? '—',
            beneficiaryVat: cleanText(p?.beneficiary_vat_number) ?? '—',
            signers: cleanText(p?.signers) ?? '—',
            assignCriteria: cleanText(r.assign_criteria) ?? '—',
            contractKind: cleanText(r.contract_type) ?? '—',
            unitsOperator: cleanText(r.units_operator) ?? '—',
            fundingCofund: cleanText(r.funding_details_cofund) ?? '—',
            fundingSelf: cleanText(r.funding_details_self_fund) ?? '—',
            fundingEspa: cleanText(r.funding_details_espa) ?? '—',
            fundingRegular: cleanText(r.funding_details_regular_budget) ?? '—',
            auctionRefNo: cleanText(r.auction_ref_no) ?? '—',
            paymentRefNo: cleanText(p?.payment_ref_no) ?? '—',
            shortDescription: firstPipePart(r.short_descriptions) ?? '—',
            rawBudget: formatEur(r.budget != null ? Number(r.budget) : null),
            contractBudget: formatEur(r.contract_budget != null ? Number(r.contract_budget) : null),
            documentUrl: diavgeiaAda ? `https://diavgeia.gov.gr/doc/${diavgeiaAda}` : null,
          }
        })

        const deduped = Array.from(
          new Map(
            cards.map((item) => {
              const ref = cleanText(item.referenceNumber)
              const contractNo = cleanText(item.contractNumber)
              const doc = cleanText(item.documentUrl)
              const identity =
                (ref && ref !== '—' ? `ref:${ref}` : null) ??
                (contractNo && contractNo !== '—' ? `contract:${contractNo}` : null) ??
                (doc && doc !== '—' ? `doc:${doc}` : null) ??
                `fallback:${item.who}|${item.what}|${item.signedAt}|${item.withoutVatAmount}`
              return [identity, item] as const
            }),
          ).values(),
        ).slice(0, 15)

        setLatestContracts(deduped)
      } finally {
        if (!cancelled) setLatestContractsLoading(false)
      }
    }

    loadLatestContracts()

    return () => { cancelled = true }
  }, [])

  useEffect(() => {
    let cancelled = false
    setOrganizationSectionLoading(true)

    const loadOrganizationSection = async () => {
      const chunk = <T,>(arr: T[], size: number): T[][] => {
        const out: T[][] = []
        for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
        return out
      }

      const monthShortEl = (iso: string | null): string => {
        if (!iso) return '—'
        const dt = new Date(iso)
        if (Number.isNaN(dt.getTime())) return '—'
        return new Intl.DateTimeFormat('el-GR', { month: 'short' })
          .format(dt)
          .replace('.', '')
          .toLocaleUpperCase('el-GR')
      }

      try {
        const { data: orgMatches } = await supabase
          .from('organization')
          .select('organization_key, organization_normalized_value, organization_value')
          .or('organization_normalized_value.ilike.%ΑΔΜΗΕ%,organization_value.ilike.%ΑΔΜΗΕ%')
          .limit(25)

        const orgRows = (orgMatches ?? []) as Array<{
          organization_key: string
          organization_normalized_value: string | null
          organization_value: string | null
        }>

        const normalizeOrgToken = (v: string | null): string => (v ?? '')
          .toLocaleUpperCase('el-GR')
          .replace(/[^0-9A-ZΑ-Ω]/g, '')
        const targetOrg = normalizeOrgToken('ΑΔΜΗΕ')
        const scoredOrgRows = [...orgRows]
          .map((r) => {
            const normalized = normalizeOrgToken(cleanText(r.organization_normalized_value))
            const raw = normalizeOrgToken(cleanText(r.organization_value))
            let score = 0
            if (normalized === targetOrg || raw === targetOrg) score = 4
            else if (normalized.startsWith(targetOrg) || raw.startsWith(targetOrg)) score = 3
            else if (normalized.includes(targetOrg) || raw.includes(targetOrg)) score = 2
            return { row: r, score }
          })
          .sort((a, b) => b.score - a.score)

        const matchedOrgRows = scoredOrgRows.filter((x) => x.score >= 2).map((x) => x.row)
        const bestOrg = scoredOrgRows[0]?.row ?? orgRows[0]

        if (!bestOrg) return

        const organizationKeys = Array.from(
          new Set((matchedOrgRows.length ? matchedOrgRows : [bestOrg]).map((r) => r.organization_key)),
        )
        const organizationName =
          cleanText(bestOrg.organization_normalized_value) ??
          cleanText(bestOrg.organization_value) ??
          'ΑΔΜΗΕ'

        const pageSize = 1000
        const procurements: Array<{ id: number; contract_signed_date: string | null; title: string | null }> = []
        let from = 0
        while (true) {
          const to = from + pageSize - 1
          const { data } = await supabase
            .from('procurement')
            .select('id, contract_signed_date, title')
            .in('organization_key', organizationKeys)
            .order('id', { ascending: true })
            .range(from, to)
          const rows = (data ?? []) as Array<{ id: number; contract_signed_date: string | null; title: string | null }>
          procurements.push(...rows)
          if (rows.length < pageSize) break
          from += pageSize
        }

        const procurementIds = procurements.map((p) => p.id)
        const paymentRows: Array<{
          procurement_id: number
          beneficiary_name: string | null
          beneficiary_vat_number: string | null
          signers: string | null
          payment_ref_no: string | null
          amount_without_vat: number | null
          amount_with_vat: number | null
        }> = []
        const cpvRows: Array<{ procurement_id: number; cpv_key: string | null; cpv_value: string | null }> = []

        for (const ids of chunk(procurementIds, 200)) {
          const [{ data: pData }, { data: cData }] = await Promise.all([
            supabase
              .from('payment')
              .select(`
                procurement_id,
                beneficiary_name,
                beneficiary_vat_number,
                signers,
                payment_ref_no,
                amount_without_vat,
                amount_with_vat
              `)
              .in('procurement_id', ids),
            supabase
              .from('cpv')
              .select('procurement_id, cpv_key, cpv_value')
              .in('procurement_id', ids),
          ])
          paymentRows.push(...((pData ?? []) as typeof paymentRows))
          cpvRows.push(...((cData ?? []) as typeof cpvRows))
        }

        const totalSpend = paymentRows.reduce((sum, row) => sum + Number(row.amount_without_vat ?? 0), 0)
        const beneficiaryCount = new Set(
          paymentRows
            .map((row) => cleanText(row.beneficiary_name))
            .filter(Boolean),
        ).size

        const cpvCounts = new Map<string, number>()
        const cpvValueCounts = new Map<string, number>()
        const cpvByProcId = new Map<number, Array<{ code: string; label: string }>>()
        for (const row of cpvRows) {
          const code = cleanText(row.cpv_key)
          const value = cleanText(row.cpv_value)
          if (!code) continue
          cpvCounts.set(code, (cpvCounts.get(code) ?? 0) + 1)
          if (value) cpvValueCounts.set(value, (cpvValueCounts.get(value) ?? 0) + 1)
          if (!cpvByProcId.has(row.procurement_id)) cpvByProcId.set(row.procurement_id, [])
          const items = cpvByProcId.get(row.procurement_id)!
          const item = { code, label: value ?? '—' }
          if (!items.find((x) => x.code === item.code && x.label === item.label)) items.push(item)
        }
        const paymentByProcId = new Map<number, {
          beneficiary_name: string | null
          beneficiary_vat_number: string | null
          signers: string | null
          payment_ref_no: string | null
          amount_without_vat: number | null
          amount_with_vat: number | null
        }>()
        for (const row of paymentRows) {
          if (!paymentByProcId.has(row.procurement_id)) paymentByProcId.set(row.procurement_id, row)
        }
        const topCpvs = [...cpvCounts.entries()]
          .sort((a, b) => b[1] - a[1])
          .slice(0, 4)
          .map(([code]) => `CPV ${code}`)
        const topCpvValue = [...cpvValueCounts.entries()]
          .sort((a, b) => b[1] - a[1])[0]?.[0] ?? null

        const { data: latestTimelineRows } = await supabase
          .from('procurement')
          .select(`
            id,
            title,
            submission_at,
            contract_signed_date,
            short_descriptions,
            procedure_type_value,
            reference_number,
            contract_number,
            contract_budget,
            budget,
            assign_criteria,
            contract_type,
            units_operator,
            funding_details_cofund,
            funding_details_self_fund,
            funding_details_espa,
            funding_details_regular_budget,
            auction_ref_no,
            organization_vat_number,
            start_date,
            end_date,
            diavgeia_ada
          `)
          .in('organization_key', organizationKeys)
          .not('contract_signed_date', 'is', null)
          .order('contract_signed_date', { ascending: false })
          .order('id', { ascending: false })
          .limit(5)
        const latestContracts = (latestTimelineRows ?? []) as Array<{
          id: number
          title: string | null
          submission_at: string | null
          contract_signed_date: string | null
          short_descriptions: string | null
          procedure_type_value: string | null
          reference_number: string | null
          contract_number: string | null
          contract_budget: number | null
          budget: number | null
          assign_criteria: string | null
          contract_type: string | null
          units_operator: string | null
          funding_details_cofund: string | null
          funding_details_self_fund: string | null
          funding_details_espa: string | null
          funding_details_regular_budget: string | null
          auction_ref_no: string | null
          organization_vat_number: string | null
          start_date: string | null
          end_date: string | null
          diavgeia_ada: string | null
        }>

        const latestSigned = latestContracts[0]?.contract_signed_date ?? null
        const timeline = latestContracts.map((p) => {
          const payment = paymentByProcId.get(p.id)
          const cpvItems = cpvByProcId.get(p.id) ?? []
          const cpv = cpvItems[0] ?? null
          const contract: LatestContractCard = {
            id: String(p.id),
            who: organizationName,
            what: cleanText(p.title) ?? '—',
            when: formatDateEl(cleanText(p.submission_at)),
            why: toSentenceCaseEl(cpv?.label ?? firstPipePart(p.short_descriptions) ?? '—'),
            beneficiary: toUpperEl(cleanText(payment?.beneficiary_name)),
            contractType: cleanText(p.procedure_type_value) ?? '—',
            howMuch: formatEur(payment?.amount_without_vat ?? null),
            withoutVatAmount: formatEur(payment?.amount_without_vat ?? null),
            withVatAmount: formatEur(payment?.amount_with_vat ?? null),
            referenceNumber: cleanText(p.reference_number) ?? '—',
            contractNumber: cleanText(p.contract_number) ?? '—',
            cpv: cpv?.label ?? '—',
            cpvCode: cpv?.code ?? '—',
            cpvItems,
            signedAt: formatDateEl(cleanText(p.contract_signed_date)),
            startDate: formatDateEl(cleanText(p.start_date)),
            endDate: formatDateEl(cleanText(p.end_date)),
            organizationVat: cleanText(p.organization_vat_number) ?? '—',
            beneficiaryVat: cleanText(payment?.beneficiary_vat_number) ?? '—',
            signers: cleanText(payment?.signers) ?? '—',
            assignCriteria: cleanText(p.assign_criteria) ?? '—',
            contractKind: cleanText(p.contract_type) ?? '—',
            unitsOperator: cleanText(p.units_operator) ?? '—',
            fundingCofund: cleanText(p.funding_details_cofund) ?? '—',
            fundingSelf: cleanText(p.funding_details_self_fund) ?? '—',
            fundingEspa: cleanText(p.funding_details_espa) ?? '—',
            fundingRegular: cleanText(p.funding_details_regular_budget) ?? '—',
            auctionRefNo: cleanText(p.auction_ref_no) ?? '—',
            paymentRefNo: cleanText(payment?.payment_ref_no) ?? '—',
            shortDescription: firstPipePart(p.short_descriptions) ?? '—',
            rawBudget: formatEur(p.budget != null ? Number(p.budget) : null),
            contractBudget: formatEur(p.contract_budget != null ? Number(p.contract_budget) : null),
            documentUrl: cleanText(p.diavgeia_ada) ? `https://diavgeia.gov.gr/doc/${cleanText(p.diavgeia_ada)}` : null,
          }
          const year = cleanText(p.contract_signed_date)?.slice(0, 4) ?? '—'
          return {
            month: monthShortEl(p.contract_signed_date),
            year,
            text: firstPipePart(p.title) ?? 'Καταχώρηση σύμβασης',
            contract,
          }
        })

        const nextState: OrganizationSectionData = {
          name: organizationName,
          yearLabel: cleanText(latestSigned)?.slice(0, 4) ?? String(new Date().getFullYear()),
          totalSpend,
          totalSpendNote: 'Συνολικό ποσό χωρίς ΦΠΑ από τις καταγεγραμμένες πληρωμές του φορέα.',
          cpvCodes: topCpvs,
          topCpvValue,
          contractCount: procurements.length,
          beneficiaryCount,
          latestSignedAt: latestSigned,
          timeline,
        }

        if (!cancelled) setOrganizationSection(nextState)
      } finally {
        if (!cancelled) setOrganizationSectionLoading(false)
      }
    }

    loadOrganizationSection()
    return () => { cancelled = true }
  }, [])

  useEffect(() => {
    let cancelled = false
    setFeaturedBeneficiariesLoading(true)

    const loadFeaturedPanels = async () => {
      const chunk = <T,>(arr: T[], size: number): T[][] => {
        const out: T[][] = []
        for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
        return out
      }

      const daysBetweenInclusive = (start: string | null, end: string | null): number | null => {
        if (!start || !end) return null
        const s = new Date(start)
        const e = new Date(end)
        if (Number.isNaN(s.getTime()) || Number.isNaN(e.getTime())) return null
        const ms = e.getTime() - s.getTime()
        if (ms < 0) return null
        return Math.floor(ms / 86_400_000) + 1
      }

      const toDate = (v: string | null): Date | null => {
        if (!v) return null
        const d = new Date(v)
        return Number.isNaN(d.getTime()) ? null : d
      }

      try {
        const yearStart = '2026-01-01'
        const yearEnd = '2026-12-31'
        const pageSize = 1000
        const procurements: Array<{
          id: number
          organization_key: string | null
          title: string | null
          submission_at: string | null
          contract_signed_date: string | null
          short_descriptions: string | null
          procedure_type_value: string | null
          reference_number: string | null
          contract_number: string | null
          contract_budget: number | null
          budget: number | null
          assign_criteria: string | null
          contract_type: string | null
          units_operator: string | null
          funding_details_cofund: string | null
          funding_details_self_fund: string | null
          funding_details_espa: string | null
          funding_details_regular_budget: string | null
          auction_ref_no: string | null
          organization_vat_number: string | null
          start_date: string | null
          end_date: string | null
          diavgeia_ada: string | null
        }> = []

        let from = 0
        while (true) {
          const to = from + pageSize - 1
          const { data, error } = await supabase
            .from('procurement')
            .select(`
              id,
              organization_key,
              title,
              submission_at,
              contract_signed_date,
              short_descriptions,
              procedure_type_value,
              reference_number,
              contract_number,
              contract_budget,
              budget,
              assign_criteria,
              contract_type,
              units_operator,
              funding_details_cofund,
              funding_details_self_fund,
              funding_details_espa,
              funding_details_regular_budget,
              auction_ref_no,
              organization_vat_number,
              start_date,
              end_date,
              diavgeia_ada
            `)
            .gte('contract_signed_date', yearStart)
            .lte('contract_signed_date', yearEnd)
            .order('id', { ascending: true })
            .range(from, to)
          if (error) break
          const rows = (data ?? []) as Array<{
            id: number
            organization_key: string | null
            title: string | null
            submission_at: string | null
            contract_signed_date: string | null
            short_descriptions: string | null
            procedure_type_value: string | null
            reference_number: string | null
            contract_number: string | null
            contract_budget: number | null
            budget: number | null
            assign_criteria: string | null
            contract_type: string | null
            units_operator: string | null
            funding_details_cofund: string | null
            funding_details_self_fund: string | null
            funding_details_espa: string | null
            funding_details_regular_budget: string | null
            auction_ref_no: string | null
            organization_vat_number: string | null
            start_date: string | null
            end_date: string | null
            diavgeia_ada: string | null
          }>
          procurements.push(...rows)
          if (rows.length < pageSize) break
          from += pageSize
        }

        const ids = procurements.map((p) => p.id)
        const orgKeys = Array.from(new Set(procurements.map((p) => cleanText(p.organization_key)).filter(Boolean))) as string[]

        const paymentRows: Array<{
          procurement_id: number
          beneficiary_name: string | null
          amount_without_vat: number | null
          amount_with_vat: number | null
          beneficiary_vat_number: string | null
          signers: string | null
          payment_ref_no: string | null
        }> = []
        for (const c of chunk(ids, 200)) {
          const { data } = await supabase
            .from('payment')
            .select(`
              procurement_id,
              beneficiary_name,
              amount_without_vat,
              amount_with_vat,
              beneficiary_vat_number,
              signers,
              payment_ref_no
            `)
            .in('procurement_id', c)
          paymentRows.push(...((data ?? []) as typeof paymentRows))
        }

        const cpvRows: Array<{ procurement_id: number; cpv_key: string | null; cpv_value: string | null }> = []
        for (const c of chunk(ids, 200)) {
          const { data } = await supabase
            .from('cpv')
            .select('procurement_id, cpv_key, cpv_value')
            .in('procurement_id', c)
          cpvRows.push(...((data ?? []) as typeof cpvRows))
        }

        const orgNameByKey = new Map<string, string>()
        for (const c of chunk(orgKeys, 200)) {
          const { data } = await supabase
            .from('organization')
            .select('organization_key, organization_normalized_value, organization_value')
            .in('organization_key', c)
          for (const row of (data ?? []) as Array<{ organization_key: string; organization_normalized_value: string | null; organization_value: string | null }>) {
            if (!orgNameByKey.has(row.organization_key)) {
              orgNameByKey.set(
                row.organization_key,
                cleanText(row.organization_normalized_value) ?? cleanText(row.organization_value) ?? row.organization_key,
              )
            }
          }
        }

        const paymentByProc = new Map<number, Array<{
          beneficiary: string
          amount: number
          amountWithVat: number | null
          beneficiaryVat: string | null
          signer: string
          paymentRefNo: string | null
        }>>()
        for (const p of paymentRows) {
          const beneficiary = toUpperEl(cleanText(p.beneficiary_name))
          if (!beneficiary) continue
          const amount = Number(p.amount_without_vat ?? 0)
          const amountWithVat = p.amount_with_vat != null ? Number(p.amount_with_vat) : null
          const beneficiaryVat = cleanText(p.beneficiary_vat_number)
          const signer = cleanText(p.signers) ?? '—'
          const paymentRefNo = cleanText(p.payment_ref_no)
          if (!paymentByProc.has(p.procurement_id)) paymentByProc.set(p.procurement_id, [])
          paymentByProc.get(p.procurement_id)!.push({ beneficiary, amount, amountWithVat, beneficiaryVat, signer, paymentRefNo })
        }

        const cpvByProc = new Map<number, Array<{ code: string; label: string }>>()
        for (const c of cpvRows) {
          const cpv = cleanText(c.cpv_value)
          const code = cleanText(c.cpv_key)
          if (!cpv && !code) continue
          if (!cpvByProc.has(c.procurement_id)) cpvByProc.set(c.procurement_id, [])
          const items = cpvByProc.get(c.procurement_id)!
          const item = { code: code ?? '—', label: cpv ?? '—' }
          if (!items.find((x) => x.code === item.code && x.label === item.label)) items.push(item)
        }

        type BeneficiaryAgg = {
          beneficiary: string
          totalAmount: number
          contractCount: number
          orgTotals: Map<string, number>
          cpvCounts: Map<string, number>
          signerCounts: Map<string, number>
          startDates: string[]
          endDates: string[]
          contractsByProc: Map<number, {
            amount: number
            amountWithVat: number | null
            beneficiaryVat: string | null
            signer: string
            paymentRefNo: string | null
          }>
        }

        const agg = new Map<string, BeneficiaryAgg>()

        for (const pr of procurements) {
          const entries = paymentByProc.get(pr.id) ?? []
          if (!entries.length) continue
          const orgKey = cleanText(pr.organization_key)
          const org = orgKey ? (orgNameByKey.get(orgKey) ?? orgKey) : '—'
          const cpvs = cpvByProc.get(pr.id) ?? []

          for (const entry of entries) {
            const key = entry.beneficiary
            if (!agg.has(key)) {
              agg.set(key, {
                beneficiary: key,
                totalAmount: 0,
                contractCount: 0,
                orgTotals: new Map<string, number>(),
                cpvCounts: new Map<string, number>(),
                signerCounts: new Map<string, number>(),
                startDates: [],
                endDates: [],
                contractsByProc: new Map<number, {
                  amount: number
                  amountWithVat: number | null
                  beneficiaryVat: string | null
                  signer: string
                  paymentRefNo: string | null
                }>(),
              })
            }
            const a = agg.get(key)!
            a.totalAmount += entry.amount
            a.contractCount += 1
            a.orgTotals.set(org, (a.orgTotals.get(org) ?? 0) + entry.amount)
            if (entry.signer && entry.signer !== '—') a.signerCounts.set(entry.signer, (a.signerCounts.get(entry.signer) ?? 0) + 1)
            for (const cpv of cpvs) a.cpvCounts.set(cpv.label, (a.cpvCounts.get(cpv.label) ?? 0) + 1)
            const start = cleanText(pr.start_date)
            const end = cleanText(pr.end_date)
            if (start) a.startDates.push(start)
            if (end) a.endDates.push(end)
            const existingContract = a.contractsByProc.get(pr.id)
            if (!existingContract || entry.amount > existingContract.amount) {
              a.contractsByProc.set(pr.id, {
                amount: entry.amount,
                amountWithVat: entry.amountWithVat,
                beneficiaryVat: entry.beneficiaryVat,
                signer: entry.signer,
                paymentRefNo: entry.paymentRefNo,
              })
            }
          }
        }

        const procurementById = new Map<number, (typeof procurements)[number]>()
        for (const pr of procurements) procurementById.set(pr.id, pr)

        const rows: BeneficiaryInsightRow[] = [...agg.values()].map((a) => {
          const organization =
            [...a.orgTotals.entries()].sort((x, y) => y[1] - x[1])[0]?.[0] ?? '—'
          const cpv =
            [...a.cpvCounts.entries()].sort((x, y) => y[1] - x[1])[0]?.[0] ?? '—'
          const signer =
            [...a.signerCounts.entries()].sort((x, y) => y[1] - x[1])[0]?.[0] ?? '—'
          const startIso = [...a.startDates].sort()[0] ?? null
          const endIso = [...a.endDates].sort().slice(-1)[0] ?? null
          const durationDays = daysBetweenInclusive(
            startIso,
            endIso,
          )
          const start = toDate(startIso)
          const end = toDate(endIso)
          const now = new Date()
          let progressPct: number | null = null
          if (start && end && end.getTime() > start.getTime()) {
            if (now <= start) progressPct = 0
            else if (now >= end) progressPct = 100
            else progressPct = ((now.getTime() - start.getTime()) / (end.getTime() - start.getTime())) * 100
          }
          const relevantContracts = [...a.contractsByProc.entries()]
            .sort((x, y) => y[1].amount - x[1].amount)
            .slice(0, 5)
            .map<LatestContractCard | null>(([procId, entry]) => {
              const pr = procurementById.get(procId)
              if (!pr) return null
              const cpvs = cpvByProc.get(pr.id) ?? []
              const topCpv = cpvs[0] ?? null
              return {
                id: String(pr.id),
                who: organization,
                what: cleanText(pr.title) ?? '—',
                when: formatDateEl(cleanText(pr.submission_at)),
                why: toSentenceCaseEl(topCpv?.label ?? firstPipePart(pr.short_descriptions) ?? '—'),
                beneficiary: a.beneficiary,
                contractType: cleanText(pr.procedure_type_value) ?? '—',
                howMuch: formatEur(entry.amount),
                withoutVatAmount: formatEur(entry.amount),
                withVatAmount: formatEur(entry.amountWithVat ?? null),
                referenceNumber: cleanText(pr.reference_number) ?? '—',
                contractNumber: cleanText(pr.contract_number) ?? '—',
                cpv: topCpv?.label ?? '—',
                cpvCode: topCpv?.code ?? '—',
                cpvItems: cpvs,
                signedAt: formatDateEl(cleanText(pr.contract_signed_date)),
                startDate: formatDateEl(cleanText(pr.start_date)),
                endDate: formatDateEl(cleanText(pr.end_date)),
                organizationVat: cleanText(pr.organization_vat_number) ?? '—',
                beneficiaryVat: entry.beneficiaryVat ?? '—',
                signers: entry.signer ?? '—',
                assignCriteria: cleanText(pr.assign_criteria) ?? '—',
                contractKind: cleanText(pr.contract_type) ?? '—',
                unitsOperator: cleanText(pr.units_operator) ?? '—',
                fundingCofund: cleanText(pr.funding_details_cofund) ?? '—',
                fundingSelf: cleanText(pr.funding_details_self_fund) ?? '—',
                fundingEspa: cleanText(pr.funding_details_espa) ?? '—',
                fundingRegular: cleanText(pr.funding_details_regular_budget) ?? '—',
                auctionRefNo: cleanText(pr.auction_ref_no) ?? '—',
                paymentRefNo: entry.paymentRefNo ?? '—',
                shortDescription: firstPipePart(pr.short_descriptions) ?? '—',
                rawBudget: formatEur(pr.budget != null ? Number(pr.budget) : null),
                contractBudget: formatEur(pr.contract_budget != null ? Number(pr.contract_budget) : null),
                documentUrl: cleanText(pr.diavgeia_ada) ? `https://diavgeia.gov.gr/doc/${cleanText(pr.diavgeia_ada)}` : null,
              }
            })
            .filter((v): v is LatestContractCard => v !== null)
          return {
            beneficiary: a.beneficiary,
            organization,
            totalAmount: a.totalAmount,
            contractCount: a.contractCount,
            cpv,
            startDate: startIso ? formatDateEl(startIso) : '—',
            endDate: endIso ? formatDateEl(endIso) : '—',
            duration: durationDays != null ? `${durationDays} ημέρες` : '—',
            progressPct: progressPct != null ? Math.max(0, Math.min(100, progressPct)) : null,
            signer,
            relevantContracts,
          }
        })

        const byTotal = [...rows].sort((a, b) => b.totalAmount - a.totalAmount).slice(0, 50)
        if (!cancelled) setFeaturedBeneficiaries(byTotal)
      } finally {
        if (!cancelled) setFeaturedBeneficiariesLoading(false)
      }
    }

    loadFeaturedPanels()
    return () => { cancelled = true }
  }, [])

  useEffect(() => {
    let cancelled = false
    setHeroStatsLoading(true)

    const loadHeroStats = async () => {
      try {
        const { data, error } = await supabase.rpc('get_hero_section_data', {
          p_year_main: currentYear,
          p_year_start: YEAR_START,
        })
        if (cancelled || error || !data) return

        const payload = data as HeroSectionRpcResponse
        setHeroStats({
          periodMainStart: cleanText(payload.period_main_start) ?? '',
          periodMainEnd: cleanText(payload.period_main_end) ?? '',
          totalMain: Number(payload.total_main ?? 0),
          totalPrev1: Number(payload.total_prev1 ?? 0),
          totalPrev2: Number(payload.total_prev2 ?? 0),
          totalVsPrev1Pct: null,
          topContractType: cleanText(payload.top_contract_type) ?? '—',
          topContractTypeCount: Number(payload.top_contract_type_count ?? 0),
          topContractTypePrevCount: Number(payload.top_contract_type_prev1_count ?? 0),
          topContractTypeVsPrev1Pct: null,
          topCpvText: toSentenceCaseEl(cleanText(payload.top_cpv_text)),
          topCpvCount: Number(payload.top_cpv_count ?? 0),
          topCpvPrevCount: Number(payload.top_cpv_prev1_count ?? 0),
          topCpvVsPrev1Pct: null,
        })

        const points: HeroCurvePoint[] = ((payload.curve_points ?? []) as HeroSectionRpcPoint[]).map((p) => ({
          year: Number(p.series_year),
          dayOfYear: Number(p.day_of_year),
          yearDays: Number(p.year_days),
          value: Number(p.cumulative_amount ?? 0),
        }))
        setHeroCurvePoints(points)
      } finally {
        if (!cancelled) setHeroStatsLoading(false)
      }
    }

    loadHeroStats()
    return () => { cancelled = true }
  }, [])

  const downloadContractPdf = (contract: LatestContractCard) => {
    openContractPdfPrintView(contract)
  }

  const scrollToSection = (id: string) => {
    const el = document.getElementById(id)
    if (!el) return
    el.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }

  return (
    <div className="pyro-app">
      <ComponentTag name="App" />
      <div className="page-grid" aria-hidden="true" />

      <DebugComponentLabel name="SiteHeader" />
      <header className="site-header">
        <div className="brand-block">
          <div className="eyebrow">παρατηρητηριο για τις δασικές πυρκαγιές</div>
          <div className="brand-line">
            <h1>Π <span className="beta-badge">BETA</span></h1>
            <span className="brand-mark">Τελευταία ενημέρωση / {lastDbUpdateLabel}</span>
          </div>
        </div>
        <nav className="top-nav" aria-label="Κύρια πλοήγηση">
          <Link to="/contracts">Συμβάσεις</Link>
          <Link to="/maps">Χάρτης</Link>
          <button type="button" onClick={() => scrollToSection('about')}>About</button>
        </nav>
      </header>

      <main>
        <DebugComponentLabel name="LatestContractsSection" />
        <section id="latest" className="news-wire section-rule" aria-label="Τελευταία ρεπορτάζ">
          <div className="news-wire__label">
            <span className="eyebrow">τελευταία</span>
            <strong>Οι πιο πρόσφατες συμβάσεις που έχουν δημοσιευτεί στο <a href = "https://eprocurement.gov.gr/">Kεντρικό Ηλεκτρονικό Μητρώο Δημοσίων Συμβάσεων</a> και αφορούν στην πρόληψη και αντιμετώπιση δασικών πυρκαγιών.</strong>
            <Link className="news-wire__all-link" to="/contracts">Δες όλες τις συμβάσεις</Link>
          </div>
          <div className="news-wire__items">
            {latestContractsLoading && (
              <article className="wire-item">
                <span className="wire-item__slug">LIVE</span>
                <h2>Φόρτωση τελευταίων συμβάσεων…</h2>
                <p>Σύνδεση με το dataset `procurement`.</p>
              </article>
            )}
            {!latestContractsLoading && latestContracts.map((item) => (
              <LatestContractCardItem
                key={item.id}
                item={item}
                onOpen={(id) => {
                  const found = latestContracts.find((x) => x.id === id)
                  if (found) setSelectedContract(found)
                }}
                contractTypeTransform={toLowerEl}
              />
            ))}
            {!latestContractsLoading && latestContracts.length === 0 && (
              <article className="wire-item">
                <h2>Δεν βρέθηκαν πρόσφατες συμβάσεις.</h2>
                <p>Ελέγξτε ότι ο πίνακας `procurement` έχει δεδομένα.</p>
              </article>
            )}
          </div>
        </section>

        <DebugComponentLabel name="HeroSection" />
        <section className="hero section-rule">
          <div className="hero-left">
            <div className="hero-chart">
              <div className="hero-chart__head">
                <span className="eyebrow">Εξέλιξη δαπανών</span>
              </div>
              <svg viewBox="0 0 760 300" role="img" aria-label="Σωρευτική πορεία δαπανών ανά έτος">
                {[1 / 3, 2 / 3].map((t, i) => {
                  const y = 230 - t * 200
                  return <line key={`hy-${i}`} x1="44" y1={y} x2="736" y2={y} stroke="rgba(17,17,17,0.12)" />
                })}

                {chartYears.map((year) => {
                  const points = chartByYear.get(year) ?? []
                  if (points.length === 0) return null
                  const styleIdx = Math.min(currentYear - year, CHART_YEAR_STYLES.length - 1)
                  const { stroke, opacity, strokeWidth } = CHART_YEAR_STYLES[styleIdx]
                  const d = points.map((p, i) => {
                    const x = 44 + dayFraction(p.dayOfYear, p.yearDays) * (736 - 44)
                    const y = 230 - (p.value / chartMax) * 200
                    return `${i === 0 ? 'M' : 'L'} ${x} ${y}`
                  }).join(' ')
                  return (
                    <path
                      key={`line-${year}`}
                      d={d}
                      fill="none"
                      stroke={stroke}
                      strokeOpacity={opacity}
                      strokeWidth={strokeWidth}
                    />
                  )
                })}

                {[0, 0.5, 1].map((t) => {
                  const y = 230 - t * 200
                  const v = chartMax * t
                  return (
                    <text
                      key={`yt-${t}`}
                      x="8"
                      y={y + 4}
                      fontFamily="var(--font-mono)"
                      fontSize="10"
                      fill="var(--ink-faint)"
                    >
                      {formatEurCompact(v)}
                    </text>
                  )
                })}

                {chartTicks.map((tick) => {
                  const dayInYear = Math.floor((Date.UTC(2025, tick.month - 1, tick.day) - Date.UTC(2025, 0, 1)) / 86_400_000) + 1
                  const x = 44 + dayFraction(dayInYear, 365) * (736 - 44)
                  return (
                    <text
                      key={`xt-${tick.label}`}
                      x={x}
                      y="252"
                      textAnchor="middle"
                      fontFamily="var(--font-mono)"
                      fontSize="10"
                      fill="var(--ink-faint)"
                    >
                      {tick.label}
                    </text>
                  )
                })}
              </svg>
              <div className="hero-chart__legend">
                {[...chartYears].reverse().map((year) => {
                  const styleIdx = Math.min(currentYear - year, CHART_YEAR_STYLES.length - 1)
                  const { stroke, opacity } = CHART_YEAR_STYLES[styleIdx]
                  return (
                    <span key={year}>
                      <i style={{ background: stroke, opacity }} />
                      {year}
                    </span>
                  )
                })}
              </div>
            </div>
          </div>

          <div className="hero-right">
            <div className="hero-background-year" aria-hidden="true">
              {currentYear}
            </div>
            <div className="hero-amount-card">
              <div className="eyebrow">
                {heroStatsLoading
                  ? 'Δαπάνες'
                  : `Δαπάνες από ${formatDateLabel(heroStats.periodMainStart)} έως ${formatDateLabel(heroStats.periodMainEnd)}`}
              </div>
              <div className="hero-amount">
                {heroStatsLoading ? '…' : formatEurCompact(heroStats.totalMain)}
              </div>
              <div className="hero-subgrid">
                <div>
                  <span className="label">Σύγκριση με {currentYear - 1}</span>
                  <strong>
                    {heroStatsLoading ? '…' : (
                      <>
                        <span style={{ color: pctColor(totalVsPrev1Pct) }}>
                          {formatPct(totalVsPrev1Pct)}
                        </span>
                        <div className="wire-item__date" style={{ marginTop: '0.2rem', display: 'block' }}>
                          ({formatEurCompact(heroStats.totalPrev1)})
                        </div>
                      </>
                    )}
                  </strong>
                </div>
                <div>
                  <span className="label">Σύγκριση με {currentYear - 2}</span>
                  <strong>
                    {heroStatsLoading ? '…' : (
                      <>
                        <span style={{ color: pctColor(totalVsPrev2Pct) }}>
                          {formatPct(totalVsPrev2Pct)}
                        </span>
                        <div className="wire-item__date" style={{ marginTop: '0.2rem', display: 'block' }}>
                          ({formatEurCompact(heroStats.totalPrev2)})
                        </div>
                      </>
                    )}
                  </strong>
                </div>
                <div>
                  <span className="label">Συχνότερη διαδικασία ανάθεσης</span>
                  <strong>
                    {heroStatsLoading ? '…' : (
                      <>
                        {heroStats.topContractTypeCount} συμβάσεις έγιναν με {heroStats.topContractType}
                        {' '} |{' '}
                        <span style={{ color: pctColor(topTypeVsPrev1Pct) }}>
                          {formatPct(topTypeVsPrev1Pct)}</span> σε σύγκριση με πέρυσι
                        <div className="note-text" style={{ marginTop: '0.2rem', display: 'block' }}>
                          σε σύγκριση με {currentYear - 1}
                        </div>
                      </>
                    )}
                  </strong>
                </div>
                <div>
                  <span className="label">Συχνότερες Εργασίες</span>
                  <strong>
                    {heroStatsLoading ? '…' : (
                      <>
                        Σε {heroStats.topCpvCount}* συμβάσεις έχουν ως περιγραφή «{heroStats.topCpvText}»
                        {' '} |{' '}
                        <span style={{ color: pctColor(topCpvVsPrev1Pct) }}>
                          {formatPct(topCpvVsPrev1Pct)} </span>σε σύγκριση με πέρυσι
                        <div className="label" style={{ marginTop: '0.2rem', display: 'block' }}>
                          <span className="note-text" style={{ display: 'block' }}> * Ορισμένες συμβάσεις έχουν περισσότερες της μίας περιγραφές</span>
                        </div>
                      </>
                    )}
                  </strong>
                </div>
              </div>
            </div>
          </div>
        </section>
        <DebugComponentLabel name="ContractAnalysis" />
        <ErrorBoundary fallback={<div className="ca-empty-note">Η ενότητα ανάλυσης δεν είναι διαθέσιμη προσωρινά.</div>}>
          <ContractAnalysis />
        </ErrorBoundary>

        <DebugComponentLabel name="FeaturedRecordsSection" />
        <section id="records" className="records section-rule">
          <div className="section-head">
            <div className="eyebrow">Top Beneficiaries / 2026</div>
            <h2>Κάρτες δικαιούχων για το 2026. Οριζόντια πλοήγηση στη λίστα.</h2>
          </div>

          <div className="records-grid records-grid--horizontal">
            {featuredBeneficiariesLoading && (
              <article className="record-card">
                <div className="record-card__header">
                  <div className="record-card__authority">Top Beneficiaries 2026</div>
                  <div className="record-card__id">Φόρτωση…</div>
                </div>
                <h3>Ανάκτηση στοιχείων από procurement/payment/cpv.</h3>
              </article>
            )}

            {!featuredBeneficiariesLoading && featuredBeneficiaries.map((row, idx) => (
              <article
                className="record-card"
                key={`${row.beneficiary}-${idx}`}
              >
                <div className="record-card__year" aria-hidden="true">2026</div>
                <div className="record-card__header">
                  <div className="record-card__authority">Top Beneficiary #{idx + 1}</div>
                  <div className="record-card__id">Συμβάσεις: {row.contractCount.toLocaleString('el-GR')}</div>
                </div>
                <h3>{row.beneficiary}</h3>
                <div className="record-card__amount">{formatEur(row.totalAmount)}</div>
                <div className="record-card__tags" aria-label="Μεταδεδομένα δικαιούχου">
                  <span>CPV: {row.cpv}</span>
                  <span>Έναρξη: {row.startDate}</span>
                </div>
                <div className="record-duration">
                  <div className="record-duration__head">
                    <span>Διάρκεια: {row.duration}</span>
                    <span>Λήξη: {row.endDate}</span>
                  </div>
                  <div className="record-duration__track" aria-label="Πρόοδος διάρκειας έργου">
                    <div
                      className="record-duration__fill"
                      style={{ width: `${row.progressPct == null ? 0 : row.progressPct}%` }}
                    />
                    <div
                      className="record-duration__today"
                      style={{ left: `${row.progressPct == null ? 0 : row.progressPct}%` }}
                      title="Σήμερα"
                    />
                  </div>
                </div>
                {row.relevantContracts.length > 0 && (
                  <div className="record-contract-amounts" aria-label="Σχετικές συμβάσεις">
                    <div className="record-contract-amounts__title">Σχετικές συμβάσεις</div>
                    <ul>
                      {row.relevantContracts.map((contract) => (
                        <li key={`${row.beneficiary}-contract-${contract.id}`}>
                          <button
                            type="button"
                            className="record-contract-link"
                            onClick={() => setSelectedContract(contract)}
                          >
                            {contract.what} - {contract.withoutVatAmount}
                          </button>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
                <div className="record-card__footer">
                  <div>
                    <span className="label">Οργανισμός</span>
                    <strong>{row.organization}</strong>
                  </div>
                  <div>
                    <span className="label">Υπογράφων</span>
                    <strong>{row.signer}</strong>
                  </div>
                </div>
              </article>
            ))}
          </div>
        </section>

        <DebugComponentLabel name="OrganizationSection" />
        <OrganizationSection
          data={organizationSection}
          loading={organizationSectionLoading}
          formatEurCompact={formatEurCompact}
          formatDateEl={formatDateEl}
          onOpenContract={(contract) => setSelectedContract(contract)}
        />

        <DebugComponentLabel name="AboutSection" />
        <section id="about" className="about-panel section-rule">
          <div className="about-panel__left">
            <div className="eyebrow">Σχετικά με το Project ΠΥΡ</div>
            <h2>Παρατηρητήριο Ετοιμότητας για Δασικές Πυρκαγιές</h2>
            <p>
              Ανεξάρτητη δημοσιογραφική πλατφόρμα παρακολούθησης: καταγράφουμε τα μέτρα πρόληψης των αρμόδιων φορέων και ενημερώνουμε σε πραγματικό χρόνο για εξελισσόμενες πυρκαγιές σε όλη την ελληνική επικράτεια.
            </p>
          </div>
          <div className="about-panel__right">
            <div className="poster-motif" aria-hidden="true">
              <div className="poster-motif__sun" />
              <div className="poster-motif__terrain" />
            </div>
            <div className="about-stats">
              <div>
                <span className="label">ΠΟΙΟΣ</span>
                <strong>
                  <a href="https://troboukis.github.io">Θανάσης Τρομπούκης</a>
                </strong>
                <span className="about-stats__sub">Δημοσιογράφος</span>
              </div>
              <div>
                <span className="label">ΠΩΣ</span>
                <strong>Με Claude, Codex και Python φυσικά.</strong>
              </div>
              <div>
                <span className="label">ΓΙΑΤΙ</span>
                <strong>Διότι η λογοδοσία είναι θεμέλιο της δημοκρατίας</strong>
              </div>
            </div>
          </div>
        </section>
      </main>

      {selectedContract && (
        <>
          <DebugComponentLabel name="ContractModal" />
          <ContractModal
            contract={selectedContract}
            onClose={() => setSelectedContract(null)}
            onDownloadPdf={() => downloadContractPdf(selectedContract)}
          />
        </>
      )}
    </div>
  )
}
