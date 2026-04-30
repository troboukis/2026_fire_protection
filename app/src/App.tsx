import { Fragment, Suspense, lazy, useEffect, useMemo, useState, type CSSProperties } from 'react'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import type { ContractModalContract } from './components/ContractModal'
import type { BeneficiaryInsightRow, FeaturedRecordContract } from './components/FeaturedRecordsSection'
import ComponentTag from './components/ComponentTag'
import FireNowTicker from './components/FireNowTicker'
import LatestContractCardItem, { type LatestContractCardView } from './components/LatestContractCard'
import type { OrganizationSectionData } from './components/OrganizationSection'
import type { RegionSectionData } from './components/RegionSection'
import DataLoadingCard from './components/DataLoadingCard'
import { buildDiavgeiaDocumentUrl, downloadContractDocument } from './lib/contractDocument'
import { buildContractsPageHref } from './lib/contractsPageHref'
import { createHomepageRpcCacheKey, loadCachedHomepageRpc, retryHomepageRpc } from './lib/homepageRpcCache'
import type { AuthorityScope } from './lib/latestContractCard'
import { supabase } from './lib/supabase'

const ContractModal = lazy(() => import('./components/ContractModal'))
const FireCopernicusSection = lazy(() => import('./components/FireCopernicusSection'))
const FeaturedRecordsSection = lazy(() => import('./components/FeaturedRecordsSection'))
const Funding = lazy(() => import('./components/Funding'))
const OrganizationSection = lazy(() => import('./components/OrganizationSection'))
const RegionSection = lazy(() => import('./components/RegionSection'))

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
  awardProcedure: string
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
  contractRelatedAda: string
  previousReferenceNumber: string
  nextReferenceNumber: string
  documentUrl: string | null
}

type LatestContractRpcRow = {
  procurement_id: number | string
  who: string | null
  title: string | null
  submission_at: string | null
  contract_signed_date: string | null
  short_description: string | null
  procedure_type_value: string | null
  beneficiary_name: string | null
  beneficiary_vat_number: string | null
  amount_without_vat: number | string | null
  amount_with_vat: number | string | null
  reference_number: string | null
  contract_number: string | null
  cpv_items: Array<{ code?: string | null; label?: string | null }> | null
  organization_vat_number: string | null
  signers: string | null
  assign_criteria: string | null
  contract_type: string | null
  award_procedure: string | null
  units_operator: string | null
  funding_details_cofund: string | null
  funding_details_self_fund: string | null
  funding_details_espa: string | null
  funding_details_regular_budget: string | null
  auction_ref_no: string | null
  payment_ref_no: string | null
  budget: number | string | null
  contract_budget: number | string | null
  contract_related_ada: string | null
  prev_reference_no: string | null
  next_ref_no: string | null
  diavgeia_ada: string | null
  start_date: string | null
  end_date: string | null
  municipality_key: string | null
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

type FeaturedRecordsRpcContract = {
  id: number | string
  organization: string | null
  title: string | null
  submission_at: string | null
  short_description: string | null
  procedure_type_value: string | null
  amount_without_vat: number | string | null
  amount_with_vat: number | string | null
  reference_number: string | null
  contract_number: string | null
  cpv_items: Array<{ code?: string | null; label?: string | null }> | null
  contract_signed_date: string | null
  start_date: string | null
  end_date: string | null
  organization_vat_number: string | null
  beneficiary_vat_number: string | null
  beneficiary_name: string | null
  signers: string | null
  assign_criteria: string | null
  contract_type: string | null
  award_procedure?: string | null
  units_operator: string | null
  funding_details_cofund: string | null
  funding_details_self_fund: string | null
  funding_details_espa: string | null
  funding_details_regular_budget: string | null
  auction_ref_no: string | null
  payment_ref_no: string | null
  budget: number | string | null
  contract_budget: number | string | null
  contract_related_ada?: string | null
  prev_reference_no?: string | null
  next_ref_no?: string | null
  diavgeia_ada: string | null
}

type FeaturedRecordsRpcRow = {
  beneficiary_vat_number: string | null
  beneficiary_name: string | null
  organization: string | null
  total_amount: number | string | null
  contract_count: number | string | null
  cpv: string | null
  start_date: string | null
  end_date: string | null
  duration_days: number | string | null
  progress_pct: number | string | null
  signer: string | null
  relevant_contracts: FeaturedRecordsRpcContract[] | null
}

type OrganizationSectionConfig = {
  fallbackName: string
  organizationKeys: string[]
  anchorId?: string
}

type RegionSectionConfig = {
  fallbackName: string
  regionKey: string
  anchorId?: string
}

type RegionDirectoryRow = {
  region_key: string | null
  region_value: string | null
  region_normalized_value: string | null
}

function cleanText(v: unknown): string | null {
  if (v == null) return null
  const s = String(v).trim()
  if (!s || s.toLowerCase() === 'nan' || s.toLowerCase() === 'none') return null
  return s
}

function buildRegionFallbackName(row: RegionDirectoryRow): string {
  const baseName = cleanText(row.region_normalized_value) ?? cleanText(row.region_value) ?? cleanText(row.region_key)
  if (!baseName) return 'Περιφέρεια'
  return baseName.startsWith('Περιφέρεια ') ? baseName : `Περιφέρεια ${baseName}`
}

function pickRandomRegionSectionConfig(rows: RegionDirectoryRow[], fallback: RegionSectionConfig): RegionSectionConfig {
  const configs = rows
    .map<RegionSectionConfig | null>((row) => {
      const regionKey = cleanText(row.region_key)
      if (!regionKey) return null
      return {
        fallbackName: buildRegionFallbackName(row),
        regionKey,
        anchorId: fallback.anchorId,
      }
    })
    .filter((config): config is RegionSectionConfig => config !== null)

  const availableConfigs = configs.length ? configs : [fallback]
  const previousRegionKey = window.localStorage.getItem(HOME_REGION_STORAGE_KEY)
  const candidateConfigs =
    previousRegionKey && availableConfigs.length > 1
      ? availableConfigs.filter((config) => config.regionKey !== previousRegionKey)
      : availableConfigs
  const selectedConfig = candidateConfigs[Math.floor(Math.random() * candidateConfigs.length)] ?? fallback
  window.localStorage.setItem(HOME_REGION_STORAGE_KEY, selectedConfig.regionKey)
  return selectedConfig
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

function toFiniteNumber(v: unknown): number | null {
  if (v == null || v === '') return null
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = []
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
  return out
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

function logLoadError(context: string, error: unknown) {
  console.error(`Failed to load ${context}`, error)
}

function dayFraction(dayOfYear: number, yearDays: number): number {
  const denom = Math.max(1, yearDays - 1)
  return Math.min(1, Math.max(0, (dayOfYear - 1) / denom))
}

function DebugClassLabel({ name, style }: { name: string, style?: CSSProperties }) {
  return <ComponentTag name={name} kind="CLASS" className="component-tag--overlay" style={style} />
}

function SectionFallback({ label }: { label: string }) {
  return (
    <section className="section-rule page-loading page-loading--section" aria-label={label}>
      <DataLoadingCard message={label} />
    </section>
  )
}

function monthShortEl(iso: string | null): string {
  if (!iso) return '—'
  const dt = new Date(iso)
  if (Number.isNaN(dt.getTime())) return '—'
  return new Intl.DateTimeFormat('el-GR', { month: 'short' })
    .format(dt)
    .replace('.', '')
    .toLocaleUpperCase('el-GR')
}

const YEAR_START = 2024

const HOME_ORGANIZATION_SECTIONS: OrganizationSectionConfig[] = [
  { fallbackName: 'ΑΔΜΗΕ', organizationKeys: ['org_a5ffb236a53cf3e9eeae'], anchorId: 'organizations' },
]

const HOME_REGION_STORAGE_KEY = 'firewatch.homepage-region.last-key'

const DEFAULT_HOME_REGION_SECTION: RegionSectionConfig = {
  fallbackName: 'Περιφέρεια Αττικής',
  regionKey: 'ΑΤΤΙΚΗΣ',
  anchorId: 'homepage-region',
}

const INITIAL_HOME_REGION_SECTION: RegionSectionConfig = {
  fallbackName: 'Περιφέρεια',
  regionKey: '',
  anchorId: 'homepage-region',
}

const CHART_TICKS = [
  { label: '01 Ιαν', month: 1, day: 1 },
  { label: '01 Μαϊ', month: 5, day: 1 },
  { label: '01 Αυγ', month: 8, day: 1 },
  { label: '31 Δεκ', month: 12, day: 31 },
]

function getChartYearStyle(year: number, currentYear: number) {
  const yearsBehind = Math.max(0, currentYear - year)
  if (yearsBehind === 0) {
    return {
      stroke: '#111111',
      opacity: 1,
      strokeWidth: 3.8,
    }
  }

  const baseOpacity = 0.95
  const fadedOpacity = Math.max(0.16, baseOpacity * (0.5 ** (yearsBehind - 1)))

  return {
    stroke: '#d9a095',
    opacity: fadedOpacity,
    strokeWidth: 2.4,
  }
}

function createEmptyOrganizationSectionData(name: string, currentYear: number, contractsPageHref: string | null = null): OrganizationSectionData {
  return {
    name,
    yearLabel: String(currentYear),
    previousYearLabel: String(currentYear - 1),
    totalSpend: 0,
    cpvCodes: [],
    cpvCodeItems: [],
    topCpvValue: null,
    previousYearTopCpvValue: null,
    contractCount: 0,
    previousYearContractCount: 0,
    beneficiaryCount: 0,
    previousYearBeneficiaryCount: 0,
    latestSignedAt: null,
    activityWorkPoints: [],
    timeline: [],
    contractsPageHref,
  }
}

export default function App() {
  const currentYear = new Date().getFullYear()
  const featuredRecordsYear = String(currentYear)
  const location = useLocation()
  const navigate = useNavigate()
  const [latestContracts, setLatestContracts] = useState<LatestContractCard[]>([])
  const [latestContractsLoading, setLatestContractsLoading] = useState(true)
  const [latestContractsError, setLatestContractsError] = useState<string | null>(null)
  const [selectedContract, setSelectedContract] = useState<LatestContractCard | null>(null)
  const [heroStatsLoading, setHeroStatsLoading] = useState(true)
  const [heroStatsError, setHeroStatsError] = useState<string | null>(null)
  const [featuredBeneficiaries, setFeaturedBeneficiaries] = useState<BeneficiaryInsightRow[]>([])
  const [featuredBeneficiariesLoading, setFeaturedBeneficiariesLoading] = useState(true)
  const [organizationSections, setOrganizationSections] = useState<OrganizationSectionData[]>(
    () => HOME_ORGANIZATION_SECTIONS.map(({ fallbackName, organizationKeys }) => (
      createEmptyOrganizationSectionData(
        fallbackName,
        currentYear,
        buildContractsPageHref({ organizationKeys }),
      )
    )),
  )
  const [organizationSectionsLoading, setOrganizationSectionsLoading] = useState(true)
  const [homeRegionConfig, setHomeRegionConfig] = useState<RegionSectionConfig>(INITIAL_HOME_REGION_SECTION)
  const [regionSection, setRegionSection] = useState<RegionSectionData>(
    () => createEmptyOrganizationSectionData(INITIAL_HOME_REGION_SECTION.fallbackName, currentYear),
  )
  const [regionSectionLoading, setRegionSectionLoading] = useState(true)
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

  useEffect(() => {
    const prefetch = () => {
      void Promise.all([
        import('./components/FireCopernicusSection'),
        import('./components/FeaturedRecordsSection'),
        import('./components/Funding'),
        import('./components/OrganizationSection'),
        import('./components/RegionSection'),
        import('./components/ContractModal'),
        import('./pages/AnalysisPage'),
        import('./pages/ContractsPage'),
        import('./pages/MunicipalitiesPage'),
        import('./pages/MapsPage'),
      ])
    }

    const idle = (window as Window & {
      requestIdleCallback?: (callback: () => void) => number
      cancelIdleCallback?: (id: number) => void
    }).requestIdleCallback

    if (idle) {
      const id = idle(prefetch)
      return () => {
        const cancelIdle = (window as Window & { cancelIdleCallback?: (id: number) => void }).cancelIdleCallback
        cancelIdle?.(id)
      }
    }

    const timeoutId = window.setTimeout(prefetch, 800)
    return () => window.clearTimeout(timeoutId)
  }, [])
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
  const chartYears = useMemo(
    () => Array.from({ length: currentYear - YEAR_START + 1 }, (_, i) => YEAR_START + i),
    [currentYear],
  )
  const chartByYear = useMemo(() => {
    const grouped = new Map<number, HeroCurvePoint[]>()
    for (const y of chartYears) grouped.set(y, [])
    for (const p of heroCurvePoints) {
      const arr = grouped.get(p.year)
      if (arr) arr.push(p)
    }
    for (const [, arr] of grouped) arr.sort((a, b) => a.dayOfYear - b.dayOfYear)
    return grouped
  }, [chartYears, heroCurvePoints])
  const chartMax = useMemo(() => {
    const vals = heroCurvePoints.map(p => p.value).filter(v => Number.isFinite(v))
    return Math.max(1, ...vals)
  }, [heroCurvePoints])

  useEffect(() => {
    if (!selectedContract) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setSelectedContract(null)
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [selectedContract])

  useEffect(() => {
    if (location.state?.scrollTo !== 'about') return

    const scrollToAbout = () => {
      document.getElementById('about')?.scrollIntoView({ behavior: 'smooth', block: 'start' })
      navigate(location.pathname, { replace: true, state: null })
    }

    const frameId = window.requestAnimationFrame(scrollToAbout)
    return () => window.cancelAnimationFrame(frameId)
  }, [location.pathname, location.state, navigate])

  useEffect(() => {
    let cancelled = false
    setLatestContractsLoading(true)
    setLatestContractsError(null)

    const loadLatestContracts = async () => {
      try {
        const rows = await loadCachedHomepageRpc(
          createHomepageRpcCacheKey('get_latest_contract_cards', { p_limit: 15 }),
          () => retryHomepageRpc(async () => {
            const { data, error } = await supabase.rpc('get_latest_contract_cards', {
              p_limit: 15,
            })
            if (error) throw error
            return (data ?? []) as LatestContractRpcRow[]
          }),
        )
        if (cancelled) return

        const cards = rows.map<LatestContractCard>((row) => {
          const cpvItems = Array.isArray(row.cpv_items)
            ? row.cpv_items
              .map((item) => ({
                code: cleanText(item.code) ?? '—',
                label: cleanText(item.label) ?? '—',
              }))
              .filter((item) => item.code !== '—' || item.label !== '—')
            : []
          const topCpv = cpvItems[0] ?? null
          const amountWithoutVat = toFiniteNumber(row.amount_without_vat)
          const amountWithVat = toFiniteNumber(row.amount_with_vat)
          const contractRelatedAda = cleanText(row.contract_related_ada)
          const diavgeiaAda = cleanText(row.diavgeia_ada)
          const latestCardView: LatestContractCardView = {
            id: String(row.procurement_id),
            who: cleanText(row.who) ?? '—',
            what: cleanText(row.title) ?? '—',
            when: formatDateEl(cleanText(row.submission_at)),
            why: toSentenceCaseEl(topCpv?.label ?? cleanText(row.short_description) ?? '—'),
            beneficiary: toUpperEl(cleanText(row.beneficiary_name)),
            beneficiaryVat: cleanText(row.beneficiary_vat_number) ?? null,
            contractType: cleanText(row.procedure_type_value) ?? '—',
            howMuch: formatEur(amountWithoutVat),
            signedAt: formatDateEl(cleanText(row.contract_signed_date)),
            documentUrl: buildDiavgeiaDocumentUrl(contractRelatedAda, diavgeiaAda),
            municipalityKey: cleanText(row.who)?.startsWith('ΔΗΜΟΣ ') ? (cleanText(row.municipality_key) ?? null) : null,
          }

          return {
            ...latestCardView,
            withoutVatAmount: formatEur(amountWithoutVat),
            withVatAmount: formatEur(amountWithVat),
            referenceNumber: cleanText(row.reference_number) ?? '—',
            contractNumber: cleanText(row.contract_number) ?? '—',
            cpv: topCpv?.label ?? '—',
            cpvCode: topCpv?.code ?? '—',
            cpvItems,
            signedAt: formatDateEl(cleanText(row.contract_signed_date)),
            startDate: formatDateEl(cleanText(row.start_date)),
            endDate: formatDateEl(cleanText(row.end_date)),
            organizationVat: cleanText(row.organization_vat_number) ?? '—',
            beneficiaryVat: cleanText(row.beneficiary_vat_number) ?? '—',
            signers: cleanText(row.signers) ?? '—',
            assignCriteria: cleanText(row.assign_criteria) ?? '—',
            contractKind: cleanText(row.contract_type) ?? '—',
            awardProcedure: cleanText(row.award_procedure) ?? '—',
            unitsOperator: cleanText(row.units_operator) ?? '—',
            fundingCofund: cleanText(row.funding_details_cofund) ?? '—',
            fundingSelf: cleanText(row.funding_details_self_fund) ?? '—',
            fundingEspa: cleanText(row.funding_details_espa) ?? '—',
            fundingRegular: cleanText(row.funding_details_regular_budget) ?? '—',
            auctionRefNo: cleanText(row.auction_ref_no) ?? '—',
            paymentRefNo: cleanText(row.payment_ref_no) ?? '—',
            shortDescription: cleanText(row.short_description) ?? '—',
            rawBudget: formatEur(toFiniteNumber(row.budget)),
            contractBudget: formatEur(toFiniteNumber(row.contract_budget)),
            contractRelatedAda: contractRelatedAda ?? '—',
            previousReferenceNumber: cleanText(row.prev_reference_no) ?? '—',
            nextReferenceNumber: cleanText(row.next_ref_no) ?? '—',
            documentUrl: latestCardView.documentUrl ?? null,
          }
        })

        setLatestContracts(cards)
      } catch (error) {
        if (!cancelled) {
          logLoadError('latest contracts', error)
          setLatestContracts([])
          setLatestContractsError('Δεν ήταν δυνατή η φόρτωση των πιο πρόσφατων συμβάσεων.')
        }
      } finally {
        if (!cancelled) setLatestContractsLoading(false)
      }
    }

    loadLatestContracts()

    return () => { cancelled = true }
  }, [])

  useEffect(() => {
    let cancelled = false
    setOrganizationSectionsLoading(true)

    const loadOrganizationSection = async (config: OrganizationSectionConfig): Promise<OrganizationSectionData> => {
      try {
        const { data: orgMatchesByKey } = await supabase
          .from('organization')
          .select('organization_key, organization_normalized_value, organization_value')
          .in('organization_key', config.organizationKeys)

        const orgRows = (orgMatchesByKey ?? []) as Array<{
          organization_key: string
          organization_normalized_value: string | null
          organization_value: string | null
        }>
        const bestOrg = orgRows[0]

        if (!bestOrg) {
          return createEmptyOrganizationSectionData(
            config.fallbackName,
            currentYear,
            buildContractsPageHref({ organizationKeys: config.organizationKeys }),
          )
        }

        const organizationKeys = Array.from(new Set(config.organizationKeys))
        const organizationName =
          cleanText(bestOrg.organization_normalized_value) ??
          cleanText(bestOrg.organization_value) ??
          config.fallbackName

        const pageSize = 1000

        const procurements: Array<{ id: number; contract_signed_date: string | null; title: string | null; municipality_key: string | null }> = []
        let from = 0
        while (true) {
          const to = from + pageSize - 1
          const { data } = await supabase
            .from('procurement')
            .select('id, contract_signed_date, title, municipality_key')
            .in('organization_key', organizationKeys)
            .order('id', { ascending: true })
            .range(from, to)
          const rows = (data ?? []) as Array<{ id: number; contract_signed_date: string | null; title: string | null; municipality_key: string | null }>
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

        const procurementYearById = new Map<number, string>()
        for (const row of procurements) {
          const year = cleanText(row.contract_signed_date)?.slice(0, 4)
          if (year) procurementYearById.set(row.id, year)
        }
        const latestProcurementYear = [...procurementYearById.values()].sort((a, b) => b.localeCompare(a))[0]
          ?? String(new Date().getFullYear())
        const previousProcurementYear = String(Number(latestProcurementYear) - 1)
        const totalSpend = paymentRows.reduce((sum, row) => {
          if (procurementYearById.get(row.procurement_id) !== latestProcurementYear) return sum
          return sum + Number(row.amount_without_vat ?? 0)
        }, 0)
        const contractCount = procurements.filter((row) => procurementYearById.get(row.id) === latestProcurementYear).length
        const previousYearContractCount = procurements.filter((row) => procurementYearById.get(row.id) === previousProcurementYear).length
        const beneficiaryCount = new Set(
          paymentRows
            .filter((row) => procurementYearById.get(row.procurement_id) === latestProcurementYear)
            .map((row) => cleanText(row.beneficiary_name))
            .filter(Boolean),
        ).size
        const previousYearBeneficiaryCount = new Set(
          paymentRows
            .filter((row) => procurementYearById.get(row.procurement_id) === previousProcurementYear)
            .map((row) => cleanText(row.beneficiary_name))
            .filter(Boolean),
        ).size

        const cpvCounts = new Map<string, number>()
        const cpvValueCounts = new Map<string, number>()
        const cpvValueCountsByYear = new Map<string, Map<string, number>>()
        const cpvLabelCountsByCode = new Map<string, Map<string, number>>()
        const cpvByProcId = new Map<number, Array<{ code: string; label: string }>>()
        for (const row of cpvRows) {
          const code = cleanText(row.cpv_key)
          const value = cleanText(row.cpv_value)
          const procurementYear = procurementYearById.get(row.procurement_id)
          if (!code) continue
          cpvCounts.set(code, (cpvCounts.get(code) ?? 0) + 1)
          if (value) {
            if (!cpvLabelCountsByCode.has(code)) cpvLabelCountsByCode.set(code, new Map<string, number>())
            const labelCounts = cpvLabelCountsByCode.get(code)!
            labelCounts.set(value, (labelCounts.get(value) ?? 0) + 1)
          }
          if (value) cpvValueCounts.set(value, (cpvValueCounts.get(value) ?? 0) + 1)
          if (value && procurementYear) {
            if (!cpvValueCountsByYear.has(procurementYear)) cpvValueCountsByYear.set(procurementYear, new Map<string, number>())
            const yearCounts = cpvValueCountsByYear.get(procurementYear)!
            yearCounts.set(value, (yearCounts.get(value) ?? 0) + 1)
          }
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
        const topCpvEntries = [...cpvCounts.entries()]
          .sort((a, b) => b[1] - a[1])
          .slice(0, 4)
        const topCpvs = topCpvEntries.map(([code]) => `CPV ${code}`)
        const topCpvItems = topCpvEntries.map(([code]) => ({
          code: `CPV ${code}`,
          label: [...(cpvLabelCountsByCode.get(code) ?? new Map<string, number>()).entries()]
            .sort((a, b) => b[1] - a[1])[0]?.[0] ?? `CPV ${code}`,
        }))
        const topCpvValue = [...(cpvValueCountsByYear.get(latestProcurementYear) ?? new Map<string, number>()).entries()]
          .sort((a, b) => b[1] - a[1])[0]?.[0]
          ?? [...cpvValueCounts.entries()]
          .sort((a, b) => b[1] - a[1])[0]?.[0] ?? null
        const previousYearTopCpvValue = [...(cpvValueCountsByYear.get(previousProcurementYear) ?? new Map<string, number>()).entries()]
          .sort((a, b) => b[1] - a[1])[0]?.[0] ?? null
        const activityWorkPoints: Array<{ lat: number; lon: number; work: string; pointName: string }> = []
        const seenWorkPoints = new Set<string>()
        let worksFrom = 0
        while (true) {
          const worksTo = worksFrom + pageSize - 1
          const { data: worksData } = await supabase
            .from('works_enriched')
            .select('lat, lon, work, point_name_canonical')
            .in('organization_key', organizationKeys)
            .not('lat', 'is', null)
            .not('lon', 'is', null)
            .order('id', { ascending: true })
            .range(worksFrom, worksTo)
          const rows = (worksData ?? []) as Array<{
            lat: number | string | null
            lon: number | string | null
            work: string | null
            point_name_canonical: string | null
          }>
          for (const row of rows) {
            const lat = Number(row.lat)
            const lon = Number(row.lon)
            if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue
            const work = cleanText(row.work) ?? '—'
            const pointName = cleanText(row.point_name_canonical) ?? '—'
            const key = `${lat.toFixed(6)}|${lon.toFixed(6)}`
            if (seenWorkPoints.has(key)) continue
            seenWorkPoints.add(key)
            activityWorkPoints.push({ lat, lon, work, pointName })
          }
          if (rows.length < pageSize) break
          worksFrom += pageSize
        }

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
            award_procedure,
            units_operator,
            funding_details_cofund,
            funding_details_self_fund,
            funding_details_espa,
            funding_details_regular_budget,
            auction_ref_no,
            contract_related_ada,
            prev_reference_no,
            next_ref_no,
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
        const timelineContracts = (latestTimelineRows ?? []) as Array<{
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
          award_procedure: string | null
          units_operator: string | null
          funding_details_cofund: string | null
          funding_details_self_fund: string | null
          funding_details_espa: string | null
          funding_details_regular_budget: string | null
          auction_ref_no: string | null
          contract_related_ada: string | null
          prev_reference_no: string | null
          next_ref_no: string | null
          organization_vat_number: string | null
          start_date: string | null
          end_date: string | null
          diavgeia_ada: string | null
        }>

        const latestSigned = timelineContracts[0]?.contract_signed_date ?? null
        const timeline = timelineContracts.map((p) => {
          const payment = paymentByProcId.get(p.id)
          const cpvItems = cpvByProcId.get(p.id) ?? []
          const cpv = cpvItems[0] ?? null
          const contractRelatedAda = cleanText(p.contract_related_ada)
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
            awardProcedure: cleanText(p.award_procedure) ?? '—',
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
            contractRelatedAda: contractRelatedAda ?? '—',
            previousReferenceNumber: cleanText(p.prev_reference_no) ?? '—',
            nextReferenceNumber: cleanText(p.next_ref_no) ?? '—',
            documentUrl: buildDiavgeiaDocumentUrl(contractRelatedAda, cleanText(p.diavgeia_ada)),
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
          yearLabel: latestProcurementYear,
          previousYearLabel: previousProcurementYear,
          totalSpend,
          cpvCodes: topCpvs,
          cpvCodeItems: topCpvItems,
          topCpvValue,
          previousYearTopCpvValue,
          contractCount,
          previousYearContractCount,
          beneficiaryCount,
          previousYearBeneficiaryCount,
          latestSignedAt: latestSigned,
          activityWorkPoints,
          timeline,
          contractsPageHref: buildContractsPageHref({ organizationKeys }),
        }

        return nextState
      } catch {
        return createEmptyOrganizationSectionData(
          config.fallbackName,
          currentYear,
          buildContractsPageHref({ organizationKeys: config.organizationKeys }),
        )
      }
    }

    const loadOrganizationSections = async () => {
      try {
        const nextSections = await Promise.all(HOME_ORGANIZATION_SECTIONS.map((config) => loadOrganizationSection(config)))
        if (!cancelled) setOrganizationSections(nextSections)
      } finally {
        if (!cancelled) setOrganizationSectionsLoading(false)
      }
    }

    loadOrganizationSections()
    return () => { cancelled = true }
  }, [currentYear])

  useEffect(() => {
    let cancelled = false
    setRegionSectionLoading(true)

    const loadRegionSection = async (config: RegionSectionConfig): Promise<RegionSectionData> => {
      try {
        const { data: regionRows } = await supabase
          .from('region')
          .select('region_key, region_value, region_normalized_value, region_afm')
          .eq('region_key', config.regionKey)
          .limit(1)

        const regionRow = (regionRows?.[0] ?? null) as {
          region_key: string
          region_value: string | null
          region_normalized_value: string | null
          region_afm: string | null
        } | null
        const regionValue = cleanText(regionRow?.region_value)
        const regionAfm = cleanText(regionRow?.region_afm)
        const regionName =
          (regionValue
            ? (regionValue.startsWith('Περιφέρεια ') ? regionValue : `Περιφέρεια ${regionValue}`)
            : null)
          ?? config.fallbackName

        const pageSize = 1000
        const baseProcurements: Array<{
          id: number
          organization_key: string | null
          organization_vat_number: string | null
          contract_signed_date: string | null
          title: string | null
          municipality_key: string | null
          canonical_owner_scope: 'municipality' | 'region' | 'organization' | null
        }> = []
        let from = 0
        while (true) {
          const to = from + pageSize - 1
          let query = supabase
            .from('procurement')
            .select('id, organization_key, organization_vat_number, contract_signed_date, title, municipality_key, canonical_owner_scope')
            .order('id', { ascending: true })
            .range(from, to)
          query = regionAfm
            ? query.eq('organization_vat_number', regionAfm)
            : query.eq('region_key', config.regionKey)
          const { data } = await query
          const rows = (data ?? []) as typeof baseProcurements
          baseProcurements.push(...rows)
          if (rows.length < pageSize) break
          from += pageSize
        }

        const orgKeys = Array.from(new Set(
          baseProcurements
            .map((row) => cleanText(row.organization_key))
            .filter(Boolean),
        )) as string[]
        const orgByKey = new Map<string, { name: string; scope: AuthorityScope }>()
        for (const keys of chunk(orgKeys, 200)) {
          const { data: orgRows } = await supabase
            .from('organization')
            .select('organization_key, organization_normalized_value, organization_value, authority_scope')
            .in('organization_key', keys)
          for (const row of (orgRows ?? []) as Array<{
            organization_key: string
            organization_normalized_value: string | null
            organization_value: string | null
            authority_scope: AuthorityScope | null
          }>) {
            orgByKey.set(row.organization_key, {
              name: cleanText(row.organization_normalized_value) ?? cleanText(row.organization_value) ?? row.organization_key,
              scope: row.authority_scope ?? 'other',
            })
          }
        }

        const procurements = regionAfm
          ? baseProcurements.filter((row) => cleanText(row.organization_vat_number) === regionAfm)
          : baseProcurements.filter((row) => {
            const canonicalOwnerScope = cleanText(row.canonical_owner_scope)
            const orgKey = cleanText(row.organization_key)
            const orgScope = orgKey ? orgByKey.get(orgKey)?.scope : null
            return canonicalOwnerScope === 'region' || orgScope === 'region' || orgScope === 'decentralized'
          })

        if (!procurements.length) {
          return {
            ...createEmptyOrganizationSectionData(
              regionName,
              currentYear,
              buildContractsPageHref({ regionKey: config.regionKey }),
            ),
            name: regionName,
          }
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

        const procurementYearById = new Map<number, string>()
        for (const row of procurements) {
          const year = cleanText(row.contract_signed_date)?.slice(0, 4)
          if (year) procurementYearById.set(row.id, year)
        }
        const latestProcurementYear = [...procurementYearById.values()].sort((a, b) => b.localeCompare(a))[0]
          ?? String(new Date().getFullYear())
        const previousProcurementYear = String(Number(latestProcurementYear) - 1)
        const totalSpend = paymentRows.reduce((sum, row) => {
          if (procurementYearById.get(row.procurement_id) !== latestProcurementYear) return sum
          return sum + Number(row.amount_without_vat ?? 0)
        }, 0)
        const contractCount = procurements.filter((row) => procurementYearById.get(row.id) === latestProcurementYear).length
        const previousYearContractCount = procurements.filter((row) => procurementYearById.get(row.id) === previousProcurementYear).length
        const beneficiaryCount = new Set(
          paymentRows
            .filter((row) => procurementYearById.get(row.procurement_id) === latestProcurementYear)
            .map((row) => cleanText(row.beneficiary_name))
            .filter(Boolean),
        ).size
        const previousYearBeneficiaryCount = new Set(
          paymentRows
            .filter((row) => procurementYearById.get(row.procurement_id) === previousProcurementYear)
            .map((row) => cleanText(row.beneficiary_name))
            .filter(Boolean),
        ).size

        const cpvCounts = new Map<string, number>()
        const cpvValueCounts = new Map<string, number>()
        const cpvValueCountsByYear = new Map<string, Map<string, number>>()
        const cpvLabelCountsByCode = new Map<string, Map<string, number>>()
        const cpvByProcId = new Map<number, Array<{ code: string; label: string }>>()
        for (const row of cpvRows) {
          const code = cleanText(row.cpv_key)
          const value = cleanText(row.cpv_value)
          const procurementYear = procurementYearById.get(row.procurement_id)
          if (!code) continue
          cpvCounts.set(code, (cpvCounts.get(code) ?? 0) + 1)
          if (value) {
            if (!cpvLabelCountsByCode.has(code)) cpvLabelCountsByCode.set(code, new Map<string, number>())
            const labelCounts = cpvLabelCountsByCode.get(code)!
            labelCounts.set(value, (labelCounts.get(value) ?? 0) + 1)
          }
          if (value) cpvValueCounts.set(value, (cpvValueCounts.get(value) ?? 0) + 1)
          if (value && procurementYear) {
            if (!cpvValueCountsByYear.has(procurementYear)) cpvValueCountsByYear.set(procurementYear, new Map<string, number>())
            const yearCounts = cpvValueCountsByYear.get(procurementYear)!
            yearCounts.set(value, (yearCounts.get(value) ?? 0) + 1)
          }
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

        const topCpvEntries = [...cpvCounts.entries()]
          .sort((a, b) => b[1] - a[1])
          .slice(0, 4)
        const topCpvs = topCpvEntries.map(([code]) => `CPV ${code}`)
        const topCpvItems = topCpvEntries.map(([code]) => ({
          code: `CPV ${code}`,
          label: [...(cpvLabelCountsByCode.get(code) ?? new Map<string, number>()).entries()]
            .sort((a, b) => b[1] - a[1])[0]?.[0] ?? `CPV ${code}`,
        }))
        const topCpvValue = [...(cpvValueCountsByYear.get(latestProcurementYear) ?? new Map<string, number>()).entries()]
          .sort((a, b) => b[1] - a[1])[0]?.[0]
          ?? [...cpvValueCounts.entries()]
          .sort((a, b) => b[1] - a[1])[0]?.[0] ?? null
        const previousYearTopCpvValue = [...(cpvValueCountsByYear.get(previousProcurementYear) ?? new Map<string, number>()).entries()]
          .sort((a, b) => b[1] - a[1])[0]?.[0] ?? null

        const activityWorkPoints: Array<{ lat: number; lon: number; work: string; pointName: string }> = []
        const seenWorkPoints = new Set<string>()
        for (const ids of chunk(procurementIds, 200)) {
          const { data: worksData } = await supabase
            .from('works_enriched')
            .select('lat, lon, work, point_name_canonical')
            .in('procurement_id', ids)
            .not('lat', 'is', null)
            .not('lon', 'is', null)
            .order('id', { ascending: true })

          const rows = (worksData ?? []) as Array<{
            lat: number | string | null
            lon: number | string | null
            work: string | null
            point_name_canonical: string | null
          }>
          for (const row of rows) {
            const lat = Number(row.lat)
            const lon = Number(row.lon)
            if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue
            const work = cleanText(row.work) ?? '—'
            const pointName = cleanText(row.point_name_canonical) ?? '—'
            const key = `${lat.toFixed(6)}|${lon.toFixed(6)}`
            if (seenWorkPoints.has(key)) continue
            seenWorkPoints.add(key)
            activityWorkPoints.push({ lat, lon, work, pointName })
          }
        }

        const timelineIds = [...procurements]
          .filter((row) => cleanText(row.contract_signed_date))
          .sort((a, b) => {
            const byDate = (cleanText(b.contract_signed_date) ?? '').localeCompare(cleanText(a.contract_signed_date) ?? '')
            if (byDate !== 0) return byDate
            return b.id - a.id
          })
          .slice(0, 5)
          .map((row) => row.id)

        let timelineContracts: Array<{
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
          award_procedure: string | null
          units_operator: string | null
          funding_details_cofund: string | null
          funding_details_self_fund: string | null
          funding_details_espa: string | null
          funding_details_regular_budget: string | null
          auction_ref_no: string | null
          contract_related_ada: string | null
          prev_reference_no: string | null
          next_ref_no: string | null
          organization_vat_number: string | null
          start_date: string | null
          end_date: string | null
          diavgeia_ada: string | null
        }> = []
        if (timelineIds.length) {
          const { data: timelineRows } = await supabase
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
              award_procedure,
              units_operator,
              funding_details_cofund,
              funding_details_self_fund,
              funding_details_espa,
              funding_details_regular_budget,
              auction_ref_no,
              contract_related_ada,
              prev_reference_no,
              next_ref_no,
              organization_vat_number,
              start_date,
              end_date,
              diavgeia_ada
            `)
            .in('id', timelineIds)

          timelineContracts = ((timelineRows ?? []) as typeof timelineContracts)
            .sort((a, b) => timelineIds.indexOf(a.id) - timelineIds.indexOf(b.id))
        }

        const latestSigned = timelineContracts[0]?.contract_signed_date ?? null
        const timeline = timelineContracts.map((p) => {
          const payment = paymentByProcId.get(p.id)
          const cpvItems = cpvByProcId.get(p.id) ?? []
          const cpv = cpvItems[0] ?? null
          const organizationName = cleanText(p.organization_key)
            ? orgByKey.get(cleanText(p.organization_key)!)?.name ?? regionName
            : regionName
          const contractRelatedAda = cleanText(p.contract_related_ada)
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
            awardProcedure: cleanText(p.award_procedure) ?? '—',
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
            contractRelatedAda: contractRelatedAda ?? '—',
            previousReferenceNumber: cleanText(p.prev_reference_no) ?? '—',
            nextReferenceNumber: cleanText(p.next_ref_no) ?? '—',
            documentUrl: buildDiavgeiaDocumentUrl(contractRelatedAda, cleanText(p.diavgeia_ada)),
          }
          const year = cleanText(p.contract_signed_date)?.slice(0, 4) ?? '—'
          return {
            month: monthShortEl(p.contract_signed_date),
            year,
            text: firstPipePart(p.title) ?? 'Καταχώρηση σύμβασης',
            contract,
          }
        })

        return {
          name: regionName,
          yearLabel: latestProcurementYear,
          previousYearLabel: previousProcurementYear,
          totalSpend,
          cpvCodes: topCpvs,
          cpvCodeItems: topCpvItems,
          topCpvValue,
          previousYearTopCpvValue,
          contractCount,
          previousYearContractCount,
          beneficiaryCount,
          previousYearBeneficiaryCount,
          latestSignedAt: latestSigned,
          activityWorkPoints,
          timeline,
          contractsPageHref: buildContractsPageHref({ regionKey: config.regionKey }),
        }
      } catch {
        return createEmptyOrganizationSectionData(
          config.fallbackName,
          currentYear,
          buildContractsPageHref({ regionKey: config.regionKey }),
        )
      }
    }

    const loadHomepageRegionSection = async () => {
      try {
        let selectedConfig = DEFAULT_HOME_REGION_SECTION
        const { data: regionRows } = await supabase
          .from('region')
          .select('region_key, region_value, region_normalized_value')

        selectedConfig = pickRandomRegionSectionConfig((regionRows ?? []) as RegionDirectoryRow[], DEFAULT_HOME_REGION_SECTION)

        if (!cancelled) {
          setHomeRegionConfig(selectedConfig)
          setRegionSection(
            createEmptyOrganizationSectionData(
              selectedConfig.fallbackName,
              currentYear,
              buildContractsPageHref({ regionKey: selectedConfig.regionKey }),
            ),
          )
        }

        const nextState = await loadRegionSection(selectedConfig)
        if (!cancelled) setRegionSection(nextState)
      } catch (error) {
        if (!cancelled) {
          logLoadError('homepage region section', error)
          setHomeRegionConfig(DEFAULT_HOME_REGION_SECTION)
          setRegionSection(
            createEmptyOrganizationSectionData(
              DEFAULT_HOME_REGION_SECTION.fallbackName,
              currentYear,
              buildContractsPageHref({
                regionKey: DEFAULT_HOME_REGION_SECTION.regionKey,
              }),
            ),
          )
        }
      } finally {
        if (!cancelled) setRegionSectionLoading(false)
      }
    }

    loadHomepageRegionSection()
    return () => { cancelled = true }
  }, [currentYear])

  useEffect(() => {
    let cancelled = false
    setFeaturedBeneficiariesLoading(true)

    const loadFeaturedPanels = async () => {
      try {
        const { data, error } = await supabase.rpc('get_featured_beneficiaries', {
          p_year_main: currentYear,
          p_limit: 12,
        })
        if (error) throw error

        const rows = ((data ?? []) as FeaturedRecordsRpcRow[]).map<BeneficiaryInsightRow>((row) => {
          const beneficiaryVat = cleanText(row.beneficiary_vat_number)
          const durationDays = toFiniteNumber(row.duration_days)
          const relevantContracts = Array.isArray(row.relevant_contracts)
            ? row.relevant_contracts.map<FeaturedRecordContract>((contract) => {
              const cpvItems = Array.isArray(contract.cpv_items)
                ? contract.cpv_items
                  .map((item) => ({
                    code: cleanText(item.code) ?? '—',
                    label: cleanText(item.label) ?? '—',
                  }))
                  .filter((item) => item.code !== '—' || item.label !== '—')
                : []
              const topCpv = cpvItems[0] ?? null
              const amountWithoutVat = toFiniteNumber(contract.amount_without_vat)
              const amountWithVat = toFiniteNumber(contract.amount_with_vat)
              const contractRelatedAda = cleanText(contract.contract_related_ada)
              const diavgeiaAda = cleanText(contract.diavgeia_ada)
              return {
                id: String(contract.id),
                who: cleanText(contract.organization) ?? cleanText(row.organization) ?? '—',
                what: cleanText(contract.title) ?? '—',
                when: formatDateEl(cleanText(contract.submission_at)),
                why: toSentenceCaseEl(topCpv?.label ?? cleanText(contract.short_description) ?? '—'),
                beneficiary: toUpperEl(cleanText(contract.beneficiary_name) ?? beneficiaryVat),
                contractType: cleanText(contract.procedure_type_value) ?? '—',
                howMuch: formatEur(amountWithoutVat),
                withoutVatAmount: formatEur(amountWithoutVat),
                withVatAmount: formatEur(amountWithVat),
                referenceNumber: cleanText(contract.reference_number) ?? '—',
                contractNumber: cleanText(contract.contract_number) ?? '—',
                cpv: topCpv?.label ?? '—',
                cpvCode: topCpv?.code ?? '—',
                cpvItems,
                signedAt: formatDateEl(cleanText(contract.contract_signed_date)),
                startDate: formatDateEl(cleanText(contract.start_date)),
                endDate: formatDateEl(cleanText(contract.end_date)),
                organizationVat: cleanText(contract.organization_vat_number) ?? '—',
                beneficiaryVat: cleanText(contract.beneficiary_vat_number) ?? beneficiaryVat ?? '—',
                signers: cleanText(contract.signers) ?? '—',
                assignCriteria: cleanText(contract.assign_criteria) ?? '—',
                contractKind: cleanText(contract.contract_type) ?? '—',
                awardProcedure: cleanText(contract.award_procedure) ?? '—',
                unitsOperator: cleanText(contract.units_operator) ?? '—',
                fundingCofund: cleanText(contract.funding_details_cofund) ?? '—',
                fundingSelf: cleanText(contract.funding_details_self_fund) ?? '—',
                fundingEspa: cleanText(contract.funding_details_espa) ?? '—',
                fundingRegular: cleanText(contract.funding_details_regular_budget) ?? '—',
                auctionRefNo: cleanText(contract.auction_ref_no) ?? '—',
                paymentRefNo: cleanText(contract.payment_ref_no) ?? '—',
                shortDescription: cleanText(contract.short_description) ?? '—',
                rawBudget: formatEur(toFiniteNumber(contract.budget)),
                contractBudget: formatEur(toFiniteNumber(contract.contract_budget)),
                contractRelatedAda: contractRelatedAda ?? '—',
                previousReferenceNumber: cleanText(contract.prev_reference_no) ?? '—',
                nextReferenceNumber: cleanText(contract.next_ref_no) ?? '—',
                documentUrl: buildDiavgeiaDocumentUrl(contractRelatedAda, diavgeiaAda),
              }
            })
            : []

          return {
            beneficiary: toUpperEl(cleanText(row.beneficiary_name) ?? beneficiaryVat),
            beneficiaryVat: beneficiaryVat ?? null,
            organization: cleanText(row.organization) ?? '—',
            totalAmount: toFiniteNumber(row.total_amount) ?? 0,
            contractCount: Math.round(toFiniteNumber(row.contract_count) ?? 0),
            cpv: cleanText(row.cpv) ?? '—',
            startDate: formatDateEl(cleanText(row.start_date)),
            endDate: formatDateEl(cleanText(row.end_date)),
            duration: durationDays != null ? `${Math.round(durationDays)} ημέρες` : '—',
            progressPct: toFiniteNumber(row.progress_pct),
            signer: cleanText(row.signer) ?? '—',
            relevantContracts,
          }
        })

        if (!cancelled) setFeaturedBeneficiaries(rows)
      } catch (error) {
        if (!cancelled) {
          logLoadError('featured beneficiaries', error)
          setFeaturedBeneficiaries([])
        }
      } finally {
        if (!cancelled) setFeaturedBeneficiariesLoading(false)
      }
    }

    loadFeaturedPanels()
    return () => { cancelled = true }
  }, [currentYear])

  useEffect(() => {
    let cancelled = false
    setHeroStatsLoading(true)
    setHeroStatsError(null)

    const loadHeroStats = async () => {
      try {
        const payload = await loadCachedHomepageRpc(
          createHomepageRpcCacheKey('get_hero_section_data', {
            p_year_main: currentYear,
            p_year_start: YEAR_START,
          }),
          () => retryHomepageRpc(async () => {
            const { data, error } = await supabase.rpc('get_hero_section_data', {
              p_year_main: currentYear,
              p_year_start: YEAR_START,
            })
            if (error) throw error
            if (!data) throw new Error('Hero section RPC returned no data')
            return data as HeroSectionRpcResponse
          }),
        )
        if (cancelled) return

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
      } catch (error) {
        if (!cancelled) {
          console.error('Failed to load hero section data', error)
          setHeroStatsError('Δεν ήταν δυνατή η φόρτωση των στοιχείων.')
        }
      } finally {
        if (!cancelled) setHeroStatsLoading(false)
      }
    }

    loadHeroStats()
    return () => { cancelled = true }
  }, [currentYear])

  return (
    <>
      <main>
        <FireNowTicker />

        <section id="latest" className="news-wire section-rule dev-tag-anchor" aria-label="Τελευταία ρεπορτάζ">
          <div className="dev-tag-stack dev-tag-stack--right">
            <ComponentTag name="LatestContractsSection" />
            <ComponentTag name="news-wire section-rule" kind="CLASS" />
          </div>
          <div className="news-wire__label dev-tag-anchor">
            <DebugClassLabel name="news-wire__label" />
            <span className="eyebrow">τελευταία</span>
            <strong>Οι πιο πρόσφατες συμβάσεις Δήμων, Υπουργείων και άλλων φορέων που έχουν δημοσιευτεί στο <a href = "https://eprocurement.gov.gr/">Kεντρικό Ηλεκτρονικό Μητρώο Δημοσίων Συμβάσεων</a> και αφορούν στην πρόληψη και αντιμετώπιση δασικών πυρκαγιών.</strong>
            <Link className="news-wire__all-link" to="/contracts">Δες όλες τις συμβάσεις</Link>
          </div>
          <div className="news-wire__items dev-tag-anchor">
            <DebugClassLabel name="news-wire__items" style={{ left: 'auto', right: '0.45rem' }} />
            {latestContractsLoading && (
              <DataLoadingCard
                className="news-wire__loading-card"
                message="Ανακτώνται οι πιο πρόσφατες συμβάσεις από το ΚΗΜΔΗΣ."
              />
            )}
            {!latestContractsLoading && latestContracts.map((item) => (
              <LatestContractCardItem
                key={item.id}
                item={item}
                onOpen={(id) => {
                  const found = latestContracts.find((x) => x.id === id)
                  if (found) setSelectedContract(found)
                }}
                onMunicipalityClick={(key) => navigate(`/municipalities?municipality=${encodeURIComponent(key)}`)}
                contractTypeTransform={toLowerEl}
              />
            ))}
            {!latestContractsLoading && latestContractsError && (
              <article className="wire-item">
                <h2>Δεν φορτώθηκαν οι πρόσφατες συμβάσεις.</h2>
                <p>{latestContractsError}</p>
              </article>
            )}
            {!latestContractsLoading && !latestContractsError && latestContracts.length === 0 && (
              <article className="wire-item">
                <h2>Δεν βρέθηκαν πρόσφατες συμβάσεις.</h2>
                <p>Ελέγξτε ότι ο πίνακας `procurement` έχει δεδομένα.</p>
              </article>
            )}
          </div>
        </section>

        <section className="hero section-rule dev-tag-anchor">
          <div className="dev-tag-stack dev-tag-stack--right">
            <ComponentTag name="HeroSection" />
            <ComponentTag name="hero section-rule" kind="CLASS" />
          </div>
          <div className="hero-left dev-tag-anchor">
            <DebugClassLabel name="hero-left" />
            <div className="hero-chart dev-tag-anchor">
              <DebugClassLabel name="hero-chart" />
              <div className="hero-chart__head">
                <span className="eyebrow">Εκτίμηση δαπανών δημοσιων συμβασεων <br /> για προληψη και αντιμετωπιση δασικων πυρκαγιων</span>
              </div>
              <div className="hero-chart__plot">
                <svg viewBox="0 0 760 300" role="img" aria-label="Σωρευτική πορεία δαπανών ανά έτος">
                  {[1 / 3, 2 / 3].map((t, i) => {
                    const y = 230 - t * 200
                    return <line key={`hy-${i}`} x1="44" y1={y} x2="736" y2={y} stroke="rgba(17,17,17,0.12)" />
                  })}

                  {chartYears.map((year) => {
                    const points = chartByYear.get(year) ?? []
                    if (points.length === 0) return null
                    const { stroke, opacity, strokeWidth } = getChartYearStyle(year, currentYear)
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
                </svg>
                <div className="hero-chart__y-axis" aria-hidden="true">
                  {[0, 0.5, 1].map((t) => {
                    const y = ((230 - t * 200) / 300) * 100
                    const v = chartMax * t
                    return (
                      <span key={`hero-chart-y-${t}`} style={{ top: `${y}%` }}>
                        {formatEurCompact(v)}
                      </span>
                    )
                  })}
                </div>
                <div className="hero-chart__x-axis" aria-hidden="true">
                  {CHART_TICKS.map((tick) => {
                    const dayInYear = Math.floor((Date.UTC(2025, tick.month - 1, tick.day) - Date.UTC(2025, 0, 1)) / 86_400_000) + 1
                    const x = ((44 + dayFraction(dayInYear, 365) * (736 - 44)) / 760) * 100
                    return (
                      <span key={`hero-chart-x-${tick.label}`} style={{ left: `${x}%` }}>
                        {tick.label}
                      </span>
                    )
                  })}
                </div>
              </div>
              <div className="hero-chart__legend">
                {[...chartYears].reverse().map((year) => {
                  const { stroke, opacity } = getChartYearStyle(year, currentYear)
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

          <div className="hero-right dev-tag-anchor">
            <DebugClassLabel name="hero-right" style={{ left: 'auto', right: '0.45rem' }} />
            <div className="hero-background-year" aria-hidden="true">
              {currentYear}
            </div>
            <div className="hero-amount-card dev-tag-anchor">
              <DebugClassLabel name="hero-amount-card" />
              <div className="eyebrow">
                {heroStatsLoading
                  ? 'Δαπάνες'
                  : heroStatsError
                    ? heroStatsError
                  : `Δαπάνες από ${formatDateLabel(heroStats.periodMainStart)} έως ${formatDateLabel(heroStats.periodMainEnd)}`}
              </div>
              <div className="hero-amount">
                {heroStatsLoading ? '…' : heroStatsError ? '—' : formatEurCompact(heroStats.totalMain)}
              </div>
              <div className="hero-subgrid">
                <div>
                  <span className="label">Σύγκριση με {currentYear - 1}</span>
                  <strong>
                    {heroStatsLoading ? '…' : heroStatsError ? '—' : (
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
                    {heroStatsLoading ? '…' : heroStatsError ? '—' : (
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
                    {heroStatsLoading ? '…' : heroStatsError ? '—' : (
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
                    {heroStatsLoading ? '…' : heroStatsError ? '—' : (
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

        <Suspense fallback={<SectionFallback label="Φόρτωση χρηματοδότησης" />}>
          <Funding currentYear={currentYear} />
        </Suspense>

        <Suspense fallback={<SectionFallback label="Φόρτωση Copernicus" />}>
          <FireCopernicusSection />
        </Suspense>

        <Suspense fallback={<SectionFallback label="Φόρτωση featured records" />}>
          <FeaturedRecordsSection
            year={featuredRecordsYear}
            rows={featuredBeneficiaries}
            loading={featuredBeneficiariesLoading}
            formatEur={formatEur}
            onOpenContract={(contract) => setSelectedContract(contract as LatestContractCard)}
          />
        </Suspense>

        <Suspense fallback={<SectionFallback label="Φόρτωση οργανισμών" />}>
          {HOME_ORGANIZATION_SECTIONS.map((config, index) => (
            <Fragment key={config.fallbackName}>
              <OrganizationSection
                data={
                  organizationSections[index] ??
                  createEmptyOrganizationSectionData(
                    config.fallbackName,
                    currentYear,
                    buildContractsPageHref({ organizationKeys: config.organizationKeys }),
                  )
                }
                loading={organizationSectionsLoading}
                anchorId={config.anchorId}
                formatEurCompact={formatEurCompact}
                formatDateEl={formatDateEl}
                onOpenContract={(contract) => setSelectedContract(contract)}
              />
            </Fragment>
          ))}
        </Suspense>

        <Suspense fallback={<SectionFallback label="Φόρτωση περιφέρειας" />}>
          <RegionSection
            data={regionSection}
            loading={regionSectionLoading}
            anchorId={homeRegionConfig.anchorId}
            formatEurCompact={formatEurCompact}
            formatDateEl={formatDateEl}
            onOpenContract={(contract) => setSelectedContract(contract)}
          />
        </Suspense>

        <section id="about" className="about-panel section-rule dev-tag-anchor">
          <div className="dev-tag-stack dev-tag-stack--right">
            <ComponentTag name="AboutSection" />
            <ComponentTag name="about-panel section-rule" kind="CLASS" />
          </div>
          <div className="about-panel__left dev-tag-anchor">
            <DebugClassLabel name="about-panel__left" />
            <div className="eyebrow">Σχετικά με το FireWatch</div>
            <h2>Παρατηρητήριο για τις δασικές πυρκαγιές</h2>
            <p>
              Ανεξάρτητη πλατφόρμα που στοχεύει αφενός στην καταγραφή των δημοσίων συμβάσεων που σχετίζονται με την πρόληψη και αντιμετώπιση δασικών πυρκαγιών και αφετέρου στην παρακολούθηση των πυρκαγιών στην Ελλάδα.
            </p>
            <p>Η ενημέρωση των δεδομένων γίνεται αυτοματοποιημένα, επομένως ενδέχεται να υπάρχουν λάθη ή παραλείψεις. Εάν εντοπίσετε κάποιο πρόβλημα με τα δεδομένα, στείλτε ένα μέιλ στο troboukis[at]gmail[dot]com</p>
          </div>
          <div className="about-panel__right dev-tag-anchor">
            <DebugClassLabel name="about-panel__right" style={{ left: 'auto', right: '0.45rem' }} />
            <figure className="about-cover-figure">
              <img className="about-cover" src={`${import.meta.env.BASE_URL}cover_square_optimized.webp`} alt="FireWatch cover" />
              <figcaption className="about-cover-caption">Εικόνα από Nano Banana 2</figcaption>
            </figure>
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
                <strong>Διαβάστε το σκεπτικό συλλογής και επεξεργασίας των δεδομένων καθώς και τον κώδικα ανάπτυξής της εφαρμογής</strong>
                <span className="about-stats__sub">
                  <a href="https://github.com/troboukis/2026_fire_protection/blob/main/Methodology.md" target="_blank" rel="noreferrer">
                    Μεθοδολογία
                  </a>
                </span>
                <span className="about-stats__sub">
                  <a href="https://github.com/troboukis/2026_fire_protection" target="_blank" rel="noreferrer">
                    Github repository
                  </a>
                </span>
              </div>
              <div>
                <span className="label">ΓΙΑΤΙ</span>
                <strong>Διότι η λογοδοσία είναι θεμέλιο της δημοκρατίας</strong>
              </div>
              <div>
                <strong>Η παρούσα εφαρμογή είναι προϊόν συνεργασίας ανθρώπινης και τεχνητής νοημοσύνης</strong>
              </div>
            </div>
          </div>
        </section>
      </main>

      {selectedContract && (
        <Suspense fallback={null}>
          <ContractModal
            contract={selectedContract}
            onClose={() => setSelectedContract(null)}
            onDownloadPdf={() => downloadContractDocument(selectedContract)}
          />
        </Suspense>
      )}
    </>
  )
}
