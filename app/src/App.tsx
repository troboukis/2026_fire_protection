import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import ContractAnalysis from './components/ContractAnalysis'
import ComponentTag from './components/ComponentTag'
import ErrorBoundary from './components/ErrorBoundary'
import ContractModal, { type ContractModalContract } from './components/ContractModal'
import LatestContractCardItem, { type LatestContractCardView } from './components/LatestContractCard'
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
  contractAmounts: number[]
  signer: string
}

type Kpi = {
  label: string
  value: string
  note: string
}

const orgKpis: Kpi[] = [
  { label: 'Συνολική Δαπάνη 2025-2026', value: '€ 24.3M', note: 'περιφερειακές + δημοτικές αποφάσεις' },
  { label: 'Συμβάσεις', value: '118', note: 'μοναδικές αποφάσεις προμηθειών' },
  { label: 'Προμηθευτές', value: '43', note: 'διακριτοί αντισυμβαλλόμενοι' },
  { label: 'Ζώνες Υψηλού Κινδύνου', value: '17', note: 'δήμοι προτεραιότητας παρέμβασης' },
]

const timelineItems = [
  { month: 'ΜΑΡ', year: '2025', text: 'Δημοσίευση κατανομής προϋπολογισμού / έγκριση πακέτου πρόληψης' },
  { month: 'ΜΑΪ', year: '2025', text: 'Άνοιγμα συμβάσεων για διαχείριση καύσιμης ύλης και καθαρισμούς' },
  { month: 'ΙΟΥΛ', year: '2025', text: 'Κορύφωση έκτακτων μισθώσεων στη διάρκεια της αντιπυρικής περιόδου' },
  { month: 'ΝΟΕ', year: '2025', text: 'Καταγραφή αποφάσεων αναπλήρωσης εξοπλισμού μετά την περίοδο' },
  { month: 'ΦΕΒ', year: '2026', text: 'Επανεκκίνηση κύκλου με δημοτικές προμήθειες συντήρησης' },
]

const mapSignals = [
  { region: 'Αττική', value: '€ 24.3M', note: 'υψηλότερος καταγεγραμμένος όγκος προμηθειών', tone: 'high' },
  { region: 'Πελοπόννησος', value: '€ 18.7M', note: 'αντιπυρικές λωρίδες + συντήρηση δρόμων πρόσβασης', tone: 'mid' },
  { region: 'Στερεά Ελλάδα', value: '€ 16.2M', note: 'εξοπλισμός και πακέτα τοπικών έργων', tone: 'mid' },
  { region: 'Κρήτη', value: '€ 9.4M', note: 'εποχική ετοιμότητα και κινητές μονάδες νερού', tone: 'low' },
]

const regionalMapCards = [
  {
    title: 'Χάρτης Εθνικής Δαπάνης',
    metric: 'Χωροπλεθικός / €',
    value: '332 δήμοι',
    note: 'Η ένταση χρώματος αποτυπώνει τον όγκο προμηθειών σε σχέση με την εθνικά καταγεγραμμένη δαπάνη.',
  },
  {
    title: 'Επικάλυψη Καμένων Εκτάσεων',
    metric: 'Συμβάντα / ha',
    value: 'Διασταύρωση ιστορικού πυρκαγιών',
    note: 'Σύγκριση συμβάσεων πρόληψης με πρόσφατη έκθεση σε πυρκαγιές και επαναλαμβανόμενα μοτίβα καύσης.',
  },
  {
    title: 'Αρχές Κάλυψης',
    metric: 'Περιφερειακό / Αποκεντρωμένο',
    value: 'Πολυεπίπεδη χαρτογραφική όψη',
    note: 'Ιχνηλάτηση των αρχών που καλύπτουν κάθε δήμο και της προέλευσης των αποφάσεων.',
  },
]

function formatCommitDateTimeEl(iso: string): string {
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

function escapeHtml(v: string): string {
  return v
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

function buildPdfTemplate(contract: LatestContractCard): string {
  const title = escapeHtml(contract.what)
  const who = escapeHtml(contract.who)
  const when = escapeHtml(contract.when)
  const why = escapeHtml(contract.why)
  const amount = escapeHtml(contract.withoutVatAmount)
  const beneficiary = escapeHtml(contract.beneficiary)
  const contractType = escapeHtml(contract.contractType)
  const ref = escapeHtml(contract.referenceNumber)
  const contractNo = escapeHtml(contract.contractNumber)
  const cpv = escapeHtml(contract.cpv)
  const cpvCode = escapeHtml(contract.cpvCode)
  const orgVat = escapeHtml(contract.organizationVat)
  const benVat = escapeHtml(contract.beneficiaryVat)
  const signedAt = escapeHtml(contract.signedAt)
  const startDate = escapeHtml(contract.startDate)
  const endDate = escapeHtml(contract.endDate)
  const withVat = escapeHtml(contract.withVatAmount)
  const description = escapeHtml(contract.shortDescription)
  return `<!doctype html>
<html lang="el">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Project ΠΥΡ</title>
  <style>
    :root{--paper:#f7f5ee;--line:#cfc8bb;--line-strong:#9d9688;--ink:#111;--ink-soft:#4d4d4d;--ink-faint:#89857c;--accent:#d3482d;}
    *{box-sizing:border-box;}
    body{margin:0;padding:24px;background:var(--paper);color:var(--ink);font-family:"IBM Plex Sans","Helvetica Neue",Arial,sans-serif;}
    .sheet{max-width:980px;margin:0 auto;border:1px solid var(--line-strong);background:var(--paper);padding:20px;}
    .head{display:flex;justify-content:space-between;gap:16px;border-bottom:1px solid var(--line);padding-bottom:12px;}
    .eyebrow{font-family:"IBM Plex Mono","SFMono-Regular",Menlo,monospace;color:var(--ink-soft);font-size:11px;letter-spacing:.08em;text-transform:uppercase;}
    h1{margin:6px 0 0;font-size:34px;line-height:1.08;}
    .subtitle{margin:14px 0 0;font-family:"IBM Plex Mono","SFMono-Regular",Menlo,monospace;color:var(--ink-soft);font-size:15px;line-height:1.4;}
    .highlight{margin-top:14px;border:1px solid var(--line);padding:10px 12px;background:linear-gradient(90deg,rgba(211,72,45,.08),transparent 35%);}
    .amount{color:var(--accent);font-weight:700;font-size:32px;line-height:1.1;}
    .route{margin-top:6px;font-family:"IBM Plex Mono","SFMono-Regular",Menlo,monospace;color:var(--ink-soft);font-size:16px;letter-spacing:.04em;text-transform:uppercase;}
    .kind{margin-top:6px;font-family:"IBM Plex Mono","SFMono-Regular",Menlo,monospace;color:var(--ink-faint);font-size:12px;text-transform:uppercase;}
    .grid{margin-top:14px;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:1px;background:var(--line);}
    .cell{background:var(--paper);padding:8px 10px;}
    .label{display:block;font-family:"IBM Plex Mono","SFMono-Regular",Menlo,monospace;color:var(--ink-faint);font-size:10px;letter-spacing:.07em;text-transform:uppercase;}
    .value{margin-top:4px;display:block;font-size:14px;line-height:1.35;word-break:break-word;}
    .source{margin-top:14px;padding-top:10px;border-top:1px solid var(--line);font-family:"IBM Plex Mono","SFMono-Regular",Menlo,monospace;font-size:11px;color:var(--ink-soft);}
    @page { size: A4; margin: 12mm; }
    @media print { body{padding:0;} .sheet{border:1px solid var(--line); box-shadow:none;} }
  </style>
</head>
<body>
  <article class="sheet">
    <header class="head">
      <div>
        <span class="eyebrow">${who}</span>
        <h1>${title}</h1>
      </div>
      <div class="eyebrow">${when}</div>
    </header>
    <p class="subtitle">${why}</p>
    <section class="highlight">
      <div class="amount">${amount}</div>
      <div class="route">→ ${beneficiary}</div>
      <div class="kind">${contractType}</div>
    </section>
    <section class="grid">
      <div class="cell"><span class="label">Κωδ. Αναφοράς</span><span class="value">${ref}</span></div>
      <div class="cell"><span class="label">Κωδ. Σύμβασης</span><span class="value">${contractNo}</span></div>
      <div class="cell"><span class="label">CPV</span><span class="value">${cpv} (${cpvCode})</span></div>
      <div class="cell"><span class="label">Ποσό με ΦΠΑ</span><span class="value">${withVat}</span></div>
      <div class="cell"><span class="label">Υπογραφή</span><span class="value">${signedAt}</span></div>
      <div class="cell"><span class="label">Έναρξη / Λήξη</span><span class="value">${startDate} - ${endDate}</span></div>
      <div class="cell"><span class="label">Φορέας ΑΦΜ</span><span class="value">${orgVat}</span></div>
      <div class="cell"><span class="label">Δικαιούχος ΑΦΜ</span><span class="value">${benVat}</span></div>
      <div class="cell"><span class="label">Περιγραφή</span><span class="value">${description}</span></div>
    </section>
    <footer class="source">PROJECT ΠΥΡ, ΠΗΓΗ: https://portal.eprocurement.gov.gr/</footer>
  </article>
</body>
<script>
  window.addEventListener('load', function () {
    setTimeout(function () {
      window.print();
    }, 120);
  });
</script>
</html>`
}

const YEAR_START = 2024

const CHART_YEAR_STYLES = [
  { stroke: '#111111', opacity: 1,    strokeWidth: 3.8 },
  { stroke: '#d9a095', opacity: 0.95, strokeWidth: 2.4 },
  { stroke: '#dadada', opacity: 0.62, strokeWidth: 2.4 },
  { stroke: '#dadada', opacity: 0.45, strokeWidth: 1.8 },
]

export default function App() {
  const lastCommitLabel = formatCommitDateTimeEl(__LAST_COMMIT_ISO__)
  const [latestContracts, setLatestContracts] = useState<LatestContractCard[]>([])
  const [latestContractsLoading, setLatestContractsLoading] = useState(true)
  const [selectedContract, setSelectedContract] = useState<LatestContractCard | null>(null)
  const [heroStatsLoading, setHeroStatsLoading] = useState(true)
  const [featuredBeneficiaries, setFeaturedBeneficiaries] = useState<BeneficiaryInsightRow[]>([])
  const [featuredBeneficiariesLoading, setFeaturedBeneficiariesLoading] = useState(true)
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

        const cpvByProcId = new Map<number, { cpv_key: string | null; cpv_value: string | null }>()
        for (const c of chunk(ids, 200)) {
          const { data: cpvData } = await supabase
            .from('cpv')
            .select('procurement_id, cpv_key, cpv_value')
            .in('procurement_id', c)
          for (const cpv of (cpvData ?? []) as Array<{ procurement_id: number; cpv_key: string | null; cpv_value: string | null }>) {
            if (!cpvByProcId.has(cpv.procurement_id)) cpvByProcId.set(cpv.procurement_id, cpv)
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
          const c = cpvByProcId.get(r.id)
          const amountWithoutVat = p?.amount_without_vat ?? null

          const why =
            cleanText(c?.cpv_value) ??
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
            cpv: cleanText(c?.cpv_value) ?? '—',
            cpvCode: cleanText(c?.cpv_key) ?? '—',
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
          contract_signed_date: string | null
          start_date: string | null
          end_date: string | null
        }> = []

        let from = 0
        while (true) {
          const to = from + pageSize - 1
          const { data, error } = await supabase
            .from('procurement')
            .select('id, organization_key, contract_signed_date, start_date, end_date')
            .gte('contract_signed_date', yearStart)
            .lte('contract_signed_date', yearEnd)
            .order('id', { ascending: true })
            .range(from, to)
          if (error) break
          const rows = (data ?? []) as Array<{
            id: number
            organization_key: string | null
            contract_signed_date: string | null
            start_date: string | null
            end_date: string | null
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
          signers: string | null
        }> = []
        for (const c of chunk(ids, 200)) {
          const { data } = await supabase
            .from('payment')
            .select('procurement_id, beneficiary_name, amount_without_vat, signers')
            .in('procurement_id', c)
          paymentRows.push(...((data ?? []) as typeof paymentRows))
        }

        const cpvRows: Array<{ procurement_id: number; cpv_value: string | null }> = []
        for (const c of chunk(ids, 200)) {
          const { data } = await supabase
            .from('cpv')
            .select('procurement_id, cpv_value')
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

        const paymentByProc = new Map<number, Array<{ beneficiary: string; amount: number; signer: string }>>()
        for (const p of paymentRows) {
          const beneficiary = toUpperEl(cleanText(p.beneficiary_name))
          if (!beneficiary) continue
          const amount = Number(p.amount_without_vat ?? 0)
          const signer = cleanText(p.signers) ?? '—'
          if (!paymentByProc.has(p.procurement_id)) paymentByProc.set(p.procurement_id, [])
          paymentByProc.get(p.procurement_id)!.push({ beneficiary, amount, signer })
        }

        const cpvByProc = new Map<number, string[]>()
        for (const c of cpvRows) {
          const cpv = cleanText(c.cpv_value)
          if (!cpv) continue
          if (!cpvByProc.has(c.procurement_id)) cpvByProc.set(c.procurement_id, [])
          cpvByProc.get(c.procurement_id)!.push(cpv)
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
          contractAmounts: number[]
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
                contractAmounts: [],
              })
            }
            const a = agg.get(key)!
            a.totalAmount += entry.amount
            a.contractCount += 1
            a.contractAmounts.push(entry.amount)
            a.orgTotals.set(org, (a.orgTotals.get(org) ?? 0) + entry.amount)
            if (entry.signer && entry.signer !== '—') a.signerCounts.set(entry.signer, (a.signerCounts.get(entry.signer) ?? 0) + 1)
            for (const cpv of cpvs) a.cpvCounts.set(cpv, (a.cpvCounts.get(cpv) ?? 0) + 1)
            const start = cleanText(pr.start_date)
            const end = cleanText(pr.end_date)
            if (start) a.startDates.push(start)
            if (end) a.endDates.push(end)
          }
        }

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
            contractAmounts: [...a.contractAmounts].sort((x, y) => y - x),
            signer,
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
    const popup = window.open('', '_blank', 'width=1024,height=900')
    if (!popup) return
    try {
      popup.document.open()
      popup.document.write(buildPdfTemplate(contract))
      popup.document.close()
      popup.document.title = 'Project ΠΥΡ'
      try {
        popup.history.replaceState({}, '', `/project-pyr-print/${contract.id}`)
      } catch {
        // Ignore if browser blocks history manipulation in this popup context.
      }
      popup.focus()
    } catch {
      popup.close()
    }
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
            <h1>PROJECT ΠΥΡ</h1>
            <span className="brand-mark">Τελευταία ενημέρωση / {lastCommitLabel}</span>
          </div>
        </div>
        <nav className="top-nav" aria-label="Κύρια πλοήγηση">
          <a href="#latest">Τελευταία</a>
          <Link to="/contracts">Συμβάσεις</Link>
          <Link to="/maps">Χάρτες</Link>
          <a href="#analysis">Ανάλυση Συμβάσεων</a>
          <a href="#organizations">Φορείς</a>
          <a href="#documents">Διαύγεια</a>
          <a href="#about">About</a>
        </nav>
        <button className="menu-button" type="button" aria-label="Άνοιγμα μενού">
          <span />
          <span />
          <span />
        </button>
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

        {false && (
          <>
        <DebugComponentLabel name="MapDeskSection" />
        <section id="mapdesk" className="map-desk section-rule">
          <div className="section-head">
            <div className="eyebrow">Γραφείο Χαρτών</div>
            <h2>Χαρτοκεντρικά πάνελ ρεπορτάζ για προμήθειες, κίνδυνο και λογοδοσία κάλυψης</h2>
          </div>

          <div className="map-desk__layout">
            <div className="map-board">
              <div className="map-board__header">
                <span className="eyebrow">Εθνική Συγκέντρωση Προμηθειών</span>
                <div className="map-board__legend" aria-label="Υπόμνημα">
                  <span>Χαμηλή</span>
                  <div aria-hidden="true" />
                  <span>Υψηλή</span>
                </div>
              </div>

              <div className="map-board__frame">
                <svg
                  className="news-map-svg"
                  viewBox="0 0 780 460"
                  role="img"
                  aria-label="Στυλιζαρισμένος εθνικός χάρτης προμηθειών με επισημασμένες περιοχές"
                >
                  <defs>
                    <pattern id="mapGrid" width="28" height="28" patternUnits="userSpaceOnUse">
                      <path d="M 28 0 L 0 0 0 28" fill="none" stroke="rgba(17,17,17,0.06)" strokeWidth="1" />
                    </pattern>
                    <linearGradient id="heatScale" x1="0%" y1="0%" x2="100%" y2="0%">
                      <stop offset="0%" stopColor="#efece2" />
                      <stop offset="60%" stopColor="#e4d8c7" />
                      <stop offset="100%" stopColor="#d3482d" />
                    </linearGradient>
                  </defs>

                  <rect x="0" y="0" width="780" height="460" fill="url(#mapGrid)" />

                  <path
                    d="M130 116 L198 76 L286 94 L324 132 L372 146 L426 192 L432 228 L406 252 L356 268 L330 306 L282 320 L238 308 L210 286 L188 248 L160 236 L126 196 L112 158 Z"
                    fill="#f2efe6"
                    stroke="rgba(17,17,17,0.25)"
                    strokeWidth="2"
                  />
                  <path
                    d="M468 248 L508 232 L554 240 L586 262 L574 294 L536 306 L494 296 L474 274 Z"
                    fill="#f2efe6"
                    stroke="rgba(17,17,17,0.2)"
                    strokeWidth="2"
                  />
                  <path
                    d="M596 184 L630 170 L662 184 L670 212 L648 232 L616 224 L600 204 Z"
                    fill="#f2efe6"
                    stroke="rgba(17,17,17,0.2)"
                    strokeWidth="2"
                  />
                  <path
                    d="M544 332 L594 324 L632 344 L620 372 L572 380 L544 360 Z"
                    fill="#f2efe6"
                    stroke="rgba(17,17,17,0.2)"
                    strokeWidth="2"
                  />

                  <path
                    d="M160 172 L218 122 L296 136 L344 176 L350 224 L312 258 L250 272 L202 248 L176 212 Z"
                    fill="rgba(211,72,45,0.22)"
                    stroke="rgba(17,17,17,0.08)"
                  />
                  <path
                    d="M238 150 L286 154 L318 184 L306 214 L268 230 L232 214 L218 184 Z"
                    fill="rgba(211,72,45,0.4)"
                    stroke="rgba(17,17,17,0.08)"
                  />
                  <path
                    d="M486 252 L532 250 L562 272 L548 292 L512 294 L490 278 Z"
                    fill="rgba(211,72,45,0.28)"
                    stroke="rgba(17,17,17,0.08)"
                  />
                  <path
                    d="M607 189 L642 194 L648 216 L620 222 L606 207 Z"
                    fill="rgba(211,72,45,0.24)"
                    stroke="rgba(17,17,17,0.08)"
                  />

                  <g fill="none" stroke="rgba(17,17,17,0.14)" strokeWidth="1">
                    <path d="M102 94 C182 132, 286 104, 366 168" />
                    <path d="M152 330 C270 286, 406 302, 522 258" />
                    <path d="M382 132 C462 164, 548 164, 664 180" />
                  </g>

                  <g>
                    <circle cx="252" cy="194" r="7" fill="#d3482d" stroke="#111" strokeWidth="1.5" />
                    <circle cx="252" cy="194" r="18" fill="none" stroke="rgba(211,72,45,0.35)" strokeWidth="1.5" />
                    <text x="272" y="188" className="map-anno">ΑΤΤΙΚΗ</text>
                    <text x="272" y="208" className="map-anno-sub">€ 24.3M καταγεγραμμένα</text>

                    <circle cx="520" cy="274" r="6" fill="#d3482d" stroke="#111" strokeWidth="1.5" />
                    <text x="538" y="270" className="map-anno">ΠΕΛΟΠΟΝΝΗΣΟΣ</text>
                    <text x="538" y="290" className="map-anno-sub">€ 18.7M</text>

                    <circle cx="622" cy="206" r="5.5" fill="#d3482d" stroke="#111" strokeWidth="1.5" />
                    <text x="640" y="203" className="map-anno">ΚΡΗΤΗ</text>
                    <text x="640" y="222" className="map-anno-sub">€ 9.4M</text>
                  </g>

                  <g transform="translate(26 334)">
                    <rect width="206" height="94" fill="#f7f5ee" stroke="rgba(17,17,17,0.18)" />
                    <text x="14" y="22" className="map-inset-title">Ένθετο Αιγαίου / νησιά</text>
                    <g fill="rgba(211,72,45,0.22)" stroke="rgba(17,17,17,0.2)" strokeWidth="1">
                      <circle cx="34" cy="50" r="7" />
                      <circle cx="68" cy="44" r="5" />
                      <circle cx="98" cy="60" r="6" />
                      <circle cx="130" cy="47" r="5" />
                      <circle cx="160" cy="58" r="7" />
                    </g>
                    <text x="14" y="80" className="map-inset-note">Μικρές κατανομές, κατακερματισμένη γεωγραφία, απαιτητικές μεταφορές.</text>
                  </g>
                </svg>

                <div className="map-board__scale" aria-hidden="true">
                  <span>€</span>
                  <div />
                  <span>Ένταση</span>
                </div>
              </div>
            </div>

            <aside className="map-sidebar">
              <div className="map-sidebar__panel">
                <div className="eyebrow">Περιφερειακά Σήματα</div>
                <ul className="signal-list">
                  {mapSignals.map((signal) => (
                    <li key={signal.region}>
                      <div className={`signal-dot signal-dot--${signal.tone}`} aria-hidden="true" />
                      <div className="signal-copy">
                        <strong>{signal.region}</strong>
                        <span>{signal.value}</span>
                        <p>{signal.note}</p>
                      </div>
                    </li>
                  ))}
                </ul>
              </div>

              <div className="map-sidebar__panel">
                <div className="eyebrow">Δημοσιογραφική Οπτική</div>
                <div className="map-questions">
                  <p>Πού συγκεντρώνεται η δαπάνη πρόληψης σε σχέση με τις πρόσφατες καμένες εκτάσεις;</p>
                  <p>Ποιοι δήμοι εξαρτώνται από περιφερειακή ή αποκεντρωμένη κάλυψη φορέων;</p>
                  <p>Συγκεντρώνονται οι ίδιοι προμηθευτές στις ίδιες ζώνες υψηλού κινδύνου κάθε χρόνο;</p>
                </div>
              </div>
            </aside>
          </div>

          <div className="map-inset-grid">
            {regionalMapCards.map((card, i) => (
              <article className="map-inset-card" key={card.title}>
                <div className="map-inset-card__head">
                  <div className="eyebrow">{card.metric}</div>
                  <h3>{card.title}</h3>
                </div>
                <div className="mini-map" aria-hidden="true">
                  <svg viewBox="0 0 320 128">
                    <rect x="0" y="0" width="320" height="128" fill="#f7f5ee" />
                    <g stroke="rgba(17,17,17,0.1)" strokeWidth="1" fill="none">
                      <path d="M0 20 H320" />
                      <path d="M0 54 H320" />
                      <path d="M0 88 H320" />
                      <path d="M80 0 V128" />
                      <path d="M160 0 V128" />
                      <path d="M240 0 V128" />
                    </g>
                    <path
                      d={
                        i === 0
                          ? 'M16 92 L62 78 L100 86 L142 56 L184 62 L228 42 L270 56 L304 28'
                          : i === 1
                            ? 'M16 78 L54 82 L96 52 L138 72 L176 38 L214 46 L256 64 L304 44'
                            : 'M16 84 L58 62 L96 66 L136 42 L178 78 L216 54 L260 58 L304 36'
                      }
                      fill="none"
                      stroke="rgba(17,17,17,0.75)"
                      strokeWidth="2"
                    />
                    <path
                      d={
                        i === 0
                          ? 'M16 104 L62 92 L100 98 L142 72 L184 76 L228 58 L270 70 L304 46'
                          : i === 1
                            ? 'M16 94 L54 98 L96 66 L138 86 L176 54 L214 62 L256 78 L304 60'
                            : 'M16 102 L58 80 L96 84 L136 58 L178 92 L216 70 L260 72 L304 54'
                      }
                      fill="none"
                      stroke="rgba(211,72,45,0.45)"
                      strokeWidth="2"
                    />
                    <g fill="#d3482d" stroke="#111" strokeWidth="1">
                      <circle cx={i === 0 ? 142 : i === 1 ? 176 : 136} cy={i === 0 ? 56 : i === 1 ? 38 : 42} r="4.5" />
                      <circle cx={i === 0 ? 228 : i === 1 ? 96 : 216} cy={i === 0 ? 42 : i === 1 ? 52 : 54} r="3.5" />
                    </g>
                  </svg>
                </div>
                <div className="map-inset-card__foot">
                  <strong>{card.value}</strong>
                  <p>{card.note}</p>
                </div>
              </article>
            ))}
          </div>
        </section>
          </>
        )}

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
              <article className="record-card" key={`${row.beneficiary}-${idx}`}>
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
                {row.contractCount > 1 && (
                  <div className="record-contract-amounts" aria-label="Ποσά ανά σύμβαση">
                    <div className="record-contract-amounts__title">Ποσό ανά σύμβαση</div>
                    <ul>
                      {row.contractAmounts.slice(0, 8).map((amount, amountIdx) => (
                        <li key={`${row.beneficiary}-amount-${amountIdx}`}>{formatEur(amount)}</li>
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
        <section id="organizations" className="organization section-rule">
          <div className="organization__header">
            <div className="eyebrow">Σελίδα Φορέα</div>
            <h2>Περιφέρεια Αττικής</h2>
            <p>
              Συντακτική διάταξη για προφίλ φορέα: μεγάλες συνολικές τιμές, λεπτοί κανόνες και
              πυκνά μπλοκ μεταδεδομένων που διαβάζονται σαν δημόσιο καθολικό.
            </p>
          </div>

          <div className="organization__hero">
            <div className="org-year" aria-hidden="true">
              2025
            </div>
            <div className="org-total">
              <span className="eyebrow">Συνολική Δαπάνη</span>
              <div className="org-total__value">€ 24.3M</div>
              <div className="org-total__note">
                Περιφέρεια + εποπτευόμενοι δήμοι / πρόληψη πυρκαγιών και ετοιμότητα
              </div>
            </div>
            <div className="org-codes">
              <div className="eyebrow">Κατηγορίες Υψηλού Όγκου</div>
              <div className="cpv-wall">
                <span>CPV 77312000-0</span>
                <span>CPV 45500000-2</span>
                <span>CPV 34144210-3</span>
                <span>CPV 44611500-1</span>
              </div>
            </div>
          </div>

          <div className="organization__grid">
            <div className="organization__kpis">
              {orgKpis.map((kpi) => (
                <article className="org-kpi" key={kpi.label}>
                  <div className="eyebrow">{kpi.label}</div>
                  <div className="org-kpi__value">{kpi.value}</div>
                  <p>{kpi.note}</p>
                </article>
              ))}
            </div>

            <div className="organization__timeline">
              <div className="eyebrow">Χρονολόγιο</div>
              <ul>
                {timelineItems.map((item) => (
                  <li key={`${item.month}-${item.year}`}>
                    <div className="timeline-date">
                      <span>{item.month}</span>
                      <strong>{item.year}</strong>
                    </div>
                    <p>{item.text}</p>
                  </li>
                ))}
              </ul>
            </div>
          </div>
        </section>

        <DebugComponentLabel name="DocumentArchiveSection" />
        <section id="documents" className="document-archive section-rule">
          <div className="section-head">
            <div className="eyebrow">Προβολή Εγγράφου</div>
            <h2>Πλαίσιο δημόσιου αρχείου με πλευρικά μεταδεδομένα και κυρίαρχη προεπισκόπηση σελίδας</h2>
          </div>

          <div className="document-shell" role="region" aria-label="Προεπισκόπηση εγγράφου">
            <aside className="document-meta">
              <div className="meta-group">
                <span className="label">Κωδικός Εγγραφής</span>
                <strong>ΠΥΡ-2026-0142</strong>
              </div>
              <div className="meta-group">
                <span className="label">Φορέας</span>
                <strong>Περιφέρεια Αττικής</strong>
              </div>
              <div className="meta-group">
                <span className="label">Ημερομηνία Έκδοσης</span>
                <strong>18 Φεβ 2026</strong>
              </div>
              <div className="meta-group">
                <span className="label">Ποσό</span>
                <strong className="accent-text">€ 3.200.000</strong>
              </div>
              <div className="meta-group">
                <span className="label">Κατάσταση</span>
                <strong>Ανατέθηκε</strong>
              </div>
              <button className="doc-action" type="button">
                Άνοιγμα PDF
              </button>
            </aside>

            <div className="document-page" aria-hidden="true">
              <div className="document-page__header">
                <span>ΕΛΛΗΝΙΚΗ ΔΗΜΟΚΡΑΤΙΑ</span>
                <span>ΑΠΟΦΑΣΗ ΑΝΑΘΕΣΗΣ</span>
                <span>Σελίδα 1 / 8</span>
              </div>

              <div className="document-title-block">
                <div className="document-watermark">0142</div>
                <h3>ΠΡΟΛΗΠΤΙΚΟΙ ΚΑΘΑΡΙΣΜΟΙ ΚΑΙ ΔΙΑΝΟΙΞΗ ΑΝΤΙΠΥΡΙΚΩΝ ΖΩΝΩΝ</h3>
                <p>
                  Απόφαση για την ανάθεση εργασιών πρόληψης πυρκαγιών σε ζώνες υψηλής
                  επικινδυνότητας με σκοπό τη μείωση καύσιμης ύλης και τη βελτίωση πρόσβασης.
                </p>
              </div>

              <div className="document-lines">
                {Array.from({ length: 12 }).map((_, i) => (
                  <div className="doc-line" key={i}>
                    <span>{String(i + 1).padStart(2, '0')}</span>
                    <div />
                  </div>
                ))}
              </div>
            </div>
          </div>
        </section>

        <DebugComponentLabel name="AboutSection" />
        <section id="about" className="about-panel section-rule">
          <div className="about-panel__left">
            <div className="eyebrow">Σχεδιαστική Σύλληψη</div>
            <h2>Δημόσιο αρχείο / πολιτικό αποθετήριο / ερευνητική πλατφόρμα</h2>
            <p>
              Μονόχρωμη δομή, υπερμεγέθη μεγέθη, ορατή λογική πλέγματος και περιορισμένη χρήση
              έμφασης δημιουργούν ένα περιβάλλον πιο θεσμικό παρά «προϊοντικό».
            </p>
          </div>
          <div className="about-panel__right">
            <div className="poster-motif" aria-hidden="true">
              <div className="poster-motif__sun" />
              <div className="poster-motif__terrain" />
            </div>
            <div className="about-stats">
              <div>
                <span className="label">Θέμα</span>
                <strong>φωτιά / γραφειοκρατία / τεκμήρια</strong>
              </div>
              <div>
                <span className="label">Γλώσσα UI</span>
                <strong>ελβετική συντακτική λογική + μπρουταλιστική εγκράτεια</strong>
              </div>
              <div>
                <span className="label">Χρήση Έμφασης</span>
                <strong>σύνδεσμοι / ενεργές καταστάσεις / κρίσιμες σημάνσεις δαπάνης</strong>
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
