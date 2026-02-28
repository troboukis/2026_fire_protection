import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import ContractAnalysis from './components/ContractAnalysis'
import ComponentTag from './components/ComponentTag'
import { supabase } from './lib/supabase'

type ProcurementRecord = {
  id: string
  authority: string
  title: string
  amount: string
  stage: string
  cpv: string
  date: string
  supplier: string
}

type Kpi = {
  label: string
  value: string
  note: string
}

const featuredRecords: ProcurementRecord[] = [
  {
    id: 'ΠΥΡ-2026-0142',
    authority: 'ΠΕΡΙΦΕΡΕΙΑ ΑΤΤΙΚΗΣ / ΠΟΛΙΤΙΚΗ ΠΡΟΣΤΑΣΙΑ',
    title: 'Καθαρισμοί δασικών ζωνών και αντιπυρικών λωρίδων, φάση Α',
    amount: '€ 3.200.000',
    stage: 'Ανατέθηκε',
    cpv: 'CPV 77312000-0',
    date: '18 Φεβ 2026',
    supplier: 'Κοινοπραξία Δασικών Έργων ΑΕ',
  },
  {
    id: 'ΠΥΡ-2026-0097',
    authority: 'ΔΗΜΟΣ ΜΑΡΑΘΩΝΑ',
    title: 'Μίσθωση μηχανημάτων έργου για προληπτική αποψίλωση',
    amount: '€ 842.500',
    stage: 'Διαγωνισμός',
    cpv: 'CPV 45500000-2',
    date: '03 Φεβ 2026',
    supplier: 'Ανοικτή διαδικασία',
  },
  {
    id: 'ΠΥΡ-2025-2218',
    authority: 'ΥΠΟΥΡΓΕΙΟ ΚΛΙΜΑΤΙΚΗΣ ΚΡΙΣΗΣ',
    title: 'Προμήθεια δεξαμενών νερού και κινητών μονάδων υποστήριξης',
    amount: '€ 12.400.000',
    stage: 'Ολοκληρώθηκε',
    cpv: 'CPV 44611500-1',
    date: '12 Νοε 2025',
    supplier: 'Helios Emergency Systems',
  },
]

const kpis: Kpi[] = [
  { label: 'Καταγεγραμμένες Αποφάσεις', value: '4,821', note: 'ευρετηριασμένες αποφάσεις προμηθειών' },
  { label: 'Δήμοι', value: '332', note: 'διασταύρωση με δεδομένα έκθεσης σε πυρκαγιές' },
  { label: 'Εκτιμώμενη Δαπάνη', value: '€ 214.7M', note: 'πρόληψη πυρκαγιών + ετοιμότητα' },
  { label: 'Τελευταία Ενημέρωση', value: '25 Φεβ 2026', note: 'συγχρονισμένο στιγμιότυπο δημόσιου αρχείου' },
]

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

type LatestContractCard = {
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

type HeroStatsRpcRow = {
  period_main_start: string | null
  period_main_end: string | null
  total_main: number | string | null
  total_prev1: number | string | null
  total_prev2: number | string | null
  total_main_vs_prev1_pct: number | string | null
  top_contract_type: string | null
  top_contract_type_count: number | string | null
  top_contract_type_prev1_count: number | string | null
  top_contract_type_vs_prev1_pct: number | string | null
  top_cpv_text: string | null
  top_cpv_count: number | string | null
  top_cpv_prev1_count: number | string | null
  top_cpv_vs_prev1_pct: number | string | null
}

type HeroCurveRpcRow = {
  series_year: number | string
  point_date: string | null
  day_of_year: number | string
  year_days: number | string
  cumulative_amount: number | string | null
}

type HeroCurvePoint = {
  year: number
  dayOfYear: number
  yearDays: number
  value: number
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
        const { data, error } = await supabase
          .from('raw_procurements')
          .select(`
            id,
            organization_value,
            title,
            submission_at,
            contract_signed_date,
            nuts_city,
            nuts_code_value,
            cpv_values,
            short_descriptions,
            procedure_type_value,
            first_member_name,
            first_member_vat_number,
            reference_number,
            contract_number,
            total_cost_without_vat,
            total_cost_with_vat,
            contract_budget,
            budget,
            cpv_values,
            cpv_keys,
            signers,
            assign_criteria,
            contract_type,
            units_operator,
            funding_details_cofund,
            funding_details_self_fund,
            funding_details_espa,
            funding_details_regular_budget,
            auction_ref_no,
            payment_ref_no,
            short_descriptions,
            organization_vat_number,
            start_date,
            end_date,
            diavgeia_ada
          `)
          .not('submission_at', 'is', null)
          .order('submission_at', { ascending: false })
          .limit(15)

        if (cancelled || error) return

        const cards: LatestContractCard[] = (data ?? []).map((row) => {
          const amountWithoutVat = row.total_cost_without_vat != null
            ? Number(row.total_cost_without_vat)
            : null

          const why =
            firstPipePart(row.cpv_values) ??
            firstPipePart(row.short_descriptions) ??
            '—'
          const diavgeiaAda = cleanText(row.diavgeia_ada)

          return {
            id: String(row.id),
            who: cleanText(row.organization_value) ?? '—',
            what: cleanText(row.title) ?? '—',
            when: formatDateEl(cleanText(row.submission_at)),
            why: toSentenceCaseEl(why),
            beneficiary: toUpperEl(cleanText(row.first_member_name)),
            contractType: cleanText(row.procedure_type_value) ?? '—',
            howMuch: formatEur(amountWithoutVat),
            withoutVatAmount: formatEur(amountWithoutVat),
            withVatAmount: formatEur(row.total_cost_with_vat != null ? Number(row.total_cost_with_vat) : null),
            referenceNumber: cleanText(row.reference_number) ?? '—',
            contractNumber: cleanText(row.contract_number) ?? '—',
            cpv: firstPipePart(row.cpv_values) ?? '—',
            cpvCode: firstPipePart(row.cpv_keys) ?? '—',
            signedAt: formatDateEl(cleanText(row.contract_signed_date)),
            startDate: formatDateEl(cleanText(row.start_date)),
            endDate: formatDateEl(cleanText(row.end_date)),
            organizationVat: cleanText(row.organization_vat_number) ?? '—',
            beneficiaryVat: cleanText(row.first_member_vat_number) ?? '—',
            signers: cleanText(row.signers) ?? '—',
            assignCriteria: cleanText(row.assign_criteria) ?? '—',
            contractKind: cleanText(row.contract_type) ?? '—',
            unitsOperator: cleanText(row.units_operator) ?? '—',
            fundingCofund: cleanText(row.funding_details_cofund) ?? '—',
            fundingSelf: cleanText(row.funding_details_self_fund) ?? '—',
            fundingEspa: cleanText(row.funding_details_espa) ?? '—',
            fundingRegular: cleanText(row.funding_details_regular_budget) ?? '—',
            auctionRefNo: cleanText(row.auction_ref_no) ?? '—',
            paymentRefNo: cleanText(row.payment_ref_no) ?? '—',
            shortDescription: firstPipePart(row.short_descriptions) ?? '—',
            rawBudget: formatEur(row.budget != null ? Number(row.budget) : null),
            contractBudget: formatEur(row.contract_budget != null ? Number(row.contract_budget) : null),
            documentUrl: diavgeiaAda ? `https://diavgeia.gov.gr/doc/${diavgeiaAda}` : null,
          }
        })

        setLatestContracts(cards)
      } finally {
        if (!cancelled) setLatestContractsLoading(false)
      }
    }

    loadLatestContracts()

    return () => { cancelled = true }
  }, [])

  useEffect(() => {
    let cancelled = false
    setHeroStatsLoading(true)

    const loadHeroStats = async () => {
      try {
        const { data, error } = await supabase.rpc('get_raw_procurements_hero_stats', {
          p_year_main: currentYear,
          p_year_prev1: currentYear - 1,
          p_year_prev2: currentYear - 2,
          p_as_of_date: new Date().toISOString().slice(0, 10),
        })

        if (cancelled || error) return
        const row = (data?.[0] as HeroStatsRpcRow | undefined) ?? null
        if (!row) return

        setHeroStats({
          periodMainStart: cleanText(row.period_main_start) ?? '',
          periodMainEnd: cleanText(row.period_main_end) ?? '',
          totalMain: Number(row.total_main ?? 0),
          totalPrev1: Number(row.total_prev1 ?? 0),
          totalPrev2: Number(row.total_prev2 ?? 0),
          totalVsPrev1Pct: row.total_main_vs_prev1_pct == null ? null : Number(row.total_main_vs_prev1_pct),
          topContractType: cleanText(row.top_contract_type) ?? '—',
          topContractTypeCount: Number(row.top_contract_type_count ?? 0),
          topContractTypePrevCount: Number(row.top_contract_type_prev1_count ?? 0),
          topContractTypeVsPrev1Pct: row.top_contract_type_vs_prev1_pct == null ? null : Number(row.top_contract_type_vs_prev1_pct),
          topCpvText: toSentenceCaseEl(cleanText(row.top_cpv_text)),
          topCpvCount: Number(row.top_cpv_count ?? 0),
          topCpvPrevCount: Number(row.top_cpv_prev1_count ?? 0),
          topCpvVsPrev1Pct: row.top_cpv_vs_prev1_pct == null ? null : Number(row.top_cpv_vs_prev1_pct),
        })

        const asOf = new Date().toISOString().slice(0, 10)
        const { data: curveData, error: curveError } = await supabase.rpc('get_raw_procurements_cumulative_curve', {
          p_as_of_date: asOf,
          p_year_main: currentYear,
          p_year_start: YEAR_START,
        })
        if (cancelled || curveError) return

        const curveRows: HeroCurveRpcRow[] = (curveData ?? []) as HeroCurveRpcRow[]
        const points: HeroCurvePoint[] = curveRows.map((r) => ({
          year: Number(r.series_year),
          dayOfYear: Number(r.day_of_year),
          yearDays: Number(r.year_days),
          value: Number(r.cumulative_amount ?? 0),
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
                <p>Σύνδεση με το dataset `raw_procurements`.</p>
              </article>
            )}
            {!latestContractsLoading && latestContracts.map((item) => (
              <article
                className="wire-item"
                key={item.id}
                role="button"
                tabIndex={0}
                onClick={() => setSelectedContract(item)}
                onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') setSelectedContract(item) }}
              >
                <div className="wire-item__head">
                  <span className="eyebrow wire-item__org">{item.who}</span>
                  <span className="wire-item__date">{item.when}</span>
                </div>
                <h2>{item.what}</h2>
                <div className="wire-item__rule" aria-hidden="true" />
                <p className="wire-item__subtitle">{item.why}</p>
                <div className="wire-item__footer">
                  <p className="wire-item__amount">
                    <span>{item.howMuch}</span>
                    <span className="wire-item__arrow">→</span>
                    <span className="wire-item__beneficiary">{item.beneficiary}</span>
                  </p>
                  <p className="wire-item__type">{toLowerEl(item.contractType)}</p>
                </div>
                {item.documentUrl && (
                  <p className="wire-item__link">
                    <a
                      href={item.documentUrl}
                      target="_blank"
                      rel="noreferrer"
                      onClick={(e) => e.stopPropagation()}
                    >
                      Άνοιγμα εγγράφου
                    </a>
                  </p>
                )}
              </article>
            ))}
            {!latestContractsLoading && latestContracts.length === 0 && (
              <article className="wire-item">
                <h2>Δεν βρέθηκαν πρόσφατες συμβάσεις.</h2>
                <p>Ελέγξτε ότι ο πίνακας `raw_procurements` έχει δεδομένα.</p>
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

        <DebugComponentLabel name="ContractAnalysis" />
        <ContractAnalysis />

        <DebugComponentLabel name="KpiRail" />
        <section className="kpi-rail section-rule" aria-label="Βασικοί δείκτες">
          {kpis.map((kpi) => (
            <article className="kpi-tile" key={kpi.label}>
              <div className="eyebrow">{kpi.label}</div>
              <div className="kpi-value">{kpi.value}</div>
              <p>{kpi.note}</p>
            </article>
          ))}
        </section>

        <DebugComponentLabel name="FeaturedRecordsSection" />
        <section id="records" className="records section-rule">
          <div className="section-head">
            <div className="eyebrow">Επιλεγμένες Εγγραφές Προμηθειών</div>
            <h2>Φύλλα εγγραφών τύπου αφίσας με ιεράρχηση που ξεκινά από τη δαπάνη</h2>
          </div>

          <div className="records-grid">
            {featuredRecords.map((record, idx) => (
              <article className="record-card" key={record.id}>
                <div className="record-card__year" aria-hidden="true">
                  {idx === 2 ? '2025' : '2026'}
                </div>
                <div className="record-card__header">
                  <div className="record-card__authority">{record.authority}</div>
                  <div className="record-card__id">{record.id}</div>
                </div>

                <h3>{record.title}</h3>

                <div className="record-card__amount">{record.amount}</div>

                <div className="record-card__tags" aria-label="Μεταδεδομένα εγγραφής">
                  <span>{record.stage}</span>
                  <span>{record.cpv}</span>
                  <span>{record.date}</span>
                </div>

                <div className="record-card__footer">
                  <div>
                    <span className="label">Προμηθευτής</span>
                    <strong>{record.supplier}</strong>
                  </div>
                  <a href="/" onClick={(e) => e.preventDefault()}>
                    Προβολή λεπτομερειών
                  </a>
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
        <div className="contract-modal-backdrop" onClick={() => setSelectedContract(null)}>
          <DebugComponentLabel name="ContractModal" />
          <article className="contract-modal" onClick={(e) => e.stopPropagation()}>
            <header className="contract-modal__header">
              <div>
                <span className="eyebrow">{selectedContract.who}</span>
                <h2>{selectedContract.what}</h2>
              </div>
              <button type="button" onClick={() => setSelectedContract(null)} aria-label="Κλείσιμο">
                ✕
              </button>
            </header>

            <p className="contract-modal__subtitle">{selectedContract.why}</p>

            <div className="contract-modal__highlight">
              <span className="contract-modal__amount">{selectedContract.withoutVatAmount}</span>
              <span className="contract-modal__arrow">→</span>
              <span className="contract-modal__beneficiary">{selectedContract.beneficiary}</span>
            </div>

            <div className="contract-modal__grid">
              <div><span>Ημερομηνία</span><strong>{selectedContract.when}</strong></div>
              <div><span>Τύπος Διαδικασίας</span><strong>{selectedContract.contractType}</strong></div>
              <div><span>Κωδ. Αναφοράς</span><strong>{selectedContract.referenceNumber}</strong></div>
              <div><span>Κωδ. Σύμβασης</span><strong>{selectedContract.contractNumber}</strong></div>
              <div><span>CPV</span><strong>{selectedContract.cpv} ({selectedContract.cpvCode})</strong></div>
              <div><span>Δικαιούχος ΑΦΜ</span><strong>{selectedContract.beneficiaryVat}</strong></div>
              <div><span>Φορέας ΑΦΜ</span><strong>{selectedContract.organizationVat}</strong></div>
              <div><span>Κριτήριο Ανάθεσης</span><strong>{selectedContract.assignCriteria}</strong></div>
              <div><span>Τύπος Σύμβασης</span><strong>{selectedContract.contractKind}</strong></div>
              <div><span>Υπογράφοντες</span><strong>{selectedContract.signers}</strong></div>
              <div><span>Υπεύθυνη Μονάδα</span><strong>{selectedContract.unitsOperator}</strong></div>
              <div><span>Περιγραφή</span><strong>{selectedContract.shortDescription}</strong></div>
              <div><span>Υπογραφή</span><strong>{selectedContract.signedAt}</strong></div>
              <div><span>Έναρξη</span><strong>{selectedContract.startDate}</strong></div>
              <div><span>Λήξη</span><strong>{selectedContract.endDate}</strong></div>
              <div><span>Ποσό με ΦΠΑ</span><strong>{selectedContract.withVatAmount}</strong></div>
              <div><span>Προϋπολογισμός</span><strong>{selectedContract.rawBudget}</strong></div>
              <div><span>Contract Budget</span><strong>{selectedContract.contractBudget}</strong></div>
              <div><span>Cofund</span><strong>{selectedContract.fundingCofund}</strong></div>
              <div><span>Self Fund</span><strong>{selectedContract.fundingSelf}</strong></div>
              <div><span>ESPA</span><strong>{selectedContract.fundingEspa}</strong></div>
              <div><span>Regular Budget</span><strong>{selectedContract.fundingRegular}</strong></div>
              <div><span>Auction Ref</span><strong>{selectedContract.auctionRefNo}</strong></div>
              <div><span>Payment Ref</span><strong>{selectedContract.paymentRefNo}</strong></div>
            </div>

            <footer className="contract-modal__footer">
              <button
                type="button"
                className="contract-modal__pdf-button"
                onClick={() => downloadContractPdf(selectedContract)}
              >
                Κατέβασε το
              </button>
              {selectedContract.documentUrl && (
                <a href={selectedContract.documentUrl} target="_blank" rel="noreferrer">
                  Άνοιγμα εγγράφου στη Διαύγεια
                </a>
              )}
            </footer>
          </article>
        </div>
      )}
    </div>
  )
}
