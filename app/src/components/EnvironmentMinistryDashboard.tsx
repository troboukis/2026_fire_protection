import { useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { MouseEvent as ReactMouseEvent } from 'react'
import * as d3 from 'd3'
import { Link } from 'react-router-dom'
import ComponentTag from './ComponentTag'
import ContractModal, { type ContractModalContract } from './ContractModal'
import DataLoadingCard from './DataLoadingCard'
import DiavgeiaDecisionCard, { type DiavgeiaDecisionCardView } from './DiavgeiaDecisionCard'
import FeaturedRecordsSection, { type BeneficiaryInsightRow, type FeaturedRecordContract } from './FeaturedRecordsSection'
import LatestContractCard from './LatestContractCard'
import MapTilerLogo from './MapTilerLogo'
import MapLegendToggle from './MapLegendToggle'
import ProfileMetricCard from './ProfileMetricCard'
import ProfileSectionCard from './ProfileSectionCard'
import { attachBeneficiaryGemi } from '../lib/beneficiaryGemi'
import { buildDiavgeiaDocumentUrl, downloadContractDocument } from '../lib/contractDocument'
import { buildContractsPageHref } from '../lib/contractsPageHref'
import { buildHillshadeTileOverlays } from '../lib/maptilerHillshade'
import { logError } from '../lib/logger'
import { loadMunicipalitiesGeojson } from '../lib/municipalitiesGeojson'
import { summarizePaymentRows } from '../lib/paymentSummary'
import { supabase } from '../lib/supabase'
import type { GeoData } from '../types'

const CURRENT_YEAR = new Date().getFullYear()
const FIRST_YEAR = CURRENT_YEAR - 2

type DashboardRpcContract = {
  id: number | string
  who: string | null
  what: string | null
  when: string | null
  why: string | null
  beneficiary: string | null
  contract_type: string | null
  amount_without_vat: number | string | null
  amount_with_vat: number | string | null
  reference_number: string | null
  contract_number: string | null
  cpv_items: Array<{ code?: string | null; label?: string | null }> | null
  contract_signed_date: string | null
  start_date: string | null
  end_date: string | null
  no_end_date: boolean | null
  organization_vat_number: string | null
  beneficiary_vat_number: string | null
  beneficiary_gemi?: string | null
  signers: string | null
  assign_criteria: string | null
  contract_kind: string | null
  award_procedure: string | null
  units_operator: string | null
  funding_cofund: string | null
  funding_self: string | null
  funding_espa: string | null
  funding_regular: string | null
  auction_ref_no: string | null
  payment_ref_no: string | null
  short_description: string | null
  raw_budget: number | string | null
  contract_budget: number | string | null
  contract_related_ada: string | null
  previous_reference_number: string | null
  next_reference_number: string | null
  diavgeia_ada: string | null
  payment_fiscal_year: number | string | null
  primary_signer: string | null
  primary_beneficiary: string | null
  primary_beneficiary_vat_number: string | null
}

type DashboardRpcDiavgeiaDecision = {
  id: number | string
  org_type: string | null
  org_name_clean: string | null
  subject: string | null
  decision_date: string | null
  ada: string | null
  diavgeia_document_type_decision_uid: string | null
  document_url: string | null
  spending_contractors_value: string | null
  municipality_key?: string | null
  protocol_number?: string | null
  thematic_categories?: string | null
  spending_signers?: string | null
  spending_contractors_name?: string | null
  spending_contractors_afm?: string | null
}

type DashboardRpcFlowRow = {
  signer: string | null
  beneficiary: string | null
  total_amount: number | string | null
  contract_count: number | string | null
  ratio: number | string | null
  lead_contract: DashboardRpcContract | null
}

type DashboardRpcTopCpv = {
  label: string | null
  code: string | null
  count: number | string | null
  share: number | string | null
}

type DashboardRpcWorkPoint = {
  id: string | null
  procurement_id: number | string | null
  lat: number | string | null
  lon: number | string | null
  work: string | null
  point_name: string | null
  contract_title: string | null
  amount_without_vat: number | string | null
  beneficiary: string | null
  assignment_type: string | null
}

type DashboardRpcResponse = {
  identification?: {
    organization_keys?: string[] | null
    rule?: string | null
  } | null
  ministry_name?: string | null
  total_spend?: number | string | null
  signed_2026_count?: number | string | null
  signed_current_amount?: number | string | null
  active_carryover_count?: number | string | null
  payment_flow_total?: number | string | null
  direct_award_amount?: number | string | null
  direct_award_with_auction_amount?: number | string | null
  direct_award_without_auction_amount?: number | string | null
  current_year_direct_award_amount?: number | string | null
  current_year_direct_award_with_auction_amount?: number | string | null
  current_year_direct_award_without_auction_amount?: number | string | null
  current_year_beneficiary_count?: number | string | null
  top_cpvs?: DashboardRpcTopCpv[] | null
  current_year_top_cpvs?: DashboardRpcTopCpv[] | null
  active_contract_top_cpvs?: DashboardRpcTopCpv[] | null
  work_points?: DashboardRpcWorkPoint[] | null
  flow_rows?: DashboardRpcFlowRow[] | null
  featured_contracts?: DashboardRpcContract[] | null
  recent_active_contracts?: DashboardRpcContract[] | null
  recent_diavgeia_decisions?: DashboardRpcDiavgeiaDecision[] | null
}

type DashboardContract = ContractModalContract & {
  id: string
  who: string
  what: string
  when: string
  why: string
  beneficiary: string
  contractType: string
  howMuch: string
  documentUrl?: string | null
  rawAmount: number
  signedAtIso: string | null
  startDateIso: string | null
  endDateIso: string | null
  noEndDate: boolean
  primarySigner: string
  primaryBeneficiary: string
  primaryBeneficiaryVat: string
}

type FlowRow = {
  signer: string
  beneficiary: string
  totalAmount: number
  contractCount: number
  ratio: number
  leadContract: DashboardContract | null
}

type TopCpvRow = {
  label: string
  code: string
  count: number
  share: number
}

type WorkPoint = {
  id: string
  procurementId: string
  lat: number
  lon: number
  work: string
  pointName: string
  contractTitle: string
  amountWithoutVat: number | null
  beneficiary: string
  assignmentType: string
}

type WorkPointTooltip = {
  id?: string
  x: number
  y: number
  title: string
  items: string[]
}

type DashboardDiavgeiaDecision = DiavgeiaDecisionCardView

type DashboardData = {
  ministryName: string
  identificationKeys: string[]
  identificationRule: string
  totalSpend: number
  signed2026Count: number
  signedCurrentAmount: number
  activeCarryoverCount: number
  paymentFlow2026Total: number
  directAwardAmount: number
  directAwardWithAuctionAmount: number
  directAwardWithoutAuctionAmount: number
  currentYearDirectAwardAmount: number
  currentYearDirectAwardWithAuctionAmount: number
  currentYearDirectAwardWithoutAuctionAmount: number
  currentYearBeneficiaryCount: number
  workPoints: WorkPoint[]
  topCpvs: TopCpvRow[]
  currentYearTopCpvs: TopCpvRow[]
  activeContractTopCpvs: TopCpvRow[]
  flowRows: FlowRow[]
  featuredContracts: DashboardContract[]
  recentActiveContracts: DashboardContract[]
  recentDiavgeiaDecisions: DashboardDiavgeiaDecision[]
}

function cleanText(value: unknown): string | null {
  if (value == null) return null
  const text = String(value).trim()
  if (!text || text.toLowerCase() === 'nan' || text.toLowerCase() === 'none') return null
  return text
}

function toFiniteNumber(value: unknown): number | null {
  if (value == null) return null
  if (typeof value === 'number') return Number.isFinite(value) ? value : null
  const text = String(value).trim()
  if (!text) return null
  const parsed = Number(text)
  return Number.isFinite(parsed) ? parsed : null
}

function normalizeDate(value: unknown): string | null {
  const text = cleanText(value)
  if (!text) return null
  const isoMatch = text.match(/^(\d{4}-\d{2}-\d{2})/)
  if (isoMatch) return isoMatch[1]
  const parsed = new Date(text)
  if (Number.isNaN(parsed.getTime())) return null
  return parsed.toISOString().slice(0, 10)
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

function formatEur(value: number | null): string {
  if (value == null || Number.isNaN(value)) return '—'
  return value.toLocaleString('el-GR', {
    style: 'currency',
    currency: 'EUR',
    maximumFractionDigits: 0,
  })
}

function formatEurCompact(value: number): string {
  if (!Number.isFinite(value)) return '—'
  const abs = Math.abs(value)
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M €`
  if (abs >= 1_000) return `${(value / 1_000).toFixed(0)}K €`
  return `${Math.round(value).toLocaleString('el-GR')} €`
}

function parseGreekAmount(value: unknown): number | null {
  const text = cleanText(value)
  if (!text) return null
  const match = text.match(/\d[\d.,]*/)
  if (!match) return null
  const normalized = match[0].includes(',')
    ? match[0].replace(/\./g, '').replace(',', '.')
    : match[0]
  const amount = Number(normalized)
  return Number.isFinite(amount) && amount > 0 ? amount : null
}

function formatEurFromGreekText(value: unknown): string | null {
  const amount = parseGreekAmount(value)
  return amount == null ? null : formatEur(amount)
}

function formatDirectAwardBreakdownNote(total: number, shareLabel: string, withAuction: number, withoutAuction: number): string {
  if (total <= 0) {
    return `Εξ αυτών τα ${formatEur(total)} (${shareLabel}) δόθηκαν με απευθείας ανάθεση.`
  }

  if (withAuction > 0 && withoutAuction <= 0) {
    return `Εξ αυτών, τα ${formatEur(total)} (${shareLabel}), σύμφωνα με το ΚΙΜΔΙΣ, δόθηκαν με απευθείας αναθέσεις. Στην πραγματικότητα, όμως, προηγήθηκε πρόσκληση υποβολής προσφορών προς συγκεκριμένες εταιρείες και επιλέχθηκαν εκείνες με τη χαμηλότερη τιμή. Ουσιαστικά, πρόκειται για συμβάσεις που εμπίπτουν σε ειδικό καθεστώς βάσει του ν. 998/1979, το οποίο προβλέπει την ανάθεση δασικών εργασιών με ειδικές διαδικασίες.`
  }

  if (withAuction <= 0 && withoutAuction > 0) {
    return `Εξ αυτών τα ${formatEur(total)} (${shareLabel}) σύμφωνα με το ΚΙΜΔΙΣ δόθηκαν με απευθείας αναθέσεις, χωρίς πρόσκληση υποβολής προτάσεων. Παρατηρούμε ότι η πλειονότητα των συμβάσεων για δασικά έργα του Υπουργείου Περιβάλλοντος κάνουν χρήση του νόμου 998/1979 άρθρο 16§5, σύμφωνα με το οποίο επιτρέπεται η ανάθεση δασικών εργασιών με ειδικές διαδικασίες, εκτός τυπικών διαγωνισμών δημοσίων συμβάσεων.`
  }

  return `Από την οποία το ${shareLabel} σύμφωνα με το ΚΙΜΔΙΣ δόθηκαν με απευθείας αναθέσεις, ωστόσο σε περιλαμβάνουν συμβάσεις που κάνουν χρήση του νόμου 998/1979.`
}

function toUpperEl(value: string | null): string {
  if (!value) return '—'
  return value.toLocaleUpperCase('el-GR')
}

function contractDocumentUrl(contractRelatedAda: string | null, diavgeiaAda: string | null): string | null {
  return buildDiavgeiaDocumentUrl(cleanText(contractRelatedAda), cleanText(diavgeiaAda))
}

function firstPipePart(value: unknown): string | null {
  const text = cleanText(value)
  if (!text) return null
  return text.split('|').map((item) => item.trim()).filter(Boolean)[0] ?? null
}

function isoDateDaysAgoLocal(days: number): string {
  const d = new Date()
  d.setDate(d.getDate() - days)
  const tzOffsetMs = d.getTimezoneOffset() * 60_000
  return new Date(d.getTime() - tzOffsetMs).toISOString().slice(0, 10)
}

function isoTodayLocal(): string {
  const d = new Date()
  const tzOffsetMs = d.getTimezoneOffset() * 60_000
  return new Date(d.getTime() - tzOffsetMs).toISOString().slice(0, 10)
}

function mapRpcContract(contract: DashboardRpcContract, ministryName: string): DashboardContract {
  const cpvItems = Array.isArray(contract.cpv_items)
    ? contract.cpv_items
      .map((item) => ({
        code: cleanText(item.code) ?? '—',
        label: cleanText(item.label) ?? '—',
      }))
    : []
  const mainCpv = cpvItems[0] ?? null
  const amountWithoutVat = toFiniteNumber(contract.amount_without_vat) ?? 0
  const amountWithVat = toFiniteNumber(contract.amount_with_vat)
  const contractRelatedAda = cleanText(contract.contract_related_ada)
  const diavgeiaAda = cleanText(contract.diavgeia_ada)
  const signedAtIso = normalizeDate(contract.contract_signed_date)
  const startDateIso = normalizeDate(contract.start_date)
  const endDateIso = normalizeDate(contract.end_date)
  const noEndDate = Boolean(contract.no_end_date)

  return {
    id: String(contract.id),
    who: cleanText(contract.who) ?? ministryName,
    what: cleanText(contract.what) ?? '—',
    when: formatDateEl(normalizeDate(contract.when)),
    why: cleanText(contract.why) ?? mainCpv?.label ?? '—',
    beneficiary: toUpperEl(cleanText(contract.beneficiary)),
    contractType: cleanText(contract.contract_type) ?? '—',
    howMuch: formatEur(amountWithoutVat),
    withoutVatAmount: formatEur(amountWithoutVat),
    withVatAmount: formatEur(amountWithVat),
    referenceNumber: cleanText(contract.reference_number) ?? '—',
    contractNumber: cleanText(contract.contract_number) ?? '—',
    cpv: mainCpv?.label ?? '—',
    cpvCode: mainCpv?.code ?? '—',
    cpvItems,
    signedAt: formatDateEl(signedAtIso),
    startDate: formatDateEl(startDateIso),
    endDate: noEndDate ? 'Χωρίς λήξη' : formatDateEl(endDateIso),
    organizationVat: cleanText(contract.organization_vat_number) ?? '—',
    beneficiaryVat: cleanText(contract.beneficiary_vat_number) ?? '—',
    beneficiaryGemi: cleanText(contract.beneficiary_gemi) ?? null,
    signers: cleanText(contract.signers) ?? '—',
    assignCriteria: cleanText(contract.assign_criteria) ?? '—',
    contractKind: cleanText(contract.contract_kind) ?? '—',
    awardProcedure: cleanText(contract.award_procedure) ?? '—',
    unitsOperator: cleanText(contract.units_operator) ?? '—',
    fundingCofund: cleanText(contract.funding_cofund) ?? '—',
    fundingSelf: cleanText(contract.funding_self) ?? '—',
    fundingEspa: cleanText(contract.funding_espa) ?? '—',
    fundingRegular: cleanText(contract.funding_regular) ?? '—',
    auctionRefNo: cleanText(contract.auction_ref_no) ?? '—',
    paymentRefNo: cleanText(contract.payment_ref_no) ?? '—',
    shortDescription: cleanText(contract.short_description) ?? '—',
    rawBudget: formatEur(toFiniteNumber(contract.raw_budget)),
    contractBudget: formatEur(toFiniteNumber(contract.contract_budget)),
    contractRelatedAda: contractRelatedAda ?? '—',
    previousReferenceNumber: cleanText(contract.previous_reference_number) ?? '—',
    nextReferenceNumber: cleanText(contract.next_reference_number) ?? '—',
    documentUrl: contractDocumentUrl(contractRelatedAda, diavgeiaAda),
    rawAmount: amountWithoutVat,
    signedAtIso,
    startDateIso,
    endDateIso,
    noEndDate,
    primarySigner: cleanText(contract.primary_signer) ?? 'Χωρίς υπογράφοντα',
    primaryBeneficiary: toUpperEl(cleanText(contract.primary_beneficiary)),
    primaryBeneficiaryVat: cleanText(contract.primary_beneficiary_vat_number) ?? '—',
  }
}

function mapRpcDiavgeiaDecision(row: DashboardRpcDiavgeiaDecision, ministryName: string): DashboardDiavgeiaDecision {
  const decisionDateIso = normalizeDate(row.decision_date)
  const ada = cleanText(row.ada) ?? '—'
  return {
    id: String(row.id),
    orgType: cleanText(row.org_type) ?? 'ΥΠΟΥΡΓΕΙΟ',
    orgNameClean: cleanText(row.org_name_clean) ?? ministryName,
    subject: cleanText(row.subject) ?? '—',
    when: formatDateEl(decisionDateIso),
    ada,
    decisionTypeUid: cleanText(row.diavgeia_document_type_decision_uid) ?? '—',
    amount: formatEurFromGreekText(row.spending_contractors_value),
    documentUrl: cleanText(row.document_url) ?? (ada !== '—' ? `https://diavgeia.gov.gr/doc/${ada}` : null),
    sortDate: decisionDateIso,
    municipalityKey: cleanText(row.municipality_key),
    protocolNumber: cleanText(row.protocol_number),
    thematicCategories: cleanText(row.thematic_categories),
    spendingSigners: cleanText(row.spending_signers),
    spendingContractorsName: cleanText(row.spending_contractors_name),
    spendingContractorsAfm: cleanText(row.spending_contractors_afm),
  }
}

function createEmptyDashboardData(): DashboardData {
  return {
    ministryName: 'Υπουργείο Περιβάλλοντος και Ενέργειας',
    identificationKeys: [],
    identificationRule: '',
    totalSpend: 0,
    signed2026Count: 0,
    signedCurrentAmount: 0,
    activeCarryoverCount: 0,
    paymentFlow2026Total: 0,
    directAwardAmount: 0,
    directAwardWithAuctionAmount: 0,
    directAwardWithoutAuctionAmount: 0,
    currentYearDirectAwardAmount: 0,
    currentYearDirectAwardWithAuctionAmount: 0,
    currentYearDirectAwardWithoutAuctionAmount: 0,
    currentYearBeneficiaryCount: 0,
    workPoints: [],
    topCpvs: [],
    currentYearTopCpvs: [],
    activeContractTopCpvs: [],
    flowRows: [],
    featuredContracts: [],
    recentActiveContracts: [],
    recentDiavgeiaDecisions: [],
  }
}

function EnvironmentWorksMap({
  workPoints,
  onOpenContract,
}: {
  workPoints: WorkPoint[]
  onOpenContract: (procurementId: string) => void
}) {
  const [geojson, setGeojson] = useState<GeoData | null>(null)
  const [tooltip, setTooltip] = useState<WorkPointTooltip | null>(null)
  const [terrainFailed, setTerrainFailed] = useState(false)
  const [showWorkPoints, setShowWorkPoints] = useState(true)
  const [isCompactViewport, setIsCompactViewport] = useState(() => (
    typeof window === 'undefined' ? false : window.matchMedia('(max-width: 760px)').matches
  ))
  const [isCoarsePointer, setIsCoarsePointer] = useState(() => (
    typeof window === 'undefined' ? false : window.matchMedia('(hover: none), (pointer: coarse)').matches
  ))
  const mapClipPathId = useId().replace(/:/g, '-')
  const mapTilerApiKey = useMemo(() => cleanText(import.meta.env.VITE_MAPTILER_API_KEY), [])
  const mapRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    let cancelled = false

    const loadGeojson = async () => {
      const data = await loadMunicipalitiesGeojson()
      if (!cancelled) setGeojson(data)
    }

    loadGeojson().catch(() => {
      if (!cancelled) setGeojson(null)
    })

    return () => { cancelled = true }
  }, [])

  useEffect(() => {
    setTooltip(null)
  }, [workPoints])

  useEffect(() => {
    setTerrainFailed(false)
  }, [isCompactViewport, mapTilerApiKey])

  useEffect(() => {
    if (typeof window === 'undefined') return undefined

    const mediaQuery = window.matchMedia('(max-width: 760px)')
    const updateMatch = () => setIsCompactViewport(mediaQuery.matches)
    updateMatch()

    if (typeof mediaQuery.addEventListener === 'function') {
      mediaQuery.addEventListener('change', updateMatch)
      return () => mediaQuery.removeEventListener('change', updateMatch)
    }

    mediaQuery.addListener(updateMatch)
    return () => mediaQuery.removeListener(updateMatch)
  }, [])

  useEffect(() => {
    if (typeof window === 'undefined') return undefined

    const mediaQuery = window.matchMedia('(hover: none), (pointer: coarse)')
    const updateMatch = () => setIsCoarsePointer(mediaQuery.matches)
    updateMatch()

    if (typeof mediaQuery.addEventListener === 'function') {
      mediaQuery.addEventListener('change', updateMatch)
      return () => mediaQuery.removeEventListener('change', updateMatch)
    }

    mediaQuery.addListener(updateMatch)
    return () => mediaQuery.removeListener(updateMatch)
  }, [])

  const pointerInMap = (
    event: ReactMouseEvent<SVGCircleElement>,
    fallback: { x: number; y: number },
  ) => {
    const rect = mapRef.current?.getBoundingClientRect()
    if (!rect) return fallback
    const x = event.clientX - rect.left
    const y = event.clientY - rect.top
    if (!Number.isFinite(x) || !Number.isFinite(y)) return fallback
    return { x, y }
  }

  const getTooltipItems = (point: WorkPoint) => [
    `Κόστος χωρίς ΦΠΑ: ${formatEur(point.amountWithoutVat)}`,
    `Ανάδοχος: ${point.beneficiary}`,
    `Ανάθεση: ${point.assignmentType}`,
  ]

  const updatePointTooltip = (
    event: ReactMouseEvent<SVGCircleElement>,
    point: WorkPoint & { x: number; y: number },
  ) => {
    const pointer = pointerInMap(event, { x: point.x, y: point.y })
    setTooltip({
      id: point.id,
      x: pointer.x,
      y: pointer.y,
      title: point.contractTitle,
      items: getTooltipItems(point),
    })
  }

  const togglePointTooltip = (
    event: ReactMouseEvent<SVGCircleElement>,
    point: WorkPoint & { x: number; y: number },
  ) => {
    const pointer = pointerInMap(event, { x: point.x, y: point.y })
    setTooltip((current) => (
      current?.id === point.id
        ? null
        : {
            id: point.id,
            x: pointer.x,
            y: pointer.y,
            title: point.contractTitle,
            items: getTooltipItems(point),
          }
    ))
  }

  const mapData = useMemo(() => {
    if (!geojson) return null

    const width = isCompactViewport ? 560 : 760
    const height = isCompactViewport ? 760 : 460
    const projection = d3.geoMercator().fitExtent(
      isCompactViewport ? [[10, 12], [width - 10, height - 18]] : [[16, 14], [width - 14, height - 22]],
      geojson as unknown as d3.ExtendedFeatureCollection,
    )
    const path = d3.geoPath().projection(projection)
    const bounds = path.bounds(geojson as unknown as d3.GeoPermissibleObjects)
    const boundsCenterX = (bounds[0][0] + bounds[1][0]) / 2
    const boundsCenterY = (bounds[0][1] + bounds[1][1]) / 2
    const transformScale = isCompactViewport ? 1.14 : 1.06
    const targetCenterX = isCompactViewport ? width / 2 - 4 : width / 2 - 8
    const targetCenterY = isCompactViewport ? height * 0.505 : height * 0.515
    const transformTranslateX = targetCenterX - (boundsCenterX * transformScale)
    const transformTranslateY = targetCenterY - (boundsCenterY * transformScale)
    const transformPoint = (x: number, y: number) => ({
      x: x * transformScale + transformTranslateX,
      y: y * transformScale + transformTranslateY,
    })

    const points = workPoints
      .map((point) => {
        const projected = projection([point.lon, point.lat])
        if (!projected) return null
        const [baseX, baseY] = projected
        if (!Number.isFinite(baseX) || !Number.isFinite(baseY)) return null
        const { x, y } = transformPoint(baseX, baseY)
        return { ...point, x, y }
      })
      .filter((point): point is WorkPoint & { x: number; y: number } => point !== null)

    return {
      width,
      height,
      transform: `translate(${transformTranslateX} ${transformTranslateY}) scale(${transformScale})`,
      paths: geojson.features.map((feature, index) => ({
        key: `${String((feature.properties as { municipality_code?: string | null }).municipality_code ?? '—')}-${index}`,
        d: path(feature as unknown as d3.GeoPermissibleObjects) ?? '',
      })),
      hillshadeTiles: buildHillshadeTileOverlays(
        geojson as unknown as d3.GeoPermissibleObjects,
        projection,
        width,
        height,
        mapTilerApiKey,
        transformPoint,
      ),
      points,
    }
  }, [geojson, isCompactViewport, mapTilerApiKey, workPoints])

  if (!mapData) {
    return <DataLoadingCard className="environment-map environment-map--loading" compact message="Προετοιμάζεται ο χάρτης εργασιών του ΥΠΕΝ." />
  }

  if (!mapData.points.length) {
    return <div className="environment-map environment-map--empty">Δεν βρέθηκαν γεωκωδικοποιημένες εργασίες για τις συμβάσεις του 2026.</div>
  }

  return (
    <div
      ref={mapRef}
      className="environment-map"
      onClick={() => {
        if (!isCoarsePointer) return
        setTooltip(null)
      }}
    >
      <div className="environment-map__canvas">
        <svg
          viewBox={`0 0 ${mapData.width} ${mapData.height}`}
          role="img"
          aria-label="Χάρτης εργασιών συμβάσεων Υπουργείου Περιβάλλοντος"
          preserveAspectRatio="xMidYMid meet"
        >
        <defs>
          <clipPath id={mapClipPathId}>
            {mapData.paths.map((feature) => (
              <path key={`clip-${feature.key}`} d={feature.d} transform={mapData.transform} />
            ))}
          </clipPath>
        </defs>
        <g className="environment-map__base" transform={mapData.transform}>
          {mapData.paths.map((feature) => (
            <path key={feature.key} d={feature.d} />
          ))}
        </g>
        {mapData.hillshadeTiles.length > 0 && !terrainFailed && (
          <g className="environment-map__terrain" clipPath={`url(#${mapClipPathId})`} aria-hidden="true">
            {mapData.hillshadeTiles.map((tile) => (
              <image
                key={tile.key}
                href={tile.href}
                x={tile.x}
                y={tile.y}
                width={tile.width}
                height={tile.height}
                preserveAspectRatio="none"
                className="environment-map__terrain-tile"
                onError={() => setTerrainFailed(true)}
              />
            ))}
          </g>
        )}
        <g className="environment-map__points">
          {showWorkPoints && mapData.points.map((point) => (
            <g key={point.id}>
              <circle
                className="environment-map__point-hitbox"
                cx={point.x}
                cy={point.y}
                r={isCoarsePointer ? 13 : 8}
                fill="transparent"
                onMouseEnter={(event) => {
                  if (isCoarsePointer) return
                  updatePointTooltip(event, point)
                }}
                onMouseMove={(event) => {
                  if (isCoarsePointer) return
                  updatePointTooltip(event, point)
                }}
                onMouseLeave={() => {
                  if (isCoarsePointer) return
                  setTooltip(null)
                }}
                onClick={(event) => {
                  event.stopPropagation()
                  if (isCoarsePointer) {
                    togglePointTooltip(event, point)
                    return
                  }
                  onOpenContract(point.procurementId)
                }}
              />
              <circle
                className="environment-map__point-dot"
                cx={point.x}
                cy={point.y}
                r={isCompactViewport ? 4.4 : 3.2}
              />
            </g>
          ))}
        </g>
        </svg>
        {mapData.hillshadeTiles.length > 0 && !terrainFailed && (
          <MapTilerLogo className="environment-map__maptiler-logo" />
        )}
      </div>
      {tooltip ? (
        <div
          className="environment-map__tooltip app-tooltip"
          style={{
            left: `${Math.max(12, tooltip.x + 14)}px`,
            top: `${Math.max(12, tooltip.y - 14)}px`,
          }}
        >
          <strong>{tooltip.title}</strong>
          {tooltip.items.map((item) => (
            <span key={item}>{item}</span>
          ))}
        </div>
      ) : null}
      <div className="environment-map__legend">
        <MapLegendToggle
          visible={showWorkPoints}
          onToggle={() => {
            setTooltip(null)
            setShowWorkPoints((visible) => !visible)
          }}
          label="Σημεία εργασιών"
        >
          <span className="environment-map__legend-dot" aria-hidden="true" />
          <span>{`${mapData.points.length.toLocaleString('el-GR')} σημεία εργασιών από συμβάσεις που υπογράφηκαν ή παρέμειναν ενεργές το ${CURRENT_YEAR}`}</span>
        </MapLegendToggle>
      </div>
    </div>
  )
}

function mapTopCpvRows(rows: DashboardRpcTopCpv[] | null | undefined): TopCpvRow[] {
  return Array.isArray(rows)
    ? rows.map<TopCpvRow>((row) => ({
      label: cleanText(row.label) ?? '—',
      code: cleanText(row.code) ?? '—',
      count: toFiniteNumber(row.count) ?? 0,
      share: toFiniteNumber(row.share) ?? 0,
    }))
    : []
}

export default function EnvironmentMinistryDashboard() {
  const [data, setData] = useState<DashboardData>(createEmptyDashboardData())
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [selectedContract, setSelectedContract] = useState<DashboardContract | null>(null)
  const [mapCardHeight, setMapCardHeight] = useState<number | null>(null)
  const recentDateFrom = useMemo(() => isoDateDaysAgoLocal(30), [])
  const recentDateTo = useMemo(() => isoTodayLocal(), [])
  const allContractsHref = useMemo(
    () => buildContractsPageHref({ organizationKeys: data.identificationKeys }),
    [data.identificationKeys],
  )
  const allDiavgeiaHref = useMemo(
    () => `/diavgeia?q=${encodeURIComponent(data.ministryName)}&allDates=1`,
    [data.ministryName],
  )
  const [isDesktopGrid, setIsDesktopGrid] = useState(() => (
    typeof window === 'undefined' ? true : window.matchMedia('(min-width: 1101px)').matches
  ))
  const mapCardRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)

    const loadDashboard = async () => {
      try {
        const { data: payload, error: rpcError } = await supabase.rpc('get_environment_ministry_dashboard', {
          p_year: CURRENT_YEAR,
        })

        if (rpcError) throw rpcError

        const response = (payload ?? {}) as DashboardRpcResponse
        const ministryName = cleanText(response.ministry_name) ?? 'Υπουργείο Περιβάλλοντος και Ενέργειας'
        const featuredContracts = Array.isArray(response.featured_contracts)
          ? response.featured_contracts.map((contract) => mapRpcContract(contract, ministryName))
          : []
        const recentActiveContracts = Array.isArray(response.recent_active_contracts)
          ? response.recent_active_contracts.map((contract) => mapRpcContract(contract, ministryName))
          : []
        const recentDiavgeiaDecisions = Array.isArray(response.recent_diavgeia_decisions)
          ? response.recent_diavgeia_decisions.map((decision) => mapRpcDiavgeiaDecision(decision, ministryName))
          : []

        const flowRows = Array.isArray(response.flow_rows)
          ? response.flow_rows.map<FlowRow>((row) => {
            const leadContract = row.lead_contract ? mapRpcContract(row.lead_contract, ministryName) : null
            return {
              signer: cleanText(row.signer) ?? 'Χωρίς υπογράφοντα',
              beneficiary: cleanText(row.beneficiary) ?? 'Χωρίς δικαιούχο',
              totalAmount: toFiniteNumber(row.total_amount) ?? 0,
              contractCount: toFiniteNumber(row.contract_count) ?? 0,
              ratio: toFiniteNumber(row.ratio) ?? 0,
              leadContract,
            }
          })
          : []

        const topCpvs = mapTopCpvRows(response.top_cpvs)
        const currentYearTopCpvs = mapTopCpvRows(response.current_year_top_cpvs)
        const activeContractTopCpvs = mapTopCpvRows(response.active_contract_top_cpvs)

        const workPoints = Array.isArray(response.work_points)
          ? response.work_points
            .map((row) => {
              const lat = toFiniteNumber(row.lat)
              const lon = toFiniteNumber(row.lon)
              if (lat == null || lon == null) return null
              return {
                id: cleanText(row.id) ?? `${lat}-${lon}`,
                procurementId: cleanText(row.procurement_id) ?? '',
                lat,
                lon,
                work: cleanText(row.work) ?? 'Εργασία πυροπροστασίας',
                pointName: cleanText(row.point_name) ?? 'Χωρίς τοπωνύμιο',
                contractTitle: cleanText(row.contract_title) ?? 'Χωρίς τίτλο σύμβασης',
                amountWithoutVat: toFiniteNumber(row.amount_without_vat),
                beneficiary: toUpperEl(cleanText(row.beneficiary)),
                assignmentType: cleanText(row.assignment_type) ?? '—',
              }
            })
            .filter((row): row is WorkPoint => row !== null)
          : []

        if (!cancelled) {
          setData({
            ministryName,
            identificationKeys: Array.isArray(response.identification?.organization_keys)
              ? response.identification!.organization_keys.filter((value): value is string => Boolean(cleanText(value)))
              : [],
            identificationRule: cleanText(response.identification?.rule) ?? '',
            totalSpend: toFiniteNumber(response.total_spend) ?? 0,
            signed2026Count: toFiniteNumber(response.signed_2026_count) ?? 0,
            signedCurrentAmount: toFiniteNumber(response.signed_current_amount) ?? 0,
            activeCarryoverCount: toFiniteNumber(response.active_carryover_count) ?? 0,
            paymentFlow2026Total: toFiniteNumber(response.payment_flow_total) ?? 0,
            directAwardAmount: toFiniteNumber(response.direct_award_amount) ?? 0,
            directAwardWithAuctionAmount: toFiniteNumber(response.direct_award_with_auction_amount) ?? 0,
            directAwardWithoutAuctionAmount: toFiniteNumber(response.direct_award_without_auction_amount) ?? 0,
            currentYearDirectAwardAmount: toFiniteNumber(response.current_year_direct_award_amount) ?? 0,
            currentYearDirectAwardWithAuctionAmount: toFiniteNumber(response.current_year_direct_award_with_auction_amount) ?? 0,
            currentYearDirectAwardWithoutAuctionAmount: toFiniteNumber(response.current_year_direct_award_without_auction_amount) ?? 0,
            currentYearBeneficiaryCount: toFiniteNumber(response.current_year_beneficiary_count) ?? 0,
            workPoints,
            topCpvs,
            currentYearTopCpvs,
            activeContractTopCpvs,
            flowRows,
            featuredContracts,
            recentActiveContracts,
            recentDiavgeiaDecisions,
          })
          setLoading(false)
        }
      } catch (loadError) {
        if (import.meta.env.DEV) logError('[EnvironmentMinistryDashboard] failed', loadError)
        if (!cancelled) {
          setData(createEmptyDashboardData())
          setError('Δεν ήταν δυνατή η φόρτωση του dashboard του Υπουργείου Περιβάλλοντος.')
          setLoading(false)
        }
      }
    }

    loadDashboard()
    return () => { cancelled = true }
  }, [])

  const directAwardShareLabel = useMemo(() => {
    if (data.totalSpend <= 0) return '0%'
    const share = (data.directAwardAmount / data.totalSpend) * 100
    return `${Math.round(share)}%`
  }, [data.directAwardAmount, data.totalSpend])

  const currentYearDirectAwardShareLabel = useMemo(() => {
    if (data.signedCurrentAmount <= 0) return '0%'
    const share = (data.currentYearDirectAwardAmount / data.signedCurrentAmount) * 100
    return `${Math.round(share)}%`
  }, [data.currentYearDirectAwardAmount, data.signedCurrentAmount])

  const directAwardBreakdownNote = useMemo(() => formatDirectAwardBreakdownNote(
    data.directAwardAmount,
    directAwardShareLabel,
    data.directAwardWithAuctionAmount,
    data.directAwardWithoutAuctionAmount,
  ), [
    data.directAwardAmount,
    data.directAwardWithAuctionAmount,
    data.directAwardWithoutAuctionAmount,
    directAwardShareLabel,
  ])

  const currentYearDirectAwardBreakdownNote = useMemo(() => formatDirectAwardBreakdownNote(
    data.currentYearDirectAwardAmount,
    currentYearDirectAwardShareLabel,
    data.currentYearDirectAwardWithAuctionAmount,
    data.currentYearDirectAwardWithoutAuctionAmount,
  ), [
    data.currentYearDirectAwardAmount,
    data.currentYearDirectAwardWithAuctionAmount,
    data.currentYearDirectAwardWithoutAuctionAmount,
    currentYearDirectAwardShareLabel,
  ])

  const topCpvLabelCurrentYear = data.currentYearTopCpvs[0]?.label ?? 'Χωρίς διαθέσιμη κατηγορία εργασιών'
  const topCpvLabelActiveContract = data.activeContractTopCpvs[0]?.label ?? 'Χωρίς διαθέσιμη κατηγορία εργασιών'

  const heroCards = useMemo(() => [
    {
      eyebrow: `${FIRST_YEAR} - ${CURRENT_YEAR}`,
      label: 'Συνολική δαπάνη πυροπροστασίας',
      value: formatEurCompact(data.totalSpend),
      note: directAwardBreakdownNote,
      tone: 'ink' as const,
    },
    {
      eyebrow: `${CURRENT_YEAR}`,
      label: `Νέες συμβάσεις`,
      value: data.signed2026Count.toLocaleString('el-GR'),
      note: `${topCpvLabelCurrentYear} η πιο συχνή κατηγορία εργασιών.`,
      tone: 'default' as const,
    },
    {
      eyebrow: 'Active',
      label: `Παλιότερες συμβάσεις που είναι ενεργές* το ${CURRENT_YEAR}`,
      value: data.activeCarryoverCount.toLocaleString('el-GR'),
      note: `${topCpvLabelActiveContract} η πιο συχνή κατηγορία στις παλιότερες ενεργές συμβάσεις.`,
      tone: 'default' as const,
    },
    {
      eyebrow: 'Δικαιούχοι',
      label: `Εταιρείες ανέλαβαν ${data.signed2026Count.toLocaleString('el-GR')} έργα το ${CURRENT_YEAR}`,
      value: data.currentYearBeneficiaryCount.toLocaleString('el-GR'),
      note: 'Αφορά μόνο νέες συμβάσεις που υπεγράφησαν μέσα στο έτος.',
      tone: 'accent' as const,
    },
  ], [data.activeCarryoverCount, data.currentYearBeneficiaryCount, data.signed2026Count, data.totalSpend, directAwardBreakdownNote, topCpvLabelActiveContract, topCpvLabelCurrentYear])

  const featuredBeneficiaryRows = useMemo<BeneficiaryInsightRow[]>(() => {
    type BeneficiaryGroup = {
      beneficiaryVat: string
      beneficiaryGemi: string | null
      beneficiaryName: string
      totalAmount: number
      contractIds: Set<string>
      startDateIso: string | null
      endDateIso: string | null
      organizationTotals: Map<string, number>
      cpvCounts: Map<string, number>
      signerCounts: Map<string, number>
      relevantContracts: DashboardContract[]
    }

    const groups = new Map<string, BeneficiaryGroup>()

    for (const contract of data.recentActiveContracts) {
      const beneficiaryVat = cleanText(contract.primaryBeneficiaryVat)
      if (!beneficiaryVat || beneficiaryVat === '—') continue

      const beneficiaryName = cleanText(contract.primaryBeneficiary) ?? cleanText(contract.beneficiary) ?? beneficiaryVat

      const group = groups.get(beneficiaryVat) ?? {
        beneficiaryVat,
        beneficiaryGemi: cleanText(contract.beneficiaryGemi),
        beneficiaryName,
        totalAmount: 0,
        contractIds: new Set<string>(),
        startDateIso: null,
        endDateIso: null,
        organizationTotals: new Map<string, number>(),
        cpvCounts: new Map<string, number>(),
        signerCounts: new Map<string, number>(),
        relevantContracts: [],
      }

      group.totalAmount += contract.rawAmount
      group.beneficiaryGemi = group.beneficiaryGemi ?? cleanText(contract.beneficiaryGemi)
      group.contractIds.add(contract.id)

      if (contract.startDateIso && (!group.startDateIso || contract.startDateIso < group.startDateIso)) {
        group.startDateIso = contract.startDateIso
      }

      if (contract.endDateIso && (!group.endDateIso || contract.endDateIso > group.endDateIso)) {
        group.endDateIso = contract.endDateIso
      }

      const organization = cleanText(contract.who) ?? data.ministryName
      group.organizationTotals.set(organization, (group.organizationTotals.get(organization) ?? 0) + contract.rawAmount)

      for (const item of contract.cpvItems ?? []) {
        const label = cleanText(item.label)
        if (!label || label === '—') continue
        group.cpvCounts.set(label, (group.cpvCounts.get(label) ?? 0) + 1)
      }

      const signer = cleanText(contract.primarySigner) ?? cleanText(contract.signers)
      if (signer) {
        group.signerCounts.set(signer, (group.signerCounts.get(signer) ?? 0) + 1)
      }

      group.relevantContracts.push(contract)
      groups.set(beneficiaryVat, group)
    }

    const today = new Date()

    return [...groups.values()]
      .map<BeneficiaryInsightRow>((group) => {
        const organization = [...group.organizationTotals.entries()]
          .sort((a, b) => {
            if (b[1] !== a[1]) return b[1] - a[1]
            return a[0].localeCompare(b[0], 'el')
          })[0]?.[0] ?? data.ministryName

        const cpv = [...group.cpvCounts.entries()]
          .sort((a, b) => {
            if (b[1] !== a[1]) return b[1] - a[1]
            return a[0].localeCompare(b[0], 'el')
          })[0]?.[0] ?? '—'

        const signer = [...group.signerCounts.entries()]
          .sort((a, b) => {
            if (b[1] !== a[1]) return b[1] - a[1]
            return a[0].localeCompare(b[0], 'el')
          })[0]?.[0] ?? '—'

        const relevantContracts = [...group.relevantContracts]
          .sort((a, b) => {
            if (b.rawAmount !== a.rawAmount) return b.rawAmount - a.rawAmount
            return (b.signedAtIso ?? '').localeCompare(a.signedAtIso ?? '', 'el')
          })

        let duration = '—'
        let progressPct: number | null = null
        const endDate = formatDateEl(group.endDateIso)

        if (group.startDateIso && group.endDateIso) {
          const start = new Date(group.startDateIso)
          const end = new Date(group.endDateIso)
          if (!Number.isNaN(start.getTime()) && !Number.isNaN(end.getTime()) && end >= start) {
            const days = Math.round((end.getTime() - start.getTime()) / 86_400_000) + 1
            duration = `${days} ημέρες`
          }
          if (end > start) {
            if (today <= start) progressPct = 0
            else if (today >= end) progressPct = 100
            else progressPct = Math.max(0, Math.min(100, ((today.getTime() - start.getTime()) / (end.getTime() - start.getTime())) * 100))
          }
        }

        return {
          beneficiary: toUpperEl(group.beneficiaryName),
          beneficiaryVat: group.beneficiaryVat,
          beneficiaryGemi: group.beneficiaryGemi,
          organization,
          totalAmount: group.totalAmount,
          contractCount: group.contractIds.size,
          cpv,
          startDate: formatDateEl(group.startDateIso),
          endDate,
          duration,
          progressPct,
          signer,
          relevantContracts: relevantContracts as FeaturedRecordContract[],
        }
      })
      .sort((a, b) => {
        if (b.totalAmount !== a.totalAmount) return b.totalAmount - a.totalAmount
        if (b.contractCount !== a.contractCount) return b.contractCount - a.contractCount
        return a.beneficiary.localeCompare(b.beneficiary, 'el')
      })
  }, [data.ministryName, data.recentActiveContracts])

  const recentActivityItems = useMemo(() => {
    const activityItems: Array<
      | { kind: 'contract'; key: string; sortDate: string | null; contract: DashboardContract }
      | { kind: 'diavgeia'; key: string; sortDate: string | null; decision: DashboardDiavgeiaDecision }
    > = [
      ...data.recentActiveContracts.map((contract) => ({
        kind: 'contract' as const,
        key: `contract-${contract.id}`,
        sortDate: contract.signedAtIso,
        contract,
      })),
      ...data.recentDiavgeiaDecisions.map((decision) => ({
        kind: 'diavgeia' as const,
        key: `diavgeia-${decision.id}`,
        sortDate: decision.sortDate ?? null,
        decision,
      })),
    ]

    return activityItems.filter((item) => item.sortDate != null && item.sortDate >= recentDateFrom && item.sortDate <= recentDateTo).sort((a, b) => {
      const aTime = a.sortDate ? Date.parse(a.sortDate) : 0
      const bTime = b.sortDate ? Date.parse(b.sortDate) : 0
      if (bTime !== aTime) return bTime - aTime
      return b.key.localeCompare(a.key, 'el')
    })
  }, [data.recentActiveContracts, data.recentDiavgeiaDecisions, recentDateFrom, recentDateTo])

  const downloadSelectedContract = async () => {
    if (!selectedContract) return
    await downloadContractDocument(selectedContract)
  }

  useEffect(() => {
    if (typeof window === 'undefined') return undefined

    const mediaQuery = window.matchMedia('(min-width: 1101px)')
    const updateMatch = () => setIsDesktopGrid(mediaQuery.matches)
    updateMatch()

    if (typeof mediaQuery.addEventListener === 'function') {
      mediaQuery.addEventListener('change', updateMatch)
      return () => mediaQuery.removeEventListener('change', updateMatch)
    }

    mediaQuery.addListener(updateMatch)
    return () => mediaQuery.removeListener(updateMatch)
  }, [])

  useLayoutEffect(() => {
    if (!isDesktopGrid || loading || error || !mapCardRef.current) {
      setMapCardHeight((current) => (current == null ? current : null))
      return
    }

    let frameId = 0

    const updateHeight = () => {
      const nextHeight = mapCardRef.current?.getBoundingClientRect().height ?? 0
      const normalizedHeight = nextHeight > 0 ? Math.round(nextHeight) : null
      setMapCardHeight((current) => (current === normalizedHeight ? current : normalizedHeight))
    }

    frameId = window.requestAnimationFrame(updateHeight)

    if (typeof ResizeObserver === 'undefined') {
      return () => window.cancelAnimationFrame(frameId)
    }

    const observer = new ResizeObserver(() => {
      window.cancelAnimationFrame(frameId)
      frameId = window.requestAnimationFrame(updateHeight)
    })
    observer.observe(mapCardRef.current)

    return () => {
      observer.disconnect()
      window.cancelAnimationFrame(frameId)
    }
  }, [data.workPoints.length, error, isDesktopGrid, loading])

  return (
    <>
      <section className="environment-dashboard section-rule">
        <ComponentTag name="EnvironmentMinistryDashboard" />

        <header className="environment-dashboard__hero">
          <div className="environment-dashboard__hero-copy">
            <div className="eyebrow">Υπουργείο Περιβάλλοντος</div>
            <h1>{`Το ${CURRENT_YEAR} δαπάνησε ${formatEur(data.signedCurrentAmount)} σε εργασίες πυροπροστασίας`}</h1>
            <p>
              {`${currentYearDirectAwardBreakdownNote} Το ποσό αφορά νέες συμβάσεις και όχι παλιότερες συμβάσεις που είναι ενεργές* το ${CURRENT_YEAR}.`}
            </p>
          </div>
          <div className="environment-dashboard__hero-year" aria-hidden="true">{CURRENT_YEAR}</div>
        </header>

        {loading && (
          <DataLoadingCard className="environment-dashboard__loading" message={`Ανακτώνται τα συγκεντρωτικά στοιχεία του ${data.ministryName}.`} />
        )}

        {!loading && error && (
          <div className="environment-dashboard__error">{error}</div>
        )}

        {!loading && !error && (
          <>
            <div className="environment-dashboard__metrics" aria-label="Κύριες μετρήσεις ΥΠΕΝ">
              {heroCards.map((card) => (
                <ProfileMetricCard
                  key={card.label}
                  eyebrow={card.eyebrow}
                  label={card.label}
                  value={card.value}
                  note={card.note}
                  tone={card.tone}
                />
              ))}
            </div>

            <div className="environment-dashboard__grid">
              <div ref={mapCardRef} className="environment-dashboard__map-panel">
                <ProfileSectionCard
                  eyebrow="Χάρτης εργασιών"
                  title="Πού εκτελούνται οι παρεμβάσεις"
                  subtitle={`Ο χάρτης αποτυπώνει ${data.workPoints.length.toLocaleString('el-GR')} σημεία εργασιών που καταφέραμε να γεωεντοπίσουμε με αυτοματοποιημένο τρόπο από τις ${data.signed2026Count.toLocaleString('el-GR')} νέες συμβάσεις που υπογράφηκαν το ${CURRENT_YEAR}.`}
                  className="environment-section environment-section--map"
                >
                  <EnvironmentWorksMap
                    workPoints={data.workPoints}
                    onOpenContract={(procurementId) => {
                    const cachedContract = data.featuredContracts.find((contract) => contract.id === procurementId)
                      ?? data.recentActiveContracts.find((contract) => contract.id === procurementId)
                      ?? data.flowRows.find((row) => row.leadContract?.id === procurementId)?.leadContract
                      ?? null

                    if (cachedContract) {
                      setSelectedContract(cachedContract)
                      return
                    }

                    const contractId = Number(procurementId)
                    if (!Number.isFinite(contractId)) return

                    void (async () => {
                      const { data: procurement } = await supabase
                        .from('procurement')
                        .select(`
                          id,
                          title,
                          submission_at,
                          contract_signed_date,
                          start_date,
                          end_date,
                          no_end_date,
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
                          diavgeia_ada,
                          organization_vat_number
                        `)
                        .eq('id', contractId)
                        .limit(1)
                        .maybeSingle()

                      if (!procurement) return

                      const [{ data: paymentRows }, { data: cpvRows }] = await Promise.all([
                        supabase
                          .from('payment')
                          .select('beneficiary_name, beneficiary_vat_number, signers, payment_ref_no, amount_without_vat, amount_with_vat')
                          .eq('procurement_id', contractId),
                        supabase
                          .from('cpv')
                          .select('cpv_key, cpv_value')
                          .eq('procurement_id', contractId),
                      ])

                      const enrichedPaymentRows = await attachBeneficiaryGemi((paymentRows ?? []) as Array<{
                        beneficiary_name: string | null
                        beneficiary_vat_number: string | null
                        signers: string | null
                        payment_ref_no: string | null
                        amount_without_vat: number | null
                        amount_with_vat: number | null
                      }>)
                      const payment = summarizePaymentRows(enrichedPaymentRows)

                      const nextContract = mapRpcContract({
                        id: procurement.id,
                        who: data.ministryName,
                        what: cleanText(procurement.title) ?? '—',
                        when: cleanText(procurement.submission_at),
                        why: firstPipePart(procurement.short_descriptions) ?? '—',
                        beneficiary: cleanText(payment.beneficiary_name) ?? '—',
                        contract_type: cleanText(procurement.procedure_type_value) ?? '—',
                        amount_without_vat: payment.amount_without_vat ?? procurement.contract_budget ?? procurement.budget,
                        amount_with_vat: payment.amount_with_vat ?? null,
                        reference_number: cleanText(procurement.reference_number) ?? '—',
                        contract_number: cleanText(procurement.contract_number) ?? '—',
                        cpv_items: ((cpvRows ?? []) as Array<{ cpv_key: string | null; cpv_value: string | null }>)
                          .map((row) => ({
                            code: cleanText(row.cpv_key),
                            label: cleanText(row.cpv_value),
                          })),
                        contract_signed_date: cleanText(procurement.contract_signed_date),
                        start_date: cleanText(procurement.start_date),
                        end_date: cleanText(procurement.end_date),
                        no_end_date: procurement.no_end_date ?? false,
                        organization_vat_number: cleanText(procurement.organization_vat_number) ?? '—',
                        beneficiary_vat_number: cleanText(payment.beneficiary_vat_number) ?? '—',
                        beneficiary_gemi: cleanText(payment.beneficiary_gemi) ?? null,
                        signers: cleanText(payment.signers) ?? '—',
                        assign_criteria: cleanText(procurement.assign_criteria) ?? '—',
                        contract_kind: cleanText(procurement.contract_type) ?? '—',
                        award_procedure: cleanText(procurement.award_procedure) ?? '—',
                        units_operator: cleanText(procurement.units_operator) ?? '—',
                        funding_cofund: cleanText(procurement.funding_details_cofund) ?? '—',
                        funding_self: cleanText(procurement.funding_details_self_fund) ?? '—',
                        funding_espa: cleanText(procurement.funding_details_espa) ?? '—',
                        funding_regular: cleanText(procurement.funding_details_regular_budget) ?? '—',
                        auction_ref_no: cleanText(procurement.auction_ref_no) ?? '—',
                        payment_ref_no: cleanText(payment.payment_ref_no) ?? '—',
                        short_description: firstPipePart(procurement.short_descriptions) ?? '—',
                        raw_budget: procurement.budget,
                        contract_budget: procurement.contract_budget,
                        contract_related_ada: cleanText(procurement.contract_related_ada) ?? '—',
                        previous_reference_number: cleanText(procurement.prev_reference_no) ?? '—',
                        next_reference_number: cleanText(procurement.next_ref_no) ?? '—',
                        diavgeia_ada: cleanText(procurement.diavgeia_ada) ?? '—',
                        payment_fiscal_year: null,
                        primary_signer: firstPipePart(payment.signers) ?? 'Χωρίς υπογράφοντα',
                        primary_beneficiary: firstPipePart(payment.beneficiary_name) ?? 'Χωρίς δικαιούχο',
                        primary_beneficiary_vat_number: firstPipePart(payment.beneficiary_vat_number) ?? '—',
                      }, data.ministryName)

                      setSelectedContract(nextContract)
                    })()
                  }}
                  />
                </ProfileSectionCard>
              </div>

              <ProfileSectionCard
                eyebrow="Τελευταίες"
                title={`${recentActivityItems.length.toLocaleString('el-GR')} εγγραφές`}
                subtitle="Συμβάσεις ΚΗΜΔΗΣ και αποφάσεις Διαύγειας των τελευταίων 30 ημερών, από την πιο πρόσφατη στην παλαιότερη."
                className="environment-section environment-section--recent-contracts"
                style={isDesktopGrid && mapCardHeight ? { height: `${mapCardHeight}px` } : undefined}
              >
                <div
                  className="environment-active-contracts-wrap"
                >
                  <div className="environment-active-contracts">
                    {recentActivityItems.length === 0 && (
                      <div className="environment-contracts__empty">Δεν βρέθηκαν συμβάσεις ή αποφάσεις Διαύγειας τις τελευταίες 30 ημέρες.</div>
                    )}

                    {recentActivityItems.map((item) => (
                      item.kind === 'contract'
                        ? (
                            <LatestContractCard
                              key={item.key}
                              item={item.contract}
                              onOpen={() => setSelectedContract(item.contract)}
                            />
                          )
                        : (
                            <DiavgeiaDecisionCard
                              key={item.key}
                              item={item.decision}
                            />
                          )
                    ))}
                  </div>
                </div>
                <Link className="news-wire__all-link environment-active-contracts__all-link" to={allContractsHref}>
                  Όλες οι συμβάσεις ΚΗΜΔΗΣ
                </Link>
                <Link className="news-wire__all-link environment-active-contracts__all-link" to={allDiavgeiaHref}>
                  Όλες οι αποφάσεις Διαύγειας
                </Link>
              </ProfileSectionCard>
            </div>

          </>
        )}
      </section>

      <FeaturedRecordsSection
        sectionId="environment-ministry-beneficiaries"
        year={String(CURRENT_YEAR)}
        rows={featuredBeneficiaryRows}
        loading={loading}
        formatEur={formatEur}
        onOpenContract={(contract) => setSelectedContract(contract as DashboardContract)}
        eyebrowText={`Αναθέσεις`}
        title="Οι εταιρείες που ανέλαβαν έργα πυροπροστασίας"
        note={`Οι ανάδοχοι ταξινομούνται με βάση το συνολικό ποσό των ενεργών* συμβάσεων του ${data.ministryName}.`}
        footerNote={<>* Ως <strong>ενεργές</strong> συμβάσεις εννοούμε τις συμβάσεις που είτε υπεγράφησαν το {CURRENT_YEAR}, είτε υπεγράφησαν πριν το {CURRENT_YEAR} αλλά είχαν ρητή ημερομηνία λήξης του έργου εντός του {CURRENT_YEAR}.</>}
        emptyMessage="Δεν βρέθηκαν δικαιούχοι ενεργών συμβάσεων για το Υπουργείο Περιβάλλοντος."
      />

      {selectedContract && (
        <ContractModal
          contract={selectedContract}
          onClose={() => setSelectedContract(null)}
          onDownloadPdf={() => void downloadSelectedContract()}
        />
      )}
    </>
  )
}
