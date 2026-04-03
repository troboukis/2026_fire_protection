import { startTransition, useEffect, useMemo, useRef, useState, type MouseEvent as ReactMouseEvent } from 'react'
import * as d3 from 'd3'
import { Link, useSearchParams } from 'react-router-dom'
import ComponentTag from '../components/ComponentTag'
import ContractModal, { type ContractModalContract } from '../components/ContractModal'
import DataLoadingCard from '../components/DataLoadingCard'
import FeaturedRecordsSection, { type BeneficiaryInsightRow, type FeaturedRecordContract } from '../components/FeaturedRecordsSection'
import LatestContractCard, { type LatestContractCardView } from '../components/LatestContractCard'
import { buildContractAuthorityLabel, type ContractAuthorityScope } from '../lib/contractAuthority'
import { buildDiavgeiaDocumentUrl, downloadContractDocument } from '../lib/contractDocument'
import { buildContractsPageHref } from '../lib/contractsPageHref'
import { isContractActiveInYear, isContractActiveOnDate } from '../lib/contractWindow'
import { buildLatestContractCardView, type AuthorityScope } from '../lib/latestContractCard'
import { getMunicipalityFireYearSource } from '../lib/municipalityFireYearSource'
import { summarizePaymentRows } from '../lib/paymentSummary'
import {
  normalizeMunicipalityKey,
  normalizeMunicipalitySearch,
  rankMunicipalityMatches,
} from '../lib/municipalitySearch'
import { supabase } from '../lib/supabase'
import type { GeoData } from '../types'

type MunicipalityListItem = {
  municipality_key: string
  dhmos: string | null
  municipality_normalized_name: string | null
  kpi_politikis_prostasias: number | string | null
}

type MunicipalityProfileRow = {
  municipality_key: string
  dhmos: string | null
  municipality_normalized_name: string | null
  kpi_politikis_prostasias: number | string | null
  plithismos_synolikos: number | string | null
  plithismos_oreinos: number | string | null
  plithismos_hmioreinos: number | string | null
  plithismos_pedinos: number | string | null
  ektasi_km2: number | string | null
  puknotita: number | string | null
  oxhmata_udrofora: number | string | null
  oxhmata_purosvestika: number | string | null
  sxedia_purkagies: number | string | null
  dilosis_katharis_plithos: number | string | null
  elegxoi_katopin_dilosis: number | string | null
  mi_symmorfosi_dilosis: number | string | null
  pososto_symmorfosis_dilosis: number | string | null
  elegxoi_aytepaggelti: number | string | null
  mi_symmorfosi_aytepaggelti: number | string | null
  kataggelies_plithos: number | string | null
  elegxoi_katopin_kataggelias: number | string | null
  mi_symmorfosi_kataggelias: number | string | null
  ektasi_vlastisis_pros_katharismo_ha: number | string | null
  katharismeni_ektasi_vlastisis_ha: number | string | null
  pososto_proliptikou_katharismou: number | string | null
  ypoleimmata_katharismwn_t: number | string | null
  dapani_puroprostasias_eur: number | string | null
}

type ForestFireRow = {
  year: number | string | null
  burned_total_ha: number | string | null
  lat: number | string | null
  lon: number | string | null
  date_start: string | null
  date_end: string | null
}

type CopernicusRow = {
  firedate: string | null
  area_ha: number | string | null
  centroid: { coordinates?: [number, number] } | string | null
  shape: GeoJSON.Geometry | string | null
}

type WorkRow = {
  id: number | string
  procurement_id: number | string | null
  organization_normalized_value: string | null
  reference_number: string | null
  contract_signed_date: string | null
  title: string | null
  work: string | null
  point_name_canonical: string | null
  lat: number | string | null
  lon: number | string | null
}

type WorkMarker = {
  key: string
  x: number
  y: number
  procurementId: number | null
  organizationName: string | null
  work: string | null
  pointName: string | null
  title: string | null
  contractSignedDate: string | null
  referenceNumber: string | null
}

type MunicipalityContractRow = {
  procurement_id: number | string
  contract_signed_date: string | null
  organization_key: string | null
  organization_value: string | null
  authority_scope: string | null
  title: string | null
  procedure_type_value: string | null
  beneficiary_name: string | null
  amount_without_vat: number | string | null
  diavgeia_ada: string | null
  reference_number: string | null
}

type MunicipalityContractProcurementRow = {
  id: number
  contract_signed_date: string | null
  start_date?: string | null
  end_date?: string | null
  no_end_date?: boolean | null
  organization_key: string | null
  canonical_owner_scope: string | null
  title: string | null
  procedure_type_value: string | null
  diavgeia_ada: string | null
  reference_number: string | null
  contract_number: string | null
  contract_budget: number | null
  budget: number | null
  cancelled: boolean | null
  next_ref_no: string | null
  prev_reference_no?: string | null
  contract_related_ada?: string | null
}

type MunicipalityContractPaymentRow = {
  procurement_id: number
  beneficiary_name: string | null
  beneficiary_vat_number?: string | null
  signers?: string | null
  payment_ref_no?: string | null
  amount_without_vat: number | null
  amount_with_vat?: number | null
}

type MunicipalityContractOrganizationRow = {
  organization_key: string
  organization_normalized_value: string | null
  organization_value: string | null
  authority_scope: string | null
}

type ContractCurvePoint = {
  year: number
  dayOfYear: number
  yearDays: number
  value: number
}

type ProcedureBreakdownItem = {
  label: string
  count: number
  amount: number
}

type ContractChartHoverState = {
  dayOfYear: number
  svgX: number
}

type FundingChartHoverState = {
  year: number
  leftPct: number
}

type MunicipalityPointTooltip = {
  id?: string
  x: number
  y: number
  title: string
  items: string[]
}

type ProcurementCpvRow = {
  procurement_id: number
  cpv_key?: string | null
  cpv_value: string | null
}

type MunicipalityFeaturedProcurementRow = MunicipalityContractProcurementRow & {
  submission_at: string | null
  short_descriptions: string | null
  assign_criteria: string | null
  contract_type: string | null
  award_procedure: string | null
  units_operator: string | null
  funding_details_cofund: string | null
  funding_details_self_fund: string | null
  funding_details_espa: string | null
  funding_details_regular_budget: string | null
  auction_ref_no: string | null
  organization_vat_number: string | null
  start_date: string | null
  end_date: string | null
  no_end_date: boolean | null
  prev_reference_no?: string | null
  contract_related_ada?: string | null
}

type CityPoint = {
  lat: number
  lon: number
  name: string
  population: number | null
  capital: string | null
}

type ContractYearSummary = {
  year: number
  count: number
  amount: number
}

type MunicipalityMapSpendRpcRow = {
  municipality_key: string | null
  amount_per_100k: number | string | null
  active_previous_count: number | string | null
}

type MunicipalityFundRow = {
  municipality_key: string | null
  year?: number | string | null
  amount_eur: number | string | null
  allocation_type: string | null
  recipient_type: string | null
  source_ada: string | null
}

type MunicipalityFundingHistoryEntry = {
  year: number
  regularAmount: number
  emergencyAmount: number
  totalAmount: number
}

type TerrainTileOverlay = {
  key: string
  href: string
  x: number
  y: number
  width: number
  height: number
}

const MOBILE_BREAKPOINT = 680
const CONTRACT_CHART_VIEWBOX_WIDTH = 760
const CONTRACT_CHART_PLOT_X0 = 44
const CONTRACT_CHART_PLOT_X1 = 736
const CONTRACT_CHART_DAY_COUNT = 365

const HILLSHADE_TILESET_ID = 'hillshade'
const HILLSHADE_TILE_SIZE = 256
const HILLSHADE_MIN_ZOOM = 4
const HILLSHADE_MAX_ZOOM = 12
const HILLSHADE_OVERSAMPLE = 1.3

function reversePolygonRings(rings: number[][][]): number[][][] {
  return rings.map((ring) => [...ring].reverse())
}

function normalizePolygonWinding(geometry: GeoJSON.Polygon | GeoJSON.MultiPolygon): GeoJSON.Polygon | GeoJSON.MultiPolygon {
  const area = d3.geoArea(geometry as d3.GeoPermissibleObjects)
  if (area <= 2 * Math.PI) return geometry
  if (geometry.type === 'Polygon') {
    return {
      ...geometry,
      coordinates: reversePolygonRings(geometry.coordinates),
    }
  }
  return {
    ...geometry,
    coordinates: geometry.coordinates.map((polygon) => reversePolygonRings(polygon)),
  }
}

function cleanText(value: unknown): string | null {
  if (value == null) return null
  const text = String(value).trim()
  if (!text || text.toLowerCase() === 'nan' || text.toLowerCase() === 'none') return null
  return text
}

function toNumber(value: unknown): number | null {
  if (value == null || value === '') return null
  const number = Number(value)
  return Number.isFinite(number) ? number : null
}

function formatNumber(value: number | null, maximumFractionDigits = 0): string {
  if (value == null || Number.isNaN(value)) return '—'
  return value.toLocaleString('el-GR', { maximumFractionDigits })
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
  if (Number.isNaN(value)) return '—'
  const abs = Math.abs(value)
  if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}B €`
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M €`
  if (abs >= 1_000) return `${(value / 1_000).toFixed(1)}K €`
  return formatEur(value)
}

function formatPer100kLowerBound(value: number | null): string {
  if (value == null || Number.isNaN(value)) return '—'
  if (value <= 0) return '0 €'

  const step = value >= 100000 ? 100000 : value >= 10000 ? 10000 : 1000
  const bucket = Math.floor(value / step) * step
  const safeBucket = bucket > 0 ? bucket : step

  if (safeBucket >= 1000) {
    return `${(safeBucket / 1000).toLocaleString('el-GR', { maximumFractionDigits: 0 })}K €`
  }

  return `${safeBucket.toLocaleString('el-GR')} €`
}

function formatActivePreviousContractsSentence(currentYear: number, count: number | null): string {
  if (count == null || Number.isNaN(count)) {
    return `Εντοπίζουμε — παλαιότερες συμβάσεις που ήταν ενεργές* το ${currentYear}.`
  }
  if (count === 1) {
    return `Εντοπίζουμε 1 παλαιότερη σύμβαση που ήταν ενεργή* το ${currentYear}.`
  }
  return `Εντοπίζουμε ${count.toLocaleString('el-GR')} παλαιότερες συμβάσεις που ήταν ενεργές* το ${currentYear}.`
}

function formatStremmataFromHa(value: number | null, maximumFractionDigits = 1): string {
  if (value == null || Number.isNaN(value)) return '—'
  const stremmata = value * 10
  const unit = stremmata === 1 ? 'στρέμμα' : 'στρέμματα'
  return `${formatNumber(stremmata, maximumFractionDigits)} ${unit}`
}

function formatStremmata(value: number | null, maximumFractionDigits = 1): string {
  if (value == null || Number.isNaN(value)) return '—'
  return `${formatNumber(value, maximumFractionDigits)} στρέμματα`
}

function formatDate(value: string | null): string {
  const text = cleanText(value)
  if (!text) return '—'
  const date = new Date(text)
  if (Number.isNaN(date.getTime())) return '—'
  return new Intl.DateTimeFormat('el-GR', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
  }).format(date)
}

function toUpperEl(value: string | null): string {
  if (!value) return '—'
  return value.toLocaleUpperCase('el-GR')
}

function toSentenceCaseEl(value: string | null): string {
  const text = cleanText(value)
  if (!text) return '—'
  const lower = text.toLocaleLowerCase('el-GR')
  return lower.charAt(0).toLocaleUpperCase('el-GR') + lower.slice(1)
}

function getWorkTooltipItems(work: WorkMarker): Array<string | null> {
  return [
    work.work ? `Εργασία: ${work.work}` : null,
    work.title ? `Σύμβαση: ${work.title}` : null,
    work.contractSignedDate ? `Υπογραφή: ${formatDate(work.contractSignedDate)}` : null,
    work.referenceNumber ? `Αρ. αναφοράς: ${work.referenceNumber}` : null,
  ]
}

function getYearDays(year: number): number {
  return Math.round((Date.UTC(year + 1, 0, 1) - Date.UTC(year, 0, 1)) / 86_400_000)
}

function getDayOfYear(value: string | null): number | null {
  const text = cleanText(value)
  if (!text) return null
  const date = new Date(text)
  if (Number.isNaN(date.getTime())) return null
  const year = date.getUTCFullYear()
  return Math.floor((Date.UTC(year, date.getUTCMonth(), date.getUTCDate()) - Date.UTC(year, 0, 1)) / 86_400_000) + 1
}

function extractYear(value: string | null): number | null {
  const text = cleanText(value)
  if (!text) return null
  const date = new Date(text)
  return Number.isNaN(date.getTime()) ? null : date.getFullYear()
}

function chunk<T>(values: T[], size: number): T[][] {
  const chunks: T[][] = []
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size))
  }
  return chunks
}

function parseShape(value: unknown): GeoJSON.Geometry | null {
  if (value && typeof value === 'object' && 'type' in (value as Record<string, unknown>)) {
    const geometry = value as GeoJSON.Geometry
    if (geometry.type === 'Polygon' || geometry.type === 'MultiPolygon') {
      return normalizePolygonWinding(geometry)
    }
    return geometry
  }
  const raw = String(value ?? '').trim()
  if (!raw) return null
  try {
    const geometry = JSON.parse(raw.replace(/'/g, '"')) as GeoJSON.Geometry
    if (geometry.type === 'Polygon' || geometry.type === 'MultiPolygon') {
      return normalizePolygonWinding(geometry)
    }
    return geometry
  } catch {
    return null
  }
}

async function fetchAllPaginatedRows<T>(
  loadPage: (from: number, to: number) => unknown,
  pageSize = 1000,
): Promise<T[]> {
  const rows: T[] = []

  for (let from = 0; ; from += pageSize) {
    const { data, error } = await loadPage(from, from + pageSize - 1) as { data: T[] | null; error: unknown }
    if (error) throw error

    const page = (data ?? []) as T[]
    rows.push(...page)

    if (page.length < pageSize) break
  }

  return rows
}

function clampLatitude(lat: number): number {
  return Math.max(-85.05112878, Math.min(85.05112878, lat))
}

function worldPixelX(lon: number, zoom: number): number {
  return ((lon + 180) / 360) * HILLSHADE_TILE_SIZE * (2 ** zoom)
}

function worldPixelY(lat: number, zoom: number): number {
  const clamped = clampLatitude(lat) * Math.PI / 180
  return (
    (0.5 - Math.log((1 + Math.sin(clamped)) / (1 - Math.sin(clamped))) / (4 * Math.PI))
    * HILLSHADE_TILE_SIZE
    * (2 ** zoom)
  )
}

function tileLongitude(tileX: number, zoom: number): number {
  return (tileX / (2 ** zoom)) * 360 - 180
}

function tileLatitude(tileY: number, zoom: number): number {
  const n = Math.PI - (2 * Math.PI * tileY) / (2 ** zoom)
  return Math.atan(Math.sinh(n)) * 180 / Math.PI
}

function chooseHillshadeZoom(
  bounds: [[number, number], [number, number]],
  targetWidth: number,
  targetHeight: number,
): number {
  const [[west, south], [east, north]] = bounds
  const safeWidth = Math.max(1, targetWidth)
  const safeHeight = Math.max(1, targetHeight)

  for (let zoom = HILLSHADE_MIN_ZOOM; zoom <= HILLSHADE_MAX_ZOOM; zoom += 1) {
    const pixelWidth = Math.abs(worldPixelX(east, zoom) - worldPixelX(west, zoom))
    const pixelHeight = Math.abs(worldPixelY(south, zoom) - worldPixelY(north, zoom))
    if (pixelWidth >= safeWidth * HILLSHADE_OVERSAMPLE && pixelHeight >= safeHeight * HILLSHADE_OVERSAMPLE) {
      return zoom
    }
  }

  return HILLSHADE_MAX_ZOOM
}

function buildHillshadeTileOverlays(
  feature: d3.GeoPermissibleObjects,
  projection: d3.GeoProjection,
  frameWidth: number,
  frameHeight: number,
  apiKey: string | null,
): TerrainTileOverlay[] {
  if (!apiKey) return []

  const bounds = d3.geoBounds(feature)
  const [[west, south], [east, north]] = bounds as [[number, number], [number, number]]
  if (![west, south, east, north].every(Number.isFinite)) return []

  const zoom = chooseHillshadeZoom(bounds as [[number, number], [number, number]], frameWidth, frameHeight)
  const worldMinX = worldPixelX(west, zoom)
  const worldMaxX = worldPixelX(east, zoom)
  const worldNorthY = worldPixelY(north, zoom)
  const worldSouthY = worldPixelY(south, zoom)
  const xStart = Math.max(0, Math.floor(worldMinX / HILLSHADE_TILE_SIZE))
  const xEnd = Math.min((2 ** zoom) - 1, Math.ceil(worldMaxX / HILLSHADE_TILE_SIZE) - 1)
  const yStart = Math.max(0, Math.floor(worldNorthY / HILLSHADE_TILE_SIZE))
  const yEnd = Math.min((2 ** zoom) - 1, Math.ceil(worldSouthY / HILLSHADE_TILE_SIZE) - 1)
  const overlays: TerrainTileOverlay[] = []

  for (let tileX = xStart; tileX <= xEnd; tileX += 1) {
    for (let tileY = yStart; tileY <= yEnd; tileY += 1) {
      const westLon = tileLongitude(tileX, zoom)
      const eastLon = tileLongitude(tileX + 1, zoom)
      const northLat = tileLatitude(tileY, zoom)
      const southLat = tileLatitude(tileY + 1, zoom)
      const topLeft = projection([westLon, northLat])
      const bottomRight = projection([eastLon, southLat])

      if (!topLeft || !bottomRight) continue

      const [x0, y0] = topLeft
      const [x1, y1] = bottomRight
      if (![x0, y0, x1, y1].every(Number.isFinite)) continue

      overlays.push({
        key: `${zoom}/${tileX}/${tileY}`,
        href: `https://api.maptiler.com/tiles/${HILLSHADE_TILESET_ID}/${zoom}/${tileX}/${tileY}?key=${encodeURIComponent(apiKey)}`,
        x: Math.min(x0, x1),
        y: Math.min(y0, y1),
        width: Math.abs(x1 - x0),
        height: Math.abs(y1 - y0),
      })
    }
  }

  return overlays
}

function formatInspectionViolations(count: number | null): string {
  if (count == null) return 'δεν υπάρχουν διαθέσιμα στοιχεία για παραβάσεις'
  if (count === 0) return 'δεν διαπιστώθηκε καμία παράβαση'
  if (count === 1) return 'διαπιστώθηκε 1 παράβαση'
  return `διαπιστώθηκαν ${formatNumber(count)} παραβάσεις`
}

function formatOutcomeViolations(count: number | null): string {
  if (count == null) return 'δεν υπάρχουν διαθέσιμα στοιχεία για παραβάσεις'
  if (count === 0) return 'δεν προέκυψε καμία παράβαση'
  if (count === 1) return 'προέκυψε 1 παράβαση'
  return `προέκυψαν ${formatNumber(count)} παραβάσεις`
}

function buildPlotCleaningNarrative(profile: MunicipalityProfileRow | null): string {
  const declarations = toNumber(profile?.dilosis_katharis_plithos)
  const declarationChecks = toNumber(profile?.elegxoi_katopin_dilosis)
  const declarationViolations = toNumber(profile?.mi_symmorfosi_dilosis)
  const ownMotionChecks = toNumber(profile?.elegxoi_aytepaggelti)
  const ownMotionViolations = toNumber(profile?.mi_symmorfosi_aytepaggelti)
  const complaints = toNumber(profile?.kataggelies_plithos)
  const complaintChecks = toNumber(profile?.elegxoi_katopin_kataggelias)
  const complaintViolations = toNumber(profile?.mi_symmorfosi_kataggelias)

  if (
    declarations == null &&
    declarationChecks == null &&
    declarationViolations == null &&
    ownMotionChecks == null &&
    ownMotionViolations == null &&
    complaints == null &&
    complaintChecks == null &&
    complaintViolations == null
  ) {
    return 'Δεν υπάρχουν διαθέσιμα στοιχεία καθαρισμού οικοπέδων για το 2024.'
  }

  const declarationsSentence = declarations === 0
    ? 'Το 2024 δεν υποβλήθηκε καμία υπεύθυνη δήλωση καθαρισμού οικοπέδων.'
    : declarations === 1
      ? 'Το 2024 υποβλήθηκε 1 υπεύθυνη δήλωση καθαρισμού οικοπέδων.'
      : `Το 2024 υποβλήθηκαν ${formatNumber(declarations)} υπεύθυνες δηλώσεις καθαρισμού οικοπέδων.`

  const declarationChecksSentence = declarationChecks === 0
    ? 'Δεν προέκυψε κανένας έλεγχος κατόπιν δήλωσης.'
    : declarationChecks === 1
      ? `Από ${declarations === 1 ? 'αυτή' : 'αυτές'} προέκυψε 1 έλεγχος, στον οποίο ${formatInspectionViolations(declarationViolations)}.`
      : `Από ${declarations === 1 ? 'αυτή' : 'αυτές'} προέκυψαν ${formatNumber(declarationChecks)} έλεγχοι, στους οποίους ${formatInspectionViolations(declarationViolations)}.`

  const ownMotionSentence = ownMotionChecks === 0
    ? 'Την ίδια χρονιά δεν πραγματοποιήθηκε κανένας αυτεπάγγελτος έλεγχος.'
    : ownMotionChecks === 1
      ? `Την ίδια χρονιά πραγματοποιήθηκε 1 αυτεπάγγελτος έλεγχος, από τον οποίο ${formatOutcomeViolations(ownMotionViolations)}.`
      : `Την ίδια χρονιά πραγματοποιήθηκαν ${formatNumber(ownMotionChecks)} αυτεπάγγελτοι έλεγχοι, από τους οποίους ${formatOutcomeViolations(ownMotionViolations)}.`

  const complaintsSentence = complaints === 0
    ? 'Παράλληλα, οι αρχές δεν έλαβαν καμία καταγγελία για μη καθαρισμό οικοπέδων.'
    : complaints === 1
      ? 'Παράλληλα, οι αρχές έλαβαν 1 καταγγελία για μη καθαρισμό οικοπέδων.'
      : `Παράλληλα, οι αρχές έλαβαν ${formatNumber(complaints)} καταγγελίες για μη καθαρισμό οικοπέδων.`

  const complaintChecksSentence = complaintChecks === 0
    ? 'Δεν διενεργήθηκε κανένας σχετικός έλεγχος.'
    : complaintChecks === 1
      ? `Διενεργήθηκε 1 σχετικός έλεγχος, στον οποίο ${formatInspectionViolations(complaintViolations)}.`
      : `Διενεργήθηκαν ${formatNumber(complaintChecks)} σχετικοί έλεγχοι, στους οποίους ${formatInspectionViolations(complaintViolations)}.`

  return [
    declarationsSentence,
    declarationChecksSentence,
    ownMotionSentence,
    complaintsSentence,
    complaintChecksSentence,
  ].join(' ')
}

function dayFraction(dayOfYear: number, yearDays: number): number {
  const denom = Math.max(1, yearDays - 1)
  return Math.min(1, Math.max(0, (dayOfYear - 1) / denom))
}

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

function formatChartDayLabel(dayOfYear: number): string {
  const date = new Date(Date.UTC(2025, 0, 1))
  date.setUTCDate(dayOfYear)
  return new Intl.DateTimeFormat('el-GR', {
    day: '2-digit',
    month: 'short',
  }).format(date)
}

function buildContractStepPath(points: ContractCurvePoint[], maxValue: number): string {
  if (points.length === 0) return ''

  const xForPoint = (point: ContractCurvePoint) => (
    CONTRACT_CHART_PLOT_X0 + dayFraction(point.dayOfYear, point.yearDays) * (CONTRACT_CHART_PLOT_X1 - CONTRACT_CHART_PLOT_X0)
  )
  const yForPoint = (point: ContractCurvePoint) => 182 - (point.value / maxValue) * 160

  const firstPoint = points[0]
  let path = `M ${xForPoint(firstPoint)} ${yForPoint(firstPoint)}`

  for (let index = 1; index < points.length; index += 1) {
    const previousPoint = points[index - 1]
    const currentPoint = points[index]
    const x = xForPoint(currentPoint)
    path += ` L ${x} ${yForPoint(previousPoint)}`
    path += ` L ${x} ${yForPoint(currentPoint)}`
  }

  return path
}

function getFireSourceDisplayLabel(source: string | null): string {
  if (source === 'forest_fire') return 'Πυροσβεστική Υπηρεσία'
  if (source === 'copernicus') return 'Copernicus'
  return '—'
}

function formatCopernicusSummarySentence(
  year: number,
  count: number,
  areaHa: number,
  tense: 'current' | 'previous',
): string {
  const areaText = formatStremmataFromHa(areaHa, 1)

  if (tense === 'current') {
    if (count === 0) {
      return `Από την αρχή του ${year} μέχρι και σήμερα δεν έχει ξεσπάσει καμία δασική πυρκαγιά.`
    }
    if (count === 1) {
      return `Από την αρχή του ${year} μέχρι και σήμερα έχει ξεσπάσει 1 δασική πυρκαγιά, η οποία έχει κάψει ${areaText}.`
    }
    return `Από την αρχή του ${year} μέχρι και σήμερα έχουν ξεσπάσει ${formatNumber(count)} δασικές πυρκαγιές, οι οποίες έχουν κάψει ${areaText}.`
  }

  if (count === 0) {
    return 'Πέρυσι, δεν είχε ξεσπάσει καμία δασική πυρκαγιά.'
  }
  if (count === 1) {
    return `Πέρυσι, είχε ξεσπάσει 1 δασική πυρκαγιά που είχε κάψει ${areaText}.`
  }
  return `Πέρυσι, είχαν ξεσπάσει ${formatNumber(count)} δασικές πυρκαγιές που είχαν κάψει ${areaText}.`
}

export default function MunicipalitiesPage() {
  const currentYear = new Date().getFullYear()
  const mapTilerApiKey = cleanText(import.meta.env.VITE_MAPTILER_API_KEY)
  const years = useMemo(() => [currentYear, currentYear - 1, currentYear - 2], [currentYear])
  const [searchParams, setSearchParams] = useSearchParams()
  const selectedMunicipalityKey = cleanText(searchParams.get('municipality'))
  const [municipalities, setMunicipalities] = useState<MunicipalityListItem[]>([])
  const [municipalitiesLoading, setMunicipalitiesLoading] = useState(true)
  const [pageLoading, setPageLoading] = useState(Boolean(selectedMunicipalityKey))
  const [pageError, setPageError] = useState<string | null>(null)
  const [search, setSearch] = useState('')
  const [searchFocused, setSearchFocused] = useState(false)
  const [activeSuggestionIndex, setActiveSuggestionIndex] = useState(0)
  const [isFireYearMenuOpen, setIsFireYearMenuOpen] = useState(false)
  const [profile, setProfile] = useState<MunicipalityProfileRow | null>(null)
  const [municipalitySpendPer100k, setMunicipalitySpendPer100k] = useState<number | null>(null)
  const [municipalityActivePreviousCount, setMunicipalityActivePreviousCount] = useState<number | null>(null)
  const [municipalityKapFundingAmount, setMunicipalityKapFundingAmount] = useState<number | null>(null)
  const [nationalKapFundingAmount, setNationalKapFundingAmount] = useState<number | null>(null)
  const [municipalityKapFundingSourceAda, setMunicipalityKapFundingSourceAda] = useState<string | null>(null)
  const [municipalityKapFundingAllocationTypes, setMunicipalityKapFundingAllocationTypes] = useState<string[]>([])
  const [municipalityFundingHistory, setMunicipalityFundingHistory] = useState<MunicipalityFundingHistoryEntry[]>([])
  const [contractYearSummary, setContractYearSummary] = useState<ContractYearSummary[]>([])
  const [contractCurvePoints, setContractCurvePoints] = useState<ContractCurvePoint[]>([])
  const [contractOrganizationById, setContractOrganizationById] = useState<Record<number, string>>({})
  const [latestMunicipalityContractRows, setLatestMunicipalityContractRows] = useState<MunicipalityContractRow[]>([])
  const [latestMunicipalityContractsLoading, setLatestMunicipalityContractsLoading] = useState(false)
  const [featuredMunicipalityBeneficiaries, setFeaturedMunicipalityBeneficiaries] = useState<BeneficiaryInsightRow[]>([])
  const [featuredMunicipalityBeneficiariesLoading, setFeaturedMunicipalityBeneficiariesLoading] = useState(false)
  const [forestFireRows, setForestFireRows] = useState<ForestFireRow[]>([])
  const [copernicusRows, setCopernicusRows] = useState<CopernicusRow[]>([])
  const [workRows, setWorkRows] = useState<WorkRow[]>([])
  const [workRowsLoading, setWorkRowsLoading] = useState(false)
  const [cityPoints, setCityPoints] = useState<CityPoint[]>([])
  const [cityPointsLoading, setCityPointsLoading] = useState(true)
  const [selectedFireYear, setSelectedFireYear] = useState<number>(currentYear)
  const [fireViewMode, setFireViewMode] = useState<'points' | 'shapes'>('points')
  const [municipalityGeojson, setMunicipalityGeojson] = useState<GeoData | null>(null)
  const [municipalityGeojsonLoading, setMunicipalityGeojsonLoading] = useState(true)
  const normalizedSearch = useMemo(() => normalizeMunicipalitySearch(search), [search])
  const fireYearMenuRef = useRef<HTMLDivElement | null>(null)
  const contractChartFrameRef = useRef<HTMLDivElement | null>(null)
  const fundingChartFrameRef = useRef<HTMLDivElement | null>(null)
  const municipalityMapFrameRef = useRef<HTMLDivElement | null>(null)
  const [contractChartHover, setContractChartHover] = useState<ContractChartHoverState | null>(null)
  const [fundingChartHover, setFundingChartHover] = useState<FundingChartHoverState | null>(null)
  const [pointTooltip, setPointTooltip] = useState<MunicipalityPointTooltip | null>(null)
  const [selectedContract, setSelectedContract] = useState<ContractModalContract | null>(null)
  const [isMobileMunicipalityMap, setIsMobileMunicipalityMap] = useState(() => {
    if (typeof window === 'undefined') return false
    return window.innerWidth <= MOBILE_BREAKPOINT
  })
  const searchSuggestions = useMemo(
    () => rankMunicipalityMatches(municipalities, search).slice(0, 8),
    [municipalities, search],
  )
  const bestMunicipalityMatch = searchSuggestions[0]?.municipality ?? null
  const searchSuggestionsVisible = searchFocused && normalizedSearch.length > 0
  const selectedMunicipalityKeyNormalized = useMemo(
    () => normalizeMunicipalityKey(selectedMunicipalityKey),
    [selectedMunicipalityKey],
  )

  useEffect(() => {
    setActiveSuggestionIndex(0)
  }, [normalizedSearch, municipalities.length])

  useEffect(() => {
    if (!searchSuggestionsVisible) return
    const activeOption = document.getElementById(`municipality-search-option-${activeSuggestionIndex}`)
    activeOption?.scrollIntoView({ block: 'nearest' })
  }, [activeSuggestionIndex, searchSuggestionsVisible])

  useEffect(() => {
    if (!isFireYearMenuOpen) return

    const onPointerDown = (event: MouseEvent) => {
      if (!fireYearMenuRef.current?.contains(event.target as Node)) {
        setIsFireYearMenuOpen(false)
      }
    }

    document.addEventListener('mousedown', onPointerDown)
    return () => {
      document.removeEventListener('mousedown', onPointerDown)
    }
  }, [isFireYearMenuOpen])

  useEffect(() => {
    if (!isFireYearMenuOpen) return
    const activeOption = document.getElementById(`municipality-fire-year-option-${selectedFireYear}`)
    activeOption?.scrollIntoView({ block: 'nearest' })
  }, [isFireYearMenuOpen, selectedFireYear])

  useEffect(() => {
    if (typeof window === 'undefined') return

    const media = window.matchMedia(`(max-width: ${MOBILE_BREAKPOINT}px)`)
    const update = () => setIsMobileMunicipalityMap(media.matches)

    update()
    media.addEventListener('change', update)
    return () => media.removeEventListener('change', update)
  }, [])

  useEffect(() => {
    let cancelled = false
    const loadMunicipalities = async () => {
      setMunicipalitiesLoading(true)
      const { data, error } = await supabase
        .from('municipality_fire_protection_data')
        .select('municipality_key, dhmos, municipality_normalized_name, kpi_politikis_prostasias')
        .order('kpi_politikis_prostasias', { ascending: false })
        .limit(400)

      if (cancelled) return
      if (error) {
        setMunicipalities([])
        setMunicipalitiesLoading(false)
        return
      }

      const rows = ((data ?? []) as MunicipalityListItem[]).slice().sort((a, b) => {
        const aName = cleanText(a.dhmos) ?? cleanText(a.municipality_normalized_name) ?? a.municipality_key
        const bName = cleanText(b.dhmos) ?? cleanText(b.municipality_normalized_name) ?? b.municipality_key
        return aName.localeCompare(bName, 'el')
      })
      setMunicipalities(rows)
      setMunicipalitiesLoading(false)
    }

    loadMunicipalities()
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    if (selectedMunicipalityKey) return
    setPageLoading(false)
    setPageError(null)
    setProfile(null)
    setContractYearSummary([])
    setContractOrganizationById({})
    setLatestMunicipalityContractRows([])
    setLatestMunicipalityContractsLoading(false)
    setFeaturedMunicipalityBeneficiaries([])
    setFeaturedMunicipalityBeneficiariesLoading(false)
    setForestFireRows([])
    setCopernicusRows([])
    setWorkRows([])
  }, [selectedMunicipalityKey])

  useEffect(() => {
    let cancelled = false
    const loadMunicipalityGeojson = async () => {
      setMunicipalityGeojsonLoading(true)
      try {
        const response = await fetch(`${import.meta.env.BASE_URL}municipalities.geojson`)
        if (!response.ok) throw new Error(`municipalities.geojson failed with ${response.status}`)
        const data = (await response.json()) as GeoData
        if (!cancelled) setMunicipalityGeojson(data)
      } catch {
        if (!cancelled) setMunicipalityGeojson(null)
      } finally {
        if (!cancelled) setMunicipalityGeojsonLoading(false)
      }
    }

    loadMunicipalityGeojson()
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    let cancelled = false
    const loadCityPoints = async () => {
      setCityPointsLoading(true)
      try {
        const response = await fetch(`${import.meta.env.BASE_URL}greek_cities.json`)
        if (!response.ok) throw new Error(`greek_cities.json failed with ${response.status}`)
        const rows = (await response.json()) as Array<{
          city?: unknown
          city_el?: unknown
          lat?: unknown
          lng?: unknown
          population?: unknown
          capital?: unknown
        }>
        const points = rows.flatMap((row) => {
          const lat = toNumber(row.lat)
          const lon = toNumber(row.lng)
          if (lat == null || lon == null) return []
          if (Math.abs(lat) > 90 || Math.abs(lon) > 180) return []
          const name = cleanText(row.city_el) ?? cleanText(row.city)
          if (!name) return []
          return [{
            lat,
            lon,
            name,
            population: toNumber(row.population),
            capital: cleanText(row.capital),
          }]
        })
        if (!cancelled) setCityPoints(points)
      } catch {
        if (!cancelled) setCityPoints([])
      } finally {
        if (!cancelled) setCityPointsLoading(false)
      }
    }

    loadCityPoints()
    return () => {
      cancelled = true
    }
  }, [])

  const selectedMunicipality = useMemo(
    () => municipalities.find((municipality) => normalizeMunicipalityKey(municipality.municipality_key) === selectedMunicipalityKeyNormalized) ?? null,
    [municipalities, selectedMunicipalityKeyNormalized],
  )

  const selectedMunicipalityFeature = useMemo(() => {
    if (!municipalityGeojson || !selectedMunicipality) return null
    const normalizedName = normalizeMunicipalitySearch(
      selectedMunicipality.dhmos ?? selectedMunicipality.municipality_normalized_name,
    )

    const byCode = municipalityGeojson.features.find((feature) => {
      const featureKey = normalizeMunicipalityKey(feature.properties.municipality_code)
      return featureKey === selectedMunicipalityKeyNormalized
    })
    if (byCode) return byCode

    return municipalityGeojson.features.find((feature) => {
      const featureName = normalizeMunicipalitySearch(feature.properties.name)
      return featureName === normalizedName
    }) ?? null
  }, [municipalityGeojson, selectedMunicipality, selectedMunicipalityKeyNormalized])

  const selectedMunicipalityMap = useMemo(() => {
    if (!selectedMunicipalityFeature) return null
    const baseExtent: [[number, number], [number, number]] = [[44, 30], [468, 418]]
    const projection = d3.geoMercator().fitExtent(
      baseExtent,
      selectedMunicipalityFeature as d3.GeoPermissibleObjects,
    )
    // This is the only place that changes the polygon's rendered size.
    const scaleFactor = 1.8
    projection.scale((projection.scale() ?? 1) * scaleFactor)
    const path = d3.geoPath().projection(projection)
    const bounds = path.bounds(selectedMunicipalityFeature as d3.GeoPermissibleObjects)
    const featureWidth = bounds[1][0] - bounds[0][0]
    const featureHeight = bounds[1][1] - bounds[0][1]
    const paddingX = 24
    const paddingY = 12
    const [translateX, translateY] = projection.translate()
    projection.translate([
      translateX + (paddingX - bounds[0][0]),
      translateY + (paddingY - bounds[0][1]),
    ])
    const width = Math.max(1, Math.ceil(featureWidth + paddingX * 2))
    const height = Math.max(1, Math.ceil(featureHeight + paddingY * 2))
    return {
      d: path(selectedMunicipalityFeature as d3.GeoPermissibleObjects) ?? '',
      markerPosition: path.centroid(selectedMunicipalityFeature as d3.GeoPermissibleObjects),
      viewBox: `0 0 ${width} ${height}`,
      frameWidth: width,
      frameHeight: height,
      projection,
    }
  }, [selectedMunicipalityFeature])

  useEffect(() => {
    let cancelled = false

    if (!selectedMunicipalityKey) {
      setLatestMunicipalityContractRows([])
      setLatestMunicipalityContractsLoading(false)
      return
    }

    const loadLatestMunicipalityContracts = async () => {
      setLatestMunicipalityContractsLoading(true)

      try {
        const municipalityLabel = cleanText(profile?.dhmos)
          ?? cleanText(selectedMunicipality?.dhmos)
          ?? cleanText(selectedMunicipality?.municipality_normalized_name)
          ?? null
        const procurementRows = await fetchAllPaginatedRows<MunicipalityContractProcurementRow>(
          (from, to) => supabase
            .from('procurement')
            .select(`
              id,
              contract_signed_date,
              start_date,
              end_date,
              no_end_date,
              organization_key,
              canonical_owner_scope,
              title,
              procedure_type_value,
              diavgeia_ada,
              reference_number,
              contract_number,
              contract_budget,
              budget,
              cancelled,
              next_ref_no
            `)
            .eq('municipality_key', selectedMunicipalityKey)
            .eq('canonical_owner_scope', 'municipality')
            .order('contract_signed_date', { ascending: false })
            .order('id', { ascending: false })
            .range(from, to),
        )

        const procurementIds = procurementRows.map((row) => row.id).filter(Number.isFinite)
        const organizationKeys = Array.from(
          new Set(procurementRows.map((row) => cleanText(row.organization_key)).filter(Boolean)),
        ) as string[]

        const paymentByProcurementId = new Map<number, { amount: number | null; beneficiaries: Set<string> }>()
        for (const ids of chunk(procurementIds, 200)) {
          const { data, error } = await supabase
            .from('payment')
            .select('procurement_id, beneficiary_name, amount_without_vat')
            .in('procurement_id', ids)
          if (error) throw error
          for (const row of ((data ?? []) as MunicipalityContractPaymentRow[])) {
            const current = paymentByProcurementId.get(row.procurement_id) ?? { amount: 0, beneficiaries: new Set<string>() }
            current.amount = (current.amount ?? 0) + (toNumber(row.amount_without_vat) ?? 0)
            const beneficiaryName = cleanText(row.beneficiary_name)
            if (beneficiaryName) current.beneficiaries.add(beneficiaryName)
            paymentByProcurementId.set(row.procurement_id, current)
          }
        }

        const organizationByKey = new Map<string, MunicipalityContractOrganizationRow>()
        for (const keys of chunk(organizationKeys, 200)) {
          const { data, error } = await supabase
            .from('organization')
            .select('organization_key, organization_normalized_value, organization_value, authority_scope')
            .in('organization_key', keys)
          if (error) throw error
          for (const row of ((data ?? []) as MunicipalityContractOrganizationRow[])) {
            if (!organizationByKey.has(row.organization_key)) organizationByKey.set(row.organization_key, row)
          }
        }

        const deduped = new Map<string, MunicipalityContractRow>()
        for (const row of procurementRows) {
          if (row.cancelled) continue
          if (cleanText(row.next_ref_no)) continue
          if (!isContractActiveInYear(row, currentYear)) continue

          const organizationKey = cleanText(row.organization_key)
          const organization = organizationKey ? organizationByKey.get(organizationKey) ?? null : null
          const payment = paymentByProcurementId.get(row.id)
          const dedupeKey = cleanText(row.reference_number)
            ?? cleanText(row.diavgeia_ada)
            ?? cleanText(row.contract_number)
            ?? `${organizationKey ?? ''}|${cleanText(row.title) ?? ''}|${cleanText(row.contract_signed_date) ?? ''}`

          deduped.set(dedupeKey, {
            procurement_id: row.id,
            contract_signed_date: row.contract_signed_date,
            organization_key: organizationKey ?? null,
            organization_value: cleanText(organization?.organization_normalized_value)
              ?? cleanText(organization?.organization_value)
              ?? municipalityLabel,
            authority_scope: 'municipality',
            title: row.title,
            procedure_type_value: row.procedure_type_value,
            beneficiary_name: payment ? Array.from(payment.beneficiaries).join(' | ') || null : null,
            amount_without_vat: payment?.amount ?? null,
            diavgeia_ada: row.diavgeia_ada,
            reference_number: row.reference_number,
          })
        }

        const rows = Array.from(deduped.values())
          .sort((a, b) => {
            const dateA = cleanText(a.contract_signed_date) ?? ''
            const dateB = cleanText(b.contract_signed_date) ?? ''
            if (dateA !== dateB) return dateB.localeCompare(dateA)
            return Number(b.procurement_id) - Number(a.procurement_id)
          })
          .slice(0, 12)

        if (!cancelled) {
          setLatestMunicipalityContractRows(rows)
        }
      } catch (error) {
        if (!cancelled) {
          console.error('[MunicipalitiesPage] latest municipality contracts failed', error)
          setLatestMunicipalityContractRows([])
        }
      } finally {
        if (!cancelled) setLatestMunicipalityContractsLoading(false)
      }
    }

    loadLatestMunicipalityContracts()
    return () => {
      cancelled = true
    }
  }, [currentYear, profile, selectedMunicipality, selectedMunicipalityKey])

  useEffect(() => {
    let cancelled = false

    if (!selectedMunicipalityKey) {
      setFeaturedMunicipalityBeneficiaries([])
      setFeaturedMunicipalityBeneficiariesLoading(false)
      return
    }

    const loadFeaturedMunicipalityBeneficiaries = async () => {
      setFeaturedMunicipalityBeneficiariesLoading(true)

      try {
        const municipalityLabel = cleanText(profile?.dhmos)
          ?? cleanText(selectedMunicipality?.dhmos)
          ?? cleanText(selectedMunicipality?.municipality_normalized_name)
          ?? null
        const todayIso = new Date().toISOString().slice(0, 10)
        const procurementRows = await fetchAllPaginatedRows<MunicipalityFeaturedProcurementRow>(
          (from, to) => supabase
            .from('procurement')
            .select(`
              id,
              contract_signed_date,
              submission_at,
              organization_key,
              canonical_owner_scope,
              title,
              short_descriptions,
              procedure_type_value,
              diavgeia_ada,
              contract_related_ada,
              reference_number,
              contract_number,
              contract_budget,
              budget,
              cancelled,
              next_ref_no,
              prev_reference_no,
              assign_criteria,
              contract_type,
              award_procedure,
              units_operator,
              funding_details_cofund,
              funding_details_self_fund,
              funding_details_espa,
              funding_details_regular_budget,
              auction_ref_no,
              organization_vat_number,
              start_date,
              end_date,
              no_end_date
            `)
            .eq('municipality_key', selectedMunicipalityKey)
            .order('contract_signed_date', { ascending: false })
            .order('id', { ascending: false })
            .range(from, to),
        )

        const procurementIds = procurementRows.map((row) => row.id).filter(Number.isFinite)
        const organizationKeys = Array.from(
          new Set(procurementRows.map((row) => cleanText(row.organization_key)).filter(Boolean)),
        ) as string[]

        const paymentRows: MunicipalityContractPaymentRow[] = []
        for (const ids of chunk(procurementIds, 200)) {
          const { data, error } = await supabase
            .from('payment')
            .select('procurement_id, beneficiary_name, beneficiary_vat_number, signers, payment_ref_no, amount_without_vat, amount_with_vat')
            .in('procurement_id', ids)
          if (error) throw error
          paymentRows.push(...((data ?? []) as MunicipalityContractPaymentRow[]))
        }

        const organizationByKey = new Map<string, MunicipalityContractOrganizationRow>()
        for (const keys of chunk(organizationKeys, 200)) {
          const { data, error } = await supabase
            .from('organization')
            .select('organization_key, organization_normalized_value, organization_value, authority_scope')
            .in('organization_key', keys)
          if (error) throw error
          for (const row of ((data ?? []) as MunicipalityContractOrganizationRow[])) {
            if (!organizationByKey.has(row.organization_key)) organizationByKey.set(row.organization_key, row)
          }
        }

        const cpvRows: ProcurementCpvRow[] = []
        for (const ids of chunk(procurementIds, 200)) {
          const { data, error } = await supabase
            .from('cpv')
            .select('procurement_id, cpv_key, cpv_value')
            .in('procurement_id', ids)
          if (error) throw error
          cpvRows.push(...((data ?? []) as ProcurementCpvRow[]))
        }

        const cpvByProcurementId = new Map<number, Array<{ code: string; label: string }>>()
        for (const row of cpvRows) {
          const procurementId = Number(row.procurement_id)
          if (!Number.isFinite(procurementId)) continue
          const item = {
            code: cleanText(row.cpv_key) ?? '—',
            label: cleanText(row.cpv_value) ?? '—',
          }
          if (!cpvByProcurementId.has(procurementId)) cpvByProcurementId.set(procurementId, [])
          const items = cpvByProcurementId.get(procurementId) as Array<{ code: string; label: string }>
          if (!items.find((existing) => existing.code === item.code && existing.label === item.label)) {
            items.push(item)
          }
        }

        const paymentsByProcurementId = new Map<number, Map<string, MunicipalityContractPaymentRow>>()
        for (const row of paymentRows) {
          const procurementId = Number(row.procurement_id)
          if (!Number.isFinite(procurementId)) continue
          const beneficiaryName = cleanText(row.beneficiary_name)
          const beneficiaryVat = cleanText(row.beneficiary_vat_number)
          const beneficiaryKey = beneficiaryVat ?? beneficiaryName
          if (!beneficiaryKey) continue
          if (!paymentsByProcurementId.has(procurementId)) paymentsByProcurementId.set(procurementId, new Map())
          const beneficiaryMap = paymentsByProcurementId.get(procurementId) as Map<string, MunicipalityContractPaymentRow>
          const existing = beneficiaryMap.get(beneficiaryKey)
          if (existing) {
            existing.amount_without_vat = (existing.amount_without_vat ?? 0) + (row.amount_without_vat ?? 0)
            existing.amount_with_vat = (existing.amount_with_vat ?? 0) + (row.amount_with_vat ?? 0)
            existing.signers = existing.signers ?? row.signers ?? null
            existing.payment_ref_no = existing.payment_ref_no ?? row.payment_ref_no ?? null
          } else {
            beneficiaryMap.set(beneficiaryKey, {
              procurement_id: procurementId,
              beneficiary_name: beneficiaryName,
              beneficiary_vat_number: beneficiaryVat,
              signers: cleanText(row.signers),
              payment_ref_no: cleanText(row.payment_ref_no),
              amount_without_vat: row.amount_without_vat ?? 0,
              amount_with_vat: row.amount_with_vat ?? 0,
            })
          }
        }

        const beneficiaryGroups = new Map<string, {
          beneficiaryName: string
          beneficiaryVat: string | null
          totalAmount: number
          contractIds: Set<number>
          startDate: string | null
          endDate: string | null
          organizationTotals: Map<string, number>
          cpvCounts: Map<string, number>
          signerCounts: Map<string, number>
          relevantContracts: FeaturedRecordContract[]
        }>()

        for (const procurement of procurementRows) {
          if (procurement.cancelled) continue
          if (cleanText(procurement.next_ref_no)) continue
          if (!isContractActiveOnDate(procurement, todayIso)) continue

          const organizationKey = cleanText(procurement.organization_key)
          const organization = organizationKey ? organizationByKey.get(organizationKey) ?? null : null
          const canonicalOwnerScope = cleanText(procurement.canonical_owner_scope)
          const authorityScope = canonicalOwnerScope === 'municipality'
            ? 'municipality'
            : (cleanText(organization?.authority_scope) ?? 'other')
          if (authorityScope !== 'municipality') continue

          const organizationName = cleanText(organization?.organization_normalized_value)
            ?? cleanText(organization?.organization_value)
            ?? municipalityLabel
            ?? '—'
          const procurementId = Number(procurement.id)
          const paymentEntries = Array.from(paymentsByProcurementId.get(procurementId)?.values() ?? [])
          if (paymentEntries.length === 0) continue

          for (const payment of paymentEntries) {
            const beneficiaryName = cleanText(payment.beneficiary_name) ?? cleanText(payment.beneficiary_vat_number) ?? '—'
            const beneficiaryVat = cleanText(payment.beneficiary_vat_number)
            const beneficiaryGroupKey = beneficiaryVat ?? `name:${beneficiaryName}`
            const amountWithoutVat = Math.max(0, toNumber(payment.amount_without_vat) ?? 0)
            const amountWithVat = Math.max(0, toNumber(payment.amount_with_vat) ?? 0)
            const cpvItems = cpvByProcurementId.get(procurementId) ?? []
            const topCpv = cpvItems[0] ?? null
            const shortDescription = cleanText(procurement.short_descriptions)?.split('|').map((item) => item.trim()).filter(Boolean)[0] ?? '—'
            const contractRelatedAda = cleanText(procurement.contract_related_ada)
            const diavgeiaAda = cleanText(procurement.diavgeia_ada)

            if (!beneficiaryGroups.has(beneficiaryGroupKey)) {
              beneficiaryGroups.set(beneficiaryGroupKey, {
                beneficiaryName,
                beneficiaryVat,
                totalAmount: 0,
                contractIds: new Set<number>(),
                startDate: cleanText(procurement.start_date),
                endDate: cleanText(procurement.end_date),
                organizationTotals: new Map<string, number>(),
                cpvCounts: new Map<string, number>(),
                signerCounts: new Map<string, number>(),
                relevantContracts: [],
              })
            }

            const group = beneficiaryGroups.get(beneficiaryGroupKey) as {
              beneficiaryName: string
              beneficiaryVat: string | null
              totalAmount: number
              contractIds: Set<number>
              startDate: string | null
              endDate: string | null
              organizationTotals: Map<string, number>
              cpvCounts: Map<string, number>
              signerCounts: Map<string, number>
              relevantContracts: FeaturedRecordContract[]
            }

            group.totalAmount += amountWithoutVat
            group.contractIds.add(procurementId)

            const startDate = cleanText(procurement.start_date)
            if (startDate && (!group.startDate || startDate < group.startDate)) group.startDate = startDate
            const endDate = cleanText(procurement.end_date)
            if (endDate && (!group.endDate || endDate > group.endDate)) group.endDate = endDate

            group.organizationTotals.set(organizationName, (group.organizationTotals.get(organizationName) ?? 0) + amountWithoutVat)
            for (const cpvItem of cpvItems) {
              if (cpvItem.label !== '—') {
                group.cpvCounts.set(cpvItem.label, (group.cpvCounts.get(cpvItem.label) ?? 0) + 1)
              }
            }
            const signer = cleanText(payment.signers)
            if (signer) {
              group.signerCounts.set(signer, (group.signerCounts.get(signer) ?? 0) + 1)
            }

            group.relevantContracts.push({
              id: String(procurementId),
              who: organizationName,
              what: cleanText(procurement.title) ?? '—',
              when: formatDate(cleanText(procurement.submission_at)),
              why: toSentenceCaseEl(topCpv?.label ?? shortDescription),
              beneficiary: toUpperEl(beneficiaryName),
              contractType: cleanText(procurement.procedure_type_value) ?? '—',
              howMuch: formatEur(amountWithoutVat),
              withoutVatAmount: formatEur(amountWithoutVat),
              withVatAmount: formatEur(amountWithVat),
              referenceNumber: cleanText(procurement.reference_number) ?? '—',
              contractNumber: cleanText(procurement.contract_number) ?? '—',
              cpv: topCpv?.label ?? '—',
              cpvCode: topCpv?.code ?? '—',
              cpvItems,
              signedAt: formatDate(cleanText(procurement.contract_signed_date)),
              startDate: formatDate(cleanText(procurement.start_date)),
              endDate: formatDate(cleanText(procurement.end_date)),
              organizationVat: cleanText(procurement.organization_vat_number) ?? '—',
              beneficiaryVat: beneficiaryVat ?? '—',
              signers: cleanText(payment.signers) ?? '—',
              assignCriteria: cleanText(procurement.assign_criteria) ?? '—',
              contractKind: cleanText(procurement.contract_type) ?? '—',
              awardProcedure: cleanText(procurement.award_procedure) ?? '—',
              unitsOperator: cleanText(procurement.units_operator) ?? '—',
              fundingCofund: cleanText(procurement.funding_details_cofund) ?? '—',
              fundingSelf: cleanText(procurement.funding_details_self_fund) ?? '—',
              fundingEspa: cleanText(procurement.funding_details_espa) ?? '—',
              fundingRegular: cleanText(procurement.funding_details_regular_budget) ?? '—',
              auctionRefNo: cleanText(procurement.auction_ref_no) ?? '—',
              paymentRefNo: cleanText(payment.payment_ref_no) ?? '—',
              shortDescription,
              rawBudget: formatEur(toNumber(procurement.budget)),
              contractBudget: formatEur(toNumber(procurement.contract_budget)),
              contractRelatedAda: contractRelatedAda ?? '—',
              previousReferenceNumber: cleanText(procurement.prev_reference_no) ?? '—',
              nextReferenceNumber: cleanText(procurement.next_ref_no) ?? '—',
              documentUrl: buildDiavgeiaDocumentUrl(contractRelatedAda, diavgeiaAda),
            })
          }
        }

        const today = new Date()
        const rows = [...beneficiaryGroups.values()]
          .map<BeneficiaryInsightRow>((group) => {
            const organization = [...group.organizationTotals.entries()]
              .sort((a, b) => {
                if (b[1] !== a[1]) return b[1] - a[1]
                return a[0].localeCompare(b[0], 'el')
              })[0]?.[0] ?? '—'
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

            const relevantContracts = group.relevantContracts
              .sort((a, b) => {
                const amountDiff = (toNumber(b.withoutVatAmount.replace(/[^\d,-]/g, '').replace(/\./g, '').replace(',', '.')) ?? 0)
                  - (toNumber(a.withoutVatAmount.replace(/[^\d,-]/g, '').replace(/\./g, '').replace(',', '.')) ?? 0)
                if (amountDiff !== 0) return amountDiff
                return String(b.signedAt).localeCompare(String(a.signedAt), 'el')
              })
              .slice(0, 5)

            const startDate = group.startDate
            const endDate = group.endDate
            let duration = '—'
            let progressPct: number | null = null
            if (startDate && endDate) {
              const start = new Date(startDate)
              const end = new Date(endDate)
              if (!Number.isNaN(start.getTime()) && !Number.isNaN(end.getTime()) && end >= start) {
                const days = Math.round((end.getTime() - start.getTime()) / 86_400_000) + 1
                duration = `${days} ημέρες`
                if (end > start) {
                  if (today <= start) progressPct = 0
                  else if (today >= end) progressPct = 100
                  else progressPct = Math.max(0, Math.min(100, ((today.getTime() - start.getTime()) / (end.getTime() - start.getTime())) * 100))
                }
              }
            }

            return {
              beneficiary: toUpperEl(group.beneficiaryName),
              organization,
              totalAmount: group.totalAmount,
              contractCount: group.contractIds.size,
              cpv,
              startDate: formatDate(startDate),
              endDate: formatDate(endDate),
              duration,
              progressPct,
              signer,
              relevantContracts,
            }
          })
          .sort((a, b) => {
            if (b.totalAmount !== a.totalAmount) return b.totalAmount - a.totalAmount
            if (b.contractCount !== a.contractCount) return b.contractCount - a.contractCount
            return a.beneficiary.localeCompare(b.beneficiary, 'el')
          })
          .slice(0, 12)

        if (!cancelled) setFeaturedMunicipalityBeneficiaries(rows)
      } catch (error) {
        if (!cancelled) {
          console.error('[MunicipalitiesPage] featured municipality beneficiaries failed', error)
          setFeaturedMunicipalityBeneficiaries([])
        }
      } finally {
        if (!cancelled) setFeaturedMunicipalityBeneficiariesLoading(false)
      }
    }

    loadFeaturedMunicipalityBeneficiaries()
    return () => {
      cancelled = true
    }
  }, [currentYear, profile, selectedMunicipality, selectedMunicipalityKey])

  const selectedMunicipalityHillshadeTiles = useMemo(() => {
    if (!selectedMunicipalityFeature || !selectedMunicipalityMap) return []
    return buildHillshadeTileOverlays(
      selectedMunicipalityFeature as d3.GeoPermissibleObjects,
      selectedMunicipalityMap.projection,
      selectedMunicipalityMap.frameWidth,
      selectedMunicipalityMap.frameHeight,
      mapTilerApiKey,
    )
  }, [mapTilerApiKey, selectedMunicipalityFeature, selectedMunicipalityMap])

  const selectedMunicipalityCityPoints = useMemo(() => {
    if (!selectedMunicipalityFeature || !selectedMunicipalityMap) return []

    const insideMunicipality = cityPoints
      .filter((city) => d3.geoContains(selectedMunicipalityFeature as d3.GeoPermissibleObjects, [city.lon, city.lat]))
      .sort((a, b) => {
        const popA = a.population ?? 0
        const popB = b.population ?? 0
        if (popB !== popA) return popB - popA
        const capA = a.capital === 'primary' || a.capital === 'admin'
        const capB = b.capital === 'primary' || b.capital === 'admin'
        if (capA !== capB) return capA ? -1 : 1
        return a.name.localeCompare(b.name, 'el')
      })

    return insideMunicipality.flatMap((city, index) => {
      const projected = selectedMunicipalityMap.projection([city.lon, city.lat])
      if (!projected) return []
      const [x, y] = projected
      if (!Number.isFinite(x) || !Number.isFinite(y)) return []

      return [{
        key: `${selectedMunicipalityKey ?? 'municipality'}-city-${index}-${city.name}-${x.toFixed(2)}-${y.toFixed(2)}`,
        x,
        y,
        name: city.name,
        population: city.population,
        capital: city.capital,
        labelled: true,
      }]
    })
  }, [cityPoints, selectedMunicipalityFeature, selectedMunicipalityKey, selectedMunicipalityMap])

  useEffect(() => {
    const nextYear = Math.max(2000, Math.min(selectedFireYear, currentYear))
    if (nextYear !== selectedFireYear) {
      setSelectedFireYear(nextYear)
    }
  }, [currentYear, selectedFireYear])

  useEffect(() => {
    setIsFireYearMenuOpen(false)
    setPointTooltip(null)
    setFireViewMode('points')
  }, [selectedMunicipalityKey])

  useEffect(() => {
    setPointTooltip(null)
    setFireViewMode('points')
  }, [selectedFireYear])

  const fireYearOptions = useMemo(() => {
    const years: number[] = []
    for (let year = 2000; year <= currentYear; year += 1) {
      years.push(year)
    }
    return years
  }, [currentYear])

  const selectedFireSource = getMunicipalityFireYearSource(selectedFireYear, currentYear)
  const selectedFireSourceLabel = getFireSourceDisplayLabel(selectedFireSource)
  const selectedTerrainSourceLabel = selectedMunicipalityHillshadeTiles.length > 0 ? 'Ανάγλυφο εδάφους: MapTiler hillshade' : null

  const selectedForestFireRows = useMemo(
    () => (selectedFireYear >= 2000 && selectedFireYear <= 2024
      ? forestFireRows.filter((row) => toNumber(row.year) === selectedFireYear)
      : []),
    [forestFireRows, selectedFireYear],
  )

  const selectedCopernicusRows = useMemo(
    () => (selectedFireYear >= 2025 && selectedFireYear <= currentYear
      ? copernicusRows.filter((row) => extractYear(row.firedate) === selectedFireYear)
      : []),
    [copernicusRows, currentYear, selectedFireYear],
  )

  const forestFireMarkers = useMemo(() => {
    if (!selectedMunicipalityMap) return []

    return selectedForestFireRows.flatMap((row, index) => {
      const lat = toNumber(row.lat)
      const lon = toNumber(row.lon)
      if (lat == null || lon == null) return []

      const projected = selectedMunicipalityMap.projection([lon, lat])
      if (!projected) return []

      const [x, y] = projected
      if (!Number.isFinite(x) || !Number.isFinite(y)) return []

      const burnedAreaHa = Math.max(0, toNumber(row.burned_total_ha) ?? 0)
      const radius = Math.max(3.5, Math.min(18, 4 + Math.sqrt(burnedAreaHa) * 0.9))
      const year = toNumber(row.year)

      return [{
        key: `${selectedMunicipalityKey ?? 'municipality'}-${index}-${x.toFixed(2)}-${y.toFixed(2)}`,
        x,
        y,
        radius,
        burnedAreaHa,
        year,
        dateStart: cleanText(row.date_start),
        dateEnd: cleanText(row.date_end),
      }]
    })
  }, [selectedForestFireRows, selectedMunicipalityKey, selectedMunicipalityMap])

  const copernicusMarkers = useMemo(() => {
    if (!selectedMunicipalityMap) return []

    return selectedCopernicusRows.flatMap((row, index) => {
      const centroid = row.centroid
      let coordinates: [number, number] | null = null

      if (centroid && typeof centroid === 'object' && Array.isArray(centroid.coordinates) && centroid.coordinates.length === 2) {
        const lon = Number(centroid.coordinates[0])
        const lat = Number(centroid.coordinates[1])
        if (Number.isFinite(lat) && Number.isFinite(lon)) {
          coordinates = [lon, lat]
        }
      } else if (typeof centroid === 'string') {
        try {
          const parsed = JSON.parse(centroid) as { coordinates?: [number, number] }
          if (Array.isArray(parsed.coordinates) && parsed.coordinates.length === 2) {
            const lon = Number(parsed.coordinates[0])
            const lat = Number(parsed.coordinates[1])
            if (Number.isFinite(lat) && Number.isFinite(lon)) {
              coordinates = [lon, lat]
            }
          }
        } catch {
          coordinates = null
        }
      }

      if (!coordinates) return []

      const projected = selectedMunicipalityMap.projection(coordinates)
      if (!projected) return []

      const [x, y] = projected
      if (!Number.isFinite(x) || !Number.isFinite(y)) return []

      const areaHa = Math.max(0, toNumber(row.area_ha) ?? 0)
      const radius = Math.max(3, Math.min(14, 3.5 + Math.sqrt(areaHa) * 0.8))

      return [{
        key: `${selectedMunicipalityKey ?? 'municipality'}-copernicus-${index}-${x.toFixed(2)}-${y.toFixed(2)}`,
        x,
        y,
        radius,
        areaHa,
        date: cleanText(row.firedate),
        year: extractYear(row.firedate),
      }]
    })
  }, [selectedCopernicusRows, selectedMunicipalityKey, selectedMunicipalityMap])

  const copernicusShapes = useMemo(() => {
    if (!selectedMunicipalityMap) return []

    const path = d3.geoPath().projection(selectedMunicipalityMap.projection)

    return selectedCopernicusRows.flatMap((row, index) => {
      const shape = parseShape(row.shape)
      if (!shape || (shape.type !== 'Polygon' && shape.type !== 'MultiPolygon')) return []

      const d = path(shape as unknown as d3.GeoPermissibleObjects)
      const centroid = path.centroid(shape as unknown as d3.GeoPermissibleObjects)
      if (
        !d ||
        !Array.isArray(centroid) ||
        centroid.length !== 2 ||
        !Number.isFinite(centroid[0]) ||
        !Number.isFinite(centroid[1])
      ) {
        return []
      }

      return [{
        key: `${selectedMunicipalityKey ?? 'municipality'}-copernicus-shape-${index}`,
        d,
        x: centroid[0],
        y: centroid[1],
        areaHa: Math.max(0, toNumber(row.area_ha) ?? 0),
        date: cleanText(row.firedate),
        year: extractYear(row.firedate),
      }]
    })
  }, [selectedCopernicusRows, selectedMunicipalityKey, selectedMunicipalityMap])

  const municipalityWorkMarkers = useMemo(() => {
    if (!selectedMunicipalityFeature || !selectedMunicipalityMap) return []

    const seen = new Set<string>()

    return workRows.flatMap((row) => {
      const lat = toNumber(row.lat)
      const lon = toNumber(row.lon)
      if (lat == null || lon == null) return []
      if (!d3.geoContains(selectedMunicipalityFeature as d3.GeoPermissibleObjects, [lon, lat])) return []

      const projected = selectedMunicipalityMap.projection([lon, lat])
      if (!projected) return []

      const [x, y] = projected
      if (!Number.isFinite(x) || !Number.isFinite(y)) return []

      const key = [
        cleanText(row.id),
        lat.toFixed(6),
        lon.toFixed(6),
        cleanText(row.reference_number),
        cleanText(row.work),
        cleanText(row.point_name_canonical),
      ]
        .filter(Boolean)
        .join('|')

      if (seen.has(key)) return []
      seen.add(key)

      return [{
        key,
        x,
        y,
        procurementId: toNumber(row.procurement_id),
        organizationName: cleanText(row.organization_normalized_value),
        work: cleanText(row.work),
        pointName: cleanText(row.point_name_canonical),
        title: cleanText(row.title),
        contractSignedDate: cleanText(row.contract_signed_date),
        referenceNumber: cleanText(row.reference_number),
      }]
    })
  }, [selectedMunicipalityFeature, selectedMunicipalityMap, workRows])

  const selectedWorksLabel = workRowsLoading
    ? 'Εργασίες: φόρτωση'
    : municipalityWorkMarkers.length > 0
      ? `Εργασίες: ${formatNumber(municipalityWorkMarkers.length)}`
      : null

  const hasCopernicusShapes = copernicusShapes.length > 0
  const municipalityMapLegendItems = useMemo(() => {
    const items: Array<{ key: string; tone: 'city' | 'work' | 'fire'; label: string }> = []

    if (selectedMunicipalityCityPoints.length > 0) {
      items.push({
        key: 'city',
        tone: 'city',
        label: 'Οικισμοί',
      })
    }

    if (municipalityWorkMarkers.length > 0 || workRowsLoading) {
      items.push({
        key: 'work',
        tone: 'work',
        label: 'Εργασίες πυροπροστασίας',
      })
    }

    if (forestFireMarkers.length > 0 || copernicusMarkers.length > 0 || hasCopernicusShapes) {
      items.push({
        key: 'fire',
        tone: 'fire',
        label: 'Δασική πυρκαγιά',
      })
    }

    return items
  }, [
    copernicusMarkers.length,
    forestFireMarkers.length,
    hasCopernicusShapes,
    municipalityWorkMarkers.length,
    selectedMunicipalityCityPoints.length,
    workRowsLoading,
  ])

  const openMunicipality = (municipalityKey: string, replace = true) => {
    startTransition(() => {
      setSearchParams({ municipality: municipalityKey }, { replace })
    })
  }

  const selectMunicipality = (municipality: MunicipalityListItem) => {
    const municipalityName = cleanText(municipality.dhmos) ?? cleanText(municipality.municipality_normalized_name) ?? municipality.municipality_key
    setSearch(municipalityName ?? '')
    setSearchFocused(false)
    openMunicipality(municipality.municipality_key)
  }

  const pointerInMunicipalityMap = (
    event: ReactMouseEvent<SVGElement>,
    fallback: { x: number; y: number },
  ) => {
    const frameRect = municipalityMapFrameRef.current?.getBoundingClientRect()
    if (!frameRect) return fallback
    const x = event.clientX - frameRect.left
    const y = event.clientY - frameRect.top
    if (!Number.isFinite(x) || !Number.isFinite(y)) return fallback
    return { x, y }
  }

  const updatePointTooltip = (
    event: ReactMouseEvent<SVGElement>,
    title: string,
    items: Array<string | null>,
    options?: {
      id?: string
      fallback?: { x: number; y: number }
    },
  ) => {
    const fallback = options?.fallback ?? { x: 0, y: 0 }
    const pointer = pointerInMunicipalityMap(event, fallback)
    setPointTooltip({
      id: options?.id,
      x: pointer.x,
      y: pointer.y,
      title,
      items: items.filter((item): item is string => Boolean(item)),
    })
  }

  const togglePointTooltip = (
    event: ReactMouseEvent<SVGElement>,
    title: string,
    items: Array<string | null>,
    id: string,
    fallback: { x: number; y: number },
  ) => {
    const pointer = pointerInMunicipalityMap(event, fallback)
    const nextItems = items.filter((item): item is string => Boolean(item))
    setPointTooltip((current) => (
      current?.id === id
        ? null
        : {
            id,
            x: pointer.x,
            y: pointer.y,
            title,
            items: nextItems,
          }
    ))
  }

  const clearPointTooltip = () => {
    setPointTooltip(null)
  }

  const downloadContractPdf = async (contract: ContractModalContract) => {
    await downloadContractDocument(contract)
  }

  const openContractModal = async (procurementId: number | null, fallbackOrganizationName?: string | null) => {
    if (procurementId == null || !Number.isFinite(procurementId)) return
    const cachedCanonicalOrganizationName = cleanText(contractOrganizationById[procurementId])

    const { data: procurement } = await supabase
      .from('procurement')
      .select(`
        id,
        organization_key,
        municipality_key,
        region_key,
        canonical_owner_scope,
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
      .eq('id', procurementId)
      .limit(1)
      .maybeSingle()

    if (!procurement) return

    const [{ data: paymentRows }, { data: cpvRows }, { data: organizationRows }, { data: regionRows }] = await Promise.all([
      supabase
        .from('payment')
        .select('beneficiary_name, beneficiary_vat_number, signers, payment_ref_no, amount_without_vat, amount_with_vat')
        .eq('procurement_id', procurementId),
      supabase
        .from('cpv')
        .select('cpv_key, cpv_value')
        .eq('procurement_id', procurementId),
      supabase
        .from('organization')
        .select('organization_key, organization_normalized_value, organization_value, authority_scope')
        .eq('organization_key', String(procurement.organization_key ?? ''))
        .limit(1),
      procurement.region_key
        ? supabase
          .from('region')
          .select('region_normalized_value, region_value')
          .eq('region_key', String(procurement.region_key))
          .limit(1)
        : Promise.resolve({ data: [] }),
    ])

    const payment = summarizePaymentRows((paymentRows ?? []) as Array<{
      beneficiary_name: string | null
      beneficiary_vat_number: string | null
      signers: string | null
      payment_ref_no: string | null
      amount_without_vat: number | null
      amount_with_vat: number | null
    }>)

    const cpvItems = ((cpvRows ?? []) as Array<{ cpv_key: string | null; cpv_value: string | null }>)
      .map((row) => ({
        code: cleanText(row.cpv_key) ?? '—',
        label: cleanText(row.cpv_value) ?? '—',
      }))
      .filter((row) => row.code !== '—' || row.label !== '—')
      .reduce<Array<{ code: string; label: string }>>((acc, current) => {
        if (!acc.find((item) => item.code === current.code && item.label === current.label)) acc.push(current)
        return acc
      }, [])

    const primaryCpv = cpvItems[0] ?? null
    const organization = (organizationRows?.[0] ?? null) as {
      organization_key: string
      organization_normalized_value: string | null
      organization_value: string | null
      authority_scope: ContractAuthorityScope | null
    } | null
    const region = (regionRows?.[0] ?? null) as {
      region_normalized_value: string | null
      region_value: string | null
    } | null
    const amountWithoutVat = payment.amount_without_vat ?? null
    const contractRelatedAda = cleanText(procurement.contract_related_ada)
    const diavgeiaAda = cleanText(procurement.diavgeia_ada)
    const organizationName = cachedCanonicalOrganizationName
      ?? cleanText(organization?.organization_normalized_value)
      ?? cleanText(organization?.organization_value)
      ?? cleanText(fallbackOrganizationName)
      ?? '—'
    const who = buildContractAuthorityLabel({
      canonicalOwnerScope: cleanText(procurement.canonical_owner_scope),
      organizationScope: cleanText(organization?.authority_scope),
      organizationName,
      municipalityLabel: selectedMunicipalityLabel,
      regionLabel: cleanText(region?.region_normalized_value) ?? cleanText(region?.region_value),
    })

    setSelectedContract({
      id: String(procurement.id),
      who,
      what: cleanText(procurement.title) ?? '—',
      when: formatDate(cleanText(procurement.submission_at)),
      why: cleanText(procurement.short_descriptions)?.split('|').map((item) => item.trim()).filter(Boolean)[0] ?? primaryCpv?.label ?? '—',
      beneficiary: cleanText(payment.beneficiary_name) ?? '—',
      contractType: cleanText(procurement.procedure_type_value) ?? '—',
      howMuch: amountWithoutVat == null ? '—' : amountWithoutVat.toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 }),
      withoutVatAmount: amountWithoutVat == null ? '—' : amountWithoutVat.toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 }),
      withVatAmount: payment.amount_with_vat == null ? '—' : Number(payment.amount_with_vat).toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 }),
      referenceNumber: cleanText(procurement.reference_number) ?? '—',
      contractNumber: cleanText(procurement.contract_number) ?? '—',
      cpv: primaryCpv?.label ?? '—',
      cpvCode: primaryCpv?.code ?? '—',
      cpvItems,
      signedAt: formatDate(cleanText(procurement.contract_signed_date)),
      startDate: formatDate(cleanText(procurement.start_date)),
      endDate: formatDate(cleanText(procurement.end_date)),
      organizationVat: cleanText(procurement.organization_vat_number) ?? '—',
      beneficiaryVat: cleanText(payment.beneficiary_vat_number) ?? '—',
      signers: cleanText(payment.signers) ?? '—',
      assignCriteria: cleanText(procurement.assign_criteria) ?? '—',
      contractKind: cleanText(procurement.contract_type) ?? '—',
      awardProcedure: cleanText(procurement.award_procedure) ?? '—',
      unitsOperator: cleanText(procurement.units_operator) ?? '—',
      fundingCofund: cleanText(procurement.funding_details_cofund) ?? '—',
      fundingSelf: cleanText(procurement.funding_details_self_fund) ?? '—',
      fundingEspa: cleanText(procurement.funding_details_espa) ?? '—',
      fundingRegular: cleanText(procurement.funding_details_regular_budget) ?? '—',
      auctionRefNo: cleanText(procurement.auction_ref_no) ?? '—',
      paymentRefNo: cleanText(payment.payment_ref_no) ?? '—',
      shortDescription: cleanText(procurement.short_descriptions)?.split('|').map((item) => item.trim()).filter(Boolean)[0] ?? '—',
      rawBudget: procurement.budget == null ? '—' : Number(procurement.budget).toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 }),
      contractBudget: procurement.contract_budget == null ? '—' : Number(procurement.contract_budget).toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 }),
      contractRelatedAda: contractRelatedAda ?? '—',
      previousReferenceNumber: cleanText(procurement.prev_reference_no) ?? '—',
      nextReferenceNumber: cleanText(procurement.next_ref_no) ?? '—',
      documentUrl: buildDiavgeiaDocumentUrl(contractRelatedAda, diavgeiaAda),
    })
  }

  useEffect(() => {
    setPointTooltip(null)
  }, [selectedMunicipalityKeyNormalized, selectedFireYear, fireViewMode])

  useEffect(() => {
    if (!selectedContract) return
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setSelectedContract(null)
    }
    document.addEventListener('keydown', onKeyDown)
    return () => document.removeEventListener('keydown', onKeyDown)
  }, [selectedContract])

  useEffect(() => {
    if (!selectedMunicipalityKey) return
    let cancelled = false

    const loadProfilePage = async () => {
      setPageLoading(true)
      setPageError(null)
      setForestFireRows([])
      setCopernicusRows([])
      setContractCurvePoints([])
      setContractOrganizationById({})
      setMunicipalitySpendPer100k(null)
      setMunicipalityActivePreviousCount(null)
      setMunicipalityKapFundingAmount(null)
      setNationalKapFundingAmount(null)
      setMunicipalityKapFundingSourceAda(null)
      setMunicipalityKapFundingAllocationTypes([])
      setMunicipalityFundingHistory([])

      try {
        const municipalityOrganizationName = cleanText(selectedMunicipality?.dhmos)
          ?? cleanText(selectedMunicipality?.municipality_normalized_name)
          ?? null
        const todayIso = new Date().toISOString().slice(0, 10)

        const contractRequests = years.map(async (year) => {
          const yearStart = `${year}-01-01`
          const yearEnd = year === currentYear ? todayIso : `${year}-12-31`
          const procurementRows = await fetchAllPaginatedRows<MunicipalityContractProcurementRow>(
            (from, to) => supabase
              .from('procurement')
              .select(`
                id,
                contract_signed_date,
                organization_key,
                canonical_owner_scope,
                title,
                procedure_type_value,
                diavgeia_ada,
                reference_number,
                contract_number,
                contract_budget,
                budget,
                cancelled,
                next_ref_no
              `)
              .eq('municipality_key', selectedMunicipalityKey)
              .gte('contract_signed_date', yearStart)
              .lte('contract_signed_date', yearEnd)
              .order('contract_signed_date', { ascending: true })
              .order('id', { ascending: true })
              .range(from, to),
          )

          const organizationKeys = Array.from(
            new Set(
              procurementRows
                .map((row) => cleanText(row.organization_key))
                .filter(Boolean),
            ),
          ) as string[]

          const paymentByProcurementId = new Map<number, { amount: number | null; beneficiaries: Set<string> }>()
          const procurementIds = procurementRows.map((row) => row.id).filter(Number.isFinite)
          for (const ids of chunk(procurementIds, 200)) {
            const { data, error } = await supabase
              .from('payment')
              .select('procurement_id, beneficiary_name, amount_without_vat')
              .in('procurement_id', ids)
            if (error) throw error
            for (const row of ((data ?? []) as MunicipalityContractPaymentRow[])) {
              const current = paymentByProcurementId.get(row.procurement_id) ?? { amount: 0, beneficiaries: new Set<string>() }
              current.amount = (current.amount ?? 0) + (toNumber(row.amount_without_vat) ?? 0)
              const beneficiaryName = cleanText(row.beneficiary_name)
              if (beneficiaryName) current.beneficiaries.add(beneficiaryName)
              paymentByProcurementId.set(row.procurement_id, current)
            }
          }

          const organizationByKey = new Map<string, MunicipalityContractOrganizationRow>()
          for (const keys of chunk(organizationKeys, 200)) {
            const { data, error } = await supabase
              .from('organization')
              .select('organization_key, organization_normalized_value, organization_value, authority_scope')
              .in('organization_key', keys)
            if (error) throw error
            for (const row of ((data ?? []) as MunicipalityContractOrganizationRow[])) {
              if (!organizationByKey.has(row.organization_key)) organizationByKey.set(row.organization_key, row)
            }
          }

          const deduped = new Map<string, MunicipalityContractRow>()
          for (const row of procurementRows) {
            if (row.cancelled) continue
            if (cleanText(row.next_ref_no)) continue

            const organizationKey = cleanText(row.organization_key)
            const organization = organizationKey ? organizationByKey.get(organizationKey) ?? null : null
            const canonicalOwnerScope = cleanText(row.canonical_owner_scope)
            const authorityScope = canonicalOwnerScope === 'municipality'
              ? 'municipality'
              : (cleanText(organization?.authority_scope) ?? 'other')
            if (authorityScope !== 'municipality') continue

            const payment = paymentByProcurementId.get(row.id)
            const dedupeKey = cleanText(row.reference_number)
              ?? cleanText(row.diavgeia_ada)
              ?? cleanText(row.contract_number)
              ?? `${organizationKey ?? ''}|${cleanText(row.title) ?? ''}|${cleanText(row.contract_signed_date) ?? ''}`

            deduped.set(dedupeKey, {
              procurement_id: row.id,
              contract_signed_date: row.contract_signed_date,
              organization_key: organizationKey ?? null,
              organization_value: cleanText(organization?.organization_normalized_value)
                ?? cleanText(organization?.organization_value)
                ?? municipalityOrganizationName,
              authority_scope: authorityScope,
              title: row.title,
              procedure_type_value: row.procedure_type_value,
              beneficiary_name: payment ? Array.from(payment.beneficiaries).join(' | ') || null : null,
              amount_without_vat: payment?.amount ?? null,
              diavgeia_ada: row.diavgeia_ada,
              reference_number: row.reference_number,
            })
          }

          return {
            year,
            rows: Array.from(deduped.values()),
          }
        })

        const [
          profileResult,
          municipalityMapSpendResult,
          municipalityFundingResult,
          municipalityFundingHistoryResult,
          nextForestRows,
          nextCopernicusRows,
          ...contractResults
        ] = await Promise.all([
          supabase
            .from('municipality_fire_protection_data')
            .select(`
              municipality_key,
              dhmos,
              municipality_normalized_name,
              kpi_politikis_prostasias,
              plithismos_synolikos,
              plithismos_oreinos,
              plithismos_hmioreinos,
              plithismos_pedinos,
              ektasi_km2,
              puknotita,
              oxhmata_udrofora,
              oxhmata_purosvestika,
              sxedia_purkagies,
              dilosis_katharis_plithos,
              elegxoi_katopin_dilosis,
              mi_symmorfosi_dilosis,
              pososto_symmorfosis_dilosis,
              elegxoi_aytepaggelti,
              mi_symmorfosi_aytepaggelti,
              kataggelies_plithos,
              elegxoi_katopin_kataggelias,
              mi_symmorfosi_kataggelias,
              ektasi_vlastisis_pros_katharismo_ha,
              katharismeni_ektasi_vlastisis_ha,
              pososto_proliptikou_katharismou,
              ypoleimmata_katharismwn_t,
              dapani_puroprostasias_eur
            `)
            .eq('municipality_key', selectedMunicipalityKey)
            .single(),
          supabase.rpc('get_municipality_map_spend_per_100k', {
            p_year: currentYear,
          }),
          supabase
            .from('fund')
            .select('municipality_key, amount_eur, allocation_type, recipient_type, source_ada')
            .eq('year', currentYear)
            .eq('recipient_type', 'δήμος'),
          supabase
            .from('fund')
            .select('year, amount_eur, municipality_key, recipient_type, allocation_type')
            .eq('municipality_key', selectedMunicipalityKey)
            .eq('recipient_type', 'δήμος')
            .order('year', { ascending: true }),
          fetchAllPaginatedRows<ForestFireRow>(
            (from, to) => supabase
              .from('forest_fire')
              .select('year, burned_total_ha, lat, lon, date_start, date_end')
              .eq('municipality_key', selectedMunicipalityKey)
              .order('year', { ascending: true })
              .order('date_start', { ascending: true })
              .range(from, to),
          ),
          fetchAllPaginatedRows<CopernicusRow>(
            (from, to) => supabase
              .from('copernicus')
              .select('firedate, area_ha, centroid, shape')
              .eq('municipality_key', selectedMunicipalityKey)
              .order('firedate', { ascending: true })
              .range(from, to),
          ),
          ...contractRequests,
        ])

        if (profileResult.error) throw profileResult.error
        if (municipalityMapSpendResult.error) throw municipalityMapSpendResult.error

        const nextProfile = profileResult.data as MunicipalityProfileRow
        const municipalityMapSpendRows = (municipalityMapSpendResult.data ?? []) as MunicipalityMapSpendRpcRow[]
        const mapSpendRow = municipalityMapSpendRows.find((row) => normalizeMunicipalityKey(row.municipality_key) === selectedMunicipalityKeyNormalized) ?? null
        const municipalityFundingRows = municipalityFundingResult.error
          ? []
          : ((municipalityFundingResult.data ?? []) as MunicipalityFundRow[])
        const selectedMunicipalityFundingRows = municipalityFundingRows.filter(
          (row) => normalizeMunicipalityKey(row.municipality_key) === selectedMunicipalityKeyNormalized,
        )
        const nextMunicipalityKapFundingAmount = selectedMunicipalityFundingRows.reduce(
          (sum, row) => sum + (toNumber(row.amount_eur) ?? 0),
          0,
        )
        const nextNationalKapFundingAmount = municipalityFundingRows.reduce(
          (sum, row) => sum + (toNumber(row.amount_eur) ?? 0),
          0,
        )
        const nextMunicipalityKapFundingSourceAda = selectedMunicipalityFundingRows
          .map((row) => cleanText(row.source_ada))
          .find(Boolean) ?? null
        const nextMunicipalityKapFundingAllocationTypes = Array.from(
          new Set(
            selectedMunicipalityFundingRows
              .map((row) => cleanText(row.allocation_type))
              .filter(Boolean),
          ),
        ) as string[]
        const municipalityFundingHistoryRows = municipalityFundingHistoryResult.error
          ? []
          : ((municipalityFundingHistoryResult.data ?? []) as MunicipalityFundRow[])
        const historyByYear = new Map<number, { regularAmount: number; emergencyAmount: number }>()
        for (const row of municipalityFundingHistoryRows) {
          const year = toNumber(row.year)
          if (year == null) continue
          const amount = toNumber(row.amount_eur) ?? 0
          const allocationType = cleanText(row.allocation_type)?.toLocaleLowerCase('el-GR') ?? null
          const current = historyByYear.get(year) ?? { regularAmount: 0, emergencyAmount: 0 }
          if (allocationType === 'τακτική') current.regularAmount += amount
          else current.emergencyAmount += amount
          historyByYear.set(year, current)
        }
        const nextMunicipalityFundingHistory = [...historyByYear.entries()]
          .map(([year, amounts]) => ({
            year,
            regularAmount: amounts.regularAmount,
            emergencyAmount: amounts.emergencyAmount,
            totalAmount: amounts.regularAmount + amounts.emergencyAmount,
          }))
          .sort((a, b) => a.year - b.year)
        const yearRows = (contractResults as Array<{ year: number; rows: MunicipalityContractRow[] }>)
        const nextContractOrganizationById: Record<number, string> = {}

        for (const entry of yearRows) {
          for (const row of entry.rows) {
            const procurementId = Number(row.procurement_id)
            const organizationName = cleanText(row.organization_value)
            if (!Number.isFinite(procurementId) || !organizationName) continue
            nextContractOrganizationById[procurementId] = organizationName
          }
        }

        const contractIds = yearRows.flatMap((entry) => entry.rows.map((row) => Number(row.procurement_id))).filter(Number.isFinite)
        const cpvRows: ProcurementCpvRow[] = []
        for (const ids of chunk(contractIds, 200)) {
          const { data, error } = await supabase
            .from('cpv')
            .select('procurement_id, cpv_value')
            .in('procurement_id', ids)
          if (error) throw error
          cpvRows.push(...((data ?? []) as ProcurementCpvRow[]))
        }

        const cpvByProcurementId = new Map<number, Set<string>>()
        for (const row of cpvRows) {
          const procurementId = Number(row.procurement_id)
          const cpvLabel = cleanText(row.cpv_value)
          if (!Number.isFinite(procurementId) || !cpvLabel) continue
          if (!cpvByProcurementId.has(procurementId)) cpvByProcurementId.set(procurementId, new Set())
          cpvByProcurementId.get(procurementId)?.add(cpvLabel)
        }

        const nextContractYearSummary = yearRows
          .map((entry) => ({
            year: entry.year,
            count: entry.rows.length,
            amount: entry.rows.reduce((sum, row) => sum + (toNumber(row.amount_without_vat) ?? 0), 0),
          }))
          .sort((a, b) => b.year - a.year)

        const nextContractCurvePoints: ContractCurvePoint[] = []
        const procedureAgg = new Map<string, ProcedureBreakdownItem>()
        const cpvAggByYear = new Map<number, Map<string, { label: string; count: number; amount: number }>>()

        for (const entry of yearRows) {
          const yearDays = getYearDays(entry.year)
          const sortedRows = entry.rows
            .slice()
            .sort((a, b) => {
              const dateA = cleanText(a.contract_signed_date) ?? ''
              const dateB = cleanText(b.contract_signed_date) ?? ''
              if (dateA !== dateB) return dateA.localeCompare(dateB)
              return String(a.procurement_id).localeCompare(String(b.procurement_id))
            })

          let cumulativeAmount = 0
          for (const row of sortedRows) {
            const signedYear = extractYear(row.contract_signed_date)
            if (signedYear !== entry.year) continue
            const amount = Math.max(0, toNumber(row.amount_without_vat) ?? 0)
            const procedureLabel = cleanText(row.procedure_type_value) ?? '—'
            const dayOfYear = getDayOfYear(row.contract_signed_date)
            const procurementId = Number(row.procurement_id)

            const currentProcedure = procedureAgg.get(procedureLabel) ?? { label: procedureLabel, count: 0, amount: 0 }
            currentProcedure.count += 1
            currentProcedure.amount += amount
            procedureAgg.set(procedureLabel, currentProcedure)

            if (Number.isFinite(procurementId)) {
              if (!cpvAggByYear.has(entry.year)) cpvAggByYear.set(entry.year, new Map())
              const cpvAgg = cpvAggByYear.get(entry.year) as Map<string, { label: string; count: number; amount: number }>
              for (const cpvLabel of (cpvByProcurementId.get(procurementId) ?? new Set<string>())) {
                const currentCpv = cpvAgg.get(cpvLabel) ?? { label: cpvLabel, count: 0, amount: 0 }
                currentCpv.count += 1
                currentCpv.amount += amount
                cpvAgg.set(cpvLabel, currentCpv)
              }
            }

            if (dayOfYear == null) continue
            cumulativeAmount += amount
            nextContractCurvePoints.push({
              year: entry.year,
              dayOfYear,
              yearDays,
              value: cumulativeAmount,
            })
          }
        }

        if (!cancelled) {
          setProfile(nextProfile)
          setMunicipalitySpendPer100k(toNumber(mapSpendRow?.amount_per_100k) ?? null)
          setMunicipalityActivePreviousCount(toNumber(mapSpendRow?.active_previous_count) ?? null)
          setMunicipalityKapFundingAmount(nextMunicipalityKapFundingAmount > 0 ? nextMunicipalityKapFundingAmount : null)
          setNationalKapFundingAmount(nextNationalKapFundingAmount > 0 ? nextNationalKapFundingAmount : null)
          setMunicipalityKapFundingSourceAda(nextMunicipalityKapFundingSourceAda)
          setMunicipalityKapFundingAllocationTypes(nextMunicipalityKapFundingAllocationTypes)
          setMunicipalityFundingHistory(nextMunicipalityFundingHistory)
          setForestFireRows(nextForestRows)
          setCopernicusRows(nextCopernicusRows)
          setContractYearSummary(nextContractYearSummary)
          setContractCurvePoints(nextContractCurvePoints)
          setContractOrganizationById(nextContractOrganizationById)
          setPageLoading(false)
        }
      } catch (error) {
        if (!cancelled) {
          setPageError(error instanceof Error ? error.message : 'Η σελίδα δεν μπόρεσε να φορτώσει τα δεδομένα του δήμου.')
          setMunicipalitySpendPer100k(null)
          setMunicipalityActivePreviousCount(null)
          setMunicipalityKapFundingAmount(null)
          setNationalKapFundingAmount(null)
          setMunicipalityKapFundingSourceAda(null)
          setMunicipalityKapFundingAllocationTypes([])
          setMunicipalityFundingHistory([])
          setContractOrganizationById({})
          setForestFireRows([])
          setCopernicusRows([])
          setPageLoading(false)
        }
      }
    }

    loadProfilePage()
    return () => {
      cancelled = true
    }
  }, [currentYear, selectedMunicipality, selectedMunicipalityKey, years])

  useEffect(() => {
    if (!selectedMunicipalityFeature) {
      setWorkRows([])
      setWorkRowsLoading(false)
      return
    }

    let cancelled = false
    const bounds = d3.geoBounds(selectedMunicipalityFeature as d3.GeoPermissibleObjects)
    const [[west, south], [east, north]] = bounds as [[number, number], [number, number]]

    if (![west, south, east, north].every(Number.isFinite)) {
      setWorkRows([])
      setWorkRowsLoading(false)
      return
    }

    const loadMunicipalityWorks = async () => {
      setWorkRowsLoading(true)

      try {
        const rows = await fetchAllPaginatedRows<WorkRow>(
          (from, to) => supabase
            .from('works_enriched')
            .select('id, procurement_id, organization_normalized_value, reference_number, contract_signed_date, title, work, point_name_canonical, lat, lon')
            .not('lat', 'is', null)
            .not('lon', 'is', null)
            .gte('lat', south)
            .lte('lat', north)
            .gte('lon', west)
            .lte('lon', east)
            .order('id', { ascending: true })
            .range(from, to),
        )

        if (!cancelled) setWorkRows(rows)
      } catch (error) {
        if (!cancelled) {
          console.error('[MunicipalitiesPage] municipality works failed', error)
          setWorkRows([])
        }
      } finally {
        if (!cancelled) setWorkRowsLoading(false)
      }
    }

    loadMunicipalityWorks()
    return () => {
      cancelled = true
    }
  }, [selectedMunicipalityFeature])

  const hasSelectedMunicipality = Boolean(selectedMunicipality)
  const isEmptySelection = !selectedMunicipalityKey
  const selectedMunicipalityLabel =
    cleanText(profile?.dhmos)
    ?? cleanText(selectedMunicipality?.dhmos)
    ?? cleanText(selectedMunicipality?.municipality_normalized_name)
    ?? null
  const selectedName = hasSelectedMunicipality
    ? selectedMunicipalityLabel ?? 'Δήμος'
    : selectedMunicipalityKey
      ? 'Φόρτωση δήμου…'
      : 'Αναζήτησε Δήμο'
  const populationValue = toNumber(profile?.plithismos_synolikos)
  const areaValue = toNumber(profile?.ektasi_km2)
  const densityValue = toNumber(profile?.puknotita)
  const directContractSummary = contractYearSummary.find((entry) => entry.year === currentYear) ?? null
  const hasDirectContractsCurrentYear = (directContractSummary?.count ?? 0) > 0
  const municipalityKapFundingNote = useMemo(() => {
    const noteParts: string[] = []

    if (nationalKapFundingAmount != null) {
      noteParts.push(`Συνολική κατανομή προς τους δήμους της χώρας: ${formatEur(nationalKapFundingAmount)}.`)
    } else {
      noteParts.push(`Δεν υπάρχει ακόμη διαθέσιμη κατανομή ΚΑΠ ${currentYear} για τους δήμους της χώρας.`)
    }

    return noteParts.join(' ')
  }, [currentYear, municipalityKapFundingAllocationTypes, municipalityKapFundingSourceAda, nationalKapFundingAmount])
  const municipalityFundingChartMax = useMemo(() => {
    const values = municipalityFundingHistory.map((entry) => entry.totalAmount).filter((value) => Number.isFinite(value))
    return values.length > 0 ? Math.max(1, ...values) : 1
  }, [municipalityFundingHistory])
  const municipalityFundingChartTicks = [1, 0.5, 0]
  const municipalityFundingDelta = useMemo(() => {
    const currentEntry = municipalityFundingHistory.find((entry) => entry.year === currentYear) ?? null
    const previousEntry = municipalityFundingHistory.find((entry) => entry.year === currentYear - 1) ?? null

    if (!currentEntry || !previousEntry) return null
    if (!Number.isFinite(previousEntry.totalAmount) || previousEntry.totalAmount <= 0) return null

    const pct = ((currentEntry.totalAmount - previousEntry.totalAmount) / previousEntry.totalAmount) * 100
    if (!Number.isFinite(pct) || pct === 0) return null

    return {
      pct,
      tone: pct > 0 ? 'positive' : 'negative',
      label: `${pct > 0 ? '+' : ''}${formatNumber(pct, 0)}%`,
    } as const
  }, [currentYear, municipalityFundingHistory])
  const civicFacts = [
    {
      label: 'Πληθυσμός',
      value: populationValue != null ? `${formatNumber(populationValue)} κάτοικοι` : '—',
      note: populationValue != null
        ? `Ορεινός: ${formatNumber(toNumber(profile?.plithismos_oreinos))} | Ημιορεινός: ${formatNumber(toNumber(profile?.plithismos_hmioreinos))} | Πεδινός: ${formatNumber(toNumber(profile?.plithismos_pedinos))} κάτοικοι`
        : 'Δεν υπάρχουν διαθέσιμα πληθυσμιακά δεδομένα.',
    },
    {
      label: 'Έκταση',
      value: areaValue != null ? `${formatNumber(areaValue)} km²` : '—',
      note: 'Εμβαδόν διοικητικής ενότητας',
    },
    {
      label: 'Πυκνότητα κατοίκησης',
      value: densityValue != null ? `${formatNumber(densityValue, 1)} κάτοικοι / km²` : '—',
      note: 'Βαθμός οικιστικής κάλυψης',
    },
    {
      label: 'Βλάστηση',
      value: formatStremmata(toNumber(profile?.ektasi_vlastisis_pros_katharismo_ha), 1),
      note: 'Έκταση περιοχών με βλάστηση που πρέπει να καθαρίζεται κάθε χρόνο',
    },
    {
      label: 'Οχήματα πυροπροστασίας',
      value: `${formatNumber(toNumber(profile?.oxhmata_udrofora))} υδροφόρες · ${formatNumber(toNumber(profile?.oxhmata_purosvestika))} πυροσβεστικά`,
      note: 'Διαθέσιμος στόλος πυρόσβεσης και υποστήριξης',
    },
  ]
  const plotCleaningNarrative = buildPlotCleaningNarrative(profile)
  const copernicusNarrativeByYear = useMemo(() => {
    const summary = new Map<number, { count: number; areaHa: number }>()

    for (const row of copernicusRows) {
      const year = extractYear(row.firedate)
      if (year == null) continue
      const current = summary.get(year) ?? { count: 0, areaHa: 0 }
      current.count += 1
      current.areaHa += Math.max(0, toNumber(row.area_ha) ?? 0)
      summary.set(year, current)
    }

    return summary
  }, [copernicusRows])
  const contractChartYears = useMemo(
    () => years.slice().sort((a, b) => a - b),
    [years],
  )
  const contractChartByYear = useMemo(() => {
    const grouped = new Map<number, ContractCurvePoint[]>()
    for (const year of contractChartYears) grouped.set(year, [])
    for (const point of contractCurvePoints) {
      const arr = grouped.get(point.year)
      if (arr) arr.push(point)
    }
    for (const [year, arr] of grouped) {
      arr.sort((a, b) => a.dayOfYear - b.dayOfYear)
      if (arr.length === 0) continue

      const yearDays = arr[0]?.yearDays ?? getYearDays(year)
      const padded: ContractCurvePoint[] = []

      if (arr[0].dayOfYear > 1) {
        padded.push({
          year,
          dayOfYear: 1,
          yearDays,
          value: 0,
        })
      }

      padded.push(...arr)

      const lastPoint = padded[padded.length - 1]
      if (year < currentYear && lastPoint.dayOfYear < yearDays) {
        padded.push({
          year,
          dayOfYear: yearDays,
          yearDays,
          value: lastPoint.value,
        })
      }

      grouped.set(year, padded)
    }
    return grouped
  }, [contractChartYears, contractCurvePoints, currentYear])
  const contractChartTicks = [
    { label: '01 Ιαν', month: 1, day: 1 },
    { label: '01 Μαϊ', month: 5, day: 1 },
    { label: '01 Αυγ', month: 8, day: 1 },
    { label: '31 Δεκ', month: 12, day: 31 },
  ]
  const contractChartMax = useMemo(() => {
    const values = contractCurvePoints.map((point) => point.value).filter((value) => Number.isFinite(value))
    return Math.max(1, ...values)
  }, [contractCurvePoints])
  const contractChartTooltip = useMemo(() => {
    if (!contractChartHover) return null

    const values = contractChartYears.map((year) => {
      const points = contractChartByYear.get(year) ?? []
      let value: number | null = null
      for (const point of points) {
        if (point.dayOfYear > contractChartHover.dayOfYear) break
        value = point.value
      }
      return { year, value: value ?? 0 }
    })

    return {
      dayLabel: formatChartDayLabel(contractChartHover.dayOfYear),
      svgX: contractChartHover.svgX,
      values,
    }
  }, [contractChartByYear, contractChartHover, contractChartYears])
  const fundingChartTooltip = useMemo(() => {
    if (!fundingChartHover) return null
    const entry = municipalityFundingHistory.find((item) => item.year === fundingChartHover.year) ?? null
    if (!entry) return null

    return {
      year: entry.year,
      regularAmount: entry.regularAmount,
      emergencyAmount: entry.emergencyAmount,
      totalAmount: entry.totalAmount,
      leftPct: fundingChartHover.leftPct,
    }
  }, [fundingChartHover, municipalityFundingHistory])
  const latestMunicipalityContracts = useMemo<LatestContractCardView[]>(
    () => latestMunicipalityContractRows.map((row) => buildLatestContractCardView({
      id: String(row.procurement_id),
      organizationName: cleanText(row.organization_value) ?? cleanText(row.organization_key) ?? '—',
      authorityScope: (cleanText(row.authority_scope) ?? 'other') as AuthorityScope,
      municipalityLabel: selectedMunicipalityLabel,
      when: formatDate(row.contract_signed_date),
      what: cleanText(row.title) ?? '—',
      why: `Διαδικασία: ${cleanText(row.procedure_type_value) ?? '—'}`,
      beneficiary: cleanText(row.beneficiary_name) ?? '—',
      contractType: cleanText(row.procedure_type_value) ?? '—',
      howMuch: formatEur(toNumber(row.amount_without_vat)),
      signedAt: formatDate(row.contract_signed_date),
      documentUrl: cleanText(row.diavgeia_ada) ? `https://diavgeia.gov.gr/doc/${cleanText(row.diavgeia_ada)}` : null,
    })),
    [latestMunicipalityContractRows, selectedMunicipalityLabel],
  )
  const municipalityContractsHref = useMemo(
    () => buildContractsPageHref({ municipalityKey: selectedMunicipalityKey }),
    [selectedMunicipalityKey],
  )
  const heroNarrative = useMemo(() => {
    if (isEmptySelection) {
      return 'Πληκτρολόγησε όνομα δήμου και επίλεξε από τα αποτελέσματα για φόρτωση των δεδομένων του συγκεκριμένου δήμου.'
    }
    if (!hasSelectedMunicipality) {
      return 'Φόρτωση profile δήμου…'
    }

    const currentYearCopernicus = copernicusNarrativeByYear.get(currentYear) ?? { count: 0, areaHa: 0 }
    const previousYearCopernicus = copernicusNarrativeByYear.get(currentYear - 1) ?? { count: 0, areaHa: 0 }
    const sentences = [
      formatCopernicusSummarySentence(currentYear, currentYearCopernicus.count, currentYearCopernicus.areaHa, 'current'),
      formatCopernicusSummarySentence(currentYear - 1, previousYearCopernicus.count, previousYearCopernicus.areaHa, 'previous'),
      'Τα δεδομένα προέρχονται από το ευρωπαϊκό σύστημα Copernicus (EFFIS) και βασίζονται σε δορυφορική εκτίμηση καμένων εκτάσεων.',
    ]
    return sentences.join(' ')
  }, [copernicusNarrativeByYear, currentYear, hasSelectedMunicipality, isEmptySelection])
  const isMunicipalityProfileLoading =
    Boolean(selectedMunicipalityKey) &&
    (
      municipalitiesLoading ||
      pageLoading ||
      municipalityGeojsonLoading ||
      cityPointsLoading ||
      workRowsLoading
    )

  const openLatestMunicipalityContract = (contractId: string) => {
    const procurementId = Number(contractId)
    if (!Number.isFinite(procurementId)) return
    const fallbackOrganizationName = latestMunicipalityContracts.find((item) => item.id === contractId)?.who ?? selectedName
    void openContractModal(procurementId, fallbackOrganizationName)
  }

  const openFeaturedMunicipalityContract = (contract: FeaturedRecordContract) => {
    const procurementId = Number(contract.id)
    if (!Number.isFinite(procurementId)) return
    void openContractModal(procurementId, contract.who)
  }

  if (isMunicipalityProfileLoading) {
    return (
      <main className="municipalities-page dev-tag-anchor">
        <div className="dev-tag-stack dev-tag-stack--right">
          <ComponentTag name="MunicipalitiesPage" />
          <ComponentTag name="municipalities-page" kind="CLASS" />
        </div>
        <section className="municipality-page-note section-rule dev-tag-anchor">
          <ComponentTag name="municipality-page-note section-rule" kind="CLASS" className="component-tag--overlay" />
          <DataLoadingCard
            title="Φόρτωση profile δήμου"
            message="Ανακτώνται τα στοιχεία του δήμου και προετοιμάζεται το προφίλ του."
          />
        </section>
      </main>
    )
  }

  return (
    <main className="municipalities-page dev-tag-anchor">
      <div className="dev-tag-stack dev-tag-stack--right">
        <ComponentTag name="MunicipalitiesPage" />
        <ComponentTag name="municipalities-page" kind="CLASS" />
      </div>

      <section className="maps-controls section-rule dev-tag-anchor" aria-busy={municipalitiesLoading}>
        <div className="dev-tag-stack">
          <ComponentTag name="maps-controls section-rule" kind="CLASS" />
        </div>
        <div className="maps-controls__row dev-tag-anchor">
          <ComponentTag
            name="maps-controls__row"
            kind="CLASS"
            className="component-tag--overlay"
            style={{ left: 'auto', right: '0.45rem' }}
          />
          <label className="maps-controls__search" htmlFor="municipality-search">
            <span>Αναζήτηση δήμου</span>
            <input
              id="municipality-search"
              type="search"
              autoComplete="off"
              autoCorrect="off"
              autoCapitalize="off"
              spellCheck={false}
              value={search}
              onFocus={() => {
                setSearchFocused(true)
                setActiveSuggestionIndex(0)
              }}
              onBlur={() => setSearchFocused(false)}
              onChange={(event) => {
                setSearch(event.target.value)
                setSearchFocused(true)
                setActiveSuggestionIndex(0)
              }}
              placeholder="Όνομα δήμου"
              role="combobox"
              aria-autocomplete="list"
              aria-expanded={searchSuggestionsVisible}
              aria-controls="municipality-search-suggestions"
              aria-activedescendant={
                searchSuggestionsVisible && searchSuggestions[activeSuggestionIndex]
                  ? `municipality-search-option-${activeSuggestionIndex}`
                  : undefined
              }
              onKeyDown={(event) => {
                if (event.key === 'ArrowDown' && searchSuggestions.length > 0) {
                  event.preventDefault()
                  setSearchFocused(true)
                  setActiveSuggestionIndex((current) => Math.min(current + 1, searchSuggestions.length - 1))
                  return
                }

                if (event.key === 'ArrowUp' && searchSuggestions.length > 0) {
                  event.preventDefault()
                  setSearchFocused(true)
                  setActiveSuggestionIndex((current) => Math.max(current - 1, 0))
                  return
                }

                if (event.key === 'Escape') {
                  setSearchFocused(false)
                  return
                }

                if (event.key !== 'Enter') return

                const activeSuggestion = searchSuggestions[activeSuggestionIndex]?.municipality ?? bestMunicipalityMatch
                if (!activeSuggestion) return

                event.preventDefault()
                selectMunicipality(activeSuggestion)
              }}
            />
            {searchSuggestionsVisible && (
              <div
                id="municipality-search-suggestions"
                className="maps-search-results"
                role="listbox"
                aria-label="Δήμοι που ταιριάζουν"
              >
                {searchSuggestions.length === 0 ? (
                  <button type="button" className="maps-search-empty" disabled>
                    Δεν βρέθηκαν δήμοι
                  </button>
                ) : (
                  searchSuggestions.map((entry, index) => {
                    const isActive = index === activeSuggestionIndex
                    return (
                      <button
                        key={entry.municipality.municipality_key}
                        id={`municipality-search-option-${index}`}
                        type="button"
                        role="option"
                        aria-selected={isActive}
                        className={isActive ? 'is-active' : undefined}
                        onMouseDown={(event) => event.preventDefault()}
                        onMouseEnter={() => setActiveSuggestionIndex(index)}
                        onClick={() => selectMunicipality(entry.municipality)}
                      >
                        <small>ΔΗΜΟΣ</small>
                        <span>{entry.name}</span>
                      </button>
                    )
                  })
                )}
              </div>
            )}
          </label>
          <button
            type="button"
            className="ca-filter-btn"
            style={{ width: '6.75rem', padding: '0.45rem 0.35rem', fontSize: '0.74rem', whiteSpace: 'nowrap' }}
            onClick={() => {
              const activeSuggestion = searchSuggestions[activeSuggestionIndex]?.municipality ?? bestMunicipalityMatch
              if (!activeSuggestion) return
              selectMunicipality(activeSuggestion)
            }}
          >
            Αναζήτηση
          </button>
          <button
            type="button"
            className="ca-filter-btn ca-filter-btn--clear"
            style={{ width: '6.75rem', padding: '0.45rem 0.35rem', fontSize: '0.74rem', whiteSpace: 'nowrap' }}
            onClick={() => {
              setSearch('')
              setSearchFocused(false)
              setActiveSuggestionIndex(0)
            }}
          >
            Καθαρισμός
          </button>
        </div>
      </section>

      <section className="municipality-profile-hero section-rule dev-tag-anchor">
        <div className="dev-tag-stack dev-tag-stack--right">
          <ComponentTag name="municipality-profile-hero section-rule" kind="CLASS" />
        </div>
        <div className="municipality-profile-hero__frame dev-tag-anchor">
          <ComponentTag name="municipality-profile-hero__frame" kind="CLASS" className="component-tag--overlay" />
          <div className="municipality-profile-hero__heading dev-tag-anchor">
            <ComponentTag name="municipality-profile-hero__heading" kind="CLASS" className="component-tag--overlay" />
            <div className="municipality-profile-hero__heading-copy dev-tag-anchor">
              <ComponentTag name="municipality-profile-hero__heading-copy" kind="CLASS" className="component-tag--overlay" />
              <div className="municipality-profile-hero__eyebrow eyebrow">
                {isEmptySelection ? 'ΠΡΟΦΙΛ ΔΗΜΟΥ' : 'Προφίλ δήμου'}
              </div>
              <h1>{selectedName}</h1>
              <p>{heroNarrative || 'Επίλεξε έναν δήμο για να δεις το συγκεντρωτικό αποτύπωμα πυροπροστασίας.'}</p>
            </div>

            {!isEmptySelection && (
              <div className="municipality-profile-hero__year" aria-hidden="true">
                {currentYear}
              </div>
            )}
          </div>

          {(hasSelectedMunicipality || selectedMunicipalityKey) && (
            <div className="municipality-profile-hero__body dev-tag-anchor">
              <ComponentTag name="municipality-profile-hero__body" kind="CLASS" className="component-tag--overlay" />
              {hasSelectedMunicipality && !pageLoading ? (
              <>
                <div className="municipality-profile-hero__facts dev-tag-anchor" aria-label="Γεωγραφικά και διοικητικά στοιχεία">
                  <ComponentTag name="municipality-profile-hero__facts" kind="CLASS" className="component-tag--overlay" />
                  {civicFacts.map((fact) => (
                    <div key={fact.label} className="municipality-profile-hero__fact">
                      <span>{fact.label}</span>
                      <strong>{fact.value}</strong>
                      <p>{fact.note}</p>
                    </div>
                  ))}
                  <div className="municipality-profile-hero__fact municipality-profile-hero__fact--narrative">
                    <span>Καθαρισμός οικοπέδων</span>
                    <p>{plotCleaningNarrative}</p>
                  </div>
                  <div className="municipality-profile-hero__facts-source record-beneficiary-row__meta">
                    <span>
                      ΠΗΓΗ:{' '}
                      <a href="https://deiktesota.gov.gr/" target="_blank" rel="noreferrer">
                        deiktesota.gov.gr
                      </a>
                    </span>
                  </div>
                </div>

                <div className="municipality-profile-hero__map-area dev-tag-anchor">
                  <ComponentTag
                    name="municipality-profile-hero__map-area"
                    kind="CLASS"
                    className="component-tag--overlay"
                    style={{ left: 'auto', right: '0.45rem' }}
                  />
                  <div className="municipality-profile-hero__map-controls">
                    <div
                      ref={fireYearMenuRef}
                      className="municipality-profile-hero__year-filter"
                    >
                      <span id="municipality-fire-year-label">Έτος πυρκαγιών</span>
                      <button
                        id="municipality-fire-year"
                        type="button"
                        className="municipality-profile-hero__year-trigger"
                        aria-haspopup="listbox"
                        aria-labelledby="municipality-fire-year-label municipality-fire-year"
                        aria-expanded={isFireYearMenuOpen}
                        aria-controls="municipality-fire-year-listbox"
                        onClick={() => setIsFireYearMenuOpen((open) => !open)}
                        onKeyDown={(event) => {
                          if (event.key === 'Escape') setIsFireYearMenuOpen(false)
                          if (event.key === 'ArrowDown' || event.key === 'Enter' || event.key === ' ') {
                            event.preventDefault()
                            setIsFireYearMenuOpen(true)
                          }
                        }}
                      >
                        <span>{selectedFireYear}</span>
                      </button>
                      {isFireYearMenuOpen && (
                        <div
                          id="municipality-fire-year-listbox"
                          className="municipality-profile-hero__year-menu"
                          role="listbox"
                          aria-labelledby="municipality-fire-year-label"
                        >
                          {fireYearOptions.map((year) => (
                            <button
                              key={year}
                              id={`municipality-fire-year-option-${year}`}
                              type="button"
                              role="option"
                              aria-selected={year === selectedFireYear}
                              className={year === selectedFireYear ? 'is-active' : undefined}
                              onClick={() => {
                                setSelectedFireYear(year)
                                setIsFireYearMenuOpen(false)
                              }}
                            >
                              {year}
                            </button>
                          ))}
                        </div>
                      )}
                    </div>
                    {hasCopernicusShapes && (
                      <div className="maps-selection-panel__fire-toggle municipality-profile-hero__fire-toggle" aria-label="Τρόπος προβολής πυρκαγιών">
                        <button
                          type="button"
                          className={fireViewMode === 'points' ? 'is-active' : ''}
                          onClick={() => {
                            setPointTooltip(null)
                            setFireViewMode('points')
                          }}
                        >
                          Σημεία
                        </button>
                        <button
                          type="button"
                          className={fireViewMode === 'shapes' ? 'is-active' : ''}
                          onClick={() => {
                            setPointTooltip(null)
                            setFireViewMode('shapes')
                          }}
                        >
                          Εκτάσεις
                        </button>
                      </div>
                    )}
                  </div>
                  <div
                    ref={municipalityMapFrameRef}
                    className="municipality-profile-hero__map-frame dev-tag-anchor"
                    onClick={() => {
                      if (!isMobileMunicipalityMap) return
                      setPointTooltip(null)
                    }}
                  >
                    <ComponentTag
                      name="municipality-profile-hero__map-frame"
                      kind="CLASS"
                      className="component-tag--overlay"
                      style={{ left: 'auto', right: '0.45rem' }}
                    />
                    {selectedMunicipalityMap ? (
                      <svg
                        width={selectedMunicipalityMap.frameWidth}
                        height={selectedMunicipalityMap.frameHeight}
                        viewBox={selectedMunicipalityMap.viewBox}
                        role="img"
                        aria-label={`Περίγραμμα δήμου ${selectedName}`}
                      >
                        <defs>
                          <clipPath id={`municipality-fire-clip-${selectedMunicipalityKeyNormalized || 'selected'}`}>
                            <path d={selectedMunicipalityMap.d} />
                          </clipPath>
                          <linearGradient id="municipality-map-fill" x1="0%" y1="0%" x2="100%" y2="100%">
                            <stop offset="0%" stopColor="rgba(211, 72, 45, 0.16)" />
                            <stop offset="100%" stopColor="rgba(17, 17, 17, 0.035)" />
                          </linearGradient>
                          <radialGradient id="municipality-map-wash" cx="50%" cy="50%" r="64%">
                            <stop offset="0%" stopColor="rgba(247, 245, 238, 0.9)" />
                            <stop offset="100%" stopColor="rgba(247, 245, 238, 0)" />
                          </radialGradient>
                        </defs>
                        <circle cx="256" cy="224" r="154" fill="url(#municipality-map-wash)" />
                        <path
                          d={selectedMunicipalityMap.d}
                          fill="url(#municipality-map-fill)"
                          stroke="rgba(17, 17, 17, 0.32)"
                          strokeWidth="0.5"
                          vectorEffect="non-scaling-stroke"
                        />
	                        <g clipPath={`url(#municipality-fire-clip-${selectedMunicipalityKeyNormalized || 'selected'})`}>
                          {selectedMunicipalityHillshadeTiles.length > 0 && (
                            <g opacity="0.12">
                              {selectedMunicipalityHillshadeTiles.map((tile) => (
                                <image
                                  key={tile.key}
                                  href={tile.href}
                                  x={tile.x}
                                  y={tile.y}
                                  width={tile.width}
                                  height={tile.height}
                                  preserveAspectRatio="none"
                                  className="municipality-profile-hero__terrain-tile"
                                />
                              ))}
                            </g>
                          )}
	                          {selectedMunicipalityCityPoints.map((city) => (
	                            <g key={city.key}>
                              <circle
                                className="municipality-profile-hero__point-hitbox"
                                cx={city.x}
                                cy={city.y}
                                r={isMobileMunicipalityMap ? 12 : 7}
                                fill="transparent"
                                onMouseEnter={(event) => {
                                  if (isMobileMunicipalityMap) return
                                  updatePointTooltip(event, city.name, [
                                    city.population != null ? `Πληθυσμός: ${formatNumber(city.population)}` : null,
                                    city.capital ? `Τύπος: ${city.capital}` : null,
                                  ], {
                                    id: city.key,
                                    fallback: { x: city.x, y: city.y },
                                  })
                                }}
                                onMouseMove={(event) => {
                                  if (isMobileMunicipalityMap) return
                                  updatePointTooltip(event, city.name, [
                                    city.population != null ? `Πληθυσμός: ${formatNumber(city.population)}` : null,
                                    city.capital ? `Τύπος: ${city.capital}` : null,
                                  ], {
                                    id: city.key,
                                    fallback: { x: city.x, y: city.y },
                                  })
                                }}
                                onMouseLeave={() => {
                                  if (isMobileMunicipalityMap) return
                                  clearPointTooltip()
                                }}
                                onClick={(event) => {
                                  event.stopPropagation()
                                  togglePointTooltip(
                                    event,
                                    city.name,
                                    [
                                      city.population != null ? `Πληθυσμός: ${formatNumber(city.population)}` : null,
                                      city.capital ? `Τύπος: ${city.capital}` : null,
                                    ],
                                    city.key,
                                    { x: city.x, y: city.y },
                                  )
                                }}
                              />
                              <circle
                                className="municipality-profile-hero__point-dot"
                                cx={city.x}
                                cy={city.y}
                                r="2.8"
                                fill="rgba(54, 93, 138, 0.78)"
                                stroke="rgba(255, 252, 248, 0.82)"
                                strokeWidth="0.55"
                                vectorEffect="non-scaling-stroke"
                                pointerEvents="none"
                              >
                                <title>
                                  {[
                                    city.name,
                                    city.population != null ? `Πληθυσμός: ${formatNumber(city.population)}` : null,
                                    city.capital ? `Τύπος: ${city.capital}` : null,
                                  ]
                                    .filter(Boolean)
                                    .join(' • ')}
                                </title>
                              </circle>
	                            </g>
	                          ))}
                          {municipalityWorkMarkers.map((work) => (
                            <g key={work.key}>
                              <circle
                                className="municipality-profile-hero__point-hitbox"
                                cx={work.x}
                                cy={work.y}
                                r={isMobileMunicipalityMap ? 13 : 10}
                                fill="rgba(0, 0, 0, 0.001)"
                                onMouseEnter={(event) => {
                                  if (isMobileMunicipalityMap) return
                                  updatePointTooltip(
                                    event,
                                    work.pointName ?? work.work ?? 'Εργασία',
                                    getWorkTooltipItems(work),
                                    { id: work.key, fallback: { x: work.x, y: work.y } },
                                  )
                                }}
                                onMouseMove={(event) => {
                                  if (isMobileMunicipalityMap) return
                                  updatePointTooltip(
                                    event,
                                    work.pointName ?? work.work ?? 'Εργασία',
                                    getWorkTooltipItems(work),
                                    { id: work.key, fallback: { x: work.x, y: work.y } },
                                  )
                                }}
                                onMouseLeave={() => {
                                  if (isMobileMunicipalityMap) return
                                  clearPointTooltip()
                                }}
                                onClick={(event) => {
                                  event.stopPropagation()
                                  setPointTooltip(null)
                                  void openContractModal(work.procurementId, work.organizationName)
                                }}
                              />
                              <circle
                                className="municipality-profile-hero__point-dot"
                                cx={work.x}
                                cy={work.y}
                                r="4.1"
                                fill="#f4cf42"
                                stroke="rgba(92, 74, 0, 0.95)"
                                strokeWidth="0.9"
                                vectorEffect="non-scaling-stroke"
                                style={{ filter: 'drop-shadow(0 1px 0 rgba(17, 17, 17, 0.18))' }}
                                pointerEvents="none"
                              >
                                <title>
                                  {[
                                    work.pointName ?? work.work ?? 'Εργασία',
                                    work.work ? `Εργασία: ${work.work}` : null,
                                    work.title ? `Σύμβαση: ${work.title}` : null,
                                    work.contractSignedDate ? `Υπογραφή: ${formatDate(work.contractSignedDate)}` : null,
                                    work.referenceNumber ? `Αρ. αναφοράς: ${work.referenceNumber}` : null,
                                  ]
                                    .filter(Boolean)
                                    .join(' • ')}
                                </title>
                              </circle>
                            </g>
                          ))}
                          {forestFireMarkers.map((fire) => (
                            <g key={fire.key}>
                              <circle
                                className="municipality-profile-hero__point-hitbox"
                                cx={fire.x}
                                cy={fire.y}
                                r={Math.max(fire.radius + (isMobileMunicipalityMap ? 7 : 5), isMobileMunicipalityMap ? 13 : 9)}
                                fill="transparent"
                                onMouseEnter={(event) => {
                                  if (isMobileMunicipalityMap) return
                                  updatePointTooltip(event, 'Δασική πυρκαγιά', [
                                    fire.dateStart ? `Έναρξη: ${formatDate(fire.dateStart)}` : null,
                                    fire.dateEnd ? `Λήξη: ${formatDate(fire.dateEnd)}` : null,
                                    fire.year != null ? `Έτος: ${fire.year}` : null,
                                    `Καμένη έκταση: ${formatStremmataFromHa(fire.burnedAreaHa, 1)}`,
                                  ], {
                                    id: fire.key,
                                    fallback: { x: fire.x, y: fire.y },
                                  })
                                }}
                                onMouseMove={(event) => {
                                  if (isMobileMunicipalityMap) return
                                  updatePointTooltip(event, 'Δασική πυρκαγιά', [
                                    fire.dateStart ? `Έναρξη: ${formatDate(fire.dateStart)}` : null,
                                    fire.dateEnd ? `Λήξη: ${formatDate(fire.dateEnd)}` : null,
                                    fire.year != null ? `Έτος: ${fire.year}` : null,
                                    `Καμένη έκταση: ${formatStremmataFromHa(fire.burnedAreaHa, 1)}`,
                                  ], {
                                    id: fire.key,
                                    fallback: { x: fire.x, y: fire.y },
                                  })
                                }}
                                onMouseLeave={() => {
                                  if (isMobileMunicipalityMap) return
                                  clearPointTooltip()
                                }}
                                onClick={(event) => {
                                  event.stopPropagation()
                                  togglePointTooltip(
                                    event,
                                    'Δασική πυρκαγιά',
                                    [
                                      fire.dateStart ? `Έναρξη: ${formatDate(fire.dateStart)}` : null,
                                      fire.dateEnd ? `Λήξη: ${formatDate(fire.dateEnd)}` : null,
                                      fire.year != null ? `Έτος: ${fire.year}` : null,
                                      `Καμένη έκταση: ${formatStremmataFromHa(fire.burnedAreaHa, 1)}`,
                                    ],
                                    fire.key,
                                    { x: fire.x, y: fire.y },
                                  )
                                }}
                              />
                              <circle
                                className="municipality-profile-hero__point-dot"
                                cx={fire.x}
                                cy={fire.y}
                                r={fire.radius}
                                fill="rgba(211, 72, 45, 0.86)"
                                stroke="rgba(255, 252, 248, 0.92)"
                                strokeWidth="0.75"
                                vectorEffect="non-scaling-stroke"
                                style={{ filter: 'drop-shadow(0 1px 0 rgba(17, 17, 17, 0.18))' }}
                                pointerEvents="none"
                              >
                                <title>
                                  {[
                                    fire.dateStart ? `Έναρξη: ${formatDate(fire.dateStart)}` : null,
                                    fire.dateEnd ? `Λήξη: ${formatDate(fire.dateEnd)}` : null,
                                    fire.year != null ? `Έτος: ${fire.year}` : null,
                                    `Καμένη έκταση: ${formatStremmataFromHa(fire.burnedAreaHa, 1)}`,
                                  ]
                                    .filter(Boolean)
                                    .join(' • ')}
                                </title>
                              </circle>
                            </g>
                          ))}
                          {fireViewMode === 'shapes'
                            ? copernicusShapes.map((fire) => (
                              <g key={fire.key}>
                                <path
                                  d={fire.d}
                                  fill="#ff3b30"
                                  fillOpacity={0.8}
                                  onMouseEnter={(event) => {
                                    if (isMobileMunicipalityMap) return
                                    updatePointTooltip(event, 'Copernicus / EFFIS', [
                                      fire.date ? `Ημερομηνία: ${formatDate(fire.date)}` : null,
                                      fire.year != null ? `Έτος: ${fire.year}` : null,
                                      `Καμένη έκταση: ${formatStremmataFromHa(fire.areaHa, 1)}`,
                                    ], {
                                      id: fire.key,
                                      fallback: { x: fire.x, y: fire.y },
                                    })
                                  }}
                                  onMouseMove={(event) => {
                                    if (isMobileMunicipalityMap) return
                                    updatePointTooltip(event, 'Copernicus / EFFIS', [
                                      fire.date ? `Ημερομηνία: ${formatDate(fire.date)}` : null,
                                      fire.year != null ? `Έτος: ${fire.year}` : null,
                                      `Καμένη έκταση: ${formatStremmataFromHa(fire.areaHa, 1)}`,
                                    ], {
                                      id: fire.key,
                                      fallback: { x: fire.x, y: fire.y },
                                    })
                                  }}
                                  onMouseLeave={() => {
                                    if (isMobileMunicipalityMap) return
                                    clearPointTooltip()
                                  }}
                                  onClick={(event) => {
                                    event.stopPropagation()
                                    togglePointTooltip(
                                      event,
                                      'Copernicus / EFFIS',
                                      [
                                        fire.date ? `Ημερομηνία: ${formatDate(fire.date)}` : null,
                                        fire.year != null ? `Έτος: ${fire.year}` : null,
                                        `Καμένη έκταση: ${formatStremmataFromHa(fire.areaHa, 1)}`,
                                      ],
                                      fire.key,
                                      { x: fire.x, y: fire.y },
                                    )
                                  }}
                                />
                              </g>
                            ))
                            : copernicusMarkers.map((fire) => (
                              <g key={fire.key}>
                                <circle
                                  className="municipality-profile-hero__point-hitbox"
                                  cx={fire.x}
                                  cy={fire.y}
                                  r={Math.max(fire.radius + (isMobileMunicipalityMap ? 7 : 5), isMobileMunicipalityMap ? 13 : 9)}
                                  fill="transparent"
                                  onMouseEnter={(event) => {
                                    if (isMobileMunicipalityMap) return
                                    updatePointTooltip(event, 'Copernicus / EFFIS', [
                                      fire.date ? `Ημερομηνία: ${formatDate(fire.date)}` : null,
                                      fire.year != null ? `Έτος: ${fire.year}` : null,
                                      `Καμένη έκταση: ${formatStremmataFromHa(fire.areaHa, 1)}`,
                                    ], {
                                      id: fire.key,
                                      fallback: { x: fire.x, y: fire.y },
                                    })
                                  }}
                                  onMouseMove={(event) => {
                                    if (isMobileMunicipalityMap) return
                                    updatePointTooltip(event, 'Copernicus / EFFIS', [
                                      fire.date ? `Ημερομηνία: ${formatDate(fire.date)}` : null,
                                      fire.year != null ? `Έτος: ${fire.year}` : null,
                                      `Καμένη έκταση: ${formatStremmataFromHa(fire.areaHa, 1)}`,
                                    ], {
                                      id: fire.key,
                                      fallback: { x: fire.x, y: fire.y },
                                    })
                                  }}
                                  onMouseLeave={() => {
                                    if (isMobileMunicipalityMap) return
                                    clearPointTooltip()
                                  }}
                                  onClick={(event) => {
                                    event.stopPropagation()
                                    togglePointTooltip(
                                      event,
                                      'Copernicus / EFFIS',
                                      [
                                        fire.date ? `Ημερομηνία: ${formatDate(fire.date)}` : null,
                                        fire.year != null ? `Έτος: ${fire.year}` : null,
                                        `Καμένη έκταση: ${formatStremmataFromHa(fire.areaHa, 1)}`,
                                      ],
                                      fire.key,
                                      { x: fire.x, y: fire.y },
                                    )
                                  }}
                                />
                                <circle
                                  className="municipality-profile-hero__point-dot"
                                  cx={fire.x}
                                  cy={fire.y}
                                  r={fire.radius}
                                  fill="rgba(211, 72, 45, 0.86)"
                                  stroke="rgba(255, 252, 248, 0.92)"
                                  strokeWidth="0.75"
                                  vectorEffect="non-scaling-stroke"
                                  style={{ filter: 'drop-shadow(0 1px 0 rgba(17, 17, 17, 0.18))' }}
                                  pointerEvents="none"
                                >
                                  <title>
                                    {[
                                      fire.date ? `Ημερομηνία: ${formatDate(fire.date)}` : null,
                                      fire.year != null ? `Έτος: ${fire.year}` : null,
                                      `Καμένη έκταση: ${formatStremmataFromHa(fire.areaHa, 1)}`,
                                    ]
                                      .filter(Boolean)
                                      .join(' • ')}
                                  </title>
                                </circle>
                              </g>
                            ))}
                        </g>
                        {selectedMunicipalityCityPoints.map((city) => {
                          if (!city.labelled) return null
                          const labelPadding = 6
                          const nearRightEdge = city.x > selectedMunicipalityMap.frameWidth - 96
                          const nearTopEdge = city.y < 18
                          return (
                            <text
                              key={`${city.key}-label`}
                              x={nearRightEdge ? city.x - labelPadding : city.x + labelPadding}
                              y={nearTopEdge ? city.y + 14 : city.y - 6}
                              textAnchor={nearRightEdge ? 'end' : 'start'}
                              fill="rgba(17, 17, 17, 0.82)"
                              fontSize="10"
                              fontWeight="600"
                              letterSpacing="0.01em"
                              pointerEvents="none"
                              style={{ paintOrder: 'stroke', stroke: 'rgba(255, 252, 248, 0.86)', strokeWidth: '2.4px' }}
                            >
                              {city.name}
                            </text>
                          )
                        })}
                      </svg>
                    ) : (
                      <div className="municipality-profile-hero__map-fallback">
                        <span>Δεν υπάρχει διαθέσιμη γεωμετρία δήμου.</span>
                      </div>
                    )}
                    {pointTooltip ? (
                      <div
                        className="municipality-profile-hero__point-tooltip app-tooltip"
                        style={{
                          left: `${Math.max(12, pointTooltip.x + 14)}px`,
                          top: `${Math.max(12, pointTooltip.y - 14)}px`,
                        }}
                      >
                        <strong>{pointTooltip.title}</strong>
                        {pointTooltip.items.map((item) => (
                          <span key={item}>{item}</span>
                        ))}
                      </div>
                    ) : null}
	                  </div>
                    <div className="municipality-profile-hero__map-footer">
                      {municipalityMapLegendItems.length > 0 && (
                        <div className="municipality-profile-hero__map-legend" aria-label="Υπόμνημα σημείων χάρτη">
                          {municipalityMapLegendItems.map((item) => (
                            <div key={item.key} className="municipality-profile-hero__map-legend-item">
                              <span
                                className={`municipality-profile-hero__map-legend-swatch municipality-profile-hero__map-legend-swatch--${item.tone}`}
                                aria-hidden="true"
                              />
                              <strong>{item.label}</strong>
                            </div>
                          ))}
                        </div>
                      )}
	                    <div className="municipality-profile-hero__map-note">
	                      <span>{[selectedFireSourceLabel, selectedTerrainSourceLabel, selectedWorksLabel].filter(Boolean).join(' • ')}</span>
	                    </div>
                    </div>
	                </div>
              </>
              ) : pageLoading ? (
                <DataLoadingCard
                  className="municipality-profile-hero__loading-card"
                  message={selectedMunicipalityKey
                    ? 'Ανακτώνται τα στοιχεία του δήμου και προετοιμάζεται το προφίλ του.'
                    : 'Επιλέξτε δήμο για να φορτωθούν τα στοιχεία του.'}
                />
              ) : (
                <div className="municipality-profile-hero__empty">
                  <strong>Δεν βρέθηκε δήμος.</strong>
                  <p>Επιλέξτε έναν έγκυρο δήμο από τη λίστα για να δείτε το προφίλ του.</p>
                </div>
              )}
            </div>
          )}
        </div>
      </section>

      {pageError && (
        <section className="municipality-page-note section-rule dev-tag-anchor">
          <ComponentTag name="municipality-page-note section-rule" kind="CLASS" className="component-tag--overlay" />
          Σφάλμα φόρτωσης: {pageError}
        </section>
      )}

      {hasSelectedMunicipality && !pageLoading ? (
        <>
          <section className="municipality-profile-metrics section-rule dev-tag-anchor" aria-label="Κύριες μετρήσεις">
            <div className="dev-tag-stack dev-tag-stack--right">
              <ComponentTag name="municipality-profile-metrics section-rule" kind="CLASS" />
            </div>
            <article className="profile-metric-card profile-metric-card--ink municipality-contract-card">
              <div className="profile-metric-card__eyebrow eyebrow">Συμβάσεις {currentYear}</div>
              <div className={`profile-metric-card__value${hasDirectContractsCurrentYear ? '' : ' municipality-contract-card__empty-value'}`}>
                {hasDirectContractsCurrentYear
                  ? formatEur(directContractSummary?.amount ?? null)
                  : 'Δεν έχουν εντοπιστεί συμβάσεις'}
              </div>
              {hasDirectContractsCurrentYear ? (
                <div className="profile-metric-card__label">Συνολικό ποσό δημοσίων συμβάσεων που υπεγράφησαν το {currentYear}</div>
              ) : null}
              <p className="profile-metric-card__note">
                {`${formatActivePreviousContractsSentence(currentYear, municipalityActivePreviousCount)} Εκτιμάται πως ο δήμος έχει ξοδέψει πάνω από ${formatPer100kLowerBound(municipalitySpendPer100k)} ανά 100 χιλιάδες κατοίκους σε δαπάνες πυροπροστασίας το ${currentYear}.`}
              </p>
            </article>

            

            <article className="profile-metric-card profile-metric-card--accent municipality-contract-card municipality-contract-card--chart">
              <div className="profile-metric-card__eyebrow eyebrow">Εξέλιξη δαπανών</div>
              {contractCurvePoints.length > 0 ? (
                <>
                  <div
                    ref={contractChartFrameRef}
                    className="municipality-contract-card__chart-frame"
                    onMouseMove={(event) => {
                      const frame = contractChartFrameRef.current
                      if (!frame) return
                      const bounds = frame.getBoundingClientRect()
                      if (bounds.width <= 0) return
                      const plotLeft = bounds.left + (CONTRACT_CHART_PLOT_X0 / CONTRACT_CHART_VIEWBOX_WIDTH) * bounds.width
                      const plotWidth = ((CONTRACT_CHART_PLOT_X1 - CONTRACT_CHART_PLOT_X0) / CONTRACT_CHART_VIEWBOX_WIDTH) * bounds.width
                      if (plotWidth <= 0) return
                      const relativePlotX = Math.min(Math.max(event.clientX - plotLeft, 0), plotWidth)
                      const plotFraction = relativePlotX / plotWidth
                      const svgX = CONTRACT_CHART_PLOT_X0 + plotFraction * (CONTRACT_CHART_PLOT_X1 - CONTRACT_CHART_PLOT_X0)
                      const dayOfYear = Math.min(
                        CONTRACT_CHART_DAY_COUNT,
                        Math.max(1, Math.round(1 + plotFraction * (CONTRACT_CHART_DAY_COUNT - 1))),
                      )
                      setContractChartHover({ dayOfYear, svgX })
                    }}
                    onMouseLeave={() => setContractChartHover(null)}
                  >
                    <svg
                      viewBox={`0 0 ${CONTRACT_CHART_VIEWBOX_WIDTH} 212`}
                      preserveAspectRatio="none"
                      role="img"
                      aria-label="Σωρευτική πορεία ποσών συμβάσεων ανά έτος"
                      className="municipality-contract-card__chart"
                    >
                      {[1 / 3, 2 / 3].map((t, index) => {
                        const y = 182 - t * 160
                        return (
                          <line
                            key={`contract-grid-${index}`}
                            x1={CONTRACT_CHART_PLOT_X0}
                            y1={y}
                            x2={CONTRACT_CHART_PLOT_X1}
                            y2={y}
                            stroke="rgba(17,17,17,0.12)"
                          />
                        )
                      })}

                      {contractChartYears.map((year) => {
                        const points = contractChartByYear.get(year) ?? []
                        if (points.length === 0) return null
                        const { stroke, opacity, strokeWidth } = getChartYearStyle(year, currentYear)
                        const d = buildContractStepPath(points, contractChartMax)
                        return <path key={`contract-line-${year}`} d={d} fill="none" stroke={stroke} strokeOpacity={opacity} strokeWidth={strokeWidth} />
                      })}

                      {contractChartTooltip ? (
                        <line
                          x1={contractChartTooltip.svgX}
                          y1="22"
                          x2={contractChartTooltip.svgX}
                          y2="182"
                          stroke="rgba(17, 17, 17, 0.22)"
                          strokeDasharray="3 3"
                        />
                      ) : null}
                    </svg>
                    <div className="municipality-contract-card__y-axis" aria-hidden="true">
                      {[1, 0.5].map((t) => (
                        <span
                          key={`contract-y-label-${t}`}
                          style={{ top: `${((182 - t * 160) / 212) * 100}%` }}
                        >
                          {formatEurCompact(contractChartMax * t)}
                        </span>
                      ))}
                    </div>
                    <div className="municipality-contract-card__x-axis" aria-hidden="true">
                      {contractChartTicks.map((tick) => {
                        const dayInYear = Math.floor((Date.UTC(2025, tick.month - 1, tick.day) - Date.UTC(2025, 0, 1)) / 86_400_000) + 1
                        const x = CONTRACT_CHART_PLOT_X0 + dayFraction(dayInYear, CONTRACT_CHART_DAY_COUNT) * (CONTRACT_CHART_PLOT_X1 - CONTRACT_CHART_PLOT_X0)
                        return (
                          <span
                            key={`contract-x-label-${tick.label}`}
                            style={{ left: `${(x / CONTRACT_CHART_VIEWBOX_WIDTH) * 100}%` }}
                          >
                            {tick.label}
                          </span>
                        )
                      })}
                    </div>
                    {contractChartTooltip ? (
                      <div
                        className="municipality-contract-card__tooltip"
                        style={{ left: `${Math.min(92, Math.max(8, (contractChartTooltip.svgX / CONTRACT_CHART_VIEWBOX_WIDTH) * 100))}%` }}
                      >
                        <strong>{contractChartTooltip.dayLabel}</strong>
                        {contractChartTooltip.values.map((entry) => (
                          <span key={entry.year}>
                            {entry.year}: {formatEur(entry.value)}
                          </span>
                        ))}
                      </div>
                    ) : null}
                  </div>
                  <div className="municipality-contract-card__legend">
                    {[...contractChartYears].reverse().map((year) => {
                      const { stroke, opacity } = getChartYearStyle(year, currentYear)
                      return (
                        <span key={year}>
                          <i style={{ background: stroke, opacity }} />
                          {year}
                        </span>
                      )
                    })}
                  </div>
                </>
              ) : pageLoading ? (
                <p className="profile-metric-card__note">Φόρτωση δεδομένων…</p>
              ) : (
                <p className="profile-metric-card__note">Δεν υπάρχουν αρκετά δεδομένα συμβάσεων για καμπύλη εξέλιξης.</p>
              )}
            </article>

            <article className="profile-metric-card municipality-contract-card">
              <div className="profile-metric-card__eyebrow eyebrow">Χρηματοδότηση</div>
              <div className="profile-metric-card__value-row">
                <div className="profile-metric-card__value">{formatEur(municipalityKapFundingAmount)}</div>
                {municipalityFundingDelta ? (
                  <span className={`profile-metric-card__delta profile-metric-card__delta--${municipalityFundingDelta.tone}`}>
                    {municipalityFundingDelta.label}
                  </span>
                ) : null}
              </div>
              <div className="profile-metric-card__label">
                {`από τους Κεντρικούς Αυτοτελείς Πόρους έτους ${currentYear}, για την κάλυψη δράσεων πυροπροστασίας `}
              </div>
              <p className="profile-metric-card__note">{municipalityKapFundingNote}</p>
            </article>

            <article className="profile-metric-card municipality-contract-card municipality-contract-card--fund-chart">
              <div className="profile-metric-card__eyebrow eyebrow">Χρηματοδότηση ανά έτος</div>
              {municipalityFundingHistory.length > 0 ? (
                <>
                  <div className="municipality-funding-chart-wrap">
                    <div className="municipality-funding-chart__y-axis" aria-hidden="true">
                      {municipalityFundingChartTicks.map((tick) => (
                        <span
                          key={`municipality-funding-tick-${tick}`}
                          style={{ top: `${((1 - tick) * 7.6) + 0.15}rem` }}
                        >
                          {tick === 0 ? '0 €' : formatEurCompact(municipalityFundingChartMax * tick)}
                        </span>
                      ))}
                    </div>
                    <div
                      ref={fundingChartFrameRef}
                      className="municipality-funding-chart"
                      aria-label="Ετήσια χρηματοδότηση ΚΑΠ του δήμου"
                      onMouseLeave={() => setFundingChartHover(null)}
                    >
                      {municipalityFundingHistory.map((entry) => {
                        const isCurrentYear = entry.year === currentYear
                        const regularHeight = (entry.regularAmount / municipalityFundingChartMax) * 100
                        const emergencyHeight = (entry.emergencyAmount / municipalityFundingChartMax) * 100
                        return (
                          <div
                            key={entry.year}
                            className="municipality-funding-chart__bar-group"
                            onMouseEnter={(event) => {
                              const frame = fundingChartFrameRef.current
                              if (!frame) return
                              const frameRect = frame.getBoundingClientRect()
                              const barRect = event.currentTarget.getBoundingClientRect()
                              if (frameRect.width <= 0) return
                              const leftPct = ((barRect.left + barRect.width / 2 - frameRect.left) / frameRect.width) * 100
                              setFundingChartHover({ year: entry.year, leftPct })
                            }}
                            onMouseMove={(event) => {
                              const frame = fundingChartFrameRef.current
                              if (!frame) return
                              const frameRect = frame.getBoundingClientRect()
                              const barRect = event.currentTarget.getBoundingClientRect()
                              if (frameRect.width <= 0) return
                              const leftPct = ((barRect.left + barRect.width / 2 - frameRect.left) / frameRect.width) * 100
                              setFundingChartHover({ year: entry.year, leftPct })
                            }}
                          >
                            <div className="municipality-funding-chart__track" aria-hidden="true">
                              <div
                                className={`municipality-funding-chart__fill municipality-funding-chart__fill--regular${isCurrentYear ? ' is-current' : ''}`}
                                style={{ height: `${Math.max(entry.regularAmount > 0 ? 4 : 0, regularHeight)}%` }}
                              />
                              <div
                                className={`municipality-funding-chart__fill municipality-funding-chart__fill--emergency${isCurrentYear ? ' is-current' : ''}`}
                                style={{
                                  height: `${Math.max(entry.emergencyAmount > 0 ? 4 : 0, emergencyHeight)}%`,
                                  bottom: `${Math.max(0, regularHeight)}%`,
                                }}
                              />
                            </div>
                            <span className="municipality-funding-chart__year">{String(entry.year).slice(-2)}</span>
                          </div>
                        )
                      })}
                      {fundingChartTooltip ? (
                        <>
                          <div
                            className="municipality-funding-chart__hover-line"
                            aria-hidden="true"
                            style={{ left: `${fundingChartTooltip.leftPct}%` }}
                          />
                          <div
                            className="municipality-contract-card__tooltip municipality-funding-chart__tooltip"
                            style={{ left: `${Math.min(92, Math.max(8, fundingChartTooltip.leftPct))}%` }}
                          >
                            <strong>{fundingChartTooltip.year}</strong>
                            <span>Τακτική: {formatEur(fundingChartTooltip.regularAmount)}</span>
                            <span>Έκτακτη: {formatEur(fundingChartTooltip.emergencyAmount)}</span>
                            <span>Σύνολο: {formatEur(fundingChartTooltip.totalAmount)}</span>
                          </div>
                        </>
                      ) : null}
                    </div>
                  </div>
                  <div className="municipality-funding-chart__legend" aria-hidden="true">
                    <span>
                      <i className="municipality-funding-chart__legend-swatch municipality-funding-chart__legend-swatch--regular" />
                      Τακτική
                    </span>
                    <span>
                      <i className="municipality-funding-chart__legend-swatch municipality-funding-chart__legend-swatch--emergency" />
                      Έκτακτη
                    </span>
                  </div>
                  <p className="profile-metric-card__note">
                    {`Από ${municipalityFundingHistory[0]?.year} έως ${municipalityFundingHistory[municipalityFundingHistory.length - 1]?.year}. Μέγιστη τιμή: ${formatEurCompact(municipalityFundingChartMax)}.`}
                  </p>
                </>
              ) : (
                <p className="profile-metric-card__note">Δεν υπάρχουν διαθέσιμα ιστορικά στοιχεία χρηματοδότησης για τον επιλεγμένο δήμο.</p>
              )}
            </article>
          </section>

          <section className="municipality-contract-latest section-rule dev-tag-anchor" aria-label="Τελευταίες συμβάσεις δήμου">
            <div className="dev-tag-stack dev-tag-stack--right">
              <ComponentTag name="municipality-contract-latest section-rule" kind="CLASS" />
            </div>
            <div className="municipality-contract-latest__head dev-tag-anchor">
              <ComponentTag name="municipality-contract-latest__head" kind="CLASS" className="component-tag--overlay" />
              <span className="eyebrow">τελευταίες συμβάσεις</span>
              <p>Οι πιο πρόσφατες συμβάσεις του {selectedName} για το {currentYear}.</p>
            </div>
            <div className="municipality-contract-strip dev-tag-anchor">
              <ComponentTag
                name="municipality-contract-strip"
                kind="CLASS"
                className="component-tag--overlay"
                style={{ left: 'auto', right: '0.45rem' }}
              />
              {latestMunicipalityContractsLoading && (
                <DataLoadingCard compact message="Ανακτώνται οι τελευταίες συμβάσεις του δήμου." />
              )}
              {!latestMunicipalityContractsLoading && latestMunicipalityContracts.map((item) => (
                <LatestContractCard
                  key={item.id}
                  item={item}
                  onOpen={openLatestMunicipalityContract}
                />
              ))}
              {!latestMunicipalityContractsLoading && latestMunicipalityContracts.length === 0 && (
                <article className="wire-item">
                  <h2>Δεν βρέθηκαν συμβάσεις για τον επιλεγμένο δήμο.</h2>
                </article>
              )}
            </div>
            <Link className="news-wire__all-link" to={municipalityContractsHref}>
              Όλες οι συμβάσεις
            </Link>
          </section>

          <FeaturedRecordsSection
            sectionId="municipality-beneficiaries"
            year={String(currentYear)}
            rows={featuredMunicipalityBeneficiaries}
            loading={featuredMunicipalityBeneficiariesLoading}
            formatEur={formatEur}
            onOpenContract={openFeaturedMunicipalityContract}
            eyebrowText={`Δικαιούχοι / ενεργές* συμβάσεις / ${selectedName}`}
            title={`Κορυφαίοι ανάδοχοι ενεργών* συμβάσεων του ${selectedName}`}
            note="Οι ανάδοχοι ταξινομούνται με βάση το συνολικό ποσό των ενεργών* συμβάσεων που έχουν σήμερα με τον επιλεγμένο δήμο."
            footerNote={<>* Ως <strong>ενεργές</strong> συμβάσεις εννοούμε τις συμβάσεις που είτε υπεγράφησαν το {currentYear}, είτε υπεγράφησαν πριν το {currentYear} αλλά είχαν ρητή ημερομηνία λήξης του έργου εντός του {currentYear}.</>}
            emptyMessage="Δεν βρέθηκαν δικαιούχοι ενεργών συμβάσεων για τον επιλεγμένο δήμο."
          />
        </>
      ) : null}

      {selectedContract && (
        <ContractModal
          contract={selectedContract}
          onClose={() => setSelectedContract(null)}
          onDownloadPdf={() => downloadContractPdf(selectedContract)}
        />
      )}
    </main>
  )
}
