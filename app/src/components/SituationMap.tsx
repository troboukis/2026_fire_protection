import { useCallback, useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from 'react'
import * as d3 from 'd3'
import { useNavigate } from 'react-router-dom'
import type { GeoData } from '../types'
import { isAbortError } from '../lib/isAbortError'
import { CURRENT_FIRE_HOVER_EVENT, type CurrentFireHoverDetail } from '../lib/currentFireHover'
import { loadMunicipalitiesGeojson } from '../lib/municipalitiesGeojson'
import { supabase } from '../lib/supabase'
import ComponentTag from './ComponentTag'
import DataLoadingCard from './DataLoadingCard'
import MapTilerLogo from './MapTilerLogo'

type CopernicusFirePoint = {
  id: string
  lat: number
  lon: number
  shape: GeoJSON.Geometry | null
  areaHa: number
  date: string | null
  commune: string | null
  province: string | null
  municipalityKey: string | null
}

type HoveredFireTooltip = {
  x: number
  y: number
  placement: 'above' | 'below'
  items: Array<{
    id: string
    areaHa: number
    date: string | null
    commune: string | null
    province: string | null
    municipalityKey: string | null
  }>
}

type CopernicusRow = {
  copernicus_id: number
  centroid: { coordinates?: [number, number] } | string | null
  shape: GeoJSON.Geometry | string | null
  area_ha: number | string | null
  firedate: string | null
  commune: string | null
  province: string | null
  municipality_key: string | null
}

type FirmsDetection = {
  id: string
  lat: number
  lon: number
  scanKm: number | null
  trackKm: number | null
  acquiredAt: string | null
  acquiredAtEl: string | null
  satellite: string
  instrument: string
  confidence: string | null
  frp: number | null
  municipalityKey: string | null
  municipalityName: string | null
}

type FirmsRow = {
  id: number | string
  acquired_at: string | null
  acquired_at_el: string | null
  latitude: number | string | null
  longitude: number | string | null
  scan: number | string | null
  track: number | string | null
  satellite: string | null
  instrument: string | null
  confidence: string | null
  frp: number | string | null
  municipality_key: string | null
  municipality_normalized_value: string | null
}

type ActiveFirePoint = {
  id: string
  lat: number
  lon: number
  municipalityKey: string | null
  municipalityName: string | null
  fuelType: string | null
  startDate: string | null
  status: string | null
}

type CurrentFireRow = {
  incident_key: string
  lat: number | string | null
  lon: number | string | null
  municipality_key: string | null
  municipality_raw: string | null
  fuel_type: string | null
  start_date: string | null
  status: string | null
}

type HoveredFirmsTooltip = {
  x: number
  y: number
  placement: 'above' | 'below'
  item: FirmsDetection
}

type HoveredActiveFireTooltip = {
  x: number
  y: number
  placement: 'above' | 'below'
  item: ActiveFirePoint
}

type HoveredStackedTooltip = {
  x: number
  y: number
  placement: 'above' | 'below'
  items: Array<{
    label: string
    type: string
  }>
}

type SituationTooltipPoint = {
  id: string
  x: number
  y: number
  type: string
  label: string
  priority: number
}

type TerrainTileOverlay = {
  key: string
  href: string
  x: number
  y: number
  width: number
  height: number
}

function parseCentroid(value: { coordinates?: [number, number] } | string | null | undefined): { lon: number; lat: number } | null {
  if (value && typeof value === 'object' && Array.isArray(value.coordinates) && value.coordinates.length === 2) {
    const [lon, lat] = value.coordinates
    if (Number.isFinite(lon) && Number.isFinite(lat)) return { lon, lat }
  }
  const s = String(value ?? '').trim()
  if (!s) return null
  try {
    const parsed = JSON.parse(s) as { coordinates?: [number, number] }
    if (Array.isArray(parsed.coordinates) && parsed.coordinates.length === 2) {
      const [lon, lat] = parsed.coordinates
      if (Number.isFinite(lon) && Number.isFinite(lat)) return { lon, lat }
    }
  } catch {
    // fall through
  }
  const match = s.match(/coordinates':\s*\[([^,\]]+),\s*([^\]]+)\]/)
  if (!match) return null
  const lon = Number(match[1])
  const lat = Number(match[2])
  if (!Number.isFinite(lon) || !Number.isFinite(lat)) return null
  return { lon, lat }
}

function mapCopernicusRows(rows: CopernicusRow[]): CopernicusFirePoint[] {
  return rows
    .map((row) => {
      const centroid = parseCentroid(row.centroid)
      if (!centroid) return null
      return {
        id: String(row.copernicus_id ?? ''),
        lat: centroid.lat,
        lon: centroid.lon,
        shape: parseShape(row.shape),
        areaHa: Number(row.area_ha ?? 0) || 0,
        date: String(row.firedate ?? '').trim() || null,
        commune: String(row.commune ?? '').trim() || null,
        province: String(row.province ?? '').trim() || null,
        municipalityKey: cleanText(row.municipality_key),
      } satisfies CopernicusFirePoint
    })
    .filter((row): row is CopernicusFirePoint => row !== null)
}

function parseShape(value: GeoJSON.Geometry | string | null | undefined): GeoJSON.Geometry | null {
  if (value && typeof value === 'object' && 'type' in value) {
    const geometry = value as GeoJSON.Geometry
    if (geometry.type === 'Polygon' || geometry.type === 'MultiPolygon') {
      return normalizePolygonWinding(geometry)
    }
    return geometry
  }
  const s = String(value ?? '').trim()
  if (!s) return null
  try {
    const geometry = JSON.parse(s.replace(/'/g, '"')) as GeoJSON.Geometry
    if (geometry.type === 'Polygon' || geometry.type === 'MultiPolygon') {
      return normalizePolygonWinding(geometry)
    }
    return geometry
  } catch {
    return null
  }
}

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

function formatDateTimeEl(iso: string | null): string {
  if (!iso) return '—'
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

function formatStremmata(areaHa: number): string {
  return `${(areaHa * 10).toLocaleString('el-GR', { maximumFractionDigits: 0 })} στρ.`
}

function formatMegawatts(value: number | null): string {
  if (value == null) return '—'
  return `${value.toLocaleString('el-GR', { maximumFractionDigits: 1 })} MW`
}

function formatObservationCount(count: number): string {
  const formattedCount = count.toLocaleString('el-GR')
  return `${formattedCount} ${count === 1 ? 'παρατήρηση' : 'παρατηρήσεις'}`
}

function toDayStart(input: Date): Date {
  return new Date(input.getFullYear(), input.getMonth(), input.getDate())
}

function addDays(input: Date, days: number): Date {
  const next = new Date(input)
  next.setDate(next.getDate() + days)
  return toDayStart(next)
}

function formatFiredateBoundary(date: Date, boundary: 'start' | 'end'): string {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  const time = boundary === 'start' ? '00:00:00' : '23:59:59'
  return `${year}-${month}-${day}T${time}`
}

function formatDateInputValue(date: Date): string {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

function parseDateInputValue(value: string): Date | null {
  const match = value.match(/^(\d{4})-(\d{2})-(\d{2})$/)
  if (!match) return null
  const year = Number(match[1])
  const month = Number(match[2])
  const day = Number(match[3])
  const date = new Date(year, month - 1, day)
  if (
    !Number.isFinite(date.getTime())
    || date.getFullYear() !== year
    || date.getMonth() !== month - 1
    || date.getDate() !== day
  ) {
    return null
  }
  return toDayStart(date)
}

function diffDays(start: Date, end: Date): number {
  const msPerDay = 24 * 60 * 60 * 1000
  return Math.max(0, Math.round((toDayStart(end).getTime() - toDayStart(start).getTime()) / msPerDay))
}

const MOBILE_BREAKPOINT = 680
const DESKTOP_MAP_WIDTH = 760
const DESKTOP_MAP_HEIGHT = 520
const MOBILE_MAP_WIDTH = 760
const MOBILE_MAP_HEIGHT = 860
const DESKTOP_MAP_SCALE = 1.08
const MOBILE_MAP_SCALE = 1.22
const MOBILE_CLUSTER_GRID_SIZE = 8
const DESKTOP_CLUSTER_GRID_SIZE = 14
const HILLSHADE_TILESET_ID = 'hillshade'
const HILLSHADE_TILE_SIZE = 256
const HILLSHADE_MIN_ZOOM = 4
const HILLSHADE_MAX_ZOOM = 12
const HILLSHADE_OVERSAMPLE = 1.3
const DEFAULT_COPERNICUS_WINDOW_DAYS = 7

function cleanText(value: unknown): string | null {
  if (value == null) return null
  const text = String(value).trim()
  return text ? text : null
}

function toNumber(value: unknown): number | null {
  const number = Number(value)
  return Number.isFinite(number) ? number : null
}

function mapCurrentFireRows(rows: CurrentFireRow[]): ActiveFirePoint[] {
  return rows
    .map((row) => {
      const lat = toNumber(row.lat)
      const lon = toNumber(row.lon)
      if (lat == null || lon == null) return null
      return {
        id: String(row.incident_key),
        lat,
        lon,
        municipalityKey: cleanText(row.municipality_key),
        municipalityName: cleanText(row.municipality_raw),
        fuelType: cleanText(row.fuel_type),
        startDate: cleanText(row.start_date),
        status: cleanText(row.status),
      } satisfies ActiveFirePoint
    })
    .filter((row): row is ActiveFirePoint => row !== null)
}

function currentFiresQuery() {
  return supabase
    .from('current_fires')
    .select('incident_key, lat, lon, municipality_key, municipality_raw, fuel_type, start_date, status')
    .eq('is_current', true)
    .not('lat', 'is', null)
    .not('lon', 'is', null)
    .or('status.is.null,status.neq.ΛΗΞΗ')
    .order('status_updated_at', { ascending: false, nullsFirst: false })
}

function buildFirmsFootprint(
  projection: d3.GeoProjection,
  detection: FirmsDetection,
  transformPoint: (x: number, y: number) => { x: number; y: number },
): { x: number; y: number; width: number; height: number } | null {
  const projected = projection([detection.lon, detection.lat])
  if (!projected) return null
  const [baseX, baseY] = projected
  if (![baseX, baseY].every(Number.isFinite)) return null

  const center = transformPoint(baseX, baseY)
  const latKm = 111.32
  const lonKm = Math.max(1, latKm * Math.cos((detection.lat * Math.PI) / 180))
  const scanKm = Math.max(0.1, detection.scanKm ?? 0.38)
  const trackKm = Math.max(0.1, detection.trackKm ?? 0.38)
  const east = projection([detection.lon + (scanKm / lonKm) / 2, detection.lat])
  const west = projection([detection.lon - (scanKm / lonKm) / 2, detection.lat])
  const north = projection([detection.lon, detection.lat + (trackKm / latKm) / 2])
  const south = projection([detection.lon, detection.lat - (trackKm / latKm) / 2])

  if (!east || !west || !north || !south) return null
  const eastPoint = transformPoint(east[0], east[1])
  const westPoint = transformPoint(west[0], west[1])
  const northPoint = transformPoint(north[0], north[1])
  const southPoint = transformPoint(south[0], south[1])
  const rawWidth = Math.abs(eastPoint.x - westPoint.x)
  const rawHeight = Math.abs(northPoint.y - southPoint.y)

  return {
    x: center.x,
    y: center.y,
    width: Math.min(24, Math.max(4.5, rawWidth)),
    height: Math.min(24, Math.max(4.5, rawHeight)),
  }
}

function getClusterMunicipalityKey(items: HoveredFireTooltip['items']): string | null {
  for (const item of items) {
    const municipalityKey = cleanText(item.municipalityKey)
    if (municipalityKey) return municipalityKey
  }
  return null
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
  transformPoint?: (x: number, y: number) => { x: number; y: number },
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

      let [x0, y0] = topLeft
      let [x1, y1] = bottomRight
      if (transformPoint) {
        const transformedTopLeft = transformPoint(x0, y0)
        const transformedBottomRight = transformPoint(x1, y1)
        x0 = transformedTopLeft.x
        y0 = transformedTopLeft.y
        x1 = transformedBottomRight.x
        y1 = transformedBottomRight.y
      }
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

export default function SituationMap() {
  const navigate = useNavigate()
  const currentYear = useMemo(() => new Date().getFullYear(), [])
  const today = useMemo(() => toDayStart(new Date()), [])
  const mapTilerApiKey = useMemo(() => cleanText(import.meta.env.VITE_MAPTILER_API_KEY), [])
  const domainStart = useMemo(() => new Date(currentYear - 2, 0, 1), [currentYear])
  const defaultStart = useMemo(() => addDays(today, -(DEFAULT_COPERNICUS_WINDOW_DAYS - 1)), [today])
  const totalDays = diffDays(domainStart, today)
  const initialFetchStartDay = diffDays(domainStart, defaultStart)
  const [geojson, setGeojson] = useState<GeoData | null>(null)
  const [allFires, setAllFires] = useState<CopernicusFirePoint[]>([])
  const [firmsDetections, setFirmsDetections] = useState<FirmsDetection[]>([])
  const [activeFires, setActiveFires] = useState<ActiveFirePoint[]>([])
  const [lastUpdatedAt, setLastUpdatedAt] = useState<string | null>(null)
  const [hasLoadedHistoricalFires, setHasLoadedHistoricalFires] = useState(false)
  const [loading, setLoading] = useState(true)
  const [viewMode, setViewMode] = useState<'points' | 'shapes'>('points')
  const [rangeStartDay, setRangeStartDay] = useState(() => diffDays(domainStart, defaultStart))
  const [rangeEndDay, setRangeEndDay] = useState(() => totalDays)
  const [hoveredFire, setHoveredFire] = useState<HoveredFireTooltip | null>(null)
  const [hoveredFirmsDetection, setHoveredFirmsDetection] = useState<HoveredFirmsTooltip | null>(null)
  const [hoveredActiveFire, setHoveredActiveFire] = useState<HoveredActiveFireTooltip | null>(null)
  const [hoveredStackedTooltip, setHoveredStackedTooltip] = useState<HoveredStackedTooltip | null>(null)
  const [highlightedActiveFireId, setHighlightedActiveFireId] = useState<string | null>(null)
  const [terrainFailed, setTerrainFailed] = useState(false)
  const [mapSize, setMapSize] = useState<{ width: number; height: number } | null>(null)
  const mapRef = useRef<HTMLDivElement | null>(null)
  const svgRef = useRef<SVGSVGElement | null>(null)
  const mapClipPathId = useId().replace(/:/g, '-')
  const [isMobileMap, setIsMobileMap] = useState(() => {
    if (typeof window === 'undefined') return false
    return window.innerWidth <= MOBILE_BREAKPOINT
  })
  const isTouchInput = isMobileMap

  const pointerInMap = useCallback((
    event: { clientX: number; clientY: number },
    fallback: { x: number; y: number },
  ): { x: number; y: number } => {
    const rect = mapRef.current?.getBoundingClientRect()
    if (!rect) return fallback
    const x = event.clientX - rect.left
    const y = event.clientY - rect.top
    if (!Number.isFinite(x) || !Number.isFinite(y)) return fallback
    return { x, y }
  }, [])

  const rangeStartDate = addDays(domainStart, rangeStartDay)
  const rangeEndDate = addDays(domainStart, rangeEndDay)
  const domainStartValue = formatDateInputValue(domainStart)
  const todayValue = formatDateInputValue(today)
  const rangeStartValue = formatDateInputValue(rangeStartDate)
  const rangeEndValue = formatDateInputValue(rangeEndDate)

  useEffect(() => {
    let cancelled = false
    const controller = new AbortController()

    const load = async () => {
      try {
        const firedateStart = formatFiredateBoundary(defaultStart, 'start')
        const firedateEnd = formatFiredateBoundary(today, 'end')
        const firmsSince = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()
        const [geoFetchResult, copernicusResult, latestUpdateResult, firmsResult, currentFiresResult] = await Promise.allSettled([
          loadMunicipalitiesGeojson(),
          supabase
            .from('copernicus')
            .select('copernicus_id, centroid, shape, area_ha, firedate, commune, province, municipality_key')
            .gte('firedate', firedateStart)
            .lte('firedate', firedateEnd)
            .order('firedate', { ascending: false })
            .abortSignal(controller.signal),
          supabase
            .from('copernicus')
            .select('updated_at')
            .order('updated_at', { ascending: false })
            .limit(1)
            .abortSignal(controller.signal)
            .maybeSingle(),
          supabase
            .from('firms_active_fire_detections')
            .select('id, acquired_at, acquired_at_el, latitude, longitude, scan, track, satellite, instrument, confidence, frp, municipality_key, municipality_normalized_value')
            .gte('acquired_at', firmsSince)
            .eq('is_in_greece', true)
            .order('acquired_at', { ascending: false })
            .limit(600)
            .abortSignal(controller.signal),
          currentFiresQuery().abortSignal(controller.signal),
        ])

        if (geoFetchResult.status === 'rejected') throw geoFetchResult.reason
        const geoData = geoFetchResult.value

        const copernicusRes = copernicusResult.status === 'fulfilled' ? copernicusResult.value : null
        const latestUpdateRes = latestUpdateResult.status === 'fulfilled' ? latestUpdateResult.value : null
        const firmsRes = firmsResult.status === 'fulfilled' ? firmsResult.value : null
        const currentFiresRes = currentFiresResult.status === 'fulfilled' ? currentFiresResult.value : null
        const copernicusRows = !copernicusRes?.error ? ((copernicusRes?.data ?? []) as CopernicusRow[]) : []
        const firmsRows = !firmsRes?.error ? ((firmsRes?.data ?? []) as FirmsRow[]) : []
        const currentFireRows = !currentFiresRes?.error ? ((currentFiresRes?.data ?? []) as CurrentFireRow[]) : []
        const nextFires = mapCopernicusRows(copernicusRows)
        const nextActiveFires = mapCurrentFireRows(currentFireRows)
        const nextFirmsDetections = firmsRows
          .map((row) => {
            const lat = toNumber(row.latitude)
            const lon = toNumber(row.longitude)
            if (lat == null || lon == null) return null
            return {
              id: String(row.id),
              lat,
              lon,
              scanKm: toNumber(row.scan),
              trackKm: toNumber(row.track),
              acquiredAt: cleanText(row.acquired_at),
              acquiredAtEl: cleanText(row.acquired_at_el) ?? cleanText(row.acquired_at),
              satellite: cleanText(row.satellite) ?? 'Άγνωστος',
              instrument: cleanText(row.instrument) ?? '—',
              confidence: cleanText(row.confidence),
              frp: toNumber(row.frp),
              municipalityKey: cleanText(row.municipality_key),
              municipalityName: cleanText(row.municipality_normalized_value),
            } satisfies FirmsDetection
          })
          .filter((row): row is FirmsDetection => row !== null)

        if (!cancelled) {
          setGeojson(geoData)
          setAllFires(nextFires)
          setFirmsDetections(nextFirmsDetections)
          setActiveFires(nextActiveFires)
          setLastUpdatedAt(!latestUpdateRes?.error ? String(latestUpdateRes?.data?.updated_at ?? '').trim() || null : null)
        }
      } catch (error) {
        if (isAbortError(error)) return
        if (!cancelled) {
          setGeojson(null)
          setAllFires([])
          setFirmsDetections([])
          setActiveFires([])
          setLastUpdatedAt(null)
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    load()
    return () => {
      cancelled = true
      controller.abort()
    }
  }, [defaultStart, today])

  useEffect(() => {
    let cancelled = false
    let refreshTimer: number | null = null

    const refreshCurrentFires = async () => {
      const { data, error } = await currentFiresQuery()
      if (cancelled) return
      if (error) {
        console.error('Failed to refresh current fires for situation map', error)
        return
      }
      const nextActiveFires = mapCurrentFireRows((data ?? []) as CurrentFireRow[])
      setActiveFires(nextActiveFires)
      setHoveredActiveFire((current) => {
        if (!current) return current
        return nextActiveFires.some((fire) => fire.id === current.item.id) ? current : null
      })
      setHighlightedActiveFireId((current) => {
        if (!current) return current
        return nextActiveFires.some((fire) => fire.id === current) ? current : null
      })
    }

    const scheduleRefresh = () => {
      if (cancelled || refreshTimer != null) return
      refreshTimer = window.setTimeout(() => {
        refreshTimer = null
        void refreshCurrentFires()
      }, 1000)
    }

    const channel = supabase
      .channel('situation_map_current_fires')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'current_fires' }, () => {
        scheduleRefresh()
      })
      .subscribe()

    return () => {
      cancelled = true
      if (refreshTimer != null) window.clearTimeout(refreshTimer)
      supabase.removeChannel(channel)
    }
  }, [])

  useEffect(() => {
    if (hasLoadedHistoricalFires || rangeStartDay >= initialFetchStartDay) return

    let cancelled = false
    const controller = new AbortController()

    const loadHistoricalFires = async () => {
      try {
        const { data, error } = await supabase
          .from('copernicus')
          .select('copernicus_id, centroid, shape, area_ha, firedate, commune, province, municipality_key')
          .gte('firedate', formatFiredateBoundary(domainStart, 'start'))
          .lte('firedate', formatFiredateBoundary(addDays(defaultStart, -1), 'end'))
          .order('firedate', { ascending: false })
          .abortSignal(controller.signal)

        if (error) throw error
        const historicalFires = mapCopernicusRows((data ?? []) as CopernicusRow[])

        if (!cancelled) {
          setAllFires((current) => {
            const byId = new Map(current.map((fire) => [fire.id, fire]))
            for (const fire of historicalFires) byId.set(fire.id, fire)
            return Array.from(byId.values())
          })
          setHasLoadedHistoricalFires(true)
        }
      } catch (error) {
        if (isAbortError(error)) return
      }
    }

    loadHistoricalFires()
    return () => {
      cancelled = true
      controller.abort()
    }
  }, [defaultStart, domainStart, hasLoadedHistoricalFires, initialFetchStartDay, rangeStartDay])

  useEffect(() => {
    setHoveredFire(null)
    setHoveredFirmsDetection(null)
    setHoveredActiveFire(null)
    setHoveredStackedTooltip(null)
  }, [rangeStartDay, rangeEndDay, viewMode])

  useEffect(() => {
    const handleCurrentFireHover = (event: Event) => {
      const detail = (event as CustomEvent<CurrentFireHoverDetail>).detail
      setHighlightedActiveFireId(cleanText(detail?.incidentKey))
    }

    window.addEventListener(CURRENT_FIRE_HOVER_EVENT, handleCurrentFireHover)
    return () => window.removeEventListener(CURRENT_FIRE_HOVER_EVENT, handleCurrentFireHover)
  }, [])

  useEffect(() => {
    if (!highlightedActiveFireId) return
    if (activeFires.some((fire) => fire.id === highlightedActiveFireId)) return
    setHighlightedActiveFireId(null)
  }, [activeFires, highlightedActiveFireId])

  useEffect(() => {
    if (typeof window === 'undefined') return

    const media = window.matchMedia(`(max-width: ${MOBILE_BREAKPOINT}px)`)
    const update = () => setIsMobileMap(media.matches)

    update()
    media.addEventListener('change', update)
    return () => media.removeEventListener('change', update)
  }, [])

  useEffect(() => {
    setTerrainFailed(false)
  }, [mapTilerApiKey, isMobileMap])

  useLayoutEffect(() => {
    if (!geojson || !mapRef.current || typeof ResizeObserver === 'undefined') return

    let frameId: number | null = null

    const updateMapSize = () => {
      frameId = null
      const rect = mapRef.current?.getBoundingClientRect()
      if (!rect) return
      const width = Math.round(rect.width)
      const height = Math.round(rect.height)
      if (width <= 0 || height <= 0) return

      setMapSize((current) => (
        current?.width === width && current.height === height
          ? current
          : { width, height }
      ))
    }

    const scheduleMapSizeUpdate = () => {
      if (frameId != null) return
      frameId = window.requestAnimationFrame(updateMapSize)
    }

    scheduleMapSizeUpdate()
    const observer = new ResizeObserver(scheduleMapSizeUpdate)
    observer.observe(mapRef.current)
    window.addEventListener('resize', scheduleMapSizeUpdate)
    window.addEventListener('orientationchange', scheduleMapSizeUpdate)

    return () => {
      if (frameId != null) window.cancelAnimationFrame(frameId)
      observer.disconnect()
      window.removeEventListener('resize', scheduleMapSizeUpdate)
      window.removeEventListener('orientationchange', scheduleMapSizeUpdate)
    }
  }, [geojson])

  const fires = useMemo(() => {
    const startMs = rangeStartDate.getTime()
    const endMs = addDays(rangeEndDate, 1).getTime() - 1
    return allFires.filter((fire) => {
      if (!fire.date) return false
      const fireMs = new Date(fire.date).getTime()
      if (Number.isNaN(fireMs)) return false
      return fireMs >= startMs && fireMs <= endMs
    })
  }, [allFires, rangeStartDate, rangeEndDate])

  const mapData = useMemo(() => {
    if (!geojson) return null
    const fallbackWidth = isMobileMap ? MOBILE_MAP_WIDTH : DESKTOP_MAP_WIDTH
    const fallbackHeight = isMobileMap ? MOBILE_MAP_HEIGHT : DESKTOP_MAP_HEIGHT
    const width = mapSize?.width ?? fallbackWidth
    const height = mapSize?.height ?? fallbackHeight
    const extent: [[number, number], [number, number]] = isMobileMap
      ? [[28, 88], [width - 26, height - 26]]
      : [[20, 24], [width - 20, height - 22]]
    const projection = d3.geoMercator().fitExtent(
      extent,
      geojson as unknown as d3.ExtendedFeatureCollection,
    )
    const path = d3.geoPath().projection(projection)
    const bounds = path.bounds(geojson as unknown as d3.GeoPermissibleObjects)
    const boundsCenterX = (bounds[0][0] + bounds[1][0]) / 2
    const boundsCenterY = (bounds[0][1] + bounds[1][1]) / 2
    const transformScale = isMobileMap ? MOBILE_MAP_SCALE : DESKTOP_MAP_SCALE
    const targetCenterX = width / 2
    const targetCenterY = isMobileMap ? height * 0.51 : height * 0.5
    const transformTranslateX = targetCenterX - (boundsCenterX * transformScale)
    const transformTranslateY = targetCenterY - (boundsCenterY * transformScale)
    const clusterGridSize = isMobileMap ? MOBILE_CLUSTER_GRID_SIZE : DESKTOP_CLUSTER_GRID_SIZE
    const transformPoint = (x: number, y: number) => ({
      x: x * transformScale + transformTranslateX,
      y: y * transformScale + transformTranslateY,
    })

    return {
      width,
      height,
      transform: `translate(${transformTranslateX} ${transformTranslateY}) scale(${transformScale})`,
      paths: geojson.features.map((feature, idx) => ({
        key: `${feature.properties.municipality_code}-${idx}`,
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
      firmsFootprints: firmsDetections
        .map((detection) => {
          const rect = buildFirmsFootprint(projection, detection, transformPoint)
          if (!rect) return null
          return { ...detection, ...rect }
        })
        .filter((detection): detection is FirmsDetection & { x: number; y: number; width: number; height: number } => detection !== null),
      activeFirePoints: activeFires
        .map((fire) => {
          const projected = projection([fire.lon, fire.lat])
          if (!projected) return null
          const [baseX, baseY] = projected
          if (![baseX, baseY].every(Number.isFinite)) return null
          const { x, y } = transformPoint(baseX, baseY)
          return { ...fire, x, y }
        })
        .filter((fire): fire is ActiveFirePoint & { x: number; y: number } => fire !== null),
      points: Object.values(
        fires.reduce<Record<string, {
          x: number
          y: number
          r: number
          items: HoveredFireTooltip['items']
        }>>((acc, fire) => {
          const projected = projection([fire.lon, fire.lat])
          if (!projected) return acc
          const [baseX, baseY] = projected
          if (!Number.isFinite(baseX) || !Number.isFinite(baseY)) return acc
          const { x, y } = transformPoint(baseX, baseY)
          const key = `${Math.round(x / clusterGridSize)}:${Math.round(y / clusterGridSize)}`
          if (!acc[key]) {
            acc[key] = {
              x,
              y,
              r: 4.5,
              items: [],
            }
          }
          acc[key].items.push({
            id: fire.id,
            areaHa: fire.areaHa,
            date: fire.date,
            commune: fire.commune,
            province: fire.province,
            municipalityKey: fire.municipalityKey,
          })
          return acc
        }, {}),
      ).sort((a, b) => b.items.length - a.items.length),
      shapes: fires
        .map((fire) => {
          if (!fire.shape) return null
          const d = path(fire.shape as unknown as d3.GeoPermissibleObjects)
          if (!d) return null
          const centroid = path.centroid(fire.shape as unknown as d3.GeoPermissibleObjects)
          const [baseX, baseY] = centroid
          if (![baseX, baseY].every(Number.isFinite)) return null
          const { x, y } = transformPoint(baseX, baseY)
          return {
            ...fire,
            d,
            x,
            y,
          }
        })
        .filter((shape): shape is CopernicusFirePoint & { d: string; x: number; y: number } => shape !== null),
    }
  }, [activeFires, firmsDetections, geojson, fires, isMobileMap, mapSize, mapTilerApiKey])

  const totalAreaHa = fires.reduce((sum, fire) => sum + fire.areaHa, 0)
  const latestFire = [...fires]
    .filter((fire) => fire.date)
    .sort((a, b) => new Date(b.date!).getTime() - new Date(a.date!).getTime())[0] ?? null
  const satellites = useMemo(() => {
    const bySatellite = new Map<string, { detection: FirmsDetection; count: number }>()
    firmsDetections.forEach((detection) => {
      const key = `${detection.satellite}/${detection.instrument}`
      const current = bySatellite.get(key)
      if (!current) {
        bySatellite.set(key, { detection, count: 1 })
        return
      }
      current.count += 1
      if (new Date(detection.acquiredAt ?? 0).getTime() > new Date(current.detection.acquiredAt ?? 0).getTime()) {
        current.detection = detection
      }
    })
    return [...bySatellite.values()]
      .filter(({ count }) => count > 0)
      .sort((a, b) => new Date(b.detection.acquiredAt ?? 0).getTime() - new Date(a.detection.acquiredAt ?? 0).getTime())
  }, [firmsDetections])

  const visibleSituationTooltipPoints = useMemo<SituationTooltipPoint[]>(() => {
    if (!mapData) return []

    const points: SituationTooltipPoint[] = []

    for (const fire of activeFires) {
      const marker = mapData.activeFirePoints.find((point) => point.id === fire.id)
      if (!marker) continue
      points.push({
        id: `current-${fire.id}`,
        x: marker.x,
        y: marker.y,
        type: 'current-fire',
        priority: 0,
        label: [
          fire.municipalityName ? `Ενεργή πυρκαγιά - ΔΗΜΟΣ ${fire.municipalityName}` : 'Ενεργή πυρκαγιά',
          fire.fuelType,
          fire.startDate ? `Ξέσπασε: ${formatDateEl(fire.startDate)}` : null,
          fire.status ?? 'Ενεργή πυρκαγιά',
        ].filter(Boolean).join(' · '),
      })
    }

    const copernicusPoints = viewMode === 'points'
      ? mapData.points.flatMap((point) => point.items.map((item) => ({ ...item, x: point.x, y: point.y })))
      : mapData.shapes

    for (const fire of copernicusPoints) {
      points.push({
        id: `copernicus-${fire.id}`,
        x: fire.x,
        y: fire.y,
        type: 'copernicus',
        priority: 1,
        label: [
          fire.commune ?? 'Copernicus / EFFIS',
          fire.province,
          formatDateEl(fire.date),
          formatStremmata(fire.areaHa),
        ].filter(Boolean).join(' · '),
      })
    }

    for (const detection of mapData.firmsFootprints) {
      points.push({
        id: `firms-${detection.id}`,
        x: detection.x,
        y: detection.y,
        type: 'firms',
        priority: 2,
        label: [
          detection.municipalityName ?? 'NASA FIRMS',
          `${detection.satellite} / ${detection.instrument}`,
          formatDateTimeEl(detection.acquiredAtEl ?? detection.acquiredAt),
          `Θερμική Ενέργεια: ${formatMegawatts(detection.frp)}`,
        ].filter(Boolean).join(' · '),
      })
    }

    return points
  }, [activeFires, mapData, viewMode])

  const openMunicipalityProfile = useCallback((municipalityKey: string | null) => {
    if (!municipalityKey) return false
    navigate(`/municipalities?municipality=${encodeURIComponent(municipalityKey)}`)
    return true
  }, [navigate])

  useLayoutEffect(() => {
    if (!mapData || !svgRef.current) return

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()
    svg
      .attr('viewBox', `0 0 ${mapData.width} ${mapData.height}`)

    const showStackedTooltip = (
      event: MouseEvent | PointerEvent,
      activeId: string,
      fallback: { x: number; y: number },
    ): boolean => {
      const overlapRadius = isTouchInput ? 18 : 12
      const overlapping = visibleSituationTooltipPoints.filter((point) => {
        const dx = point.x - fallback.x
        const dy = point.y - fallback.y
        return Math.hypot(dx, dy) <= overlapRadius
      })

      if (overlapping.length < 2) return false

      const pointer = pointerInMap(event, fallback)
      const ordered = overlapping.slice().sort((a, b) => {
        if (a.id === activeId) return -1
        if (b.id === activeId) return 1
        if (a.priority !== b.priority) return a.priority - b.priority
        return a.label.localeCompare(b.label, 'el')
      })

      setHoveredFire(null)
      setHoveredFirmsDetection(null)
      setHoveredActiveFire(null)
      setHoveredStackedTooltip({
        x: pointer.x,
        y: pointer.y,
        placement: pointer.y < 96 ? 'below' : 'above',
        items: ordered.map((point) => ({
          label: point.label,
          type: point.type,
        })),
      })
      return true
    }

    const updatePointTooltip = (event: MouseEvent, fire: (typeof mapData.points)[number]) => {
      const pointer = pointerInMap(event, { x: fire.x, y: fire.y })
      if (showStackedTooltip(event, `copernicus-${fire.items[0]?.id ?? ''}`, { x: fire.x, y: fire.y })) return
      setHoveredStackedTooltip(null)
      setHoveredFirmsDetection(null)
      setHoveredActiveFire(null)
      setHoveredFire({
        x: pointer.x,
        y: pointer.y,
        placement: pointer.y < 96 ? 'below' : 'above',
        items: fire.items,
      })
    }

    const clearPointTooltip = () => {
      setHoveredFire(null)
      setHoveredStackedTooltip(null)
    }

    const updateFirmsTooltip = (event: MouseEvent | PointerEvent, footprint: (typeof mapData.firmsFootprints)[number]) => {
      const pointer = pointerInMap(event, { x: footprint.x, y: footprint.y })
      if (showStackedTooltip(event, `firms-${footprint.id}`, { x: footprint.x, y: footprint.y })) return
      setHoveredFire(null)
      setHoveredActiveFire(null)
      setHoveredStackedTooltip(null)
      setHoveredFirmsDetection({
        x: pointer.x,
        y: pointer.y,
        placement: pointer.y < 96 ? 'below' : 'above',
        item: footprint,
      })
    }

    const updateActiveFireTooltip = (event: MouseEvent | PointerEvent, fire: (typeof mapData.activeFirePoints)[number]) => {
      const pointer = pointerInMap(event, { x: fire.x, y: fire.y })
      if (showStackedTooltip(event, `current-${fire.id}`, { x: fire.x, y: fire.y })) return
      setHoveredFire(null)
      setHoveredFirmsDetection(null)
      setHoveredStackedTooltip(null)
      setHoveredActiveFire({
        x: pointer.x,
        y: pointer.y,
        placement: pointer.y < 96 ? 'below' : 'above',
        item: fire,
      })
    }

    const handlePointClick = (event: MouseEvent | PointerEvent, fire: (typeof mapData.points)[number]) => {
      event.preventDefault()
      event.stopPropagation()
      const municipalityKey = getClusterMunicipalityKey(fire.items)
      if (openMunicipalityProfile(municipalityKey)) return
      updatePointTooltip(event, fire)
    }

    const handleFirmsClick = (event: MouseEvent | PointerEvent, footprint: (typeof mapData.firmsFootprints)[number]) => {
      event.preventDefault()
      event.stopPropagation()
      if (openMunicipalityProfile(footprint.municipalityKey)) return
      updateFirmsTooltip(event, footprint)
    }

    const handleActiveFireClick = (event: MouseEvent | PointerEvent, fire: (typeof mapData.activeFirePoints)[number]) => {
      event.preventDefault()
      event.stopPropagation()
      if (openMunicipalityProfile(fire.municipalityKey)) return
      updateActiveFireTooltip(event, fire)
    }

    const drawActiveFires = () => {
      const activeFireGroups = svg
        .append('g')
        .attr('class', 'fire-current__points')
        .selectAll<SVGGElement, (typeof mapData.activeFirePoints)[number]>('g')
        .data(mapData.activeFirePoints)
        .join('g')
        .classed('is-highlighted', (fire) => fire.id === highlightedActiveFireId)
        .classed('is-dimmed', (fire) => highlightedActiveFireId != null && fire.id !== highlightedActiveFireId)

      activeFireGroups
        .append('circle')
        .attr('class', 'fire-current__point-pulse')
        .attr('cx', (fire) => fire.x)
        .attr('cy', (fire) => fire.y)
        .attr('r', (fire) => fire.id === highlightedActiveFireId ? 5.2 : 4.2)
        .attr('pointer-events', 'none')

      activeFireGroups
        .append('circle')
        .attr('class', 'fire-current__point-marker')
        .attr('cx', (fire) => fire.x)
        .attr('cy', (fire) => fire.y)
        .attr('r', (fire) => fire.id === highlightedActiveFireId ? 5.2 : 4.2)
        .attr('pointer-events', 'none')

      activeFireGroups
        .append('circle')
        .attr('class', 'fire-current__point-hit-area')
        .attr('cx', (fire) => fire.x)
        .attr('cy', (fire) => fire.y)
        .attr('r', 17)
        .attr('fill', 'rgba(0, 0, 0, 0.001)')
        .attr('pointer-events', 'all')
        .style('cursor', 'pointer')
        .on('mouseover', (event: MouseEvent, fire) => {
          if (isTouchInput) return
          updateActiveFireTooltip(event, fire)
        })
        .on('mousemove', (event: MouseEvent, fire) => {
          if (isTouchInput) return
          updateActiveFireTooltip(event, fire)
        })
        .on('mouseleave', () => {
          if (isTouchInput) return
          setHoveredActiveFire(null)
          setHoveredStackedTooltip(null)
        })
        .on('pointerdown', (event: PointerEvent, fire) => {
          handleActiveFireClick(event, fire)
        })
        .on('click', (event: MouseEvent, fire) => {
          handleActiveFireClick(event, fire)
        })
    }

    const defs = svg.append('defs')
    const clipPath = defs.append('clipPath').attr('id', mapClipPathId)
    clipPath
      .selectAll<SVGPathElement, (typeof mapData.paths)[number]>('path')
      .data(mapData.paths)
      .join('path')
      .attr('d', (feature) => feature.d)
      .attr('transform', mapData.transform)

    svg
      .append('g')
      .attr('class', 'fire-copernicus__base')
      .attr('transform', mapData.transform)
      .selectAll<SVGPathElement, (typeof mapData.paths)[number]>('path')
      .data(mapData.paths)
      .join('path')
      .attr('d', (feature) => feature.d)

    if (mapData.hillshadeTiles.length > 0 && !terrainFailed) {
      svg
        .append('g')
        .attr('class', 'fire-copernicus__terrain')
        .attr('clip-path', `url(#${mapClipPathId})`)
        .attr('aria-hidden', 'true')
        .selectAll<SVGImageElement, (typeof mapData.hillshadeTiles)[number]>('image')
        .data(mapData.hillshadeTiles)
        .join('image')
        .attr('href', (tile) => tile.href)
        .attr('x', (tile) => tile.x)
        .attr('y', (tile) => tile.y)
        .attr('width', (tile) => tile.width)
        .attr('height', (tile) => tile.height)
        .attr('preserveAspectRatio', 'none')
        .attr('class', 'fire-copernicus__terrain-tile')
        .on('error', () => setTerrainFailed(true))
    }

    svg
      .append('g')
      .attr('class', 'fire-firms__footprints')
      .selectAll<SVGRectElement, (typeof mapData.firmsFootprints)[number]>('rect')
      .data(mapData.firmsFootprints)
      .join('rect')
      .attr('class', 'fire-firms__footprint')
      .attr('x', (footprint) => footprint.x - footprint.width / 2)
      .attr('y', (footprint) => footprint.y - footprint.height / 2)
      .attr('width', (footprint) => footprint.width)
      .attr('height', (footprint) => footprint.height)
      .attr('pointer-events', 'all')
      .on('mouseover', (event: MouseEvent, footprint) => {
        if (isTouchInput) return
        updateFirmsTooltip(event, footprint)
      })
      .on('mousemove', (event: MouseEvent, footprint) => {
        if (isTouchInput) return
        updateFirmsTooltip(event, footprint)
      })
      .on('mouseleave', () => {
        if (isTouchInput) return
        setHoveredFirmsDetection(null)
        setHoveredStackedTooltip(null)
      })
      .on('pointerdown', (event: PointerEvent, footprint) => {
        handleFirmsClick(event, footprint)
      })
      .on('click', (event: MouseEvent, footprint) => {
        handleFirmsClick(event, footprint)
      })

    if (viewMode === 'points') {
      svg
        .on('mousemove', (event: MouseEvent) => {
          if (isTouchInput) return
          const target = event.target instanceof Element ? event.target : null
          const isOverCopernicusPoint = target?.classList.contains('fire-copernicus__point-hit-area') ?? false
          const isOverFirmsFootprint = target?.classList.contains('fire-firms__footprint') ?? false
          const isOverCurrentFirePoint = target?.classList.contains('fire-current__point-hit-area') ?? false

          if (!isOverCopernicusPoint && !isOverFirmsFootprint && !isOverCurrentFirePoint) {
            clearPointTooltip()
            setHoveredFirmsDetection(null)
            setHoveredActiveFire(null)
            setHoveredStackedTooltip(null)
          }
        })
        .on('mouseleave', () => {
          clearPointTooltip()
          setHoveredFirmsDetection(null)
          setHoveredActiveFire(null)
          setHoveredStackedTooltip(null)
        })

      const pointGroups = svg
        .append('g')
        .attr('class', 'fire-copernicus__points')
        .selectAll<SVGGElement, (typeof mapData.points)[number]>('g')
        .data(mapData.points)
        .join('g')

      pointGroups
        .append('circle')
        .attr('class', 'fire-copernicus__point-marker')
        .attr('cx', (fire) => fire.x)
        .attr('cy', (fire) => fire.y)
        .attr('r', (fire) => fire.r)
        .attr('pointer-events', 'none')

      pointGroups
        .filter((fire) => fire.items.length > 1)
        .append('text')
        .attr('x', (fire) => fire.x)
        .attr('y', (fire) => fire.y)
        .attr('text-anchor', 'middle')
        .attr('dominant-baseline', 'central')
        .attr('class', 'fire-copernicus__cluster-count')
        .attr('pointer-events', 'none')
        .text((fire) => String(fire.items.length))

      pointGroups
        .append('circle')
        .attr('class', 'fire-copernicus__point-hit-area')
        .attr('cx', (fire) => fire.x)
        .attr('cy', (fire) => fire.y)
        .attr('r', 14)
        .attr('fill', 'rgba(0, 0, 0, 0.001)')
        .attr('pointer-events', 'all')
        .style('cursor', 'pointer')
        .on('mouseover', (event: MouseEvent, fire) => {
          if (isTouchInput) return
          updatePointTooltip(event, fire)
        })
        .on('mousemove', (event: MouseEvent, fire) => {
          if (isTouchInput) return
          updatePointTooltip(event, fire)
        })
        .on('mouseleave', () => {
          clearPointTooltip()
        })
        .on('pointerdown', (event: PointerEvent, fire) => {
          handlePointClick(event, fire)
        })
        .on('click', (event: MouseEvent, fire) => {
          handlePointClick(event, fire)
        })
    } else {
      svg
        .append('g')
        .attr('class', 'fire-copernicus__shapes')
        .attr('transform', mapData.transform)
        .selectAll<SVGPathElement, (typeof mapData.shapes)[number]>('path')
        .data(mapData.shapes)
        .join('path')
        .attr('d', (fire) => fire.d)
        .on('mouseenter', (event: MouseEvent, fire) => {
          if (isTouchInput) return
          const pointer = pointerInMap(event, { x: fire.x, y: fire.y })
          if (showStackedTooltip(event, `copernicus-${fire.id}`, { x: fire.x, y: fire.y })) return
          setHoveredStackedTooltip(null)
          setHoveredFirmsDetection(null)
          setHoveredActiveFire(null)
          setHoveredFire({
            x: pointer.x,
            y: pointer.y,
            placement: pointer.y < 96 ? 'below' : 'above',
            items: [{
              id: fire.id,
              areaHa: fire.areaHa,
              date: fire.date,
              commune: fire.commune,
              province: fire.province,
              municipalityKey: fire.municipalityKey,
            }],
          })
        })
        .on('mousemove', (event: MouseEvent, fire) => {
          if (isTouchInput) return
          const pointer = pointerInMap(event, { x: fire.x, y: fire.y })
          if (showStackedTooltip(event, `copernicus-${fire.id}`, { x: fire.x, y: fire.y })) return
          setHoveredFire((current) => (
            current ? { ...current, x: pointer.x, y: pointer.y, placement: pointer.y < 96 ? 'below' : 'above' } : current
          ))
        })
        .on('mouseleave', () => {
          if (isTouchInput) return
          setHoveredFire(null)
          setHoveredStackedTooltip(null)
        })
        .on('click', (event: MouseEvent, fire) => {
          if (openMunicipalityProfile(fire.municipalityKey)) return
          const pointer = pointerInMap(event, { x: fire.x, y: fire.y })
          if (showStackedTooltip(event, `copernicus-${fire.id}`, { x: fire.x, y: fire.y })) return
          setHoveredFire((current) => (
            current?.items.length === 1 && current.items[0]?.id === fire.id
              ? null
              : {
                  x: pointer.x,
                  y: pointer.y,
                  placement: pointer.y < 96 ? 'below' : 'above',
                  items: [{
                    id: fire.id,
                    areaHa: fire.areaHa,
                    date: fire.date,
                    commune: fire.commune,
                    province: fire.province,
                    municipalityKey: fire.municipalityKey,
                  }],
                }
          ))
        })
    }

    drawActiveFires()
  }, [highlightedActiveFireId, isTouchInput, mapClipPathId, mapData, openMunicipalityProfile, pointerInMap, terrainFailed, viewMode, visibleSituationTooltipPoints])

  if (loading) {
    return (
      <section id="situationmap" className="fire-copernicus section-rule dev-tag-anchor">
        <div className="dev-tag-stack dev-tag-stack--right">
          <ComponentTag name="SituationMap" />
          <ComponentTag name="fire-copernicus section-rule" kind="CLASS" />
        </div>
        <div className="fire-copernicus__intro dev-tag-anchor">
          <ComponentTag name="fire-copernicus__intro" kind="CLASS" className="component-tag--overlay" />
          <div className="eyebrow">Situation Map</div>
          <h2>Δασικές πυρκαγιές & Θερμικές ανωμαλίες εδάφους</h2>
          <p>
            Ο χάρτης απεικονίζει ενεργές δασικές πυρκαγιές <span className="fire-current__legend-icon" aria-hidden="true" />, δασικές πυρκαγιές <span className="fire-copernicus__legend-dot" aria-hidden="true" /> όπως καταγράφηκαν από το ευρωπαϊκό δορυφορικό σύστημα <a href="https://forest-fire.emergency.copernicus.eu/">Copernicus EFFIS</a> και θερμικές ανωμαλίες εδάφους <span className="fire-copernicus__legend-dot fire-firms__legend-square" aria-hidden="true" /> όπως καταγράφονται από δορυφόρους της NASA FIRMS.
          </p>
        </div>
        <DataLoadingCard
          className="fire-copernicus__loading-card"
          message="Ανακτώνται οι νεότερες εγγραφές Copernicus και προετοιμάζεται ο χάρτης."
        />
      </section>
    )
  }

  return (
    <section id="situationmap" className="fire-copernicus section-rule dev-tag-anchor">
      <div className="dev-tag-stack dev-tag-stack--right">
        <ComponentTag name="SituationMap" />
        <ComponentTag name="fire-copernicus section-rule" kind="CLASS" />
      </div>
      <div className="fire-copernicus__intro dev-tag-anchor">
        <ComponentTag name="fire-copernicus__intro" kind="CLASS" className="component-tag--overlay" />
        <div className="eyebrow">Situation Map</div>
        <h2>Δασικές πυρκαγιές & Θερμικές ανωμαλίες εδάφους</h2>
        <div className="brand-mark fire-copernicus__brand-mark">
          Τελευταία ενημέρωση / {formatDateTimeEl(lastUpdatedAt)}
        </div>
        <p>
          Ο χάρτης απεικονίζει ενεργές δασικές πυρκαγιές <span className="fire-current__legend-icon" aria-hidden="true" />, δασικές πυρκαγιές και καμένες εκτάσεις <span className="fire-copernicus__legend-dot" aria-hidden="true" /> όπως καταγράφονται στην ευρωπαϊκή υπηρεσία <a href="https://forest-fire.emergency.copernicus.eu/">Copernicus EFFIS</a>, καθώς και δορυφορικές παρατηρήσεις θερμικών ανωμαλιών στο έδαφος <span className="fire-copernicus__legend-dot fire-firms__legend-square" aria-hidden="true" /> από τη <a href="https://firms.modaps.eosdis.nasa.gov/">NASA FIRMS</a> κατά το <b>τελευταίο 24ωρο</b>.
        </p>
        <div className="fire-firms__satellites" aria-label="Τελευταίες διελεύσεις δορυφόρων NASA FIRMS">
          {satellites.length > 0 ? satellites.map(({ detection, count }) => (
            <div key={`${detection.satellite}-${detection.instrument}`}>
              <strong>{detection.satellite} / {detection.instrument} · {formatObservationCount(count)}</strong>
              <span>{formatDateTimeEl(detection.acquiredAtEl ?? detection.acquiredAt)}</span>
            </div>
          )) : (
            <div>
              <strong>Δεν υπάρχουν διαθέσιμες διελεύσεις</strong>
              <span>—</span>
            </div>
          )}
        </div>
        
        <div className="fire-copernicus__section-divider" aria-hidden="true" />
        <div className="fire-copernicus__date-filter-selected">
          <span className="label">Βλέπετε δεδομένα δασικών πυρκαγιών για το διάστημα</span>
          <div className="fire-copernicus__date-filter fire-copernicus__date-filter--pickers" aria-label="Φίλτρο ημερομηνιών Copernicus">
            <label className="fire-copernicus__date-picker">
              <span className="label">Από</span>
              <input
                type="date"
                min={domainStartValue}
                max={rangeEndValue}
                value={rangeStartValue}
                aria-label="Έναρξη φίλτρου ημερομηνίας"
                onChange={(event) => {
                  const nextDate = parseDateInputValue(event.target.value)
                  if (!nextDate) return
                  setRangeStartDay(Math.min(diffDays(domainStart, nextDate), rangeEndDay))
                }}
              />
            </label>
            <label className="fire-copernicus__date-picker">
              <span className="label">Έως</span>
              <input
                type="date"
                min={rangeStartValue}
                max={todayValue}
                value={rangeEndValue}
                aria-label="Λήξη φίλτρου ημερομηνίας"
                onChange={(event) => {
                  const nextDate = parseDateInputValue(event.target.value)
                  if (!nextDate) return
                  setRangeEndDay(Math.max(diffDays(domainStart, nextDate), rangeStartDay))
                }}
              />
            </label>
          </div>
        </div>
        <div className="fire-copernicus__stats">
          <div>
            <span className="label">Συμβάντα</span>
            <strong>{fires.length.toLocaleString('el-GR')}</strong>
          </div>
          <div>
            <span className="label">Καμένη Έκταση</span>
            <strong>{formatStremmata(totalAreaHa)}</strong>
          </div>
          <div>
            <span className="label">Τελευταία Εγγραφή</span>
            <strong>{formatDateEl(latestFire?.date ?? null)}</strong>
          </div>
        </div>
        <p className="fire-copernicus__note">
          Σημείωση: Οι ημερομηνίες και τα μεγέθη ενδέχεται να αναθεωρηθούν καθώς ενημερώνεται η βάση δεδομένων.
        </p>
      </div>

      <div className="fire-copernicus__map-wrap dev-tag-anchor">
        <ComponentTag
          name="fire-copernicus__map-wrap"
          kind="CLASS"
          className="component-tag--overlay"
          style={{ left: 'auto', right: '0.45rem' }}
        />
        {!mapData && <div className="fire-copernicus__empty">Δεν ήταν δυνατή η φόρτωση των δεδομένων Copernicus.</div>}
        {mapData && (
          <div
            ref={mapRef}
            className="fire-copernicus__map fire-firms__map dev-tag-anchor"
            onMouseLeave={() => {
              setHoveredFire(null)
              setHoveredFirmsDetection(null)
              setHoveredActiveFire(null)
              setHoveredStackedTooltip(null)
            }}
          >
            <ComponentTag
              name="fire-copernicus__map fire-firms__map"
              kind="CLASS"
              className="component-tag--overlay"
              style={{ left: 'auto', right: '0.45rem' }}
            />
            <div className="fire-copernicus__toggle dev-tag-anchor" aria-label="Τρόπος προβολής Copernicus">
              <ComponentTag
                name="fire-copernicus__toggle"
                kind="CLASS"
                className="component-tag--overlay"
                style={{ top: '2.9rem' }}
              />
              <button
                type="button"
                className={viewMode === 'points' ? 'is-active' : ''}
                onClick={() => {
                  setHoveredFire(null)
                  setHoveredFirmsDetection(null)
                  setHoveredActiveFire(null)
                  setHoveredStackedTooltip(null)
                  setViewMode('points')
                }}
              >
                Σημεία
              </button>
              <button
                type="button"
                className={viewMode === 'shapes' ? 'is-active' : ''}
                onClick={() => {
                  setHoveredFire(null)
                  setHoveredFirmsDetection(null)
                  setHoveredActiveFire(null)
                  setHoveredStackedTooltip(null)
                  setViewMode('shapes')
                }}
              >
                Εκτάσεις
              </button>
            </div>
            <svg
              ref={svgRef}
              viewBox={`0 0 ${mapData.width} ${mapData.height}`}
              role="img"
              aria-label="Χάρτης πυρκαγιών Copernicus στην Ελλάδα"
            />
            {mapData.hillshadeTiles.length > 0 && !terrainFailed && (
              <MapTilerLogo className="fire-copernicus__maptiler-logo" />
            )}
            {hoveredStackedTooltip && (
              <div
                className="fire-copernicus__tooltip fire-copernicus__tooltip--stacked app-tooltip"
                style={{
                  left: `${Math.max(12, hoveredStackedTooltip.x + 14)}px`,
                  top: `${hoveredStackedTooltip.placement === 'below' ? hoveredStackedTooltip.y + 14 : Math.max(12, hoveredStackedTooltip.y - 14)}px`,
                  transform: hoveredStackedTooltip.placement === 'below' ? 'none' : undefined,
                  pointerEvents: 'none',
                }}
              >
                <div className="fire-copernicus__tooltip-item">
                  <strong>{hoveredStackedTooltip.items.length} σημεία στο ίδιο σημείο</strong>
                  {hoveredStackedTooltip.items.map((item, index, items) => (
                    <span
                      key={`${index}-${item.label}`}
                      className={index > 0 && items[index - 1]?.type !== item.type ? 'tooltip-type-separator' : undefined}
                    >
                      {item.label}
                    </span>
                  ))}
                </div>
              </div>
            )}
            {hoveredFire && (
              <div
                className="fire-copernicus__tooltip app-tooltip"
                style={{
                  left: `${Math.max(12, hoveredFire.x + 14)}px`,
                  top: `${hoveredFire.placement === 'below' ? hoveredFire.y + 14 : Math.max(12, hoveredFire.y - 14)}px`,
                  transform: hoveredFire.placement === 'below' ? 'none' : undefined,
                  pointerEvents: 'none',
                }}
              >
                {hoveredFire.items.map((item) => (
                  <div key={item.id} className="fire-copernicus__tooltip-item">
                    <strong>{item.commune ?? 'Άγνωστος δήμος'}</strong>
                    <span>{item.province ?? '—'}</span>
                    <span>{formatDateEl(item.date)}</span>
                    <span>{formatStremmata(item.areaHa)}</span>
                  </div>
                ))}
              </div>
            )}
            {hoveredFirmsDetection && (
              <div
                className="fire-copernicus__tooltip app-tooltip"
                style={{
                  left: `${Math.max(12, hoveredFirmsDetection.x + 14)}px`,
                  top: `${hoveredFirmsDetection.placement === 'below' ? hoveredFirmsDetection.y + 14 : Math.max(12, hoveredFirmsDetection.y - 14)}px`,
                  transform: hoveredFirmsDetection.placement === 'below' ? 'none' : undefined,
                  pointerEvents: 'none',
                }}
              >
                <div className="fire-copernicus__tooltip-item">
                  <strong>{hoveredFirmsDetection.item.municipalityName ?? 'Άγνωστη περιοχή'}</strong>
                  <span>{hoveredFirmsDetection.item.satellite} / {hoveredFirmsDetection.item.instrument}</span>
                  <span>{formatDateTimeEl(hoveredFirmsDetection.item.acquiredAtEl ?? hoveredFirmsDetection.item.acquiredAt)}</span>
                  <span>Θερμική Ενέργεια: {formatMegawatts(hoveredFirmsDetection.item.frp)}</span>
                </div>
              </div>
            )}
            {hoveredActiveFire && (
              <div
                className="fire-copernicus__tooltip app-tooltip"
                style={{
                  left: `${Math.max(12, hoveredActiveFire.x + 14)}px`,
                  top: `${hoveredActiveFire.placement === 'below' ? hoveredActiveFire.y + 14 : Math.max(12, hoveredActiveFire.y - 14)}px`,
                  transform: hoveredActiveFire.placement === 'below' ? 'none' : undefined,
                  pointerEvents: 'none',
                }}
              >
                <div className="fire-copernicus__tooltip-item">
                  <strong>{hoveredActiveFire.item.municipalityName ? `ΔΗΜΟΣ ${hoveredActiveFire.item.municipalityName}` : 'Άγνωστη περιοχή'}</strong>
                  <span>{hoveredActiveFire.item.fuelType ?? '—'}</span>
                  <span>Ξέσπασε: {formatDateEl(hoveredActiveFire.item.startDate)}</span>
                  <span>{hoveredActiveFire.item.status ?? 'Ενεργή πυρκαγιά'}</span>
                </div>
              </div>
            )}
          </div>
        )}
        {mapData && (
          <div className="fire-copernicus__legend fire-copernicus__legend--map" aria-label="Υπόμνημα Copernicus EFFIS">
            <span className="fire-copernicus__legend-row">
              <span className="fire-copernicus__legend-dot" aria-hidden="true" />
              <span>{viewMode === 'points' ? 'Καταγεγραμμένη πυρκαγιά Copernicus EFFIS' : 'Καμένη έκταση Copernicus EFFIS'}</span>
            </span>
            <span className="fire-copernicus__legend-row">
              <span className="fire-copernicus__legend-dot fire-firms__legend-square" aria-hidden="true" />
              <span>Ενεργή θερμική ανωμαλία NASA FIRMS</span>
            </span>
            <span className="fire-copernicus__legend-row">
              <span className="fire-current__legend-icon" aria-hidden="true" />
              <span>Ενεργή πυρκαγιά (θέση κατά προσέγγιση)</span>
            </span>
          </div>
        )}
      </div>
    </section>
  )
}
