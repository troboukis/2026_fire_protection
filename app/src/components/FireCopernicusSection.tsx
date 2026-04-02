import { useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { MouseEvent as ReactMouseEvent } from 'react'
import * as d3 from 'd3'
import { useNavigate } from 'react-router-dom'
import type { GeoData } from '../types'
import { supabase } from '../lib/supabase'
import ComponentTag from './ComponentTag'
import DataLoadingCard from './DataLoadingCard'

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

function formatDateOnlyEl(date: Date): string {
  return new Intl.DateTimeFormat('el-GR', {
    day: 'numeric',
    month: 'short',
    year: 'numeric',
  }).format(date)
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

function toDayStart(input: Date): Date {
  return new Date(input.getFullYear(), input.getMonth(), input.getDate())
}

function addDays(input: Date, days: number): Date {
  const next = new Date(input)
  next.setDate(next.getDate() + days)
  return toDayStart(next)
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

function cleanText(value: unknown): string | null {
  if (value == null) return null
  const text = String(value).trim()
  return text ? text : null
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

export default function FireCopernicusSection() {
  const navigate = useNavigate()
  const currentYear = useMemo(() => new Date().getFullYear(), [])
  const today = useMemo(() => toDayStart(new Date()), [])
  const mapTilerApiKey = useMemo(() => cleanText(import.meta.env.VITE_MAPTILER_API_KEY), [])
  const domainStart = new Date(2024, 0, 1)
  const defaultStart = new Date(currentYear, 0, 1)
  const totalDays = diffDays(domainStart, today)
  const [geojson, setGeojson] = useState<GeoData | null>(null)
  const [allFires, setAllFires] = useState<CopernicusFirePoint[]>([])
  const [lastUpdatedAt, setLastUpdatedAt] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [viewMode, setViewMode] = useState<'points' | 'shapes'>('points')
  const [rangeStartDay, setRangeStartDay] = useState(() => diffDays(domainStart, defaultStart))
  const [rangeEndDay, setRangeEndDay] = useState(() => totalDays)
  const [hoveredFire, setHoveredFire] = useState<HoveredFireTooltip | null>(null)
  const mapRef = useRef<HTMLDivElement | null>(null)
  const tooltipRef = useRef<HTMLDivElement | null>(null)
  const mapClipPathId = useId().replace(/:/g, '-')
  const [isMobileMap, setIsMobileMap] = useState(() => {
    if (typeof window === 'undefined') return false
    return window.innerWidth <= MOBILE_BREAKPOINT
  })
  const isTouchInput = isMobileMap

  const pointerInMap = (
    event: ReactMouseEvent<SVGElement | SVGGElement | SVGPathElement>,
    fallback: { x: number; y: number },
  ): { x: number; y: number } => {
    const rect = mapRef.current?.getBoundingClientRect()
    if (!rect) return fallback
    const x = event.clientX - rect.left
    const y = event.clientY - rect.top
    if (!Number.isFinite(x) || !Number.isFinite(y)) return fallback
    return { x, y }
  }

  const rangeStartDate = addDays(domainStart, rangeStartDay)
  const rangeEndDate = addDays(domainStart, rangeEndDay)
  const rangeStartPercent = totalDays > 0 ? (rangeStartDay / totalDays) * 100 : 0
  const rangeEndPercent = totalDays > 0 ? (rangeEndDay / totalDays) * 100 : 100
  const yearMarkers = [2024, 2025, 2026]
    .filter((year) => year <= today.getFullYear())
    .map((year) => ({
      year,
      left: totalDays > 0 ? (diffDays(domainStart, new Date(year, 0, 1)) / totalDays) * 100 : 0,
    }))

  useEffect(() => {
    let cancelled = false

    const load = async () => {
      try {
        const firedateStart = `2024-01-01T00:00:00`
        const firedateEnd = `${today.toISOString().slice(0, 10)}T23:59:59`
        const [geoRes, copernicusRes, latestUpdateRes] = await Promise.all([
          fetch(`${import.meta.env.BASE_URL}municipalities.geojson`),
          supabase
            .from('copernicus')
            .select('copernicus_id, centroid, shape, area_ha, firedate, commune, province, municipality_key')
            .gte('firedate', firedateStart)
            .lte('firedate', firedateEnd)
            .order('firedate', { ascending: false }),
          supabase
            .from('copernicus')
            .select('updated_at')
            .order('updated_at', { ascending: false })
            .limit(1)
            .maybeSingle(),
        ])
        if (copernicusRes.error) throw copernicusRes.error
        if (latestUpdateRes.error) throw latestUpdateRes.error
        const geoData = (await geoRes.json()) as GeoData
        const nextFires = ((copernicusRes.data ?? []) as CopernicusRow[])
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

        if (!cancelled) {
          setGeojson(geoData)
          setAllFires(nextFires)
          setLastUpdatedAt(String(latestUpdateRes.data?.updated_at ?? '').trim() || null)
        }
      } catch {
        if (!cancelled) {
          setGeojson(null)
          setAllFires([])
          setLastUpdatedAt(null)
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    load()
    return () => {
      cancelled = true
    }
  }, [currentYear, today])

  useEffect(() => {
    setHoveredFire(null)
  }, [rangeStartDay, rangeEndDay, viewMode])

  useLayoutEffect(() => {
    if (!hoveredFire || !mapRef.current || !tooltipRef.current) return

    const mapRect = mapRef.current.getBoundingClientRect()
    const tooltipRect = tooltipRef.current.getBoundingClientRect()
    const margin = 10
    const gap = 8

    let left = hoveredFire.x
    left = Math.max(margin, Math.min(left, mapRect.width - tooltipRect.width - margin))

    let top = hoveredFire.y - tooltipRect.height - gap
    if (top < margin) {
      top = Math.min(mapRect.height - tooltipRect.height - margin, hoveredFire.y + gap)
    }
    top = Math.max(margin, top)

    tooltipRef.current.style.left = `${left}px`
    tooltipRef.current.style.top = `${top}px`
  }, [hoveredFire])

  useEffect(() => {
    if (typeof window === 'undefined') return

    const media = window.matchMedia(`(max-width: ${MOBILE_BREAKPOINT}px)`)
    const update = () => setIsMobileMap(media.matches)

    update()
    media.addEventListener('change', update)
    return () => media.removeEventListener('change', update)
  }, [])

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
    const width = isMobileMap ? MOBILE_MAP_WIDTH : DESKTOP_MAP_WIDTH
    const height = isMobileMap ? MOBILE_MAP_HEIGHT : DESKTOP_MAP_HEIGHT
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
  }, [geojson, fires, isMobileMap, mapTilerApiKey])

  const totalAreaHa = fires.reduce((sum, fire) => sum + fire.areaHa, 0)
  const latestFire = [...fires]
    .filter((fire) => fire.date)
    .sort((a, b) => new Date(b.date!).getTime() - new Date(a.date!).getTime())[0] ?? null

  const openMunicipalityProfile = (municipalityKey: string | null) => {
    if (!municipalityKey) return false
    navigate(`/municipalities?municipality=${encodeURIComponent(municipalityKey)}`)
    return true
  }

  const getClusterMunicipalityKey = (items: HoveredFireTooltip['items']): string | null => {
    const municipalityKeys = Array.from(
      new Set(
        items
          .map((item) => cleanText(item.municipalityKey))
          .filter((key): key is string => Boolean(key)),
      ),
    )
    return municipalityKeys.length === 1 ? municipalityKeys[0] : null
  }

  if (loading) {
    return (
      <section id="copernicus" className="fire-copernicus section-rule dev-tag-anchor">
        <div className="dev-tag-stack dev-tag-stack--right">
          <ComponentTag name="FireCopernicusSection" />
          <ComponentTag name="fire-copernicus section-rule" kind="CLASS" />
        </div>
        <div className="fire-copernicus__intro dev-tag-anchor">
          <ComponentTag name="fire-copernicus__intro" kind="CLASS" className="component-tag--overlay" />
          <div className="eyebrow">Copernicus</div>
          <h2>Χάρτης δασικών πυρκαγιών</h2>
          <p>
            Τα περιστατικά προέρχονται από το ευρωπαϊκό σύστημα <a href="https://forest-fire.emergency.copernicus.eu/">Copernicus (EFFIS)</a> και βασίζονται σε δορυφορική εκτίμηση καμένων εκτάσεων.<br></br>
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
    <section id="copernicus" className="fire-copernicus section-rule dev-tag-anchor">
      <div className="dev-tag-stack dev-tag-stack--right">
        <ComponentTag name="FireCopernicusSection" />
        <ComponentTag name="fire-copernicus section-rule" kind="CLASS" />
      </div>
      <div className="fire-copernicus__intro dev-tag-anchor">
        <ComponentTag name="fire-copernicus__intro" kind="CLASS" className="component-tag--overlay" />
        <div className="eyebrow">Copernicus</div>
        <h2>Χάρτης δασικών πυρκαγιών</h2>
        <p>
          Τα περιστατικά προέρχονται από το ευρωπαϊκό σύστημα <a href="https://forest-fire.emergency.copernicus.eu/">Copernicus (EFFIS)</a> και βασίζονται σε δορυφορική εκτίμηση καμένων εκτάσεων.<br></br>
        </p>
        <div className="brand-mark fire-copernicus__brand-mark">
          Τελευταία ενημέρωση / {formatDateTimeEl(lastUpdatedAt)}
        </div>
        <div className="fire-copernicus__section-divider" aria-hidden="true" />
        <div className="fire-copernicus__date-filter-selected">
          <span className="label">Βλέπετε δεδομένα για το διάστημα</span>
          <strong>{formatDateOnlyEl(rangeStartDate)} - {formatDateOnlyEl(rangeEndDate)}</strong>
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
        <div className="fire-copernicus__date-filter" aria-label="Φίλτρο ημερομηνιών Copernicus">
          <div className="fire-copernicus__date-filter-track">
            <div
              className="fire-copernicus__date-filter-range"
              style={{
                left: `${rangeStartPercent}%`,
                width: `${Math.max(rangeEndPercent - rangeStartPercent, 0)}%`,
              }}
            />
          </div>
          <input
            type="range"
            min={0}
            max={totalDays}
            step={1}
            value={rangeStartDay}
            className="fire-copernicus__date-filter-input"
            aria-label="Έναρξη φίλτρου ημερομηνίας"
            onChange={(event) => {
              const next = Number(event.target.value)
              setRangeStartDay(Math.min(next, rangeEndDay))
            }}
          />
          <input
            type="range"
            min={0}
            max={totalDays}
            step={1}
            value={rangeEndDay}
            className="fire-copernicus__date-filter-input"
            aria-label="Λήξη φίλτρου ημερομηνίας"
            onChange={(event) => {
              const next = Number(event.target.value)
              setRangeEndDay(Math.max(next, rangeStartDay))
            }}
          />
          <div className="fire-copernicus__date-filter-years" aria-hidden="true">
            {yearMarkers.map((marker) => (
              <span
                key={marker.year}
                className="fire-copernicus__date-filter-year"
                style={{ left: `${marker.left}%` }}
              >
                <i />
                <em>{marker.year}</em>
              </span>
            ))}
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
          <div ref={mapRef} className="fire-copernicus__map dev-tag-anchor">
            <ComponentTag
              name="fire-copernicus__map"
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
                  setViewMode('shapes')
                }}
              >
                Εκτάσεις
              </button>
            </div>
            <svg
              viewBox={`0 0 ${mapData.width} ${mapData.height}`}
              role="img"
              aria-label="Χάρτης πυρκαγιών Copernicus στην Ελλάδα"
            >
              <defs>
                <clipPath id={mapClipPathId}>
                  {mapData.paths.map((feature) => (
                    <path
                      key={`clip-${feature.key}`}
                      d={feature.d}
                      transform={mapData.transform}
                    />
                  ))}
                </clipPath>
              </defs>
              <g className="fire-copernicus__base" transform={mapData.transform}>
                {mapData.paths.map((feature) => (
                  <path key={feature.key} d={feature.d} />
                ))}
              </g>
              {mapData.hillshadeTiles.length > 0 && (
                <g className="fire-copernicus__terrain" clipPath={`url(#${mapClipPathId})`} aria-hidden="true">
                  {mapData.hillshadeTiles.map((tile) => (
                    <image
                      key={tile.key}
                      href={tile.href}
                      x={tile.x}
                      y={tile.y}
                      width={tile.width}
                      height={tile.height}
                      preserveAspectRatio="none"
                      className="fire-copernicus__terrain-tile"
                    />
                  ))}
                </g>
              )}
              {viewMode === 'points' ? (
                <g className="fire-copernicus__points">
                  {mapData.points.map((fire) => (
                    <g
                      key={`${fire.x}-${fire.y}`}
                      onMouseEnter={(event) => {
                        if (isTouchInput) return
                        const pointer = pointerInMap(event, { x: fire.x, y: fire.y })
                        setHoveredFire({
                          x: pointer.x,
                          y: pointer.y,
                          items: fire.items,
                        })
                      }}
                      onMouseMove={(event) => {
                        if (isTouchInput) return
                        const pointer = pointerInMap(event, { x: fire.x, y: fire.y })
                        setHoveredFire((current) => (
                          current
                            ? { ...current, x: pointer.x, y: pointer.y }
                            : current
                        ))
                      }}
                      onMouseLeave={() => {
                        if (isTouchInput) return
                        setHoveredFire(null)
                      }}
                      onClick={(event) => {
                        const municipalityKey = getClusterMunicipalityKey(fire.items)
                        if (openMunicipalityProfile(municipalityKey)) return
                        const pointer = pointerInMap(event, { x: fire.x, y: fire.y })
                        setHoveredFire((current) => (
                          current?.items.length === fire.items.length && current.items[0]?.id === fire.items[0]?.id
                            ? null
                            : { x: pointer.x, y: pointer.y, items: fire.items }
                        ))
                      }}
                    >
                      <circle
                        className="fire-copernicus__point-hit-area"
                        cx={fire.x}
                        cy={fire.y}
                        r={12}
                        fill="transparent"
                        pointerEvents="all"
                      />
                      <circle
                        className="fire-copernicus__point-marker"
                        cx={fire.x}
                        cy={fire.y}
                        r={fire.r}
                      />
                      {fire.items.length > 1 && (
                        <text
                          x={fire.x}
                          y={fire.y}
                          textAnchor="middle"
                          dominantBaseline="central"
                          className="fire-copernicus__cluster-count"
                        >
                          {fire.items.length}
                        </text>
                      )}
                    </g>
                  ))}
                </g>
              ) : (
                <g className="fire-copernicus__shapes" transform={mapData.transform}>
                  {mapData.shapes.map((fire) => (
                    <path
                      key={fire.id}
                      d={fire.d}
                      onMouseEnter={(event) => {
                        if (isTouchInput) return
                        const pointer = pointerInMap(event, { x: fire.x, y: fire.y })
                        setHoveredFire({
                          x: pointer.x,
                          y: pointer.y,
                          items: [{
                            id: fire.id,
                            areaHa: fire.areaHa,
                            date: fire.date,
                            commune: fire.commune,
                            province: fire.province,
                            municipalityKey: fire.municipalityKey,
                          }],
                        })
                      }}
                      onMouseMove={(event) => {
                        if (isTouchInput) return
                        const pointer = pointerInMap(event, { x: fire.x, y: fire.y })
                        setHoveredFire((current) => (
                          current
                            ? { ...current, x: pointer.x, y: pointer.y }
                            : current
                        ))
                      }}
                      onMouseLeave={() => {
                        if (isTouchInput) return
                        setHoveredFire(null)
                      }}
                      onClick={(event) => {
                        if (openMunicipalityProfile(fire.municipalityKey)) return
                        const pointer = pointerInMap(event, { x: fire.x, y: fire.y })
                        setHoveredFire((current) => (
                          current?.items.length === 1 && current.items[0]?.id === fire.id
                            ? null
                            : {
                                x: pointer.x,
                                y: pointer.y,
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
                      }}
                    />
                  ))}
                </g>
              )}
            </svg>
            {hoveredFire && (
              <div
                ref={tooltipRef}
                className="fire-copernicus__tooltip app-tooltip"
                style={{
                  left: `${hoveredFire.x}px`,
                  top: `${hoveredFire.y}px`,
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
          </div>
        )}
        {mapData && (
          <div className="fire-copernicus__legend fire-copernicus__legend--map" aria-label="Υπόμνημα Copernicus">
            <span className="fire-copernicus__legend-dot" aria-hidden="true" />
            <span>{viewMode === 'points' ? 'Καταγεγραμμένη πυρκαγιά Copernicus' : 'Καμένη έκταση Copernicus'}</span>
          </div>
        )}
      </div>
    </section>
  )
}
