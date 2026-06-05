import { useCallback, useEffect, useId, useLayoutEffect, useMemo, useRef, useState } from 'react'
import * as d3 from 'd3'
import { useNavigate } from 'react-router-dom'
import type { GeoData } from '../types'
import { isAbortError } from '../lib/isAbortError'
import { loadMunicipalitiesGeojson } from '../lib/municipalitiesGeojson'
import { supabase } from '../lib/supabase'
import ComponentTag from './ComponentTag'
import DataLoadingCard from './DataLoadingCard'
import MapTilerLogo from './MapTilerLogo'

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
  sourceProduct: string
  confidence: string | null
  frp: number | null
  municipalityKey: string | null
  municipalityName: string | null
}

type FirmsRow = {
  id: number | string
  source_product: string | null
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

type FirmsTooltip = {
  x: number
  y: number
  placement: 'above' | 'below'
  items: FirmsDetection[]
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
const DESKTOP_MAP_WIDTH = 760
const DESKTOP_MAP_HEIGHT = 520
const MOBILE_MAP_WIDTH = 760
const MOBILE_MAP_HEIGHT = 860
const DESKTOP_MAP_SCALE = 1.08
const MOBILE_MAP_SCALE = 1.22
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

function toNumber(value: unknown): number | null {
  const number = Number(value)
  return Number.isFinite(number) ? number : null
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

function formatMegawatts(value: number | null): string {
  if (value == null) return '—'
  return `${value.toLocaleString('el-GR', { maximumFractionDigits: 1 })} MW`
}

function formatObservationCount(count: number): string {
  const formattedCount = count.toLocaleString('el-GR')
  return `${formattedCount} ${count === 1 ? 'παρατήρηση' : 'παρατηρήσεις'}`
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

function buildFootprint(
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

export default function FireFirmsSection() {
  const navigate = useNavigate()
  const mapTilerApiKey = useMemo(() => cleanText(import.meta.env.VITE_MAPTILER_API_KEY), [])
  const [geojson, setGeojson] = useState<GeoData | null>(null)
  const [detections, setDetections] = useState<FirmsDetection[]>([])
  const [loading, setLoading] = useState(true)
  const [hoveredDetection, setHoveredDetection] = useState<FirmsTooltip | null>(null)
  const [terrainFailed, setTerrainFailed] = useState(false)
  const [mapSize, setMapSize] = useState<{ width: number; height: number } | null>(null)
  const mapRef = useRef<HTMLDivElement | null>(null)
  const mapClipPathId = useId().replace(/:/g, '-')
  const [isMobileMap, setIsMobileMap] = useState(() => {
    if (typeof window === 'undefined') return false
    return window.innerWidth <= MOBILE_BREAKPOINT
  })

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

  useEffect(() => {
    let cancelled = false
    const controller = new AbortController()

    const load = async () => {
      try {
        const firmsSince = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()
        const [geoFetchResult, firmsResult] = await Promise.allSettled([
          loadMunicipalitiesGeojson(),
          supabase
            .from('firms_active_fire_detections')
            .select('id, source_product, acquired_at, acquired_at_el, latitude, longitude, scan, track, satellite, instrument, confidence, frp, municipality_key, municipality_normalized_value')
            .gte('acquired_at', firmsSince)
            .eq('is_in_greece', true)
            .order('acquired_at', { ascending: false })
            .limit(600)
            .abortSignal(controller.signal),
        ])

        if (geoFetchResult.status === 'rejected') throw geoFetchResult.reason
        const geoData = geoFetchResult.value
        const firmsRes = firmsResult.status === 'fulfilled' ? firmsResult.value : null
        const rows = !firmsRes?.error ? ((firmsRes?.data ?? []) as FirmsRow[]) : []
        const nextDetections = rows
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
              sourceProduct: cleanText(row.source_product) ?? 'FIRMS',
              confidence: cleanText(row.confidence),
              frp: toNumber(row.frp),
              municipalityKey: cleanText(row.municipality_key),
              municipalityName: cleanText(row.municipality_normalized_value),
            } satisfies FirmsDetection
          })
          .filter((row): row is FirmsDetection => row !== null)

        if (!cancelled) {
          setGeojson(geoData)
          setDetections(nextDetections)
        }
      } catch (error) {
        if (isAbortError(error)) return
        if (!cancelled) {
          setGeojson(null)
          setDetections([])
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
  }, [])

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

    const updateMapSize = () => {
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

    updateMapSize()
    const observer = new ResizeObserver(updateMapSize)
    observer.observe(mapRef.current)
    window.addEventListener('orientationchange', updateMapSize)

    return () => {
      observer.disconnect()
      window.removeEventListener('orientationchange', updateMapSize)
    }
  }, [geojson])

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
    const transformPoint = (x: number, y: number) => ({
      x: x * transformScale + transformTranslateX,
      y: y * transformScale + transformTranslateY,
    })

    const footprints = detections
      .map((detection) => {
        const rect = buildFootprint(projection, detection, transformPoint)
        if (!rect) return null
        return { ...detection, ...rect }
      })
      .filter((detection): detection is FirmsDetection & { x: number; y: number; width: number; height: number } => detection !== null)

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
      footprints,
    }
  }, [detections, geojson, isMobileMap, mapSize, mapTilerApiKey])

  const latestDetection = detections
    .filter((detection) => detection.acquiredAt)
    .sort((a, b) => new Date(b.acquiredAt!).getTime() - new Date(a.acquiredAt!).getTime())[0] ?? null

  const satellites = useMemo(() => {
    const bySatellite = new Map<string, { detection: FirmsDetection; count: number }>()
    detections.forEach((detection) => {
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
  }, [detections])

  const openMunicipalityProfile = useCallback((municipalityKey: string | null) => {
    if (!municipalityKey) return false
    navigate(`/municipalities?municipality=${encodeURIComponent(municipalityKey)}`)
    return true
  }, [navigate])

  if (loading) {
    return (
      <section id="firms" className="fire-copernicus fire-firms section-rule dev-tag-anchor">
        <div className="dev-tag-stack dev-tag-stack--right">
          <ComponentTag name="FireFirmsSection" />
          <ComponentTag name="fire-copernicus fire-firms section-rule" kind="CLASS" />
        </div>
        <div className="fire-copernicus__intro dev-tag-anchor">
          <ComponentTag name="fire-copernicus__intro" kind="CLASS" className="component-tag--overlay" />
          <div className="eyebrow">NASA FIRMS</div>
          <h2>Θερμικές ανωμαλίες εδάφους</h2>
          <p>
            Δορυφορικές παρατηρήσεις από δορυφόρους της NASA που καταγράφουν θερμικές ανωμαλίες στο έδαφος της Ελλάδας. Μία θερμική ανωμαλία δεν συνεπάγεται οτι αποτελεί πυρκαγιά.
          </p>
        </div>
        <DataLoadingCard
          className="fire-copernicus__loading-card"
          message="Ανακτώνται οι ενεργές θερμικές ανωμαλίες NASA FIRMS."
        />
      </section>
    )
  }

  return (
    <section id="firms" className="fire-copernicus fire-firms section-rule dev-tag-anchor">
      <div className="dev-tag-stack dev-tag-stack--right">
        <ComponentTag name="FireFirmsSection" />
        <ComponentTag name="fire-copernicus fire-firms section-rule" kind="CLASS" />
      </div>
      <div className="fire-copernicus__intro dev-tag-anchor">
        <ComponentTag name="fire-copernicus__intro" kind="CLASS" className="component-tag--overlay" />
        <div className="eyebrow">NASA FIRMS</div>
        <h2>Θερμικές ανωμαλίες εδάφους</h2>
        <p>
          Δορυφορικές παρατηρήσεις από δορυφόρους της NASA που καταγράφουν θερμικές ανωμαλίες στο έδαφος της Ελλάδας. Μία θερμική ανωμαλία δεν συνεπάγεται οτι αποτελεί πυρκαγιά.
        </p>
        <div className="brand-mark fire-copernicus__brand-mark">
          Τελευταία παρατήρηση / {formatDateTimeEl(latestDetection?.acquiredAtEl ?? latestDetection?.acquiredAt ?? null)}
        </div>
        <div className="fire-firms__satellites" aria-label="Τελευταίες διελεύσεις δορυφόρων NASA FIRMS">
          {satellites.length > 0 ? satellites.map(({ detection, count }) => (
            <div key={`${detection.satellite}-${detection.instrument}-${detection.sourceProduct}`}>
              <span>{detection.satellite} / {detection.instrument} · {formatObservationCount(count)}</span>
              <strong>{formatDateTimeEl(detection.acquiredAtEl ?? detection.acquiredAt)}</strong>
            </div>
          )) : (
            <div>
              <span>Δεν υπάρχουν διαθέσιμες διελεύσεις</span>
              <strong>—</strong>
            </div>
          )}
        </div>
        <p className="fire-copernicus__note">
          Τα τετράγωνα αποτυπώνουν το δορυφορικό ίχνος κάθε καταγραφής με βάση τις τιμές scan και track του FIRMS.
        </p>
      </div>

      <div className="fire-copernicus__map-wrap dev-tag-anchor">
        <ComponentTag
          name="fire-copernicus__map-wrap"
          kind="CLASS"
          className="component-tag--overlay"
          style={{ left: 'auto', right: '0.45rem' }}
        />
        {!mapData && <div className="fire-copernicus__empty">Δεν ήταν δυνατή η φόρτωση των δεδομένων NASA FIRMS.</div>}
        {mapData && (
          <div ref={mapRef} className="fire-copernicus__map fire-firms__map dev-tag-anchor" onMouseLeave={() => setHoveredDetection(null)}>
            <ComponentTag
              name="fire-copernicus__map fire-firms__map"
              kind="CLASS"
              className="component-tag--overlay"
              style={{ left: 'auto', right: '0.45rem' }}
            />
            <svg
              viewBox={`0 0 ${mapData.width} ${mapData.height}`}
              role="img"
              aria-label="Χάρτης ενεργών θερμικών ανωμαλιών NASA FIRMS στην Ελλάδα"
            >
              <defs>
                <clipPath id={mapClipPathId}>
                  {mapData.paths.map((feature) => (
                    <path key={feature.key} d={feature.d} transform={mapData.transform} />
                  ))}
                </clipPath>
              </defs>
              <g className="fire-copernicus__base" transform={mapData.transform}>
                {mapData.paths.map((feature) => (
                  <path key={feature.key} d={feature.d} />
                ))}
              </g>
              {mapData.hillshadeTiles.length > 0 && !terrainFailed && (
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
                      onError={() => setTerrainFailed(true)}
                    />
                  ))}
                </g>
              )}
              <g className="fire-firms__footprints">
                {mapData.footprints.map((footprint, index) => (
                  <g
                    key={`${footprint.id}-${index}`}
                    onMouseEnter={(event) => {
                      if (isMobileMap) return
                      const pointer = pointerInMap(event, { x: footprint.x, y: footprint.y })
                      setHoveredDetection({
                        x: pointer.x,
                        y: pointer.y,
                        placement: pointer.y < 96 ? 'below' : 'above',
                        items: [footprint],
                      })
                    }}
                    onMouseMove={(event) => {
                      if (isMobileMap) return
                      const pointer = pointerInMap(event, { x: footprint.x, y: footprint.y })
                      setHoveredDetection((current) => (
                        current ? { ...current, x: pointer.x, y: pointer.y, placement: pointer.y < 96 ? 'below' : 'above' } : current
                      ))
                    }}
                    onMouseLeave={() => {
                      if (isMobileMap) return
                      setHoveredDetection(null)
                    }}
                    onClick={(event) => {
                      event.preventDefault()
                      if (openMunicipalityProfile(footprint.municipalityKey)) return
                      const pointer = pointerInMap(event, { x: footprint.x, y: footprint.y })
                      setHoveredDetection((current) => (
                        current?.items[0]?.id === footprint.id
                          ? null
                          : {
                              x: pointer.x,
                              y: pointer.y,
                              placement: pointer.y < 96 ? 'below' : 'above',
                              items: [footprint],
                            }
                      ))
                    }}
                  >
                    <rect
                      className="fire-firms__footprint"
                      x={footprint.x - footprint.width / 2}
                      y={footprint.y - footprint.height / 2}
                      width={footprint.width}
                      height={footprint.height}
                    />
                  </g>
                ))}
              </g>
            </svg>
            {mapData.hillshadeTiles.length > 0 && !terrainFailed && (
              <MapTilerLogo className="fire-copernicus__maptiler-logo" />
            )}
            {hoveredDetection && (
              <div
                className="fire-copernicus__tooltip app-tooltip"
                style={{
                  left: `${Math.max(12, hoveredDetection.x + 14)}px`,
                  top: `${hoveredDetection.placement === 'below' ? hoveredDetection.y + 14 : Math.max(12, hoveredDetection.y - 14)}px`,
                  transform: hoveredDetection.placement === 'below' ? 'none' : undefined,
                  pointerEvents: 'none',
                }}
              >
                {hoveredDetection.items.slice(0, 4).map((item) => (
                  <div key={item.id} className="fire-copernicus__tooltip-item">
                    <strong>{item.municipalityName ?? 'Άγνωστη περιοχή'}</strong>
                    <span>{item.satellite} / {item.instrument}</span>
                    <span>{formatDateTimeEl(item.acquiredAtEl ?? item.acquiredAt)}</span>
                    <span>Θερμική Ενέργεια: {formatMegawatts(item.frp)}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
        {mapData && (
          <div className="fire-copernicus__legend fire-copernicus__legend--map" aria-label="Υπόμνημα NASA FIRMS">
            <span className="fire-copernicus__legend-dot fire-firms__legend-square" aria-hidden="true" />
            <span>Ενεργή θερμική ανωμαλία NASA FIRMS</span>
          </div>
        )}
      </div>
    </section>
  )
}
