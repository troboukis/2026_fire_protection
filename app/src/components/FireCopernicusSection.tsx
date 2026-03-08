import { useEffect, useMemo, useState } from 'react'
import * as d3 from 'd3'
import type { GeoData } from '../types'
import { supabase } from '../lib/supabase'

type CopernicusFirePoint = {
  id: string
  lat: number
  lon: number
  shape: GeoJSON.Geometry | null
  areaHa: number
  date: string | null
  commune: string | null
  province: string | null
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

const CLUSTER_GRID_SIZE = 8

export default function FireCopernicusSection() {
  const currentYear = useMemo(() => new Date().getFullYear(), [])
  const today = useMemo(() => toDayStart(new Date()), [])
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
            .select('copernicus_id, centroid, shape, area_ha, firedate, commune, province')
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
    const width = 760
    const height = 460
    const projection = d3.geoMercator().fitExtent(
      [[24, 18], [width - 24, height - 26]],
      geojson as unknown as d3.ExtendedFeatureCollection,
    )
    const path = d3.geoPath().projection(projection)

    return {
      paths: geojson.features.map((feature, idx) => ({
        key: `${feature.properties.municipality_code}-${idx}`,
        d: path(feature as unknown as d3.GeoPermissibleObjects) ?? '',
      })),
      points: Object.values(
        fires.reduce<Record<string, {
          x: number
          y: number
          r: number
          items: HoveredFireTooltip['items']
        }>>((acc, fire) => {
          const projected = projection([fire.lon, fire.lat])
          if (!projected) return acc
          const [x, y] = projected
          if (!Number.isFinite(x) || !Number.isFinite(y)) return acc
          const key = `${Math.round(x / CLUSTER_GRID_SIZE)}:${Math.round(y / CLUSTER_GRID_SIZE)}`
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
          const [x, y] = centroid
          if (![x, y].every(Number.isFinite)) return null
          return {
            ...fire,
            d,
            x,
            y,
          }
        })
        .filter((shape): shape is CopernicusFirePoint & { d: string; x: number; y: number } => shape !== null),
    }
  }, [geojson, fires])

  const totalAreaHa = fires.reduce((sum, fire) => sum + fire.areaHa, 0)
  const latestFire = [...fires]
    .filter((fire) => fire.date)
    .sort((a, b) => new Date(b.date!).getTime() - new Date(a.date!).getTime())[0] ?? null

  return (
    <section id="copernicus" className="fire-copernicus section-rule">
      <div className="fire-copernicus__intro">
        <div className="eyebrow">Copernicus</div>
        <h2>Χάρτης δασικών πυρκαγιών</h2>
        <p>
          Τα περιστατικά προέρχονται από το ευρωπαϊκό σύστημα <a href="https://forest-fire.emergency.copernicus.eu/">Copernicus (EFFIS)</a> και βασίζονται σε δορυφορική εκτίμηση καμένων εκτάσεων.<br></br>
        </p>
        <div className="brand-mark fire-copernicus__brand-mark">
          Τελευταία ενημέρωση / {loading ? '…' : formatDateTimeEl(lastUpdatedAt)}
        </div>
        <div className="fire-copernicus__section-divider" aria-hidden="true" />
        <div className="fire-copernicus__date-filter-selected">
          <span className="label">Βλέπετε δεδομένα για το διάστημα</span>
          <strong>{formatDateOnlyEl(rangeStartDate)} - {formatDateOnlyEl(rangeEndDate)}</strong>
        </div>
        <div className="fire-copernicus__stats">
          <div>
            <span className="label">Συμβάντα</span>
            <strong>{loading ? '…' : fires.length.toLocaleString('el-GR')}</strong>
          </div>
          <div>
            <span className="label">Καμένη Έκταση</span>
            <strong>{loading ? '…' : formatStremmata(totalAreaHa)}</strong>
          </div>
          <div>
            <span className="label">Τελευταία Εγγραφή</span>
            <strong>{loading ? '…' : formatDateEl(latestFire?.date ?? null)}</strong>
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
          <div className="fire-copernicus__section-divider" aria-hidden="true" />
        </div>
        <div className="fire-copernicus__legend" aria-label="Υπόμνημα Copernicus">
          <span className="fire-copernicus__legend-dot" aria-hidden="true" />
          <span>{viewMode === 'points' ? 'Καταγεγραμμένη πυρκαγιά Copernicus' : 'Καμένη έκταση Copernicus'}</span>
        </div>
        <p className="fire-copernicus__note">
          Σημείωση: Οι ημερομηνίες και τα μεγέθη ενδέχεται να αναθεωρηθούν καθώς ενημερώνεται η βάση δεδομένων.
        </p>
      </div>

      <div className="fire-copernicus__map-wrap">
        {loading && <div className="fire-copernicus__empty">Φόρτωση Copernicus δεδομένων…</div>}
        {!loading && !mapData && <div className="fire-copernicus__empty">Δεν ήταν δυνατή η φόρτωση των δεδομένων Copernicus.</div>}
        {!loading && mapData && (
          <div className="fire-copernicus__map">
            <div className="fire-copernicus__toggle" aria-label="Τρόπος προβολής Copernicus">
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
            <svg viewBox="0 0 760 460" role="img" aria-label="Χάρτης πυρκαγιών Copernicus στην Ελλάδα">
              <g className="fire-copernicus__base">
                {mapData.paths.map((feature) => (
                  <path key={feature.key} d={feature.d} />
                ))}
              </g>
              {viewMode === 'points' ? (
                <g className="fire-copernicus__points">
                  {mapData.points.map((fire) => (
                    <g
                      key={`${fire.x}-${fire.y}`}
                      onMouseEnter={() => setHoveredFire({
                        x: fire.x,
                        y: fire.y,
                        items: fire.items,
                      })}
                      onMouseLeave={() => setHoveredFire(null)}
                    >
                      <circle
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
                <g className="fire-copernicus__shapes">
                  {mapData.shapes.map((fire) => (
                    <path
                      key={fire.id}
                      d={fire.d}
                      onMouseEnter={() => setHoveredFire({
                        x: fire.x,
                        y: fire.y,
                        items: [{
                          id: fire.id,
                          areaHa: fire.areaHa,
                          date: fire.date,
                          commune: fire.commune,
                          province: fire.province,
                        }],
                      })}
                      onMouseLeave={() => setHoveredFire(null)}
                    />
                  ))}
                </g>
              )}
            </svg>
            {hoveredFire && (
              <div
                className="fire-copernicus__tooltip"
                style={{
                  left: `${Math.min(hoveredFire.x + 14, 760 - 220)}px`,
                  top: `${Math.max(hoveredFire.y - 18, 12)}px`,
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
      </div>
    </section>
  )
}
