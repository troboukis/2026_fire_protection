import { useCallback, useMemo, useState } from 'react'
import * as d3 from 'd3'
import ComponentTag from './ComponentTag'
import EditorialLead from './EditorialLead'
import LatestContractCard, { type LatestContractCardView } from './LatestContractCard'
import type { GeoFeature } from '../types'

export type SelectionSource = 'dropdown' | 'map' | 'search'
export type SelectionKind = 'region' | 'municipality'

function normalizeGreekName(input: string): string {
  return input
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/ς/g, 'σ')
    .trim()
}

function shouldUseHeartMarker(name: string): boolean {
  const n = normalizeGreekName(name)
  return n === 'αντικυρα' || n === 'ασπρα σπιτια'
}

const PREVIEW_WIDTH = 520
const PREVIEW_HEIGHT = 320

type Props = {
  source: SelectionSource | null
  kind: SelectionKind | null
  label: string
  municipalityLatestContracts?: LatestContractCardView[]
  municipalityLatestLoading?: boolean
  regionLatestContracts?: LatestContractCardView[]
  regionLatestLoading?: boolean
  municipalityFeature?: GeoFeature | null
  municipalityFirePoints?: Array<{
    lat: number
    lon: number
    period: 'current' | 'previous'
    areaHa: number
    commune: string
    province: string
    shape: GeoJSON.Geometry | null
  }>
  municipalityDirectWorkPoints?: Array<{ lat: number; lon: number; work: string; pointName: string }>
  municipalityRegionalWorkPoints?: Array<{ lat: number; lon: number; work: string; pointName: string }>
  municipalityWorkLoading?: boolean
  municipalityFireLoading?: boolean
  cityPoints?: Array<{ lat: number; lon: number; name: string }>
  currentYear: number
  regionCurrentYearCount?: number | null
  municipalityCurrentYearCount?: number | null
  onContractOpen?: (id: string) => void
}

export default function MapSelectionPanel({
  source,
  kind,
  label,
  municipalityLatestContracts = [],
  municipalityLatestLoading = false,
  regionLatestContracts = [],
  regionLatestLoading = false,
  municipalityFeature,
  municipalityFirePoints = [],
  municipalityDirectWorkPoints = [],
  municipalityRegionalWorkPoints = [],
  municipalityWorkLoading = false,
  municipalityFireLoading = false,
  cityPoints = [],
  currentYear,
  regionCurrentYearCount,
  municipalityCurrentYearCount,
  onContractOpen,
}: Props) {
  const isTouchInput = useMemo(() => {
    if (typeof window === 'undefined') return false
    return window.matchMedia('(hover: none), (pointer: coarse)').matches
  }, [])
  const [fireViewMode, setFireViewMode] = useState<'points' | 'shapes'>('points')
  const [hoveredFirePoint, setHoveredFirePoint] = useState<{
    x: number
    y: number
    period: 'current' | 'previous'
    areaHa: number
    commune: string
    province: string
  } | null>(null)
  const [hoveredWorkPoint, setHoveredWorkPoint] = useState<{
    x: number
    y: number
    work: string
    pointName: string
    scope: 'municipality' | 'region'
  } | null>(null)
  const previewGeometry = useMemo(() => {
    if (!municipalityFeature) return null
    const width = PREVIEW_WIDTH
    const height = PREVIEW_HEIGHT
    const pad = 12
    const projection = d3.geoMercator().fitExtent(
      [[pad, pad], [width - pad, height - pad]],
      municipalityFeature as unknown as d3.GeoPermissibleObjects,
    )
    const path = d3.geoPath().projection(projection)
    return {
      path: path(municipalityFeature as unknown as d3.GeoPermissibleObjects),
      projection,
    }
  }, [municipalityFeature])

  const projectedFireShapes = useMemo(() => {
    if (!previewGeometry) return [] as Array<{
      d: string
      x: number
      y: number
      period: 'current' | 'previous'
      areaHa: number
      commune: string
      province: string
    }>
    const path = d3.geoPath().projection(previewGeometry.projection)
    const out: Array<{
      d: string
      x: number
      y: number
      period: 'current' | 'previous'
      areaHa: number
      commune: string
      province: string
    }> = []
    for (const fire of municipalityFirePoints) {
      let d = ''
      let x = 0
      let y = 0
      if (fire.shape) {
        const shapePath = path(fire.shape as unknown as d3.GeoPermissibleObjects)
        const centroid = path.centroid(fire.shape as unknown as d3.GeoPermissibleObjects)
        if (
          shapePath &&
          Array.isArray(centroid) &&
          centroid.length === 2 &&
          Number.isFinite(centroid[0]) &&
          Number.isFinite(centroid[1])
        ) {
          d = shapePath
          ;[x, y] = centroid
        }
      }
      if (!d) {
        const projected = previewGeometry.projection([fire.lon, fire.lat])
        if (!projected) continue
        ;[x, y] = projected
        if (!Number.isFinite(x) || !Number.isFinite(y)) continue
        d = `M ${x} ${y} m -4 0 a 4 4 0 1 0 8 0 a 4 4 0 1 0 -8 0`
      }
      out.push({
        d,
        x,
        y,
        period: fire.period,
        areaHa: fire.areaHa,
        commune: fire.commune,
        province: fire.province,
      })
    }
    return out
  }, [previewGeometry, municipalityFirePoints])

  const projectedCurrentFireShapes = projectedFireShapes.filter((p) => p.period === 'current')
  const projectedPreviousFireShapes = projectedFireShapes.filter((p) => p.period === 'previous')
  const projectedFirePoints = useMemo(() => {
    if (!previewGeometry) return [] as Array<{
      x: number
      y: number
      period: 'current' | 'previous'
      areaHa: number
      commune: string
      province: string
    }>
    const out: Array<{
      x: number
      y: number
      period: 'current' | 'previous'
      areaHa: number
      commune: string
      province: string
    }> = []
    for (const point of municipalityFirePoints) {
      const projected = previewGeometry.projection([point.lon, point.lat])
      if (!projected) continue
      const [x, y] = projected
      if (!Number.isFinite(x) || !Number.isFinite(y)) continue
      out.push({
        x,
        y,
        period: point.period,
        areaHa: point.areaHa,
        commune: point.commune,
        province: point.province,
      })
    }
    return out
  }, [previewGeometry, municipalityFirePoints])

  const projectedCurrentFirePoints = projectedFirePoints.filter((p) => p.period === 'current')
  const projectedPreviousFirePoints = projectedFirePoints.filter((p) => p.period === 'previous')

  const projectWorkDots = useCallback((
    points: Array<{ lat: number; lon: number; work: string; pointName: string }>,
    scope: 'municipality' | 'region',
  ) => {
    if (!previewGeometry) return [] as Array<{ x: number; y: number; work: string; pointName: string; scope: 'municipality' | 'region' }>
    const out: Array<{ x: number; y: number; work: string; pointName: string; scope: 'municipality' | 'region' }> = []
    for (const point of points) {
      if (municipalityFeature) {
        const inside = d3.geoContains(
          municipalityFeature as unknown as d3.GeoPermissibleObjects,
          [point.lon, point.lat],
        )
        if (!inside) continue
      }
      const projected = previewGeometry.projection([point.lon, point.lat])
      if (!projected) continue
      const [x, y] = projected
      if (!Number.isFinite(x) || !Number.isFinite(y)) continue
      out.push({ x, y, work: point.work, pointName: point.pointName, scope })
    }
    return out
  }, [municipalityFeature, previewGeometry])

  const projectedMunicipalityWorkDots = useMemo(
    () => projectWorkDots(municipalityDirectWorkPoints, 'municipality'),
    [municipalityDirectWorkPoints, projectWorkDots],
  )

  const projectedRegionalWorkDots = useMemo(
    () => projectWorkDots(municipalityRegionalWorkPoints, 'region'),
    [municipalityRegionalWorkPoints, projectWorkDots],
  )

  const uniqueFireLocations = useMemo(() => {
    const uniq = new Set<string>()
    for (const p of municipalityFirePoints) {
      uniq.add(`${p.lat.toFixed(6)}|${p.lon.toFixed(6)}`)
    }
    return uniq.size
  }, [municipalityFirePoints])

  const projectedCityPoints = useMemo(() => {
    if (!previewGeometry || !municipalityFeature || cityPoints.length === 0) {
      return [] as Array<{ x: number; y: number; name: string; textX: number; textY: number; textAnchor: 'start' | 'end' }>
    }

    type Box = { x0: number; y0: number; x1: number; y1: number }
    type Candidate = {
      textX: number
      textY: number
      textAnchor: 'start' | 'end'
      box: Box
    }

    const width = 520
    const height = 320
    const fontPx = 12.8
    const charPx = 7.1
    const gap = 4.2
    const pad = 2
    const placedBoxes: Box[] = []
    const out: Array<{ x: number; y: number; name: string; textX: number; textY: number; textAnchor: 'start' | 'end' }> = []

    const overlapArea = (a: Box, b: Box) => {
      const w = Math.max(0, Math.min(a.x1, b.x1) - Math.max(a.x0, b.x0))
      const h = Math.max(0, Math.min(a.y1, b.y1) - Math.max(a.y0, b.y0))
      return w * h
    }

    const makeCandidate = (
      x: number,
      y: number,
      nameWidth: number,
      dx: number,
      dy: number,
      anchor: 'start' | 'end',
    ): Candidate => {
      const textX = x + dx
      const textY = y + dy
      const xStart = anchor === 'start' ? textX : textX - nameWidth
      const xEnd = anchor === 'start' ? textX + nameWidth : textX
      return {
        textX,
        textY,
        textAnchor: anchor,
        box: {
          x0: xStart - pad,
          x1: xEnd + pad,
          y0: textY - fontPx + 1,
          y1: textY + 2,
        },
      }
    }

    for (const c of cityPoints) {
      const inside = d3.geoContains(
        municipalityFeature as unknown as d3.GeoPermissibleObjects,
        [c.lon, c.lat],
      )
      if (!inside) continue
      const projected = previewGeometry.projection([c.lon, c.lat])
      if (!projected) continue
      const [x, y] = projected
      if (!Number.isFinite(x) || !Number.isFinite(y)) continue
      const nameWidth = Math.max(16, c.name.length * charPx)
      const candidates: Candidate[] = [
        makeCandidate(x, y, nameWidth, gap, -3, 'start'),
        makeCandidate(x, y, nameWidth, -(gap + 1), -3, 'end'),
        makeCandidate(x, y, nameWidth, gap, -(fontPx + 4), 'start'),
        makeCandidate(x, y, nameWidth, -(gap + 1), -(fontPx + 4), 'end'),
        makeCandidate(x, y, nameWidth, gap, fontPx - 2, 'start'),
        makeCandidate(x, y, nameWidth, -(gap + 1), fontPx - 2, 'end'),
        makeCandidate(x, y, nameWidth, -nameWidth / 2, -(fontPx + 6), 'start'),
        makeCandidate(x, y, nameWidth, -nameWidth / 2, fontPx, 'start'),
      ]

      let best = candidates[0]
      let bestScore = Number.POSITIVE_INFINITY
      for (const cand of candidates) {
        const outOfBoundsPenalty =
          (cand.box.x0 < 0 ? 1 : 0) +
          (cand.box.y0 < 0 ? 1 : 0) +
          (cand.box.x1 > width ? 1 : 0) +
          (cand.box.y1 > height ? 1 : 0)

        let overlap = 0
        for (const b of placedBoxes) overlap += overlapArea(cand.box, b)
        const score = overlap + outOfBoundsPenalty * 10000
        if (score < bestScore) {
          bestScore = score
          best = cand
        }
      }

      placedBoxes.push(best.box)
      out.push({
        x,
        y,
        name: c.name,
        textX: best.textX,
        textY: best.textY,
        textAnchor: best.textAnchor,
      })
    }
    return out
  }, [previewGeometry, municipalityFeature, cityPoints])

  const title =
    kind === 'municipality' && label
      ? `Δήμος ${label}`
      : kind === 'region' && label
        ? `Περιφέρεια ${label}`
        : 'Επιλογή περιοχής'

  const subtitle =
    !kind
      ? 'Μπορείτε να επιλέξετε δήμο στον χάρτη ή με τα αντίστοιχα φίλτρα.'
      : kind === 'municipality'
        ? `Το ${currentYear} έχουν δημοσιευθεί ${(municipalityCurrentYearCount ?? 0).toLocaleString('el-GR')} συμβάσεις που σχετίζονται με την πρόληψη ή αντιμετώπιση δασικών πυρκαγιών.`
        : `Το ${currentYear} έχουν δημοσιευθεί ${(regionCurrentYearCount ?? 0).toLocaleString('el-GR')} συμβάσεις που σχετίζονται με την πρόληψη ή αντιμετώπιση δασικών πυρκαγιών στην επιλεγμένη περιφέρεια.`

  return (
    <aside className="maps-selection-panel" data-selection-source={source ?? 'none'}>
      <ComponentTag name="MapSelectionPanel" />
      <EditorialLead eyebrow="Ανάλυση" title={title} subtitle={subtitle} />

      {kind === 'municipality' && previewGeometry && (
        <div className="maps-selection-panel__shape" aria-label="Πολύγωνο δήμου">
          {municipalityFirePoints.length > 0 && (
            <div className="maps-selection-panel__fire-toggle" aria-label="Τρόπος προβολής πυρκαγιών">
              <button
                type="button"
                className={fireViewMode === 'points' ? 'is-active' : ''}
                onClick={() => {
                  setHoveredFirePoint(null)
                  setFireViewMode('points')
                }}
              >
                Σημεία
              </button>
              <button
                type="button"
                className={fireViewMode === 'shapes' ? 'is-active' : ''}
                onClick={() => {
                  setHoveredFirePoint(null)
                  setFireViewMode('shapes')
                }}
              >
                Εκτάσεις
              </button>
            </div>
          )}
          <svg viewBox="0 0 520 320" role="img" aria-label={`Polygon ${label}`}>
            <path d={previewGeometry.path ?? ''} fill="#1a3a5c" fillOpacity="0.92" stroke="#0f2237" strokeWidth="1.3" />
            {fireViewMode === 'shapes'
              ? (
                <>
                  {projectedPreviousFireShapes.map((p, idx) => (
                    <g
                      key={`fire-shape-prev-${idx}`}
                      onMouseEnter={() => {
                        if (isTouchInput) return
                        setHoveredFirePoint({
                          x: p.x,
                          y: p.y,
                          period: 'previous',
                          areaHa: p.areaHa,
                          commune: p.commune,
                          province: p.province,
                        })
                      }}
                      onMouseLeave={() => setHoveredFirePoint((current) => (
                        isTouchInput
                          ? current
                          :
                        current?.x === p.x && current?.y === p.y && current?.period === 'previous' ? null : current
                      ))}
                      onClick={() => setHoveredFirePoint((current) => (
                        current?.x === p.x && current?.y === p.y && current?.period === 'previous'
                          ? null
                          : { x: p.x, y: p.y, period: 'previous', areaHa: p.areaHa, commune: p.commune, province: p.province }
                      ))}
                    >
                      <path
                        d={p.d}
                        fill="#dadada"
                        fillOpacity={0.85}
                      />
                    </g>
                  ))}
                  {projectedCurrentFireShapes.map((p, idx) => (
                    <g
                      key={`fire-shape-current-${idx}`}
                      onMouseEnter={() => {
                        if (isTouchInput) return
                        setHoveredFirePoint({
                          x: p.x,
                          y: p.y,
                          period: 'current',
                          areaHa: p.areaHa,
                          commune: p.commune,
                          province: p.province,
                        })
                      }}
                      onMouseLeave={() => setHoveredFirePoint((current) => (
                        isTouchInput
                          ? current
                          :
                        current?.x === p.x && current?.y === p.y && current?.period === 'current' ? null : current
                      ))}
                      onClick={() => setHoveredFirePoint((current) => (
                        current?.x === p.x && current?.y === p.y && current?.period === 'current'
                          ? null
                          : { x: p.x, y: p.y, period: 'current', areaHa: p.areaHa, commune: p.commune, province: p.province }
                      ))}
                    >
                      <path
                        d={p.d}
                        fill="#ff3b30"
                        fillOpacity={0.8}
                      />
                    </g>
                  ))}
                </>
              )
              : (
                <>
                  {projectedPreviousFirePoints.map((p, idx) => (
                    <g
                      key={`fire-point-prev-${idx}`}
                      onMouseEnter={() => {
                        if (isTouchInput) return
                        setHoveredFirePoint({
                          x: p.x,
                          y: p.y,
                          period: 'previous',
                          areaHa: p.areaHa,
                          commune: p.commune,
                          province: p.province,
                        })
                      }}
                      onMouseLeave={() => setHoveredFirePoint((current) => (
                        isTouchInput
                          ? current
                          :
                        current?.x === p.x && current?.y === p.y && current?.period === 'previous' ? null : current
                      ))}
                      onClick={() => setHoveredFirePoint((current) => (
                        current?.x === p.x && current?.y === p.y && current?.period === 'previous'
                          ? null
                          : { x: p.x, y: p.y, period: 'previous', areaHa: p.areaHa, commune: p.commune, province: p.province }
                      ))}
                    >
                      <circle cx={p.x} cy={p.y} r={12} fill="rgba(0, 0, 0, 0.001)" />
                      <circle cx={p.x} cy={p.y} r={4} fill="#dadada" fillOpacity={0.85} />
                    </g>
                  ))}
                  {projectedCurrentFirePoints.map((p, idx) => (
                    <g
                      key={`fire-point-current-${idx}`}
                      onMouseEnter={() => {
                        if (isTouchInput) return
                        setHoveredFirePoint({
                          x: p.x,
                          y: p.y,
                          period: 'current',
                          areaHa: p.areaHa,
                          commune: p.commune,
                          province: p.province,
                        })
                      }}
                      onMouseLeave={() => setHoveredFirePoint((current) => (
                        isTouchInput
                          ? current
                          :
                        current?.x === p.x && current?.y === p.y && current?.period === 'current' ? null : current
                      ))}
                      onClick={() => setHoveredFirePoint((current) => (
                        current?.x === p.x && current?.y === p.y && current?.period === 'current'
                          ? null
                          : { x: p.x, y: p.y, period: 'current', areaHa: p.areaHa, commune: p.commune, province: p.province }
                      ))}
                    >
                      <circle cx={p.x} cy={p.y} r={12} fill="rgba(0, 0, 0, 0.001)" />
                      <circle cx={p.x} cy={p.y} r={4.5} fill="#ff3b30" fillOpacity={0.8} />
                    </g>
                  ))}
                </>
              )}
            {projectedCityPoints.map((p, idx) => (
              <g key={`city-${p.name}-${idx}`} className="maps-city-point">
                {shouldUseHeartMarker(p.name) ? (
                  <text className="maps-city-heart" x={p.x} y={p.y + 1.8} textAnchor="middle">♥</text>
                ) : (
                  <circle cx={p.x} cy={p.y} r={2.6} fill="#ffffff" />
                )}
                <line x1={p.x} y1={p.y} x2={p.textX} y2={p.textY - 4} />
                <text x={p.textX} y={p.textY} textAnchor={p.textAnchor}>{p.name}</text>
              </g>
            ))}
            {projectedRegionalWorkDots.map((p, idx) => (
              <g
                key={`regional-work-dot-${idx}`}
                className="maps-work-dot maps-work-dot--regional"
                onMouseEnter={() => {
                  if (isTouchInput) return
                  setHoveredWorkPoint(p)
                }}
                onMouseLeave={() => setHoveredWorkPoint((current) => (
                  isTouchInput
                    ? current
                    :
                  current?.x === p.x && current?.y === p.y && current?.scope === p.scope ? null : current
                ))}
                onClick={() => setHoveredWorkPoint((current) => (
                  current?.x === p.x && current?.y === p.y && current?.scope === p.scope ? null : p
                ))}
              >
                <circle cx={p.x} cy={p.y} r={12} fill="rgba(0, 0, 0, 0.001)" />
                <circle cx={p.x} cy={p.y} r={5} fill="#ffffff" fillOpacity={0.94} />
                <circle cx={p.x} cy={p.y} r={3.25} fill="#9fdb6f" stroke="#26410f" strokeWidth={0.9} />
              </g>
            ))}
            {projectedMunicipalityWorkDots.map((p, idx) => (
              <g
                key={`municipality-work-dot-${idx}`}
                className="maps-work-dot maps-work-dot--municipality"
                onMouseEnter={() => {
                  if (isTouchInput) return
                  setHoveredWorkPoint(p)
                }}
                onMouseLeave={() => setHoveredWorkPoint((current) => (
                  isTouchInput
                    ? current
                    :
                  current?.x === p.x && current?.y === p.y && current?.scope === p.scope ? null : current
                ))}
                onClick={() => setHoveredWorkPoint((current) => (
                  current?.x === p.x && current?.y === p.y && current?.scope === p.scope ? null : p
                ))}
              >
                <circle cx={p.x} cy={p.y} r={12} fill="rgba(0, 0, 0, 0.001)" />
                <circle cx={p.x} cy={p.y} r={4.7} fill="#ffffff" fillOpacity={0.94} />
                <circle cx={p.x} cy={p.y} r={3} fill="#f4cf42" stroke="#5c4a00" strokeWidth={0.9} />
              </g>
            ))}
          </svg>
          {hoveredFirePoint && (
            <div
              className="maps-selection-panel__fire-tooltip app-tooltip"
              style={{
                left: `${(hoveredFirePoint.x / PREVIEW_WIDTH) * 100}%`,
                top: `${(hoveredFirePoint.y / PREVIEW_HEIGHT) * 100}%`,
              }}
            >
              <strong>{hoveredFirePoint.commune}</strong>
              <span>{hoveredFirePoint.province}</span>
              <span>{`${(hoveredFirePoint.areaHa * 10).toLocaleString('el-GR', { maximumFractionDigits: 0 })} στρ.`}</span>
            </div>
          )}
          {hoveredWorkPoint && (
            <div
              className="maps-selection-panel__work-tooltip app-tooltip"
              style={{
                left: `${(hoveredWorkPoint.x / PREVIEW_WIDTH) * 100}%`,
                top: `${(hoveredWorkPoint.y / PREVIEW_HEIGHT) * 100}%`,
              }}
            >
              <strong>{hoveredWorkPoint.pointName}</strong>
              <em>{hoveredWorkPoint.scope === 'municipality' ? 'Εργασία δήμου' : 'Εργασία περιφέρειας'}</em>
              <span>{hoveredWorkPoint.work}</span>
            </div>
          )}
          <div className="maps-selection-panel__shape-meta">
            {municipalityFireLoading
              ? 'Φόρτωση δασικών πυρκαγιών'
              : projectedFireShapes.length > 0
                ? (
                  <>
                    <span className="maps-legend-dot maps-legend-dot--fire" aria-hidden="true" />
                    {` Πυρκαγιές 2026 — ${projectedCurrentFireShapes.length.toLocaleString('el-GR')}`}
                    {projectedPreviousFireShapes.length > 0 && (
                      <>
                        {' · '}
                        <span className="maps-legend-dot maps-legend-dot--fire-previous" aria-hidden="true" />
                        {` Πυρκαγιές 2024-2025 — ${projectedPreviousFireShapes.length.toLocaleString('el-GR')}`}
                      </>
                    )}
                    {' · '}
                    <span className="maps-selection-panel__shape-source">Πηγή: Copernicus</span>
                    {` · ${uniqueFireLocations.toLocaleString('el-GR')} σημεία`}
                  </>
                )
                : 'Δεν βρέθηκαν δασικές πυρκαγιές'}
            {!municipalityWorkLoading && projectedMunicipalityWorkDots.length > 0 && (
              <>
                {' · '}
                <span className="maps-legend-dot maps-legend-dot--work" aria-hidden="true" />
                {` ΕΡΓΑΣΙΕΣ ΔΗΜΟΥ — ${projectedMunicipalityWorkDots.length.toLocaleString('el-GR')}`}
              </>
            )}
            {!municipalityWorkLoading && projectedRegionalWorkDots.length > 0 && (
              <>
                {' · '}
                <span className="maps-legend-dot maps-legend-dot--regional-work" aria-hidden="true" />
                {` ΕΡΓΑΣΙΕΣ ΠΕΡΙΦΕΡΕΙΑΣ — ${projectedRegionalWorkDots.length.toLocaleString('el-GR')}`}
              </>
            )}
          </div>
        </div>
      )}

      {kind === 'municipality' && (
        <section className="maps-selection-panel__latest" aria-label="Τελευταίες συμβάσεις δήμου">
          <div className="maps-selection-panel__latest-label">
            <span className="eyebrow">τελευταίες συμβάσεις</span>
          </div>
          <div className="maps-selection-panel__latest-items">
            {municipalityLatestLoading && (
              <article className="wire-item">
                <h2>Φόρτωση συμβάσεων…</h2>
              </article>
            )}
            {!municipalityLatestLoading && municipalityLatestContracts.map((item) => (
              <LatestContractCard key={item.id} item={item} onOpen={onContractOpen} />
            ))}
            {!municipalityLatestLoading && municipalityLatestContracts.length === 0 && (
              <article className="wire-item">
                <h2>Δεν βρέθηκαν συμβάσεις για τον επιλεγμένο δήμο.</h2>
              </article>
            )}
          </div>
        </section>
      )}

      {kind === 'region' && (
        <section className="maps-selection-panel__latest" aria-label="Τελευταίες συμβάσεις περιφέρειας">
          <div className="maps-selection-panel__latest-label">
            <span className="eyebrow">τελευταίες συμβάσεις περιφέρειας</span>
          </div>
          <div className="maps-selection-panel__latest-items">
            {regionLatestLoading && (
              <article className="wire-item">
                <h2>Φόρτωση συμβάσεων…</h2>
              </article>
            )}
            {!regionLatestLoading && regionLatestContracts.map((item) => (
              <LatestContractCard key={item.id} item={item} onOpen={onContractOpen} />
            ))}
            {!regionLatestLoading && regionLatestContracts.length === 0 && (
              <article className="wire-item">
                <h2>Δεν βρέθηκαν συμβάσεις για την επιλεγμένη περιφέρεια.</h2>
              </article>
            )}
          </div>
        </section>
      )}
    </aside>
  )
}
