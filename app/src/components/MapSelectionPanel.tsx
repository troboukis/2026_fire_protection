import { useMemo } from 'react'
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

function useHeartMarker(name: string): boolean {
  const n = normalizeGreekName(name)
  return n === 'αντικυρα' || n === 'ασπρα σπιτια'
}

type Props = {
  source: SelectionSource | null
  kind: SelectionKind | null
  label: string
  municipalityLatestContracts?: LatestContractCardView[]
  municipalityLatestLoading?: boolean
  regionLatestContracts?: LatestContractCardView[]
  regionLatestLoading?: boolean
  municipalityFeature?: GeoFeature | null
  municipalityFirePoints?: Array<{ lat: number; lon: number }>
  municipalityFireLoading?: boolean
  municipalityFireYear?: number | null
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
  municipalityFireLoading = false,
  municipalityFireYear = null,
  cityPoints = [],
  currentYear,
  regionCurrentYearCount,
  municipalityCurrentYearCount,
  onContractOpen,
}: Props) {
  const previewGeometry = useMemo(() => {
    if (!municipalityFeature) return null
    const width = 520
    const height = 320
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

  const projectedFireDots = useMemo(() => {
    if (!previewGeometry) return [] as Array<{ x: number; y: number }>
    const out: Array<{ x: number; y: number }> = []
    for (const point of municipalityFirePoints) {
      const projected = previewGeometry.projection([point.lon, point.lat])
      if (!projected) continue
      const [x, y] = projected
      if (!Number.isFinite(x) || !Number.isFinite(y)) continue
      out.push({ x, y })
    }
    return out
  }, [previewGeometry, municipalityFirePoints])

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
      ? 'Επιλέξτε Δήμο ή Περιφέρεια στον χάρτη'
      : kind === 'municipality'
        ? `Το ${currentYear} έχουν δημοσιευθεί ${(municipalityCurrentYearCount ?? 0).toLocaleString('el-GR')} συμβάσεις που αφορούν στην πρόληψη ή αντιμετώπιση δασικών πυρκαγιών.`
        : `Το ${currentYear} έχουν δημοσιευθεί ${(regionCurrentYearCount ?? 0).toLocaleString('el-GR')} συμβάσεις που αφορούν στην πρόληψη ή αντιμετώπιση δασικών πυρκαγιών στην επιλεγμένη περιφέρεια.`

  return (
    <aside className="maps-selection-panel" data-selection-source={source ?? 'none'}>
      <ComponentTag name="MapSelectionPanel" />
      <EditorialLead eyebrow="Ανάλυση" title={title} subtitle={subtitle} />

      {kind === 'municipality' && previewGeometry && (
        <div className="maps-selection-panel__shape" aria-label="Polygon Δήμου">
          <svg viewBox="0 0 520 320" role="img" aria-label={`Polygon ${label}`}>
            <path d={previewGeometry.path ?? ''} fill="#1a3a5c" fillOpacity="0.92" stroke="#0f2237" strokeWidth="1.3" />
            {projectedFireDots.map((p, idx) => (
              <circle
                key={`fire-dot-${idx}`}
                cx={p.x}
                cy={p.y}
                r={2.2}
                fill="#ff3b30"
                fillOpacity={0.5}
                stroke="#ffd7d2"
                strokeWidth={0.35}
              />
            ))}
            {projectedCityPoints.map((p, idx) => (
              <g key={`city-${p.name}-${idx}`} className="maps-city-point">
                {useHeartMarker(p.name) ? (
                  <text className="maps-city-heart" x={p.x} y={p.y + 1.8} textAnchor="middle">♥</text>
                ) : (
                  <circle cx={p.x} cy={p.y} r={2.6} fill="#ffffff" />
                )}
                <line x1={p.x} y1={p.y} x2={p.textX} y2={p.textY - 4} />
                <text x={p.textX} y={p.textY} textAnchor={p.textAnchor}>{p.name}</text>
              </g>
            ))}
          </svg>
          <div className="maps-selection-panel__shape-meta">
            {municipalityFireLoading
              ? 'Φόρτωση καταγεγραμμένων πυρκαγιών…'
              : municipalityFireYear != null
                ? (
                  <>
                    <span className="maps-legend-dot maps-legend-dot--fire" aria-hidden="true" />
                    {` Καταγεγραμμένες πυρκαγιές (${municipalityFireYear}) — ${projectedFireDots.length.toLocaleString('el-GR')} συμβάντα σε ${uniqueFireLocations.toLocaleString('el-GR')} σημεία`}
                  </>
                )
                : 'Δεν βρέθηκαν καταγεγραμμένες πυρκαγιές με συντεταγμένες'}
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
