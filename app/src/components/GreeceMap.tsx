import { useCallback, useEffect, useRef, useState } from 'react'
import * as d3 from 'd3'
import ComponentTag from './ComponentTag'
import type { GeoData, GeoFeature } from '../types'

const FILL_DEFAULT  = '#e6e4de'
const FILL_SELECTED = '#1a3a5c'   /* dark navy */
const STROKE        = 'rgba(153,148,140,0.42)'
const STROKE_W      = 0.5
const MAP_SCALE_BOOST       = .98
const MAP_TRANSLATE_Y_RATIO = 0.000
const ATTICA_FOCUS_WINDOW: [[number, number], [number, number]] = [
  [23.32, 37.43],
  [24.12, 38.43],
]

function intersectsFocusWindow(feature: GeoFeature, [[west, south], [east, north]]: [[number, number], [number, number]]): boolean {
  const [[fx0, fy0], [fx1, fy1]] = d3.geoBounds(feature as unknown as d3.GeoPermissibleObjects)
  return !(fx1 < west || fx0 > east || fy1 < south || fy0 > north)
}

interface Tooltip { x: number; y: number; name: string; pct: number | null }

function fmtPct(v: number): string {
  if (v >= 1)   return v.toFixed(2) + '%'
  if (v >= 0.1) return v.toFixed(3) + '%'
  return v.toFixed(4) + '%'
}

function buildColorScale(data: Record<string, number>): d3.ScalePower<number, number> {
  const vals = Object.values(data).filter(v => v > 0)
  const maxVal = vals.length > 0 ? Math.max(...vals) : 1
  return d3.scalePow().exponent(0.3).domain([0, maxVal]).range([0.05, 1]).clamp(true)
}

function municipalityCode(d: GeoFeature): string {
  const raw = String((d.properties as { municipality_code?: string | null }).municipality_code ?? '').trim()
  const noDecimal = raw.replace(/\.0+$/, '')
  if (/^\d+$/.test(noDecimal)) return String(Number(noDecimal))
  return noDecimal
}

interface Props {
  geojson: GeoData | null
  choroplethData: Record<string, number>   // municipality_id → pct_of_national
  procMunicipalities: Set<string>
  viewMode?: 'greece' | 'attica'
  onDeselect: () => void
  onMunicipalityClick?: (municipalityId: string) => void
  selectedMunicipalityIds?: Set<string> | null
  municipalityLabelById?: Map<string, string>
}

export function GreeceMap({
  geojson,
  choroplethData,
  procMunicipalities,
  viewMode = 'greece',
  onDeselect,
  onMunicipalityClick,
  selectedMunicipalityIds,
  municipalityLabelById,
}: Props) {
  const svgRef                = useRef<SVGSVGElement>(null)
  const selectedRef           = useRef<Set<string>>(new Set())
  const choroplethRef         = useRef<Record<string, number>>({})
  const colorScaleRef         = useRef<((v: number) => number) | null>(null)
  const procMunicipalitiesRef = useRef<Set<string>>(new Set())
  const pathGenRef            = useRef<d3.GeoPath | null>(null)
  const onDeselectRef         = useRef<() => void>(() => {})
  const onMunicipalityClickRef = useRef<((municipalityId: string) => void) | undefined>(undefined)
  const municipalityLabelByIdRef = useRef<Map<string, string>>(new Map())
  const [tooltip, setTooltip] = useState<Tooltip | null>(null)
  const [svgSize, setSvgSize] = useState({ width: 0, height: 0 })

  selectedRef.current           = selectedMunicipalityIds ?? new Set()
  choroplethRef.current         = choroplethData
  procMunicipalitiesRef.current = procMunicipalities
  onDeselectRef.current         = onDeselect
  onMunicipalityClickRef.current = onMunicipalityClick
  municipalityLabelByIdRef.current = municipalityLabelById ?? new Map()

  // Watch SVG element for real layout dimensions
  useEffect(() => {
    if (!svgRef.current) return
    const ro = new ResizeObserver(entries => {
      const r = entries[0]?.contentRect
      if (r) setSvgSize({ width: Math.round(r.width), height: Math.round(r.height) })
    })
    ro.observe(svgRef.current)
    return () => ro.disconnect()
  }, [])

  const buildProjection = useCallback((width: number, height: number) => {
    const focusCollection = (() => {
      if (!geojson) return null
      if (viewMode !== 'attica') {
        return geojson as unknown as d3.ExtendedFeatureCollection
      }
      const features = geojson.features.filter((feature) => intersectsFocusWindow(feature, ATTICA_FOCUS_WINDOW))
      if (features.length === 0) return geojson as unknown as d3.ExtendedFeatureCollection
      return { ...geojson, features } as unknown as d3.ExtendedFeatureCollection
    })()

    const framePad = viewMode === 'attica' ? 26 : 18
    const projection = d3.geoMercator().fitExtent(
      [[framePad, framePad], [width - framePad, height - framePad]],
      focusCollection as d3.GeoPermissibleObjects,
    )
    const [tx, ty] = projection.translate()
    const k = viewMode === 'attica' ? 1.144 : MAP_SCALE_BOOST
    // Zoom from viewport center so Greece stays centered after the scale boost
    projection
      .scale(projection.scale() * k)
      .translate([
        k * tx - (k - 1) * (width / 2),
        k * ty - (k - 1) * (height / 2) + height * MAP_TRANSLATE_Y_RATIO,
      ])
    return projection
  }, [geojson, viewMode])

  // Helper: compute fill for a given municipality code
  const getFill = (code: string): string => {
    if (selectedRef.current.has(code)) return FILL_SELECTED
    const val = choroplethRef.current[code] ?? 0
    if (val <= 0 || !colorScaleRef.current) return FILL_DEFAULT
    return d3.interpolateReds(colorScaleRef.current(val))
  }

  // Draw map when geojson loads or SVG gets real dimensions
  useEffect(() => {
    if (!geojson || !svgRef.current) return
    const { width, height } = svgSize
    if (width === 0 || height === 0) return

    // Build choropleth color scale — power (exponent 0.3) anchored at 0
    colorScaleRef.current = buildColorScale(choroplethRef.current)

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()

    const projection = buildProjection(width, height)
    const pathGen = d3.geoPath().projection(projection)
    pathGenRef.current = pathGen

    // Background rect — click outside municipalities to deselect
    svg.append('rect')
      .attr('width', width)
      .attr('height', height)
      .attr('fill', 'transparent')
      .attr('cursor', 'default')
      .on('click', () => {
        onDeselectRef.current()
      })

    svg
      .append('g')
      .selectAll<SVGPathElement, GeoFeature>('path')
      .data(geojson.features)
      .join('path')
      .attr('d', d => pathGen(d as unknown as d3.GeoPermissibleObjects) ?? '')
      .attr('fill', d => getFill(municipalityCode(d)))
      .attr('stroke', STROKE)
      .attr('stroke-width', STROKE_W)
      .attr('cursor', 'pointer')
      .attr('data-id', d => municipalityCode(d))
      .on('mouseenter', function (event: MouseEvent, d: GeoFeature) {
        const code = municipalityCode(d)
        if (!selectedRef.current.has(code)) {
          const base = getFill(code)
          const darker = d3.color(base)?.darker(0.3)
          d3.select(this).attr('fill', darker ? darker.toString() : base)
        }
        const dbLabel = municipalityLabelByIdRef.current.get(code)
        const fallbackGeoName = String((d.properties as { name?: string | null }).name ?? '').trim()
        const pct = choroplethRef.current[code] ?? null
        setTooltip({ x: event.offsetX, y: event.offsetY, name: dbLabel || fallbackGeoName || code, pct })
      })
      .on('mousemove', (event: MouseEvent) => {
        setTooltip(prev =>
          prev ? { ...prev, x: event.offsetX, y: event.offsetY } : null
        )
      })
      .on('mouseleave', function (_, d: GeoFeature) {
        d3.select(this)
          .attr('fill', getFill(municipalityCode(d)))
          .attr('stroke', STROKE)
          .attr('stroke-width', STROKE_W)
        setTooltip(null)
      })
      .on('click', (event: MouseEvent, d: GeoFeature) => {
        event.stopPropagation()
        const code = municipalityCode(d)
        if (!code) return
        const dbLabel = municipalityLabelByIdRef.current.get(code)
        const fallbackGeoName = String((d.properties as { name?: string | null }).name ?? '').trim()
        const pct = choroplethRef.current[code] ?? null
        setTooltip({ x: event.offsetX, y: event.offsetY, name: dbLabel || fallbackGeoName || code, pct })
        onMunicipalityClickRef.current?.(code)
      })
    // Dots layer — draw immediately and keep in sync via the proc-dots effect
    svg.append('g').attr('class', 'proc-dots')
      .selectAll<SVGCircleElement, GeoFeature>('circle')
      .data(geojson.features.filter(
        f => procMunicipalitiesRef.current.has(f.properties.municipality_code)
      ))
      .join('circle')
      .attr('cx', d => pathGen.centroid(d as unknown as d3.GeoPermissibleObjects)[0])
      .attr('cy', d => pathGen.centroid(d as unknown as d3.GeoPermissibleObjects)[1])
      .attr('r', 2.5)
      .attr('fill', '#22c55e')
      .attr('stroke', 'rgba(255,255,255,0.75)')
      .attr('stroke-width', 0.8)
      .attr('pointer-events', 'none')
  }, [buildProjection, geojson, svgSize, viewMode])

  // Draw/update procurement dots when data or map changes
  useEffect(() => {
    if (!svgRef.current || !geojson || !pathGenRef.current) return
    const pathGen = pathGenRef.current
    d3.select(svgRef.current)
      .select<SVGGElement>('g.proc-dots')
      .selectAll<SVGCircleElement, GeoFeature>('circle')
      .data(geojson.features.filter(
        f => procMunicipalitiesRef.current.has(f.properties.municipality_code)
      ))
      .join('circle')
      .attr('cx', d => pathGen.centroid(d as unknown as d3.GeoPermissibleObjects)[0])
      .attr('cy', d => pathGen.centroid(d as unknown as d3.GeoPermissibleObjects)[1])
      .attr('r', 2.5)
      .attr('fill', '#22c55e')
      .attr('stroke', 'rgba(255,255,255,0.75)')
      .attr('stroke-width', 0.8)
      .attr('pointer-events', 'none')
  }, [procMunicipalities, geojson, svgSize, viewMode])

  // Update choropleth fills (without full redraw) when data loads
  useEffect(() => {
    if (!svgRef.current || !geojson) return
    colorScaleRef.current = buildColorScale(choroplethData)

    d3.select(svgRef.current)
      .selectAll<SVGPathElement, GeoFeature>('path[data-id]')
      .attr('fill', d => getFill(municipalityCode(d)))
      .attr('stroke', STROKE)
      .attr('stroke-width', STROKE_W)
  }, [choroplethData, geojson])

  // Update fill when URL selection changes
  useEffect(() => {
    if (!svgRef.current) return
    d3.select(svgRef.current)
      .selectAll<SVGPathElement, GeoFeature>('path[data-id]')
      .attr('fill', d => getFill(municipalityCode(d)))
      .attr('stroke', STROKE)
      .attr('stroke-width', STROKE_W)
  }, [geojson, selectedMunicipalityIds])


  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      <ComponentTag name="GreeceMap" className="component-tag--overlay" />
      {!geojson && (
        <div className="map-loading">Φόρτωση χάρτη…</div>
      )}
      <svg
        ref={svgRef}
        style={{ width: '100%', height: '100%', display: 'block' }}
      />
      {tooltip && (
        <div
          className="map-tooltip app-tooltip"
          style={{ left: tooltip.x, top: tooltip.y - 10 }}
        >
          <span className="map-tooltip-name">{tooltip.name}</span>
          {tooltip.pct != null && (
            <span className="map-tooltip-pct">{fmtPct(tooltip.pct)} εθν. συνόλου</span>
          )}
        </div>
      )}
    </div>
  )
}
