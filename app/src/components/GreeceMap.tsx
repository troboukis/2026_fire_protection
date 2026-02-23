import { useEffect, useRef, useState } from 'react'
import * as d3 from 'd3'
import { useNavigate, useParams } from 'react-router-dom'
import type { GeoData, GeoFeature } from '../types'

const FILL_DEFAULT  = '#e6e4de'
const FILL_SELECTED = '#1a3a5c'   /* navy */
const STROKE        = 'rgba(153,148,140,0.42)'
const STROKE_W      = 0.5
const MAP_SCALE_BOOST       = .98
const MAP_TRANSLATE_Y_RATIO = 0.000

interface Tooltip { x: number; y: number; name: string; pct: number | null }

function fmtPct(v: number): string {
  if (v >= 1)   return v.toFixed(2) + '%'
  if (v >= 0.1) return v.toFixed(3) + '%'
  return v.toFixed(4) + '%'
}

interface Props {
  geojson: GeoData | null
  choroplethData: Record<string, number>   // municipality_id → pct_of_national
  procMunicipalities: Set<string>
  onDeselect: () => void
}

export function GreeceMap({ geojson, choroplethData, procMunicipalities, onDeselect }: Props) {
  const svgRef                = useRef<SVGSVGElement>(null)
  const selectedRef           = useRef<string | undefined>(undefined)
  const choroplethRef         = useRef<Record<string, number>>({})
  const colorScaleRef         = useRef<((v: number) => number) | null>(null)
  const procMunicipalitiesRef = useRef<Set<string>>(new Set())
  const pathGenRef            = useRef<d3.GeoPath | null>(null)
  const onDeselectRef         = useRef<() => void>(() => {})
  const navigate         = useNavigate()
  const { id }           = useParams<{ id: string }>()
  const [tooltip, setTooltip] = useState<Tooltip | null>(null)
  const [svgSize, setSvgSize] = useState({ width: 0, height: 0 })

  selectedRef.current           = id
  choroplethRef.current         = choroplethData
  procMunicipalitiesRef.current = procMunicipalities
  onDeselectRef.current         = onDeselect

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

  const buildProjection = (width: number, height: number) => {
    const projection = d3.geoMercator().fitSize(
      [width, height],
      geojson as unknown as d3.ExtendedFeatureCollection
    )
    const [tx, ty] = projection.translate()
    const k = MAP_SCALE_BOOST
    // Zoom from viewport center so Greece stays centered after the scale boost
    projection
      .scale(projection.scale() * k)
      .translate([
        k * tx - (k - 1) * (width / 2),
        k * ty - (k - 1) * (height / 2) + height * MAP_TRANSLATE_Y_RATIO,
      ])
    return projection
  }

  // Helper: compute fill for a given municipality code
  const getFill = (code: string): string => {
    if (code === selectedRef.current) return FILL_SELECTED
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
    const vals = Object.values(choroplethRef.current).filter(v => v > 0)
    const maxVal = vals.length > 0 ? Math.max(...vals) : 1
    colorScaleRef.current = d3.scalePow().exponent(0.3).domain([0, maxVal]).range([0.05, 1]).clamp(true)

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
      .on('click', () => onDeselectRef.current())

    svg
      .append('g')
      .selectAll<SVGPathElement, GeoFeature>('path')
      .data(geojson.features)
      .join('path')
      .attr('d', d => pathGen(d as unknown as d3.GeoPermissibleObjects) ?? '')
      .attr('fill', d => getFill(d.properties.municipality_code))
      .attr('stroke', STROKE)
      .attr('stroke-width', STROKE_W)
      .attr('cursor', 'pointer')
      .attr('data-id', d => d.properties.municipality_code)
      .on('mouseenter', function (event: MouseEvent, d: GeoFeature) {
        const code = d.properties.municipality_code
        if (code !== selectedRef.current) {
          const base = getFill(code)
          const darker = d3.color(base)?.darker(0.3)
          d3.select(this).attr('fill', darker ? darker.toString() : base)
        }
        const pct = choroplethRef.current[code] ?? null
        setTooltip({ x: event.offsetX, y: event.offsetY, name: d.properties.name, pct })
      })
      .on('mousemove', (event: MouseEvent) => {
        setTooltip(prev =>
          prev ? { ...prev, x: event.offsetX, y: event.offsetY } : null
        )
      })
      .on('mouseleave', function (_, d: GeoFeature) {
        d3.select(this).attr('fill', getFill(d.properties.municipality_code))
        setTooltip(null)
      })
      .on('click', (event: MouseEvent, d: GeoFeature) => {
        event.stopPropagation()
        navigate(`/municipality/${d.properties.municipality_code}`)
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
  }, [geojson, navigate, svgSize])

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
  }, [procMunicipalities, geojson, svgSize])

  // Update choropleth fills (without full redraw) when data loads
  useEffect(() => {
    if (!svgRef.current || !geojson) return
    const vals = Object.values(choroplethData).filter(v => v > 0)
    const maxVal = vals.length > 0 ? Math.max(...vals) : 1
    colorScaleRef.current = d3.scalePow().exponent(0.3).domain([0, maxVal]).range([0.05, 1]).clamp(true)

    d3.select(svgRef.current)
      .selectAll<SVGPathElement, GeoFeature>('path[data-id]')
      .attr('fill', d => getFill(d.properties.municipality_code))
  }, [choroplethData])

  // Update fill when URL selection changes
  useEffect(() => {
    if (!svgRef.current) return
    d3.select(svgRef.current)
      .selectAll<SVGPathElement, GeoFeature>('path[data-id]')
      .attr('fill', d => getFill(d.properties.municipality_code))
  }, [id])


  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      {!geojson && (
        <div className="map-loading">Φόρτωση χάρτη…</div>
      )}
      <svg
        ref={svgRef}
        style={{ width: '100%', height: '100%', display: 'block' }}
      />
      {tooltip && (
        <div
          className="map-tooltip"
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
