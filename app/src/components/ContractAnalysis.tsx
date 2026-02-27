// Ανάλυση Συμβάσεων — D3 charts, χωρίς ΦΠΑ, Ιαν 2024 – Φεβ 2026
import { useEffect, useRef, useState, useCallback } from 'react'
import * as d3 from 'd3'
import { HEATMAP_CELLS, ALL_ORGS, TOP10_ORGS, TOP10_BY_TOTAL, DIMOS_ORGS, OTHER_ORGS } from '../data/heatmapData'

// ── Τύποι ────────────────────────────────────────────────────────────
type BarItem = {
  label: string
  value: number
  total_m: number
  pct: number
  tone: 'high' | 'mid' | 'low' | 'faint'
}

interface MonthlyPoint {
  month: string    // '2024-01'
  label: string    // "Ιαν '24"
  count: number
  total_k: number  // €K χωρίς ΦΠΑ
}

// ── Μηνιαία δεδομένα (χωρίς ΦΠΑ) ────────────────────────────────────
const MONTHLY: MonthlyPoint[] = [
  { month: '2024-01', label: "Ιαν '24", count: 41,  total_k: 8677.7  },
  { month: '2024-02', label: "Φεβ '24", count: 84,  total_k: 67201.1 },
  { month: '2024-03', label: "Μαρ '24", count: 106, total_k: 75146.2 },
  { month: '2024-04', label: "Απρ '24", count: 146, total_k: 9605.4  },
  { month: '2024-05', label: "Μαϊ '24", count: 193, total_k: 71886.0 },
  { month: '2024-06', label: "Ιουν '24",count: 194, total_k: 18020.3 },
  { month: '2024-07', label: "Ιουλ '24",count: 245, total_k: 13884.7 },
  { month: '2024-08', label: "Αυγ '24", count: 121, total_k: 5089.8  },
  { month: '2024-09', label: "Σεπ '24", count: 130, total_k: 46038.0 },
  { month: '2024-10', label: "Οκτ '24", count: 146, total_k: 36012.5 },
  { month: '2024-11', label: "Νοε '24", count: 109, total_k: 3513.5  },
  { month: '2024-12', label: "Δεκ '24", count: 223, total_k: 31940.8 },
  { month: '2025-01', label: "Ιαν '25", count: 43,  total_k: 2888.4  },
  { month: '2025-02', label: "Φεβ '25", count: 56,  total_k: 36316.0 },
  { month: '2025-03', label: "Μαρ '25", count: 112, total_k: 30038.8 },
  { month: '2025-04', label: "Απρ '25", count: 169, total_k: 50255.2 },
  { month: '2025-05', label: "Μαϊ '25", count: 251, total_k: 70895.1 },
  { month: '2025-06', label: "Ιουν '25",count: 237, total_k: 32593.0 },
  { month: '2025-07', label: "Ιουλ '25",count: 313, total_k: 36469.4 },
  { month: '2025-08', label: "Αυγ '25", count: 265, total_k: 17377.2 },
  { month: '2025-09', label: "Σεπ '25", count: 235, total_k: 29796.7 },
  { month: '2025-10', label: "Οκτ '25", count: 122, total_k: 21996.1 },
  { month: '2025-11', label: "Νοε '25", count: 109, total_k: 4556.5  },
  { month: '2025-12', label: "Δεκ '25", count: 164, total_k: 23775.7 },
  { month: '2026-01', label: "Ιαν '26", count: 26,  total_k: 11125.4 },
  { month: '2026-02', label: "Φεβ '26", count: 32,  total_k: 4717.5  },
]

// Αντιπυρικοί μήνες
const FIRE_MONTHS = new Set([
  '2024-05','2024-06','2024-07','2024-08',
  '2025-05','2025-06','2025-07','2025-08',
])

// ── Στατικά breakdown data (χωρίς ΦΠΑ) ──────────────────────────────
const contractTypeData: BarItem[] = [
  { label: 'Υπηρεσίες',  value: 3108, total_m: 474.9, pct: 80.2, tone: 'high'  },
  { label: 'Προμήθειες', value: 581,  total_m: 191.0, pct: 15.0, tone: 'mid'   },
  { label: 'Έργα',       value: 122,  total_m:  92.7, pct:  3.1, tone: 'low'   },
  { label: 'Λοιπές',     value: 64,   total_m:   2.1, pct:  1.7, tone: 'faint' },
]

const procedureData: BarItem[] = [
  { label: 'Απευθείας Ανάθεση',  value: 2697, total_m: 408.9, pct: 69.6, tone: 'high'  },
  { label: 'Ανοιχτή Διαδικασία', value: 313,  total_m: 313.0, pct:  8.1, tone: 'mid'   },
  { label: 'Διαπραγμάτευση',     value: 788,  total_m:  34.2, pct: 20.3, tone: 'low'   },
  { label: 'Άλλη',               value: 77,   total_m:   4.6, pct:  2.0, tone: 'faint' },
]

const topOrgs = [
  { name: 'Υπ. Περιβάλλοντος & Ενέργειας',             contracts: 187, total_m: 438.4 },
  { name: 'Υπ. Κλιματικής Κρίσης & Πολιτικής Προστ.', contracts: 200, total_m: 159.7 },
  { name: 'Κοινωνία της Πληροφορίας ΑΕ',               contracts: 1,   total_m:  21.8 },
  { name: 'Περιφέρεια Αττικής',                         contracts: 645, total_m:  17.8 },
  { name: 'Δήμος Ηλιούπολης',                           contracts: 7,   total_m:  14.1 },
  { name: 'ΑΔΜΗΕ ΑΕ',                                   contracts: 34,  total_m:   9.2 },
  { name: 'ΕΥΔΑΠ ΑΕ',                                   contracts: 8,   total_m:   5.6 },
  { name: 'Περιφέρεια Αν. Μακεδονίας - Θράκης',         contracts: 24,  total_m:   3.4 },
]

const topCpv = [
  { cpv: '77231300-1', desc: 'Υπηρεσίες διαχείρισης δασών',               count: 141, total_m: 428.3 },
  { cpv: '77200000-2', desc: 'Υπηρεσίες δασοκομίας',                      count: 250, total_m: 423.4 },
  { cpv: '77340000-5', desc: 'Κλάδεμα δένδρων και θάμνων',                count: 669, total_m: 415.2 },
  { cpv: '77211000-2', desc: 'Υπηρ. σχετιζόμενες με την υλοτομία',        count: 134, total_m: 382.1 },
  { cpv: '77211100-3', desc: 'Υπηρεσίες περισυλλογής ξυλείας',            count: 132, total_m: 382.0 },
  { cpv: '77211300-5', desc: 'Υπηρεσίες υλοτόμισης',                      count: 125, total_m: 368.3 },
  { cpv: '45112400-9', desc: 'Εργασίες εκσκαφών',                         count: 113, total_m: 327.6 },
  { cpv: '77312000-0', desc: 'Υπηρ. εκκαθάρισης από αγριόχορτα',          count: 86,  total_m: 294.5 },
]

// ── Βοηθητικό HBar ────────────────────────────────────────────────────
function HBar({ item, max }: { item: BarItem; max: number }) {
  const toneMap = {
    high:  'rgba(211,72,45,0.9)',
    mid:   'rgba(211,72,45,0.55)',
    low:   'rgba(211,72,45,0.32)',
    faint: 'rgba(211,72,45,0.18)',
  }
  return (
    <div className="ca-bar-row">
      <div className="ca-bar-label">
        <span>{item.label}</span>
        <span className="ca-bar-pct">{item.pct}%</span>
      </div>
      <div className="ca-bar-track">
        <div className="ca-bar-fill" style={{ width: `${(item.total_m / max) * 100}%`, background: toneMap[item.tone] }} />
      </div>
      <div className="ca-bar-meta">
        <strong>€ {item.total_m.toFixed(1)}M</strong>
        <span>{item.value.toLocaleString('el-GR')} συμβ.</span>
      </div>
    </div>
  )
}

// ── D3 Bar Chart ──────────────────────────────────────────────────────
type BarMetric = 'count' | 'total'

function BarChart({ metric }: { metric: BarMetric }) {
  const containerRef = useRef<HTMLDivElement>(null)
  const svgRef       = useRef<SVGSVGElement>(null)

  const draw = useCallback(() => {
    if (!svgRef.current || !containerRef.current) return
    const containerW = containerRef.current.clientWidth
    if (containerW === 0) return

    const margin = { top: 18, right: 16, bottom: 58, left: 68 }
    const W = containerW
    const H = 240
    const innerW = W - margin.left - margin.right
    const innerH = H - margin.top - margin.bottom

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()
    svg.attr('width', W).attr('height', H)

    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`)

    // Scales
    const x = d3.scaleBand<string>()
      .domain(MONTHLY.map(d => d.month))
      .range([0, innerW])
      .padding(0.22)

    const yVal = (d: MonthlyPoint) => metric === 'count' ? d.count : d.total_k
    const yMax = d3.max(MONTHLY, yVal) ?? 1

    const y = d3.scaleLinear()
      .domain([0, yMax * 1.1])
      .range([innerH, 0])
      .nice()

    // Horizontal grid lines
    g.append('g')
      .attr('class', 'grid')
      .call(
        d3.axisLeft(y).tickSize(-innerW).tickFormat(() => '').ticks(5)
      )
      .call(sel => sel.select('.domain').remove())
      .call(sel => sel.selectAll<SVGLineElement, unknown>('.tick line')
        .attr('stroke', 'rgba(17,17,17,0.06)')
        .attr('stroke-dasharray', '3,4')
      )

    // Y axis
    g.append('g')
      .call(
        d3.axisLeft(y)
          .ticks(5)
          .tickFormat((v) => {
            const n = v as number
            if (metric === 'count') return n >= 1000 ? `${(n / 1000).toFixed(1)}k` : `${n}`
            return n >= 1000 ? `${(n / 1000).toFixed(0)}M€` : `${n}K€`
          })
      )
      .call(sel => sel.select('.domain').attr('stroke', 'rgba(17,17,17,0.18)'))
      .call(sel => sel.selectAll('.tick line').attr('stroke', 'rgba(17,17,17,0.12)'))
      .call(sel => sel.selectAll<SVGTextElement, unknown>('.tick text')
        .attr('font-family', 'IBM Plex Mono, monospace')
        .attr('font-size', '10')
        .attr('fill', 'rgba(17,17,17,0.55)')
        .attr('dx', '-4')
      )

    // X axis
    g.append('g')
      .attr('transform', `translate(0,${innerH})`)
      .call(
        d3.axisBottom(x)
          .tickFormat((m) => MONTHLY.find(p => p.month === m)?.label ?? m)
      )
      .call(sel => sel.select('.domain').attr('stroke', 'rgba(17,17,17,0.18)'))
      .call(sel => sel.selectAll('.tick line').attr('stroke', 'rgba(17,17,17,0.12)'))
      .call(sel => sel.selectAll<SVGTextElement, unknown>('.tick text')
        .attr('font-family', 'IBM Plex Mono, monospace')
        .attr('font-size', '9.5')
        .attr('fill', 'rgba(17,17,17,0.55)')
        .attr('transform', 'rotate(-40)')
        .attr('text-anchor', 'end')
        .attr('dy', '0.35em')
        .attr('dx', '-0.4em')
      )

    // Bars
    const tooltip = d3.select('#ca-bar-tooltip')

    g.selectAll<SVGRectElement, MonthlyPoint>('.bar')
      .data(MONTHLY)
      .join('rect')
      .attr('class', 'bar')
      .attr('x',      d => x(d.month) ?? 0)
      .attr('width',  x.bandwidth())
      .attr('y',      d => y(yVal(d)))
      .attr('height', d => innerH - y(yVal(d)))
      .attr('fill',   d => FIRE_MONTHS.has(d.month) ? 'rgba(211,72,45,0.82)' : 'rgba(17,17,17,0.28)')
      .attr('rx', 1)
      .on('mouseover', (event: MouseEvent, d) => {
        const rect = containerRef.current!.getBoundingClientRect()
        const val = metric === 'count'
          ? `${d.count} συμβάσεις`
          : `€ ${(d.total_k / 1000).toFixed(2)}M (χωρίς ΦΠΑ)`
        tooltip
          .style('display', 'block')
          .style('left',  `${(event.clientX - rect.left + 10)}px`)
          .style('top',   `${(event.clientY - rect.top  - 36)}px`)
          .html(`<strong>${d.label}</strong><br/>${val}`)
        d3.select(event.currentTarget as Element).attr('opacity', 0.75)
      })
      .on('mousemove', (event: MouseEvent) => {
        const rect = containerRef.current!.getBoundingClientRect()
        tooltip
          .style('left', `${(event.clientX - rect.left + 10)}px`)
          .style('top',  `${(event.clientY - rect.top  - 36)}px`)
      })
      .on('mouseout', (event: MouseEvent) => {
        tooltip.style('display', 'none')
        d3.select(event.currentTarget as Element).attr('opacity', 1)
      })

  }, [metric])

  useEffect(() => {
    draw()
    const ro = new ResizeObserver(draw)
    if (containerRef.current) ro.observe(containerRef.current)
    return () => ro.disconnect()
  }, [draw])

  return (
    <div ref={containerRef} className="ca-bar-container">
      <svg ref={svgRef} className="ca-d3-bar-svg" />
      <div id="ca-bar-tooltip" className="ca-tooltip" />
    </div>
  )
}

// ── D3 Heatmap ────────────────────────────────────────────────────────
const MONTHS_LIST = MONTHLY.map(m => m.month)   // 26 μήνες

function Heatmap({ selectedOrgs, metric }: { selectedOrgs: string[]; metric: BarMetric }) {
  const containerRef = useRef<HTMLDivElement>(null)
  const svgRef       = useRef<SVGSVGElement>(null)

  const draw = useCallback(() => {
    if (!svgRef.current || !containerRef.current || selectedOrgs.length === 0) return
    const containerW = containerRef.current.clientWidth
    if (containerW === 0) return

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()

    // Διαστάσεις
    const LABEL_W = Math.min(200, containerW * 0.22)
    const RIGHT_PAD = 16
    const TOP_PAD = 72    // enough room for rotated month labels going upward
    const BOTTOM_PAD = 24 // room for legend
    const cellW = (containerW - LABEL_W - RIGHT_PAD) / MONTHS_LIST.length
    const cellH = 26
    const H = TOP_PAD + selectedOrgs.length * cellH + BOTTOM_PAD

    svg.attr('width', containerW).attr('height', H)

    const g = svg.append('g')

    // Lookup table
    const lookup = new Map<string, number>()
    for (const cell of HEATMAP_CELLS) {
      if (selectedOrgs.includes(cell.organization_value)) {
        const key = `${cell.month_str}|${cell.organization_value}`
        lookup.set(key, metric === 'count' ? cell.count : cell.total_k)
      }
    }

    const allVals    = [...lookup.values()]
    const nonZero    = allVals.filter(v => v > 0)
    const minNonZero = d3.min(nonZero) ?? 1
    const maxVal     = d3.max(nonZero) ?? 1

    // Log scale: small differences visible; 0 = distinct "no data" color
    // scaleSequentialLog requires domain > 0, so clamp min to 1 (count) or 0.1k (total)
    const logMin = Math.max(minNonZero, metric === 'count' ? 1 : 0.1)
    const colorScale = d3.scaleSequentialLog<string>()
      .domain([logMin, maxVal])
      .interpolator(d3.interpolateRgb('#f9dfc0', '#d3482d'))
      .clamp(true)

    const NO_DATA_COLOR = '#e5e1d6'  // neutral muted beige for empty cells

    // Month header labels — rotate(+55) sends left-anchored text UP-LEFT (correct for top axis)
    // rotate(-55) sends it DOWN-LEFT (was the bug: labels hidden under cells)
    MONTHS_LIST.forEach((month, i) => {
      const label = MONTHLY.find(p => p.month === month)?.label ?? month
      const cx = LABEL_W + i * cellW + cellW / 2
      const anchorY = TOP_PAD - 2
      g.append('text')
        .attr('x', cx)
        .attr('y', anchorY)
        .attr('text-anchor', 'end')
        .attr('font-family', 'IBM Plex Mono, monospace')
        .attr('font-size', Math.min(9.5, cellW * 0.6))
        .attr('fill', 'rgba(17,17,17,0.58)')
        .attr('transform', `rotate(55,${cx},${anchorY})`)
        .text(label)
    })

    // Tooltip
    const tooltip = d3.select('#ca-hm-tooltip')

    // Org rows + cells
    selectedOrgs.forEach((org, row) => {
      const cy = TOP_PAD + row * cellH

      // Alternating row background
      if (row % 2 === 0) {
        g.append('rect')
          .attr('x', 0).attr('y', cy)
          .attr('width', containerW).attr('height', cellH)
          .attr('fill', 'rgba(17,17,17,0.025)')
      }

      // Org label (truncated)
      const maxLabelLen = Math.floor(LABEL_W / 5.5)
      const shortName = org.length > maxLabelLen ? org.slice(0, maxLabelLen - 1) + '…' : org
      g.append('text')
        .attr('x', LABEL_W - 6)
        .attr('y', cy + cellH / 2 + 3.5)
        .attr('text-anchor', 'end')
        .attr('font-family', 'IBM Plex Mono, monospace')
        .attr('font-size', 9.5)
        .attr('fill', 'rgba(17,17,17,0.72)')
        .text(shortName)
        .append('title')
        .text(org)  // full name on hover via SVG title

      // Cells
      MONTHS_LIST.forEach((month, col) => {
        const key = `${month}|${org}`
        const val = lookup.get(key) ?? 0
        const cx  = LABEL_W + col * cellW
        const pad = 1.5

        g.append('rect')
          .attr('x', cx + pad)
          .attr('y', cy + pad)
          .attr('width',  cellW - pad * 2)
          .attr('height', cellH - pad * 2)
          .attr('rx', 2)
          .attr('fill', val > 0 ? colorScale(val) : NO_DATA_COLOR)
          .attr('stroke', 'rgba(17,17,17,0.06)')
          .attr('stroke-width', 0.5)
          .style('cursor', 'default')
          .on('mouseover', (event: MouseEvent) => {
            const rect = containerRef.current!.getBoundingClientRect()
            const label = MONTHLY.find(p => p.month === month)?.label ?? month
            const valStr = metric === 'count'
              ? `${val} συμβάσεις`
              : val > 0 ? `€ ${(val).toFixed(1)}K (χωρίς ΦΠΑ)` : '—'
            tooltip
              .style('display', 'block')
              .style('left',  `${event.clientX - rect.left + 12}px`)
              .style('top',   `${event.clientY - rect.top  - 42}px`)
              .html(`<strong>${org}</strong><br/>${label}: ${valStr}`)
          })
          .on('mousemove', (event: MouseEvent) => {
            const rect = containerRef.current!.getBoundingClientRect()
            tooltip
              .style('left', `${event.clientX - rect.left + 12}px`)
              .style('top',  `${event.clientY - rect.top  - 42}px`)
          })
          .on('mouseout', () => tooltip.style('display', 'none'))
      })
    })

    // Color legend (gradient bar)
    const legendW = 140, legendH = 8
    const legendX = LABEL_W + 4
    const legendY = H - BOTTOM_PAD - legendH - 2

    const defs = svg.append('defs')
    const grad = defs.append('linearGradient').attr('id', 'hm-legend-grad')
    grad.append('stop').attr('offset', '0%').attr('stop-color', '#f9dfc0')
    grad.append('stop').attr('offset', '100%').attr('stop-color', '#d3482d')

    g.append('rect')
      .attr('x', legendX).attr('y', legendY)
      .attr('width', legendW).attr('height', legendH)
      .attr('fill', 'url(#hm-legend-grad)')
      .attr('rx', 2)

    g.append('text')
      .attr('x', legendX).attr('y', legendY - 3)
      .attr('font-family', 'IBM Plex Mono, monospace')
      .attr('font-size', 8).attr('fill', 'rgba(17,17,17,0.45)').text('Χαμηλό')
    g.append('text')
      .attr('x', legendX + legendW).attr('y', legendY - 3)
      .attr('text-anchor', 'end')
      .attr('font-family', 'IBM Plex Mono, monospace')
      .attr('font-size', 8).attr('fill', 'rgba(17,17,17,0.45)').text('Υψηλό')

  }, [selectedOrgs, metric])

  useEffect(() => {
    draw()
    const ro = new ResizeObserver(draw)
    if (containerRef.current) ro.observe(containerRef.current)
    return () => ro.disconnect()
  }, [draw])

  return (
    <div ref={containerRef} className="ca-hm-container">
      <svg ref={svgRef} style={{ overflow: 'visible' }} />
      <div id="ca-hm-tooltip" className="ca-tooltip" />
    </div>
  )
}

// ── Accent/case-insensitive normalizer ────────────────────────────────
// Decomposes Unicode (NFD) so "ά" → "α" + combining mark, then strips
// all combining diacritical marks (U+0300–U+036F) and uppercases.
// Works for Greek (τόνοι, διαλυτικά) and Latin characters alike.
function normalizeSearch(str: string): string {
  return str
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toUpperCase()
    .trim()
}

// ── OrgFilter panel ───────────────────────────────────────────────────
type OrgCategory = 'all' | 'dimos' | 'other'

function OrgFilter({
  selected,
  onChange,
}: {
  selected: string[]
  onChange: (orgs: string[]) => void
}) {
  const [search, setSearch]       = useState('')
  const [category, setCategory]   = useState<OrgCategory>('all')

  // Pool of orgs based on category filter
  const pool: string[] =
    category === 'dimos' ? DIMOS_ORGS :
    category === 'other' ? OTHER_ORGS :
    ALL_ORGS

  // Accent + case insensitive substring match
  const normQuery = normalizeSearch(search)
  const filtered = normQuery === ''
    ? pool
    : pool.filter(o => normalizeSearch(o).includes(normQuery))

  const toggle = (org: string) => {
    onChange(
      selected.includes(org)
        ? selected.filter(o => o !== org)
        : [...selected, org]
    )
  }

  const catLabel: Record<OrgCategory, string> = {
    all:   `Όλοι (${ALL_ORGS.length})`,
    dimos: `Δήμοι (${DIMOS_ORGS.length})`,
    other: `Άλλοι (${OTHER_ORGS.length})`,
  }

  return (
    <div className="ca-org-filter">
      <div className="ca-org-filter__head">
        <span className="eyebrow">Φορείς ({selected.length} επιλεγμένοι από {ALL_ORGS.length})</span>
        <div className="ca-org-filter__actions">
          <button onClick={() => onChange(TOP10_ORGS)} className="ca-filter-btn">Top 10 (συμβ.)</button>
          <button onClick={() => onChange(TOP10_BY_TOTAL)} className="ca-filter-btn">Top 10 (δαπάνη)</button>
          <button onClick={() => onChange(ALL_ORGS.slice(0, 20))} className="ca-filter-btn">Top 20</button>
          <button onClick={() => onChange(DIMOS_ORGS)} className="ca-filter-btn">Δήμοι</button>
          <button onClick={() => onChange(OTHER_ORGS)} className="ca-filter-btn">Άλλοι</button>
          <button onClick={() => onChange([])} className="ca-filter-btn ca-filter-btn--clear">Καθαρισμός</button>
        </div>
      </div>

      {/* Category tabs */}
      <div className="ca-org-tabs">
        {(['all', 'dimos', 'other'] as OrgCategory[]).map(cat => (
          <button
            key={cat}
            className={`ca-org-tab${category === cat ? ' ca-org-tab--active' : ''}`}
            onClick={() => setCategory(cat)}
          >
            {catLabel[cat]}
          </button>
        ))}
      </div>

      <input
        type="text"
        placeholder="Αναζήτηση φορέα…"
        value={search}
        onChange={e => setSearch(e.target.value)}
        className="ca-org-search"
        aria-label="Αναζήτηση φορέα"
      />
      <div className="ca-org-list" role="group" aria-label="Λίστα φορέων">
        {filtered.length === 0 && search !== '' && (
          <p className="ca-empty-note">
            Δεν βρέθηκε φορέας για «{search}» στα δεδομένα πυροπροστασίας 2024–2026.
          </p>
        )}
        {filtered.map(org => (
          <label key={org} className="ca-org-item">
            <input
              type="checkbox"
              checked={selected.includes(org)}
              onChange={() => toggle(org)}
            />
            <span title={org}>
              {org.length > 44 ? org.slice(0, 42) + '…' : org}
            </span>
          </label>
        ))}
      </div>
    </div>
  )
}

// ── Κύριο component ───────────────────────────────────────────────────
export default function ContractAnalysis() {
  const [barMetric,    setBarMetric]    = useState<BarMetric>('total')
  const [hmMetric,     setHmMetric]     = useState<BarMetric>('count')
  const [selectedOrgs, setSelectedOrgs] = useState<string[]>(TOP10_ORGS)
  const [showOrgFilter, setShowOrgFilter] = useState(false)

  // When metric changes, auto-switch to the appropriate top 10
  const handleHmMetricChange = (metric: BarMetric) => {
    setHmMetric(metric)
    setSelectedOrgs(metric === 'count' ? TOP10_ORGS : TOP10_BY_TOTAL)
  }

  const maxContractTypeM = Math.max(...contractTypeData.map(d => d.total_m))
  const maxProcM         = Math.max(...procedureData.map(d => d.total_m))
  const maxOrgM          = Math.max(...topOrgs.map(d => d.total_m))

  return (
    <section id="analysis" className="ca-section section-rule" aria-label="Ανάλυση Συμβάσεων">

      {/* ── Header ── */}
      <div className="ca-header section-head">
        <div className="eyebrow">Ανάλυση Δεδομένων / raw_procurements.csv · χωρίς ΦΠΑ</div>
        <h2>Ανάλυση Συμβάσεων Πυροπροστασίας 2024–2026</h2>
        <p className="ca-header-note">
          Στατιστική επεξεργασία <strong>3.875 καταχωρημένων συμβάσεων</strong> από τα αρχεία Διαύγεια / ΚΗΜΔΗΣ.
          Συνολική δαπάνη <strong>€ 760,7M χωρίς ΦΠΑ</strong> — υπηρεσίες καθαρισμού,
          προμήθειες εξοπλισμού και έργα υποδομής.
        </p>
      </div>

      {/* ── KPIs ── */}
      <div className="ca-kpi-grid">
        {[
          { label: 'Σύνολο Συμβάσεων',     value: '3.875',     note: 'ανεξαρτήτως κατηγορίας' },
          { label: 'Συνολική Δαπάνη',       value: '€ 760,7M',  note: 'χωρίς ΦΠΑ · 2024–2026' },
          { label: 'Μέσο Ποσό',            value: '€ 196.305',  note: 'διάμεσος: € 13.032 χ.ΦΠΑ' },
          { label: 'Κορύφωση Συμβάσεων',   value: 'Ιούλ 2025',  note: '313 συμβάσεις σε 1 μήνα' },
          { label: 'Κορύφωση Δαπάνης',     value: 'Μαρ 2024',   note: '€ 75,1M σε 1 μήνα χ.ΦΠΑ' },
          { label: 'Απευθείας Αναθέσεις',  value: '69,6%',      note: '2.697 από τις 3.875 συμβ.' },
        ].map(k => (
          <article className="kpi-tile" key={k.label}>
            <div className="eyebrow">{k.label}</div>
            <div className="kpi-value">{k.value}</div>
            <p>{k.note}</p>
          </article>
        ))}
      </div>

      {/* ── Bar Chart ── */}
      <div className="ca-chart-block">
        <div className="ca-chart-head">
          <div className="eyebrow">Μηνιαία Εξέλιξη Συμβάσεων — Ιαν 2024 έως Φεβ 2026</div>
          <div className="ca-metric-toggle">
            <button
              className={`ca-toggle-btn${barMetric === 'total' ? ' ca-toggle-btn--active' : ''}`}
              onClick={() => setBarMetric('total')}
            >Δαπάνη (χ.ΦΠΑ)</button>
            <button
              className={`ca-toggle-btn${barMetric === 'count' ? ' ca-toggle-btn--active' : ''}`}
              onClick={() => setBarMetric('count')}
            >Αριθμός Συμβάσεων</button>
          </div>
          <div className="ca-chart-legend">
            <span className="ca-legend-dot ca-legend-dot--fire" aria-hidden="true" />
            <span>Αντιπυρική περίοδος (Μαΐ–Αυγ)</span>
          </div>
        </div>
        <BarChart metric={barMetric} />
        <p className="ca-chart-note">
          {barMetric === 'total'
            ? 'Εμφανείς κορυφώσεις Φεβ–Μαρ 2024 (μεγάλες κεντρικές συμβάσεις) και Μαΐ 2025 (αντιπυρική περίοδος). Χαμηλές αξίες Νοε–Ιαν εκφράζουν χειμερινή επιβράδυνση.'
            : 'Ο αριθμός συμβάσεων κορυφώνεται Ιούλ–Αυγ 2025 (313 και 265 αντίστοιχα) — εποχική συγκέντρωση στους θερινούς μήνες υψηλής πυρκαγιαστικής επικινδυνότητας.'
          }
        </p>
      </div>

      {/* ── Heatmap ── */}
      <div className="ca-heatmap-block">
        <div className="ca-chart-head">
          <div className="eyebrow">Heatmap Φορέων ανά Μήνα — {selectedOrgs.length} φορείς επιλεγμένοι</div>
          <div className="ca-heatmap-controls">
            <div className="ca-metric-toggle">
              <button
                className={`ca-toggle-btn${hmMetric === 'count' ? ' ca-toggle-btn--active' : ''}`}
                onClick={() => handleHmMetricChange('count')}
              >Αριθμός Συμβ.</button>
              <button
                className={`ca-toggle-btn${hmMetric === 'total' ? ' ca-toggle-btn--active' : ''}`}
                onClick={() => handleHmMetricChange('total')}
              >Δαπάνη (χ.ΦΠΑ)</button>
            </div>
            <button
              className="ca-filter-btn ca-filter-btn--toggle"
              onClick={() => setShowOrgFilter(v => !v)}
              aria-expanded={showOrgFilter}
            >
              {showOrgFilter ? '▲ Απόκρυψη φορέων' : '▼ Επιλογή φορέων'}
            </button>
          </div>
        </div>

        {showOrgFilter && (
          <OrgFilter selected={selectedOrgs} onChange={setSelectedOrgs} />
        )}

        {selectedOrgs.length === 0 ? (
          <p className="ca-empty-note">Επιλέξτε τουλάχιστον έναν φορέα για προβολή του heatmap.</p>
        ) : (
          <Heatmap selectedOrgs={selectedOrgs} metric={hmMetric} />
        )}

        <p className="ca-chart-note">
          Κάθε κελί = 1 μήνας × 1 φορέας · χρώμα κλίμακας: ανοιχτό (χαμηλό) → κόκκινο (υψηλό) ·
          {ALL_ORGS.length} φορείς διαθέσιμοι (265 Δήμοι + 206 άλλοι) · hover για λεπτομέρειες.
        </p>
      </div>

      {/* ── Type & Procedure breakdowns ── */}
      <div className="ca-double-grid">
        <div className="ca-breakdown-block">
          <div className="eyebrow">Κατανομή κατά Τύπο Σύμβασης (χ.ΦΠΑ)</div>
          <div className="ca-bars">
            {contractTypeData.map(item => <HBar key={item.label} item={item} max={maxContractTypeM} />)}
          </div>
          <p className="ca-sub-note">
            Οι υπηρεσίες (καθαρισμοί, αποψίλωση, δασική διαχείριση) κυριαρχούν τόσο σε
            αριθμό όσο και σε αξία. Οι προμήθειες αφορούν κυρίως εξοπλισμό και οχήματα.
          </p>
        </div>
        <div className="ca-breakdown-block">
          <div className="eyebrow">Κατανομή κατά Διαδικασία Ανάθεσης (χ.ΦΠΑ)</div>
          <div className="ca-bars">
            {procedureData.map(item => <HBar key={item.label} item={item} max={maxProcM} />)}
          </div>
          <p className="ca-sub-note">
            Σχεδόν 7 στις 10 συμβάσεις ανατίθενται απευθείας. Ωστόσο, οι ανοικτές
            διαδικασίες συγκεντρώνουν μεγαλύτερη αναλογική αξία λόγω μεγαλύτερων έργων.
          </p>
        </div>
      </div>

      {/* ── Top organizations ── */}
      <div className="ca-table-block">
        <div className="eyebrow">Κορυφαίοι Φορείς κατά Συνολική Δαπάνη (χ.ΦΠΑ)</div>
        <div className="ca-table">
          <div className="ca-table__head">
            <span>Φορέας</span>
            <span className="ca-col-right">Συμβάσεις</span>
            <span className="ca-col-right">Δαπάνη (€M)</span>
            <span>Κλίμακα</span>
          </div>
          {topOrgs.map(org => (
            <div className="ca-table__row" key={org.name}>
              <span className="ca-org-name">{org.name}</span>
              <span className="ca-col-right ca-mono">{org.contracts.toLocaleString('el-GR')}</span>
              <span className="ca-col-right ca-accent ca-mono">{org.total_m.toFixed(1)}</span>
              <div className="ca-inline-bar">
                <div className="ca-inline-bar__fill" style={{ width: `${(org.total_m / maxOrgM) * 100}%` }} />
              </div>
            </div>
          ))}
        </div>
        <p className="ca-sub-note">
          Τα δύο Υπουργεία (Περιβάλλοντος + Κλιματικής Κρίσης) συγκεντρώνουν €598,1M —
          το <strong>78,6%</strong> της συνολικής δαπάνης χωρίς ΦΠΑ.
        </p>
      </div>

      {/* ── Top CPV ── */}
      <div className="ca-table-block">
        <div className="eyebrow">Κορυφαίες Κατηγορίες CPV — Αντικείμενο Σύμβασης (χ.ΦΠΑ)</div>
        <div className="ca-cpv-grid">
          {topCpv.map((c, i) => (
            <article className="ca-cpv-card" key={c.cpv}>
              <div className="ca-cpv-rank">#{i + 1}</div>
              <div className="ca-cpv-code eyebrow">{c.cpv}</div>
              <div className="ca-cpv-desc">{c.desc}</div>
              <div className="ca-cpv-stats">
                <div>
                  <span className="label">Αξία</span>
                  <strong className="ca-accent">€ {c.total_m.toFixed(1)}M</strong>
                </div>
                <div>
                  <span className="label">Συμβάσεις</span>
                  <strong>{c.count}</strong>
                </div>
              </div>
            </article>
          ))}
        </div>
        <p className="ca-sub-note">
          Οι 8 κορυφαίες CPV αφορούν αποκλειστικά δασικές υπηρεσίες και αποψίλωση —
          συστηματική προσέγγιση μείωσης καύσιμης ύλης ως βασική στρατηγική πρόληψης.
        </p>
      </div>

      {/* ── Findings ── */}
      <div className="ca-findings">
        <div className="eyebrow">Βασικά Ευρήματα Ανάλυσης</div>
        <div className="ca-findings__grid">
          {[
            {
              num: '01',
              title: 'Κυριαρχία Άμεσων Αναθέσεων',
              text: 'Το 69,6% των συμβάσεων γίνεται χωρίς ανοικτό διαγωνισμό. Αυτό επιταχύνει τις αναθέσεις πριν την αντιπυρική περίοδο αλλά περιορίζει τον ανταγωνισμό.',
            },
            {
              num: '02',
              title: 'Εποχική Συγκέντρωση',
              text: 'Η κορύφωση εμφανίζεται Μαΐο–Ιούλιο. Το heatmap αποκαλύπτει τη διαφορετική εποχικότητα ανά φορέα — οι κεντρικοί φορείς ενεργοποιούνται νωρίτερα.',
            },
            {
              num: '03',
              title: 'Συγκέντρωση σε 2 Φορείς',
              text: 'Τα δύο Υπουργεία συγκεντρώνουν €598M χ.ΦΠΑ (78,6%) ενώ οι 645 συμβάσεις της Περιφέρειας Αττικής αντιπροσωπεύουν μόλις €17,8M — υψηλό πλήθος, μικρές αξίες.',
            },
            {
              num: '04',
              title: 'Υψηλή Διασπορά Ποσών',
              text: 'Η διάμεσος (€13.032 χ.ΦΠΑ) είναι πολύ κατώτερη του μέσου (€196.305), εκφράζοντας πολυπληθείς μικρές δημοτικές αναθέσεις δίπλα σε λίγες, πολύ μεγάλες συμβάσεις.',
            },
          ].map(f => (
            <article className="ca-finding-card" key={f.num}>
              <div className="ca-finding-num">{f.num}</div>
              <div className="ca-finding-body">
                <strong>{f.title}</strong>
                <p>{f.text}</p>
              </div>
            </article>
          ))}
        </div>
      </div>

      <div className="ca-footer-note">
        <span className="eyebrow">Πηγή δεδομένων</span>
        <span>
          raw_procurements.csv · 3.875 εγγραφές · Ιαν 2024 – Φεβ 2026 · Διαύγεια / ΚΗΜΔΗΣ ·
          Τιμές χωρίς ΦΠΑ · Τελευταία επεξεργασία: Φεβ 2026
        </span>
      </div>
    </section>
  )
}
