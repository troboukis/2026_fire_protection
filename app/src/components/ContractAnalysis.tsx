// Ανάλυση Συμβάσεων — D3 charts, χωρίς ΦΠΑ, από το 2024 έως την τρέχουσα χρονιά
import { useEffect, useRef, useState, useCallback, useMemo } from 'react'
import * as d3 from 'd3'
import { supabase } from '../lib/supabase'
import ComponentTag from './ComponentTag'
import DataLoadingCard from './DataLoadingCard'
import TopAuthoritiesSection from './TopAuthoritiesSection'

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

type TopOrgItem = {
  name: string
  contracts: number
  total_m: number
}

type TopCpvItem = {
  cpv: string
  desc: string
  count: number
  mainProcedure: string
  mainProcedurePct: number
}

type SunburstDatum = {
  name: string
  value?: number
  children?: SunburstDatum[]
}

type SectionRow = {
  signedDate: string | null
  effectiveStart: string
  effectiveEnd: string
  orgName: string
  authorityLabel: string
  contractType: string
  procedure: string
  amount: number
  cpvs: string[]
}

type AnalysisData = {
  monthly: MonthlyPoint[]
  contractTypeData: BarItem[]
  procedureData: BarItem[]
  topOrgs: TopOrgItem[]
  topCpv: TopCpvItem[]
  sectionRows: SectionRow[]
  totalContracts: number
  totalAmount: number
  avgAmount: number
  medianAmount: number
  peakContractsMonthLabel: string
  peakContractsMonthCount: number
  peakSpendMonthLabel: string
  peakSpendMonthAmount: number
  directAwardPct: number
}

type ContractAnalysisRpcPayload = {
  sectionRows?: unknown
}

const ANALYSIS_START = '2024-01-01'
const CURRENT_YEAR = new Date().getFullYear()
const ANALYSIS_END = `${CURRENT_YEAR}-12-31`

const BREAKPOINT_XS = 360
const BREAKPOINT_SM = 440
const BREAKPOINT_MD = 640

const MONTH_NAMES_SHORT = ['Ιαν', 'Φεβ', 'Μαρ', 'Απρ', 'Μαϊ', 'Ιουν', 'Ιουλ', 'Αυγ', 'Σεπ', 'Οκτ', 'Νοε', 'Δεκ']

function monthLabelFromMonthKey(month: string): string {
  const [y, m] = month.split('-')
  const mi = Number(m) - 1
  const yy = y.slice(2)
  return `${MONTH_NAMES_SHORT[mi] ?? m} '${yy}`
}

function toneForPct(pct: number): BarItem['tone'] {
  if (pct >= 50) return 'high'
  if (pct >= 20) return 'mid'
  if (pct >= 5) return 'low'
  return 'faint'
}

function buildRangeMonths(startYm: string, endYm: string): string[] {
  const out: string[] = []
  let [y, m] = startYm.split('-').map(Number)
  const [ey, em] = endYm.split('-').map(Number)
  while (y < ey || (y === ey && m <= em)) {
    out.push(`${y}-${String(m).padStart(2, '0')}`)
    m += 1
    if (m > 12) {
      m = 1
      y += 1
    }
  }
  return out
}

function cleanDateString(v: string | null | undefined): string | null {
  const s = String(v ?? '').trim()
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null
  return s
}

// ── Live aggregates loaded from Supabase ─────────────────────────────

// ── Βοηθητικό HBar ────────────────────────────────────────────────────
function HBar({ item }: { item: BarItem }) {
  const toneMap = {
    high:  'rgba(211,72,45,0.9)',
    mid:   'rgba(211,72,45,0.55)',
    low:   'rgba(211,72,45,0.32)',
    faint: 'rgba(211,72,45,0.18)',
  }
  return (
    <div className="ca-bar-row">
      <div className="ca-bar-label">
        <span className="ca-bar-title">
          <span>{item.label}</span>
          <span className="ca-bar-title__dot" aria-hidden="true" />
          <span className="ca-bar-title__meta">
            <strong>€ {item.total_m.toFixed(1)}M</strong>
            <span> ({item.value.toLocaleString('el-GR')} συμβ.)</span>
          </span>
        </span>
        <span className="ca-bar-pct">{item.pct}%</span>
      </div>
      <div className="ca-bar-track">
        <div className="ca-bar-fill" style={{ width: `${item.pct}%`, background: toneMap[item.tone] }} />
      </div>
    </div>
  )
}

// ── D3 Bar Chart ──────────────────────────────────────────────────────
type BarMetric = 'count' | 'total'

function BarChart({ metric, monthly }: { metric: BarMetric; monthly: MonthlyPoint[] }) {
  const containerRef = useRef<HTMLDivElement>(null)
  const svgRef       = useRef<SVGSVGElement>(null)

  const draw = useCallback(() => {
    if (!svgRef.current || !containerRef.current) return
    const containerW = containerRef.current.clientWidth
    if (containerW === 0) return

    const isNarrow = containerW <= BREAKPOINT_MD
    const isMobile = containerW <= BREAKPOINT_SM
    const margin = {
      top: isMobile ? 10 : 18,
      right: isMobile ? 8 : 16,
      bottom: isMobile ? 38 : (isNarrow ? 52 : 58),
      left: isMobile ? 48 : 68,
    }
    const W = containerW
    const H = isMobile ? 184 : 240
    const innerW = W - margin.left - margin.right
    const innerH = H - margin.top - margin.bottom

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()
    svg.attr('width', W).attr('height', H)

    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`)

    // Scales
    const monthDomain = monthly.map(d => d.month)
    const x = d3.scaleBand<string>()
      .domain(monthDomain)
      .range([0, innerW])
      .padding(0.22)

    const yVal = (d: MonthlyPoint) => metric === 'count' ? d.count : d.total_k
    const yMax = d3.max(monthly, yVal) ?? 1

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

    const mobileTickStep =
      containerW <= BREAKPOINT_XS ? 5 :
      containerW <= BREAKPOINT_SM ? 4 :
      containerW <= BREAKPOINT_MD ? 3 : 1
    const xTickValues = isNarrow
      ? monthDomain.filter((_m, i) => i % mobileTickStep === 0 || i === monthDomain.length - 1)
      : monthDomain
    const xTickLabel = (monthKey: string): string => {
      const point = monthly.find((p) => p.month === monthKey)
      if (!point) return monthKey
      if (!isNarrow) return point.label
      const [yearStr, monthStr] = monthKey.split('-')
      const monthNum = Number(monthStr)
      const monthName = MONTH_NAMES_SHORT[monthNum - 1] ?? monthStr
      if (monthStr === '01' || monthKey === monthDomain[monthDomain.length - 1]) {
        return `${monthName} '${yearStr.slice(2)}`
      }
      return monthName
    }

    // X axis
    g.append('g')
      .attr('transform', `translate(0,${innerH})`)
      .call(
        d3.axisBottom(x)
          .tickValues(xTickValues)
          .tickFormat((m) => xTickLabel(String(m)))
      )
      .call(sel => sel.select('.domain').attr('stroke', 'rgba(17,17,17,0.18)'))
      .call(sel => sel.selectAll('.tick line').attr('stroke', 'rgba(17,17,17,0.12)'))
      .call(sel => sel.selectAll<SVGTextElement, unknown>('.tick text')
        .attr('font-family', 'IBM Plex Mono, monospace')
        .attr('font-size', isNarrow ? '8.5' : '9.5')
        .attr('fill', 'rgba(17,17,17,0.55)')
        .attr('transform', isNarrow ? 'rotate(-30)' : 'rotate(-40)')
        .attr('text-anchor', 'end')
        .attr('dy', '0.35em')
        .attr('dx', isNarrow ? '-0.32em' : '-0.4em')
      )

    // Bars
    const tooltip = d3.select('#ca-bar-tooltip')

    g.selectAll<SVGRectElement, MonthlyPoint>('.bar')
      .data(monthly)
      .join('rect')
      .attr('class', 'bar')
      .attr('x',      d => x(d.month) ?? 0)
      .attr('width',  x.bandwidth())
      .attr('y',      d => y(yVal(d)))
      .attr('height', d => innerH - y(yVal(d)))
      .attr('fill',   d => ['05', '06', '07', '08'].includes(d.month.slice(5, 7)) ? 'rgba(211,72,45,0.82)' : 'rgba(17,17,17,0.28)')
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
      .on('click', (event: MouseEvent, d) => {
        event.stopPropagation()
        const rect = containerRef.current!.getBoundingClientRect()
        const val = metric === 'count'
          ? `${d.count} συμβάσεις`
          : `€ ${(d.total_k / 1000).toFixed(2)}M (χωρίς ΦΠΑ)`
        tooltip
          .style('display', 'block')
          .style('left', `${(event.clientX - rect.left + 10)}px`)
          .style('top', `${(event.clientY - rect.top - 36)}px`)
          .html(`<strong>${d.label}</strong><br/>${val}`)
      })
      .on('mouseout', (event: MouseEvent) => {
        tooltip.style('display', 'none')
        d3.select(event.currentTarget as Element).attr('opacity', 1)
      })

  }, [metric, monthly])

  useEffect(() => {
    draw()
    const ro = new ResizeObserver(draw)
    if (containerRef.current) ro.observe(containerRef.current)
    return () => ro.disconnect()
  }, [draw])

  return (
    <div ref={containerRef} className="ca-bar-container" onClick={() => d3.select('#ca-bar-tooltip').style('display', 'none')}>
      <svg ref={svgRef} className="ca-d3-bar-svg" />
      <div id="ca-bar-tooltip" className="ca-tooltip app-tooltip" />
    </div>
  )
}

function buildProcedureAuthoritySunburstData(rows: SectionRow[]): SunburstDatum | null {
  const procedureToAuthorities = new Map<string, Map<string, number>>()

  for (const row of rows) {
    if (!procedureToAuthorities.has(row.procedure)) procedureToAuthorities.set(row.procedure, new Map())
    const authorityMap = procedureToAuthorities.get(row.procedure)!
    authorityMap.set(row.authorityLabel, (authorityMap.get(row.authorityLabel) ?? 0) + 1)
  }

  const children = [...procedureToAuthorities.entries()]
    .map(([procedure, authorityMap]) => ({
      name: procedure,
      children: [...authorityMap.entries()]
        .map(([authority, count]) => ({ name: authority, value: count }))
        .sort((a, b) => (b.value ?? 0) - (a.value ?? 0) || a.name.localeCompare(b.name, 'el')),
    }))
    .sort((a, b) => {
      const aTotal = a.children.reduce((sum, item) => sum + (item.value ?? 0), 0)
      const bTotal = b.children.reduce((sum, item) => sum + (item.value ?? 0), 0)
      return bTotal - aTotal || a.name.localeCompare(b.name, 'el')
    })

  if (children.length === 0) return null

  return {
    name: 'Συμβάσεις',
    children,
  }
}

function buildTopCpvList(
  cpvAggMap: Map<string, { count: number; procedures: Map<string, number> }>,
  limit = 8,
): TopCpvItem[] {
  return [...cpvAggMap.entries()]
    .map(([cpv, v]) => {
      const main = [...v.procedures.entries()].sort((a, b) => b[1] - a[1])[0]
      const mainCount = main?.[1] ?? 0
      return {
        cpv,
        desc: cpv,
        count: v.count,
        mainProcedure: main?.[0] ?? '—',
        mainProcedurePct: Number(((mainCount / Math.max(v.count, 1)) * 100).toFixed(1)),
      }
    })
    .sort((a, b) => b.count - a.count)
    .slice(0, limit)
}

function toSectionRow(value: unknown): SectionRow | null {
  if (!value || typeof value !== 'object') return null
  const row = value as Record<string, unknown>
  const signedDate = cleanDateString(String(row.signedDate ?? '')) ?? null
  const effectiveStart = cleanDateString(String(row.effectiveStart ?? '')) ?? signedDate
  const effectiveEnd = cleanDateString(String(row.effectiveEnd ?? '')) ?? effectiveStart
  if (!effectiveStart || !effectiveEnd) return null

  const cpvs = Array.isArray(row.cpvs)
    ? row.cpvs.map((item) => String(item ?? '').trim()).filter(Boolean)
    : []

  return {
    signedDate,
    effectiveStart,
    effectiveEnd,
    orgName: String(row.orgName ?? '—').trim() || '—',
    authorityLabel: String(row.authorityLabel ?? '—').trim() || '—',
    contractType: String(row.contractType ?? 'Λοιπές').trim() || 'Λοιπές',
    procedure: String(row.procedure ?? 'Άλλη').trim() || 'Άλλη',
    amount: Number(row.amount ?? 0) || 0,
    cpvs,
  }
}

function buildAnalysisDataFromRows(sectionRows: SectionRow[]): AnalysisData {
  const periodStart = ANALYSIS_START
  const periodEnd = ANALYSIS_END

  const monthlyMap = new Map<string, { count: number; total_k: number }>()
  const contractTypeMap = new Map<string, { count: number; total: number }>()
  const procedureMap = new Map<string, { count: number; total: number }>()
  const cpvAggMap = new Map<string, { count: number; procedures: Map<string, number> }>()
  const amounts: number[] = []
  const monthKeys: string[] = []

  let totalContracts = 0
  let totalAmount = 0
  let signedWindowContracts = 0
  let signedWindowDirectAwards = 0

  for (const row of sectionRows) {
    const baseDateForMonth = row.signedDate ?? row.effectiveStart
    const monthAnchorDate = baseDateForMonth < periodStart ? periodStart : baseDateForMonth
    const month = monthAnchorDate.slice(0, 7)
    if (!/^\d{4}-\d{2}$/.test(month)) continue
    if (month < periodStart.slice(0, 7) || month > periodEnd.slice(0, 7)) continue

    monthKeys.push(month)
    totalContracts += 1
    totalAmount += row.amount
    if (row.amount > 0) amounts.push(row.amount)

    if (row.signedDate && row.signedDate >= periodStart && row.signedDate <= periodEnd) {
      signedWindowContracts += 1
      if (row.procedure === 'Απευθείας Ανάθεση') signedWindowDirectAwards += 1
    }

    const monthly = monthlyMap.get(month) ?? { count: 0, total_k: 0 }
    monthly.count += 1
    monthly.total_k += row.amount / 1000
    monthlyMap.set(month, monthly)

    const ct = contractTypeMap.get(row.contractType) ?? { count: 0, total: 0 }
    ct.count += 1
    ct.total += row.amount
    contractTypeMap.set(row.contractType, ct)

    const pr = procedureMap.get(row.procedure) ?? { count: 0, total: 0 }
    pr.count += 1
    pr.total += row.amount
    procedureMap.set(row.procedure, pr)

    for (const cpv of row.cpvs) {
      const ca = cpvAggMap.get(cpv) ?? { count: 0, procedures: new Map<string, number>() }
      ca.count += 1
      ca.procedures.set(row.procedure, (ca.procedures.get(row.procedure) ?? 0) + 1)
      cpvAggMap.set(cpv, ca)
    }
  }

  const fallbackStartMonth = periodStart.slice(0, 7)
  const fallbackEndMonth = periodEnd.slice(0, 7)
  const sortedMonthKeys = [...monthKeys].sort()
  const minMonth = sortedMonthKeys[0] ?? fallbackStartMonth
  const maxMonth = sortedMonthKeys[sortedMonthKeys.length - 1] ?? fallbackEndMonth
  const monthly: MonthlyPoint[] = buildRangeMonths(minMonth, maxMonth).map((month) => {
    const data = monthlyMap.get(month) ?? { count: 0, total_k: 0 }
    return {
      month,
      label: monthLabelFromMonthKey(month),
      count: data.count,
      total_k: Number(data.total_k.toFixed(1)),
    }
  })

  const toBarData = (map: Map<string, { count: number; total: number }>, labels: string[]): BarItem[] =>
    labels.map((label) => {
      const v = map.get(label) ?? { count: 0, total: 0 }
      const pct = totalAmount > 0 ? (v.total / totalAmount) * 100 : 0
      return {
        label,
        value: v.count,
        total_m: v.total / 1_000_000,
        pct: Number(pct.toFixed(1)),
        tone: toneForPct(pct),
      }
    }).sort((a, b) => b.pct - a.pct || b.total_m - a.total_m || a.label.localeCompare(b.label, 'el'))

  const sortedAmounts = [...amounts].sort((a, b) => a - b)
  const medianAmount = sortedAmounts.length === 0
    ? 0
    : sortedAmounts.length % 2 === 1
      ? sortedAmounts[(sortedAmounts.length - 1) / 2]
      : (sortedAmounts[sortedAmounts.length / 2 - 1] + sortedAmounts[sortedAmounts.length / 2]) / 2

  const peakCountMonth = monthly.reduce((acc, cur) => (cur.count > acc.count ? cur : acc), monthly[0] ?? { month: '2024-01', label: "Ιαν '24", count: 0, total_k: 0 })
  const peakSpendMonth = monthly.reduce((acc, cur) => (cur.total_k > acc.total_k ? cur : acc), monthly[0] ?? { month: '2024-01', label: "Ιαν '24", count: 0, total_k: 0 })

  return {
    monthly,
    contractTypeData: toBarData(contractTypeMap, ['Υπηρεσίες', 'Προμήθειες', 'Έργα', 'Λοιπές']),
    procedureData: toBarData(procedureMap, ['Απευθείας Ανάθεση', 'Ανοιχτή Διαδικασία', 'Διαπραγμάτευση', 'Άλλη']),
    topOrgs: [],
    topCpv: buildTopCpvList(cpvAggMap),
    sectionRows,
    totalContracts,
    totalAmount,
    avgAmount: totalContracts > 0 ? totalAmount / totalContracts : 0,
    medianAmount,
    peakContractsMonthLabel: peakCountMonth.label,
    peakContractsMonthCount: peakCountMonth.count,
    peakSpendMonthLabel: peakSpendMonth.label,
    peakSpendMonthAmount: peakSpendMonth.total_k * 1000,
    directAwardPct: signedWindowContracts > 0 ? (signedWindowDirectAwards / signedWindowContracts) * 100 : 0,
  }
}

function truncateLabel(label: string, maxLength: number): string {
  if (label.length <= maxLength) return label
  return `${label.slice(0, Math.max(0, maxLength - 1)).trimEnd()}…`
}

function ZoomableSunburst({ data }: { data: SunburstDatum | null }) {
  const containerRef = useRef<HTMLDivElement>(null)
  const svgRef = useRef<SVGSVGElement>(null)

  const draw = useCallback(() => {
    if (!svgRef.current || !containerRef.current || !data) return

    type ArcSlice = {
      x0: number
      x1: number
      y0: number
      y1: number
    }

    type SunburstNode = d3.HierarchyRectangularNode<SunburstDatum> & {
      current: ArcSlice
      target: ArcSlice
    }

    const containerW = containerRef.current.clientWidth
    if (containerW === 0) return

    const isCompact = containerW <= BREAKPOINT_SM
    const size = Math.max(320, Math.min(containerW, 720))

    const rootHierarchy = d3.hierarchy(data)
      .sum((d) => d.value ?? 0)
      .sort((a, b) => (b.value ?? 0) - (a.value ?? 0))
    const root = d3.partition<SunburstDatum>()
      .size([2 * Math.PI, rootHierarchy.height + 1])(rootHierarchy) as SunburstNode

    const outerRadius = size / 2 - (isCompact ? 6 : 0)
    const centerRadius = isCompact ? size * 0.21 : size / 6
    const ringWidth = (outerRadius - centerRadius) / Math.max(rootHierarchy.height, 1)
    const radialAt = (depth: number) => {
      if (depth <= 0) return 0
      return centerRadius + (depth - 1) * ringWidth
    }

    root.each((node) => {
      node.current = { x0: node.x0, x1: node.x1, y0: node.y0, y1: node.y1 }
      node.target = { x0: node.x0, x1: node.x1, y0: node.y0, y1: node.y1 }
    })

    const procedureNames = data.children?.map((item) => item.name) ?? []
    const color = d3.scaleOrdinal<string, string>()
      .domain(procedureNames)
      .range(['#d3482d', '#244b67', '#b9852f', '#796f63', '#5f8b55', '#8b3a4a'])

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()
    svg
      .attr('viewBox', `${-size / 2} ${-size / 2} ${size} ${size}`)
      .attr('width', size)
      .attr('height', size)

    const tooltip = d3.select(containerRef.current).select<HTMLDivElement>('.ca-sunburst-tooltip')

    const arc = d3.arc<ArcSlice>()
      .startAngle((d) => d.x0)
      .endAngle((d) => d.x1)
      .padAngle((d) => Math.min((d.x1 - d.x0) / 2, 0.005))
      .padRadius(outerRadius * 0.72)
      .innerRadius((d) => radialAt(d.y0))
      .outerRadius((d) => Math.max(radialAt(d.y0), radialAt(d.y1) - 1))

    const arcVisible = (d: ArcSlice) => d.y1 <= 3 && d.y0 >= 1 && d.x1 > d.x0
    const labelVisible = (d: ArcSlice) => {
      if (!(d.y1 <= 3 && d.y0 >= 1 && d.x1 > d.x0)) return false
      const angle = d.x1 - d.x0
      if (isCompact) {
        return d.y0 === 1 ? angle > 0.34 : angle > 0.18
      }
      return d.y0 === 1 ? angle > 0.16 : angle > 0.1
    }
    const labelTransform = (d: ArcSlice) => {
      const x = ((d.x0 + d.x1) / 2) * 180 / Math.PI
      const y = (radialAt(d.y0) + radialAt(d.y1)) / 2
      return `rotate(${x - 90}) translate(${y},0) rotate(${x < 180 ? 0 : 180})`
    }
    const labelFontSize = (node: SunburstNode) => {
      const d = node.current
      const ringThickness = Math.max(1, radialAt(d.y1) - radialAt(d.y0))
      const midRadius = (radialAt(d.y0) + radialAt(d.y1)) / 2
      const arcLength = Math.max(1, midRadius * (d.x1 - d.x0))
      const charCount = Math.max(1, node.data.name.length)
      const maxByArc = arcLength / Math.max(charCount * 0.62, 1)
      const maxByRing = ringThickness * (node.depth === 1 ? 0.34 : 0.26)
      const maxSize = Math.min(maxByArc, maxByRing, isCompact ? (node.depth === 1 ? 7.2 : 6.1) : (node.depth === 1 ? 10 : 9))
      const minSize = isCompact ? (node.depth === 1 ? 6.1 : 5.2) : (node.depth === 1 ? 7.2 : 6.2)
      return Number(Math.max(minSize, maxSize).toFixed(1))
    }
    const labelCanFit = (node: SunburstNode) => {
      const d = node.current
      const ringThickness = Math.max(1, radialAt(d.y1) - radialAt(d.y0))
      const midRadius = (radialAt(d.y0) + radialAt(d.y1)) / 2
      const arcLength = Math.max(1, midRadius * (d.x1 - d.x0))
      const fontSize = labelFontSize(node)
      const estimatedTextWidth = truncateLabel(
        node.data.name,
        isCompact ? (node.depth === 1 ? 14 : 11) : (node.depth === 1 ? 18 : 16),
      ).length * fontSize * 0.58
      return estimatedTextWidth <= arcLength * 0.92 && fontSize <= ringThickness * 0.82
    }

    const g = svg.append('g')
    const center = g.append('g')
      .attr('class', 'ca-sunburst-center')
      .style('pointer-events', 'none')

    const centerLabelPrimary = center.append('text')
      .attr('class', 'ca-sunburst-center__label')
      .attr('text-anchor', 'middle')
      .attr('x', 0)
    const centerLabelSecondary = center.append('text')
      .attr('class', 'ca-sunburst-center__label')
      .attr('text-anchor', 'middle')
      .attr('x', 0)
    const centerValue = center.append('text')
      .attr('class', 'ca-sunburst-center__value')
      .attr('text-anchor', 'middle')
      .attr('x', 0)
    const centerHint = center.append('text')
      .attr('class', 'ca-sunburst-center__hint')
      .attr('text-anchor', 'middle')
      .attr('x', 0)

    const updateCenter = (node: SunburstNode) => {
      const total = Number(node.value ?? 0).toLocaleString('el-GR')
      const primaryLabel = node.depth === 0
        ? (isCompact ? 'Διαδικασία' : 'Διαδικασία -> Φορέας')
        : truncateLabel(node.data.name, isCompact ? 16 : 28)
      const secondaryLabel = node.depth === 0 && isCompact ? 'και φορέας' : ''
      centerLabelPrimary
        .attr('y', secondaryLabel ? -20 : -10)
        .text(primaryLabel)
      centerLabelSecondary
        .attr('y', -1)
        .text(secondaryLabel)
        .attr('opacity', secondaryLabel ? 1 : 0)
      centerValue
        .attr('y', secondaryLabel ? 22 : 12)
        .text(node.depth === 0 ? `${total} εγγραφές` : `${total} συμβάσεις`)
      centerHint
        .attr('y', secondaryLabel ? 40 : 32)
        .text(node.depth === 0 ? 'Κλικ στον εσωτερικό δακτύλιο για zoom' : 'Κλικ στο κέντρο για επιστροφή')
        .attr('opacity', isCompact ? 0 : 1)
    }


    const path = g.append('g')
      .selectAll('path')
      .data(root.descendants().slice(1))
      .join('path')
      .attr('fill', (node) => {
        let current = node
        while (current.depth > 1 && current.parent) current = current.parent
        return color(current.data.name)
      })
      .attr('fill-opacity', (node) => {
        if (!arcVisible(node.current)) return 0
        return node.depth === 1 ? 0.85 : 0.58
      })
      .attr('pointer-events', (node) => arcVisible(node.current) ? 'auto' : 'none')
      .attr('d', (node) => arc(node.current) ?? '')

    path
      .filter((node) => Boolean(node.children))
      .style('cursor', 'pointer')

    const label = g.append('g')
      .attr('pointer-events', 'none')
      .attr('text-anchor', 'middle')
      .style('user-select', 'none')
      .selectAll('text')
      .data(root.descendants().slice(1))
      .join('text')
      .attr('dy', '0.35em')
      .attr('fill-opacity', (node) => (labelVisible(node.current) && labelCanFit(node as SunburstNode)) ? 1 : 0)
      .attr('transform', (node) => labelTransform(node.current))
      .attr('class', (node) => `ca-sunburst-label${node.depth === 1 ? ' ca-sunburst-label--inner' : ''}`)
      .style('font-size', (node) => `${labelFontSize(node as SunburstNode)}px`)
      .text((node) => truncateLabel(node.data.name, isCompact ? (node.depth === 1 ? 14 : 11) : (node.depth === 1 ? 18 : 16)))

    let focus = root

    const parent = g.append('circle')
      .datum(root)
      .attr('r', centerRadius)
      .attr('fill', 'none')
      .attr('pointer-events', 'all')
      .style('cursor', 'pointer')

    center.raise()

    updateCenter(root)

    const showTooltip = (event: MouseEvent, node: SunburstNode) => {
      const rect = containerRef.current!.getBoundingClientRect()
      const pathText = node.ancestors()
        .reverse()
        .slice(1)
        .map((item) => item.data.name)
        .join(' -> ')
      const nodeType = node.depth === 1 ? 'Διαδικασία ανάθεσης' : 'Τύπος φορέα'

      tooltip
        .style('display', 'block')
        .style('left', `${event.clientX - rect.left + 12}px`)
        .style('top', `${event.clientY - rect.top - 8}px`)
        .html(`<strong>${pathText}</strong><span>${nodeType}</span><em>${Number(node.value ?? 0).toLocaleString('el-GR')} συμβάσεις</em>`)
    }

    const moveTooltip = (event: MouseEvent) => {
      const rect = containerRef.current!.getBoundingClientRect()
      tooltip
        .style('left', `${event.clientX - rect.left + 12}px`)
        .style('top', `${event.clientY - rect.top - 8}px`)
    }

    const hideTooltip = () => {
      tooltip.style('display', 'none')
    }

    const clicked = (event: MouseEvent, clickedNode: SunburstNode) => {
      focus = clickedNode
      parent.datum(focus.parent ?? root)
      updateCenter(clickedNode)

      root.each((node) => {
        node.target = {
          x0: Math.max(0, Math.min(1, (node.x0 - clickedNode.x0) / (clickedNode.x1 - clickedNode.x0))) * 2 * Math.PI,
          x1: Math.max(0, Math.min(1, (node.x1 - clickedNode.x0) / (clickedNode.x1 - clickedNode.x0))) * 2 * Math.PI,
          y0: Math.max(0, node.y0 - clickedNode.depth),
          y1: Math.max(0, node.y1 - clickedNode.depth),
        }
      })

      const duration = event.altKey ? 1500 : 750

      path.transition()
        .duration(duration)
        .tween('data', (node) => {
          const interpolate = d3.interpolate(node.current, node.target)
          return (t) => {
            node.current = interpolate(t)
          }
        })
        .attr('fill-opacity', (node) => {
          if (!arcVisible(node.target)) return 0
          return node.depth === 1 ? 0.85 : 0.58
        })
        .attr('pointer-events', (node) => arcVisible(node.target) ? 'auto' : 'none')
        .attrTween('d', (node) => () => arc(node.current) ?? '')

      label.transition()
        .duration(duration)
        .attr('fill-opacity', (node) => (labelVisible(node.target) && labelCanFit(node as SunburstNode)) ? 1 : 0)
        .style('font-size', (node) => `${labelFontSize(node as SunburstNode)}px`)
        .attrTween('transform', (node) => () => labelTransform(node.current))

      hideTooltip()
    }

    parent.on('click', (event, node) => {
      if (focus === root) return
      clicked(event as MouseEvent, node as SunburstNode)
    })

    path
      .on('click', (event, node) => {
        if (!node.children) return
        clicked(event as MouseEvent, node as SunburstNode)
      })
      .on('mouseover', (event, node) => {
        showTooltip(event as MouseEvent, node as SunburstNode)
        d3.select(event.currentTarget as SVGPathElement).attr('stroke', 'rgba(255,255,255,0.95)').attr('stroke-width', 1.5)
      })
      .on('mousemove', (event) => {
        moveTooltip(event as MouseEvent)
      })
      .on('mouseout', (event) => {
        hideTooltip()
        d3.select(event.currentTarget as SVGPathElement).attr('stroke', null).attr('stroke-width', null)
      })
  }, [data])

  useEffect(() => {
    if (!data) return
    draw()
    const observer = new ResizeObserver(draw)
    if (containerRef.current) observer.observe(containerRef.current)
    return () => observer.disconnect()
  }, [data, draw])

  if (!data) {
    return <p className="ca-empty-note">Δεν υπάρχουν διαθέσιμα δεδομένα διαδικασίας και τύπου φορέα για το επιλεγμένο έτος.</p>
  }

  return (
    <div
      ref={containerRef}
      className="ca-sunburst-wrap"
      onClick={() => d3.select(containerRef.current).select('.ca-sunburst-tooltip').style('display', 'none')}
    >
      <svg ref={svgRef} className="ca-sunburst-svg" aria-label="Zoomable sunburst διαδικασίας ανάθεσης και τύπου φορέα" />
      <div className="ca-sunburst-tooltip ca-tooltip app-tooltip" />
    </div>
  )
}

// ── Κύριο component ───────────────────────────────────────────────────
export default function ContractAnalysis() {
  const [barMetric,    setBarMetric]    = useState<BarMetric>('total')
  const [analysisPeriod, setAnalysisPeriod] = useState<'all' | string>(String(CURRENT_YEAR))
  const [analysis, setAnalysis] = useState<AnalysisData | null>(null)
  const [analysisError, setAnalysisError] = useState<string | null>(null)
  const availableAnalysisYears = useMemo(
    () => Array.from({ length: CURRENT_YEAR - 2024 + 1 }, (_, i) => String(2024 + i)).reverse(),
    [],
  )

  useEffect(() => {
    let cancelled = false
    const controller = new AbortController()
    ;(async () => {
      try {
        const { data, error } = await supabase
          .rpc('get_contract_analysis', { p_year_start: 2024 })
          .abortSignal(controller.signal)
        if (error) throw error

        const payload = (data ?? {}) as ContractAnalysisRpcPayload
        const sectionRows = Array.isArray(payload.sectionRows)
          ? payload.sectionRows.map(toSectionRow).filter((row): row is SectionRow => row !== null)
          : []

        if (cancelled) return
        setAnalysis(buildAnalysisDataFromRows(sectionRows))
        setAnalysisError(null)
      } catch (e) {
        if (e instanceof DOMException && e.name === 'AbortError') return
        if (cancelled) return
        setAnalysisError(e instanceof Error ? e.message : 'Αποτυχία φόρτωσης ανάλυσης')
      }
    })()

    return () => {
      cancelled = true
      controller.abort()
    }

  }, [])

  const monthly = useMemo(() => analysis?.monthly ?? [], [analysis])

  const sectionFiltered = useMemo(() => {
    if (!analysis) {
      return {
        contractTypeData: [] as BarItem[],
        procedureData: [] as BarItem[],
        topOrgs: [] as TopOrgItem[],
        totalSpendM: 0,
        topCpv: [] as TopCpvItem[],
        sunburstData: null as SunburstDatum | null,
      }
    }

    const periodStart = analysisPeriod === 'all' ? ANALYSIS_START : `${analysisPeriod}-01-01`
    const periodEnd = analysisPeriod === 'all' ? ANALYSIS_END : `${analysisPeriod}-12-31`

    const rows = analysis.sectionRows.filter((row) => {
      if (!row.signedDate) return false
      return row.signedDate >= periodStart && row.signedDate <= periodEnd
    })

    const totalAmount = rows.reduce((sum, row) => sum + row.amount, 0)
    const contractTypeMap = new Map<string, { count: number; total: number }>()
    const procedureMap = new Map<string, { count: number; total: number }>()
    const cpvAggMap = new Map<string, { count: number; procedures: Map<string, number> }>()
    const topOrgMap = new Map<string, { contracts: number; total: number }>()

    for (const row of rows) {
      const ct = contractTypeMap.get(row.contractType) ?? { count: 0, total: 0 }
      ct.count += 1
      ct.total += row.amount
      contractTypeMap.set(row.contractType, ct)

      const pr = procedureMap.get(row.procedure) ?? { count: 0, total: 0 }
      pr.count += 1
      pr.total += row.amount
      procedureMap.set(row.procedure, pr)

      const org = topOrgMap.get(row.authorityLabel) ?? { contracts: 0, total: 0 }
      org.contracts += 1
      org.total += row.amount
      topOrgMap.set(row.authorityLabel, org)

      for (const cpv of row.cpvs) {
        const ca = cpvAggMap.get(cpv) ?? { count: 0, procedures: new Map<string, number>() }
        ca.count += 1
        ca.procedures.set(row.procedure, (ca.procedures.get(row.procedure) ?? 0) + 1)
        cpvAggMap.set(cpv, ca)
      }
    }

    const toBarData = (
      map: Map<string, { count: number; total: number }>,
      labels: string[],
    ): BarItem[] =>
      labels.map((label) => {
        const v = map.get(label) ?? { count: 0, total: 0 }
        const pct = totalAmount > 0 ? (v.total / totalAmount) * 100 : 0
        return {
          label,
          value: v.count,
          total_m: v.total / 1_000_000,
          pct: Number(pct.toFixed(1)),
          tone: toneForPct(pct),
        }
      }).sort((a, b) => b.pct - a.pct || b.total_m - a.total_m || a.label.localeCompare(b.label, 'el'))

    const contractTypeData = toBarData(contractTypeMap, ['Υπηρεσίες', 'Προμήθειες', 'Έργα', 'Λοιπές'])
    const procedureData = toBarData(procedureMap, ['Απευθείας Ανάθεση', 'Ανοιχτή Διαδικασία', 'Διαπραγμάτευση', 'Άλλη'])

    const topCpv = buildTopCpvList(cpvAggMap)
    const topOrgs = [...topOrgMap.entries()]
      .map(([name, item]) => ({
        name,
        contracts: item.contracts,
        total_m: item.total / 1_000_000,
      }))
      .sort((a, b) => b.total_m - a.total_m || b.contracts - a.contracts || a.name.localeCompare(b.name, 'el'))
      .slice(0, 8)

    return {
      contractTypeData,
      procedureData,
      topOrgs,
      totalSpendM: totalAmount / 1_000_000,
      topCpv,
      sunburstData: buildProcedureAuthoritySunburstData(rows),
    }
  }, [analysis, analysisPeriod])

  const chartNote = useMemo(() => {
    if (monthly.length === 0) return 'Δεν υπάρχουν διαθέσιμα μηνιαία δεδομένα.'

    if (barMetric === 'total') {
      const byTotalDesc = [...monthly].sort((a, b) => b.total_k - a.total_k)
      const top1 = byTotalDesc[0]
      const top2 = byTotalDesc[1] ?? byTotalDesc[0]
      const low = [...monthly]
        .filter((m) => m.total_k > 0)
        .sort((a, b) => a.total_k - b.total_k)[0] ?? [...monthly].sort((a, b) => a.total_k - b.total_k)[0]

      const top1M = (top1.total_k / 1000).toLocaleString('el-GR', { maximumFractionDigits: 1 })
      const top2M = (top2.total_k / 1000).toLocaleString('el-GR', { maximumFractionDigits: 1 })
      const lowM = (low.total_k / 1000).toLocaleString('el-GR', { maximumFractionDigits: 1 })

      return `Τα μεγαλύτερα ποσά καταγράφονται στους μήνες ${top1.label} (€ ${top1M}M) και ${top2.label} (€ ${top2M}M), ενώ η χαμηλότερη δαπάνη εμφανίζεται τον ${low.label} (€ ${lowM}M).`
    }

    const byCountDesc = [...monthly].sort((a, b) => b.count - a.count)
    const top1 = byCountDesc[0]
    const top2 = byCountDesc[1] ?? byCountDesc[0]
    const low = [...monthly]
      .filter((m) => m.count > 0)
      .sort((a, b) => a.count - b.count)[0] ?? [...monthly].sort((a, b) => a.count - b.count)[0]

    return `Οι περισσότερες συμβάσεις υπογράφονται τους μήνες ${top1.label} (${top1.count.toLocaleString('el-GR')}) και ${top2.label} (${top2.count.toLocaleString('el-GR')}), ενώ ο μήνας με τη χαμηλότερη δραστηριότητα είναι ο ${low.label} (${low.count.toLocaleString('el-GR')}).`
  }, [barMetric, monthly])

  const analysisLoading = !analysis && !analysisError

  if (analysisLoading) {
    return (
      <section id="analysis" className="ca-section section-rule" aria-label="Ανάλυση Συμβάσεων">
        <ComponentTag name="ContractAnalysis" />
        <div className="ca-header section-head">
          <div className="eyebrow">Ανάλυση Δεδομένων</div>
          <h2>Δημόσιες Συμβάσεις Πυροπροστασίας</h2>
          <p className="ca-header-note">
            Ανακτώνται οι δημοσιευμένες συμβάσεις και οι συγκεντρωτικές μετρήσεις για το διάστημα 2024 έως σήμερα.
          </p>
        </div>
        <DataLoadingCard message="Υπολογίζονται οι δείκτες, τα γραφήματα και οι κατανομές της ανάλυσης." />
      </section>
    )
  }

  return (
    <section id="analysis" className="ca-section section-rule" aria-label="Ανάλυση Συμβάσεων">
      <ComponentTag name="ContractAnalysis" />

      {/* ── Header ── */}
      <div className="ca-header section-head">
        <div className="eyebrow">Ανάλυση Δεδομένων</div>
        <h2>Δημόσιες Συμβάσεις Πυροπροστασίας</h2>
        <p className="ca-header-note">
          Ανάλυση <strong>{analysis ? analysis.totalContracts.toLocaleString('el-GR') : '…'} δημοσιευμένων συμβάσεων</strong> που καλύπτουν τα έτη 2024 έως και σήμερα.
          Η συνολική δαπάνη για υπηρεσίες καθαρισμού,
          προμήθειες εξοπλισμού και έργα υποδομής υπολογίζεται στα <strong>€ {analysis ? (analysis.totalAmount / 1_000_000).toFixed(1) : '…'}M </strong>(χωρίς ΦΠΑ).
        </p>
        {analysisError && <p className="ca-empty-note">Σφάλμα φόρτωσης: {analysisError}</p>}
      </div>

      {/* ── Bar Chart ── */}
      <div className="ca-chart-block">
        <div className="ca-chart-head">
          <div className="eyebrow">Εξέλιξη ανά μήνα</div>
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
        <BarChart metric={barMetric} monthly={monthly} />
        <p className="ca-chart-note">{chartNote}</p>
      </div>

      {/* ── Type & Procedure breakdowns ── */}
      <ComponentTag name="ContractTypeProcedureSection" />
      <div className="ca-chart-head" style={{ padding: '0.75rem 1rem 0.35rem 1rem' }}>
       
        <div className="ca-metric-toggle">
          {availableAnalysisYears.map((year) => (
            <button
              key={year}
              className={`ca-toggle-btn${analysisPeriod === year ? ' ca-toggle-btn--active' : ''}`}
              onClick={() => setAnalysisPeriod(year)}
            >{year}</button>
          ))}
          <button
            className={`ca-toggle-btn${analysisPeriod === 'all' ? ' ca-toggle-btn--active' : ''}`}
            onClick={() => setAnalysisPeriod('all')}
          >{`2024–${CURRENT_YEAR}`}</button>
        </div>
      </div>
      <div className="ca-double-grid">
        <div className="ca-breakdown-block">
          <div className="eyebrow">Τύποι συμβάσεων</div>
          <div className="ca-bars">
            {sectionFiltered.contractTypeData.map(item => <HBar key={item.label} item={item} />)}
          </div>
        </div>
        <div className="ca-breakdown-block">
          <div className="eyebrow">Διαδικασία ανάθεσης</div>
          <div className="ca-bars">
            {sectionFiltered.procedureData.map(item => <HBar key={item.label} item={item} />)}
          </div>
        </div>
      </div>

      <div className="ca-findings" style={{ borderTop: '1px solid var(--line)' }}>
        <div className="eyebrow">GOOD TO KNOW</div>
        <div className="ca-findings__grid">
          {[
            {
              num: '01',
              title: 'Υπηρεσίες',
              text: 'Συμβάσεις παροχής υπηρεσιών από τρίτους προς την αναθέτουσα αρχή. Δεν παράγουν υλικό αποτέλεσμα (έργο) ούτε μεταβιβάζουν κυριότητα αγαθού.',
            },
            {
              num: '02',
              title: 'Προμήθειες',
              text: 'Συμβάσεις αγοράς, χρηματοδοτικής μίσθωσης ή μίσθωσης αγαθών/εξοπλισμού που μεταβιβάζονται ή παραχωρούνται στην αναθέτουσα αρχή.',
            },
            {
              num: '03',
              title: 'Έργα',
              text: 'Συμβάσεις εκτέλεσης οικοδομικών ή τεχνικών εργασιών (κατασκευή, ανακαίνιση, αποκατάσταση), με αποτέλεσμα ακίνητο τεχνικό έργο.',
            },
            {
              num: '04',
              title: 'Απευθείας Ανάθεση',
              text: 'Η πιο απλοποιημένη διαδικασία, χωρίς πλήρη διαγωνιστική προκήρυξη, όπου η αναθέτουσα αρχή επιλέγει απευθείας ανάδοχο.',
            },
            {
              num: '05',
              title: 'Ανοιχτή Διαδικασία',
              text: 'Κάθε ενδιαφερόμενος οικονομικός φορέας μπορεί να υποβάλει προσφορά μετά από δημοσίευση στο ΚΗΜΔΗΣ και όπου απαιτείται στην ΕΕ.',
            },
            {
              num: '06',
              title: 'Διαπραγμάτευση',
              text: 'Η αναθέτουσα αρχή διαπραγματεύεται με έναν ή περισσότερους φορείς, μόνο σε ειδικές περιπτώσεις που προβλέπει ο νόμος.',
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

      <TopAuthoritiesSection rows={sectionFiltered.topOrgs} totalSpendM={sectionFiltered.totalSpendM} loading={false} />

      {/* ── Top CPV ── */}
      <div className="ca-table-block">
        <ComponentTag name="TopCpvSection" />
        <div className="ca-sunburst-block">
          <div className="ca-sunburst-copy">
            <div className="eyebrow">ΑΝΑΘΕΣΕΙΣ</div>
            <strong>Διαδραστικό sunburst που απεικονίζει τη σχέση διαδικασίας ανάθεσης και φορέα</strong>
            <p>
              Κέντρο: διαδικασία ανάθεσης — Εξωτερικός δακτύλιος: συγκεκριμένοι φορείς, όπως δήμοι, περιφέρειες και οργανισμοί. Το μέγεθος των πεδίων αποτυπώνει το πλήθος και όχι το συνολικό ποσό των συμβάσεων. Κλικ σε τμήμα για εστίαση, κλικ στο κέντρο για επαναφορά.
            </p>
          </div>
          <ZoomableSunburst data={sectionFiltered.sunburstData} />
        </div>
        <div className="ca-cpv-grid">
          {sectionFiltered.topCpv.map((c, i) => (
            <article className="ca-cpv-card" key={c.cpv}>
              <div className="ca-cpv-rank">#{i + 1}</div>
              <div className="ca-cpv-desc">{c.desc}</div>
              <div className="ca-cpv-stats">
                <div>
                  <span className="label">Συμβάσεις</span>
                  <strong>{c.count}</strong>
                </div>
                <div>
                  <span className="label">Κύρια διαδικασία</span>
                  <strong><span className="ca-accent">{c.mainProcedurePct.toFixed(1).replace('.', ',')}%</span> {c.mainProcedure}</strong>
                </div>
              </div>
            </article>
          ))}
        </div>
        <p className="ca-sub-note">
          Για κάθε CPV εμφανίζεται το πλήθος συμβάσεων και η επικρατέστερη διαδικασία ανάθεσης.
        </p>
      </div>

      

      <div className="ca-footer-note">
        <span className="eyebrow">Πηγή δεδομένων</span>
        <span>
          Kεντρικό Ηλεκτρονικό Μητρώο ΔΗμοσίων Συμβάσεων · {analysis ? analysis.totalContracts.toLocaleString('el-GR') : '…'} εγγραφές ·
          Δυναμική ημερήσια ανανέωση δεδομένων · Τιμές χωρίς ΦΠΑ
        </span>
      </div>
    </section>
  )
}
