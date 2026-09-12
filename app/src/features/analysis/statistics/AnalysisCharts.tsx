import { useCallback, useEffect, useRef } from 'react'
import * as d3 from 'd3'
import {
  BREAKPOINT_MD,
  BREAKPOINT_SM,
  BREAKPOINT_XS,
  MONTH_NAMES_SHORT,
  type BarItem,
  type MonthlyPoint,
  type SunburstDatum,
} from './analysisData'

// ── Βοηθητικό HBar ────────────────────────────────────────────────────
export function HBar({ item }: { item: BarItem }) {
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
export type BarMetric = 'count' | 'total'

export function BarChart({ metric, monthly }: { metric: BarMetric; monthly: MonthlyPoint[] }) {
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

function truncateLabel(label: string, maxLength: number): string {
  if (label.length <= maxLength) return label
  return `${label.slice(0, Math.max(0, maxLength - 1)).trimEnd()}…`
}

function sunburstDisplayLabelLines(label: string, depth: number, isCompact: boolean): string[] {
  if (!isCompact) return [truncateLabel(label, depth === 1 ? 18 : 16)]

  const compactProcedureLabels: Record<string, string[]> = {
    'Απευθείας Ανάθεση': ['Απευθείας', 'Ανάθεση'],
    'Ανοιχτή Διαδικασία': ['Ανοιχτή', 'Διαδικασία'],
    'Διαπραγμάτευση': ['Διαπραγμ.'],
  }

  const mappedLines = depth === 1 ? compactProcedureLabels[label] : undefined
  if (mappedLines) return mappedLines

  const words = label.split(/\s+/).filter(Boolean)
  if (words.length <= 1) return [truncateLabel(label, depth === 1 ? 11 : 8)]

  const lineLimit = depth === 1 ? 2 : 1
  const maxLineLength = depth === 1 ? 11 : 8
  const lines = words.slice(0, lineLimit).map((word) => truncateLabel(word, maxLineLength))
  if (words.length > lineLimit && lines.length > 0) {
    lines[lines.length - 1] = truncateLabel(`${lines[lines.length - 1]}…`, maxLineLength)
  }
  return lines
}

export function ZoomableSunburst({ data }: { data: SunburstDatum | null }) {
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
    const size = isCompact
      ? Math.max(260, Math.min(containerW, 360))
      : Math.max(320, Math.min(containerW, 720))

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
      const lines = sunburstDisplayLabelLines(node.data.name, node.depth, isCompact)
      const charCount = Math.max(1, ...lines.map((line) => line.length))
      const maxByArc = arcLength / Math.max(charCount * 0.62, 1)
      const maxByRing = ringThickness / (lines.length * 1.15)
      const maxSize = Math.min(maxByArc, maxByRing, isCompact ? (node.depth === 1 ? 6.4 : 5.4) : (node.depth === 1 ? 10 : 9))
      const minSize = isCompact ? (node.depth === 1 ? 5.2 : 4.7) : (node.depth === 1 ? 7.2 : 6.2)
      return Number(Math.max(minSize, maxSize).toFixed(1))
    }
    const labelCanFit = (node: SunburstNode) => {
      const d = node.current
      const ringThickness = Math.max(1, radialAt(d.y1) - radialAt(d.y0))
      const midRadius = (radialAt(d.y0) + radialAt(d.y1)) / 2
      const arcLength = Math.max(1, midRadius * (d.x1 - d.x0))
      const fontSize = labelFontSize(node)
      const lines = sunburstDisplayLabelLines(node.data.name, node.depth, isCompact)
      const estimatedTextWidth = Math.max(...lines.map((line) => line.length)) * fontSize * 0.6
      const estimatedTextHeight = lines.length * fontSize * 1.15
      return estimatedTextWidth <= arcLength * (isCompact ? 0.78 : 0.92) && estimatedTextHeight <= ringThickness * 0.78
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

    label.each(function (node) {
      const text = d3.select(this)
      const lines = sunburstDisplayLabelLines(node.data.name, node.depth, isCompact)
      const firstDy = lines.length > 1 ? `${-0.3 * (lines.length - 1)}em` : '0.35em'
      lines.forEach((line, index) => {
        text.append('tspan')
          .attr('x', 0)
          .attr('dy', index === 0 ? firstDy : '1.05em')
          .text(line)
      })
    })

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
