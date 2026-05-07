import { useEffect, useRef, useState, useCallback } from 'react'
import * as d3 from 'd3'
import { isAbortError } from '../lib/isAbortError'
import { supabase } from '../lib/supabase'
import ComponentTag from './ComponentTag'
import DataLoadingCard from './DataLoadingCard'

type HistoBin = { bin_lo: number; bin_hi: number; cnt: number | string; total_count?: number | string | null }

function HistogramChart({ bins }: { bins: HistoBin[] }) {
  const containerRef = useRef<HTMLDivElement>(null)
  const svgRef       = useRef<SVGSVGElement>(null)

  const draw = useCallback(() => {
    if (!svgRef.current || !containerRef.current || bins.length === 0) return
    const containerW = containerRef.current.clientWidth
    if (containerW === 0) return

    // Trim leading/trailing empty bins so the chart starts/ends where data is
    const activeBins = bins.filter(b => Number(b.cnt) > 0)
    if (activeBins.length === 0) return

    const isMobile = containerW <= 440
    const margin = { top: 10, right: isMobile ? 2 : 4, bottom: isMobile ? 44 : 36, left: isMobile ? 44 : 56 }
    const W = containerW
    const H = isMobile ? 210 : 280
    const innerW = W - margin.left - margin.right
    const innerH = H - margin.top - margin.bottom

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()
    svg.attr('width', W).attr('height', H)

    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`)

    const minVal = activeBins[0].bin_lo
    const maxVal = Math.min(activeBins[activeBins.length - 1].bin_hi, 5_000_000)

    const x = d3.scaleLog()
      .domain([minVal, maxVal])
      .range([0, innerW])

    const yMax = d3.max(bins, b => Number(b.cnt)) ?? 1
    const y = d3.scaleLinear()
      .domain([0, Math.max(1, yMax * 1.04)])
      .range([innerH, 0])

    // Grid lines
    g.append('g')
      .call(d3.axisLeft(y).tickSize(-innerW).tickFormat(() => '').ticks(4))
      .call(sel => sel.select('.domain').remove())
      .call(sel => sel.selectAll<SVGLineElement, unknown>('.tick line')
        .attr('stroke', 'rgba(17,17,17,0.06)')
        .attr('stroke-dasharray', '3,4'))

    // Y axis
    g.append('g')
      .call(d3.axisLeft(y).ticks(4))
      .call(sel => sel.select('.domain').attr('stroke', 'rgba(17,17,17,0.18)'))
      .call(sel => sel.selectAll('.tick line').attr('stroke', 'rgba(17,17,17,0.12)'))
      .call(sel => sel.selectAll<SVGTextElement, unknown>('.tick text')
        .attr('font-family', 'IBM Plex Mono, monospace')
        .attr('font-size', isMobile ? '9' : '10')
        .attr('fill', 'rgba(17,17,17,0.55)')
        .attr('dx', '-4'))

    const fmtEur = (v: number) => {
      if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(0)}M€`
      if (v >= 1_000)     return `${(v / 1_000).toFixed(0)}K€`
      return `${Math.round(v)}€`
    }

    const formatAmount = (v: number) => {
      if (v >= 1_000_000) return `${Number((v / 1_000_000).toFixed(v >= 10_000_000 ? 0 : 1)).toLocaleString('el-GR')}M€`
      if (v >= 1_000) return `${Number((v / 1_000).toFixed(v >= 10_000 ? 0 : 1)).toLocaleString('el-GR')}K€`
      return `${Math.round(v).toLocaleString('el-GR')}€`
    }

    const candidateTicks = [
      100, 300, 1_000, 3_000, 10_000, 30_000,
      100_000, 300_000, 1_000_000, 3_000_000, 5_000_000,
    ].filter(v => v >= minVal && v <= maxVal)
    const maxTickCount = isMobile ? 5 : containerW < 760 ? 7 : 9
    const tickStep = Math.max(1, Math.ceil(candidateTicks.length / maxTickCount))
    const xTickValues = candidateTicks.filter((_, index) => index % tickStep === 0)
    const lastCandidateTick = candidateTicks[candidateTicks.length - 1]
    if (lastCandidateTick && xTickValues[xTickValues.length - 1] !== lastCandidateTick) {
      xTickValues.push(lastCandidateTick)
    }

    g.append('g')
      .attr('transform', `translate(0,${innerH})`)
      .call(d3.axisBottom(x).tickValues(xTickValues).tickFormat(v => formatAmount(v as number)).tickSizeOuter(0).tickPadding(isMobile ? 7 : 8))
      .call(sel => sel.select('.domain').attr('stroke', 'rgba(17,17,17,0.18)'))
      .call(sel => sel.selectAll('.tick line').attr('stroke', 'rgba(17,17,17,0.12)'))
      .call(sel => sel.selectAll<SVGTextElement, unknown>('.tick text')
        .attr('font-family', 'IBM Plex Mono, monospace')
        .attr('font-size', isMobile ? '8' : '10')
        .attr('fill', 'rgba(17,17,17,0.55)')
        .attr('transform', isMobile ? 'rotate(-32)' : null)
        .attr('text-anchor', isMobile ? 'end' : 'middle')
        .attr('dy', isMobile ? '0.35em' : '0.75em')
        .attr('dx', isMobile ? '-0.35em' : '0'))

    // Bars — drawn from all bins in range (empty bins between active ones show as gaps)
    const visibleBins = bins.filter(b => b.bin_lo >= minVal && b.bin_lo < maxVal)
    const tooltip = d3.select(containerRef.current).select<HTMLDivElement>('.da-hist-tooltip')

    const showTooltip = (event: MouseEvent, b: HistoBin) => {
      const count = Number(b.cnt)
      if (count === 0) return
      const rect = containerRef.current!.getBoundingClientRect()
      tooltip
        .style('display', 'block')
        .style('transform', 'none')
        .style('left', `${event.clientX - rect.left + 12}px`)
        .style('top',  `${event.clientY - rect.top  - 42}px`)
        .html(`<strong>${fmtEur(b.bin_lo)} – ${fmtEur(b.bin_hi)}</strong>${count.toLocaleString('el-GR')} συμβάσεις`)
    }

    g.selectAll<SVGRectElement, HistoBin>('.da-bar')
      .data(visibleBins)
      .join('rect')
      .attr('class', 'da-bar')
      .attr('x',      b => x(b.bin_lo))
      .attr('width',  b => Math.max(0, x(Math.min(b.bin_hi, maxVal)) - x(b.bin_lo) - 0.8))
      .attr('y',      b => y(Number(b.cnt)))
      .attr('height', b => Math.max(0, innerH - y(Number(b.cnt))))
      .attr('fill', 'rgba(17,17,17,0.28)')
      .attr('rx', 1)
      .on('mouseover', (event: MouseEvent, b) => {
        showTooltip(event, b)
        d3.select(event.currentTarget as Element).attr('fill', 'rgba(211,72,45,0.82)')
      })
      .on('mousemove', (event: MouseEvent) => {
        const rect = containerRef.current!.getBoundingClientRect()
        tooltip
          .style('transform', 'none')
          .style('left', `${event.clientX - rect.left + 12}px`)
          .style('top',  `${event.clientY - rect.top  - 42}px`)
      })
      .on('mouseout', (_event: MouseEvent) => {
        tooltip.style('display', 'none')
        g.selectAll<SVGRectElement, HistoBin>('.da-bar').attr('fill', 'rgba(17,17,17,0.28)')
      })
      .on('click', (event: MouseEvent, b) => {
        event.stopPropagation()
        const count = Number(b.cnt)
        if (count === 0) return
        const rect = containerRef.current!.getBoundingClientRect()
        const barEl = event.currentTarget as SVGRectElement
        const barRect = barEl.getBoundingClientRect()
        const cx = (barRect.left + barRect.right) / 2 - rect.left
        tooltip
          .style('display', 'block')
          .style('left',  `${cx}px`)
          .style('top',   `${barRect.top - rect.top - 8}px`)
          .style('transform', 'translate(-50%, -100%)')
          .html(`<strong>${fmtEur(b.bin_lo)} – ${fmtEur(b.bin_hi)}</strong>${count.toLocaleString('el-GR')} συμβάσεις`)
        g.selectAll<SVGRectElement, HistoBin>('.da-bar')
          .attr('fill', eb => eb === b ? 'rgba(211,72,45,0.82)' : 'rgba(17,17,17,0.28)')
      })

  }, [bins])

  useEffect(() => {
    draw()
    const ro = new ResizeObserver(draw)
    if (containerRef.current) ro.observe(containerRef.current)
    return () => ro.disconnect()
  }, [draw])

  return (
    <div
      ref={containerRef}
      className="ca-bar-container da-hist-container"
      onClick={() => d3.select(containerRef.current).select('.da-hist-tooltip').style('display', 'none')}
    >
      <svg ref={svgRef} className="ca-d3-bar-svg" />
      <div className="da-hist-tooltip ca-tooltip app-tooltip" />
    </div>
  )
}

export default function DirectAwardHistogram() {
  const [bins,    setBins]    = useState<HistoBin[]>([])
  const [total,   setTotal]   = useState(0)
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState<string | null>(null)

  useEffect(() => {
    const controller = new AbortController()
    let cancelled = false

    ;(async () => {
      try {
        const { data, error: err } = await supabase
          .rpc('get_direct_award_distribution')
          .abortSignal(controller.signal)

        if (err) throw err
        if (cancelled) return

        const rows = data as HistoBin[]
        setBins(rows)
        setTotal(Number(rows[0]?.total_count ?? rows.reduce((s, b) => s + Number(b.cnt), 0)))
      } catch (e) {
        if (isAbortError(e)) return
        if (cancelled) return
        setError(e instanceof Error ? e.message : 'Αποτυχία φόρτωσης δεδομένων')
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()

    return () => { cancelled = true; controller.abort() }
  }, [])

  if (loading) {
    return (
      <section className="ca-section section-rule" aria-label="Κατανομή Απευθείας Αναθέσεων">
        <DataLoadingCard message="Φόρτωση κατανομής αξιών απευθείας αναθέσεων…" />
      </section>
    )
  }

  return (
    <section className="ca-section section-rule" aria-label="Κατανομή Απευθείας Αναθέσεων">
      <ComponentTag name="DirectAwardHistogram" />

      <div className="ca-header section-head">
        <div className="eyebrow">Κατανομή</div>
        <h2>Απευθείας Αναθέσεις</h2>
        <p className="ca-header-note">
          Ανάλυση <strong>{total.toLocaleString('el-GR')} απευθείας αναθέσεων</strong> (2024–σήμερα). Το ιστόγραμμα απεικονίζει όσες έχουν γνωστή αξία άνω των €100, ανά αξία σύμβασης (χωρίς ΦΠΑ).
          Ο οριζόντιος άξονας είναι λογαριθμικός.
        </p>
        {error && <p className="ca-empty-note">Σφάλμα: {error}</p>}
      </div>

      <div className="ca-chart-block da-hist-chart-block">
        <div className="ca-chart-head">
          <div className="eyebrow">Αξία σύμβασης χ.ΦΠΑ · λογαριθμική κλίμακα</div>
        </div>
        {bins.length > 0
          ? <HistogramChart bins={bins} />
          : <p className="ca-empty-note">Δεν βρέθηκαν απευθείας αναθέσεις.</p>
        }
      </div>
    </section>
  )
}
