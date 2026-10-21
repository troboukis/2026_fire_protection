import { useCallback, useEffect, useRef, useState, type ReactNode, type RefObject } from 'react'
import { scaleBand, scaleLinear, scaleLog } from 'd3'

// ── Shared pieces ──────────────────────────────────────────────────────

export type Series = { key: string; label: string; color: string }

type TooltipState = { x: number; y: number; content: ReactNode } | null

function useWidth<T extends HTMLElement>() {
  const ref = useRef<T>(null)
  const [width, setWidth] = useState(0)
  useEffect(() => {
    const node = ref.current
    if (!node) return
    const update = () => setWidth(node.clientWidth)
    update()
    const observer = new ResizeObserver(update)
    observer.observe(node)
    return () => observer.disconnect()
  }, [])
  return [ref, width] as const
}

function useTooltip(containerRef: RefObject<HTMLDivElement>) {
  const [tooltip, setTooltip] = useState<TooltipState>(null)
  const show = useCallback((target: Element, content: ReactNode) => {
    const container = containerRef.current
    if (!container) return
    const box = target.getBoundingClientRect()
    const frame = container.getBoundingClientRect()
    setTooltip({ x: box.left - frame.left + box.width / 2, y: box.top - frame.top, content })
  }, [containerRef])
  const hide = useCallback(() => setTooltip(null), [])
  return { tooltip, show, hide }
}

function Tooltip({ state, width }: { state: TooltipState; width: number }) {
  if (!state) return null
  const left = Math.min(Math.max(state.x, 90), Math.max(width - 90, 90))
  return <div className="yr-tooltip" role="status" style={{ left, top: state.y }}>{state.content}</div>
}

/** Rounded data-end (top), square at the baseline. */
function columnPath(x: number, y: number, w: number, h: number) {
  if (h <= 0 || w <= 0) return ''
  const r = Math.min(4, w / 2, h)
  return `M${x},${y + h}V${y + r}Q${x},${y} ${x + r},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + h}Z`
}

/** Rounded data-end (right), square at the baseline. */
function barPath(x: number, y: number, w: number, h: number) {
  if (h <= 0 || w <= 0) return ''
  const r = Math.min(4, h / 2, w)
  return `M${x},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + h - r}Q${x + w},${y + h} ${x + w - r},${y + h}H${x}Z`
}

/** Ink or white text, whichever reads better on a filled mark. */
function textOn(hex: string) {
  const value = hex.replace('#', '')
  const [r, g, b] = [0, 2, 4].map(i => parseInt(value.slice(i, i + 2), 16) / 255)
    .map(channel => (channel <= 0.03928 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4))
  const luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b
  return luminance > 0.3 ? '#111111' : '#ffffff'
}

function niceTicks(max: number, count = 4) {
  return scaleLinear().domain([0, max]).nice(count).ticks(count)
}

export function Legend({ series }: { series: Series[] }) {
  return (
    <ul className="yr-legend" aria-label="Υπόμνημα">
      {series.map(item => (
        <li key={item.key}><span className="yr-legend__swatch" style={{ background: item.color }} aria-hidden="true" />{item.label}</li>
      ))}
    </ul>
  )
}

export type TableColumn = { key: string; label: string; numeric?: boolean }

export function DataTable({ columns, rows, caption }: { columns: TableColumn[]; rows: Record<string, ReactNode>[]; caption: string }) {
  return (
    <details className="yr-table">
      <summary>Δείτε τα δεδομένα του γραφήματος</summary>
      <div className="yr-table__scroll">
        <table>
          <caption>{caption}</caption>
          <thead><tr>{columns.map(column => <th key={column.key} scope="col" className={column.numeric ? 'is-numeric' : undefined}>{column.label}</th>)}</tr></thead>
          <tbody>
            {rows.map((row, index) => (
              <tr key={index}>{columns.map(column => <td key={column.key} className={column.numeric ? 'is-numeric' : undefined}>{row[column.key]}</td>)}</tr>
            ))}
          </tbody>
        </table>
      </div>
    </details>
  )
}

export function ChartFrame({ title, subtitle, legend, children, table, note }: {
  title: string
  subtitle?: string
  legend?: Series[]
  children: ReactNode
  table?: ReactNode
  note?: ReactNode
}) {
  return (
    <figure className="yr-chart">
      <figcaption>
        <strong className="yr-chart__title">{title}</strong>
        {subtitle && <span className="yr-chart__subtitle">{subtitle}</span>}
      </figcaption>
      {legend && legend.length > 1 && <Legend series={legend} />}
      {children}
      {note && <p className="yr-chart__note">{note}</p>}
      {table}
    </figure>
  )
}

// ── Column chart (one series, optional emphasis and shaded band) ────────

export type ColumnDatum = { label: string; value: number; emphasis?: boolean; tooltip: ReactNode; ariaLabel: string }

export function ColumnChart({ data, formatAxis, color, mutedColor, band, axisTitle, labelAnchor = 0, height = 240 }: {
  data: ColumnDatum[]
  formatAxis: (value: number) => string
  color: string
  mutedColor?: string
  band?: { from: number; to: number; label: string }
  /** Title under the category labels, e.g. the unit of the bins. */
  axisTitle?: string
  /** On narrow screens every other label is shown; this index is always one of them. */
  labelAnchor?: number
  height?: number
}) {
  const [ref, width] = useWidth<HTMLDivElement>()
  const { tooltip, show, hide } = useTooltip(ref)
  const margin = { top: 12, right: 8, bottom: axisTitle ? 44 : 26, left: 48 }
  const innerW = Math.max(width - margin.left - margin.right, 0)
  const innerH = height - margin.top - margin.bottom
  const max = Math.max(...data.map(item => item.value), 1)
  const ticks = niceTicks(max)
  const y = scaleLinear().domain([0, ticks[ticks.length - 1] || max]).range([innerH, 0])
  const x = scaleBand<string>().domain(data.map((_, i) => String(i))).range([0, innerW]).paddingInner(0.25).paddingOuter(0.1)
  const barW = Math.min(x.bandwidth(), 24)
  const compact = width < 520

  return (
    <div className="yr-plot" ref={ref}>
      {width > 0 && (
        <svg width={width} height={height} role="img" aria-label="Ραβδόγραμμα">
          <g transform={`translate(${margin.left},${margin.top})`}>
            {band && (() => {
              const x0 = (x(String(band.from)) ?? 0) - (x.step() * x.paddingInner()) / 2
              const x1 = (x(String(band.to)) ?? 0) + x.bandwidth() + (x.step() * x.paddingInner()) / 2
              return (
                <g className="yr-band">
                  <rect x={x0} y={0} width={Math.max(x1 - x0, 0)} height={innerH} />
                  <text x={x0 + 4} y={10}>{band.label}</text>
                </g>
              )
            })()}
            {ticks.map(tick => (
              <g key={tick} transform={`translate(0,${y(tick)})`}>
                <line className="yr-grid" x1={0} x2={innerW} />
                <text className="yr-axis-label" x={-8} dy="0.32em" textAnchor="end">{formatAxis(tick)}</text>
              </g>
            ))}
            {data.map((item, i) => {
              const bx = (x(String(i)) ?? 0) + (x.bandwidth() - barW) / 2
              const by = y(item.value)
              const fill = item.emphasis === false && mutedColor ? mutedColor : color
              return (
                <g key={item.label} className="yr-mark" tabIndex={0} role="graphics-symbol" aria-label={item.ariaLabel}
                  onPointerEnter={e => show(e.currentTarget, item.tooltip)} onPointerLeave={hide}
                  onFocus={e => show(e.currentTarget, item.tooltip)} onBlur={hide}>
                  <rect className="yr-hit" x={x(String(i))} y={0} width={x.bandwidth()} height={innerH} />
                  <path d={columnPath(bx, by, barW, innerH - by)} fill={fill} />
                  {(!compact || (i - labelAnchor) % 2 === 0) && (
                    <text className="yr-axis-label" x={(x(String(i)) ?? 0) + x.bandwidth() / 2} y={innerH + 16} textAnchor="middle">{item.label}</text>
                  )}
                </g>
              )
            })}
            <line className="yr-baseline" x1={0} x2={innerW} y1={innerH} y2={innerH} />
            {axisTitle && <text className="yr-axis-title" x={innerW / 2} y={innerH + 36} textAnchor="middle">{axisTitle}</text>}
          </g>
        </svg>
      )}
      <Tooltip state={tooltip} width={width} />
    </div>
  )
}

// ── Histogram on a log scale with a threshold marker ───────────────────

export type HistogramBin = { lo: number; hi: number; count: number }

export function LogHistogram({ bins, threshold, thresholdLabel, emphasize, color, mutedColor, formatAmount, height = 240 }: {
  bins: HistogramBin[]
  threshold: number
  thresholdLabel: string
  emphasize: (bin: HistogramBin) => boolean
  color: string
  mutedColor: string
  formatAmount: (value: number) => string
  height?: number
}) {
  const [ref, width] = useWidth<HTMLDivElement>()
  const { tooltip, show, hide } = useTooltip(ref)
  const margin = { top: 16, right: 12, bottom: 30, left: 40 }
  const innerW = Math.max(width - margin.left - margin.right, 0)
  const innerH = height - margin.top - margin.bottom
  const lo = bins[0]?.lo ?? 100
  const hi = bins[bins.length - 1]?.hi ?? 1_000_000
  const x = scaleLog().domain([lo, hi]).range([0, innerW])
  const max = Math.max(...bins.map(bin => bin.count), 1)
  const ticks = niceTicks(max)
  const y = scaleLinear().domain([0, ticks[ticks.length - 1] || max]).range([innerH, 0])
  const compact = width < 520
  const axisValues = (compact ? [1_000, 10_000, 100_000, 1_000_000] : [100, 1_000, 10_000, 30_000, 100_000, 1_000_000])
    .filter(value => value >= lo && value <= hi)
  const tx = x(threshold)

  return (
    <div className="yr-plot" ref={ref}>
      {width > 0 && (
        <svg width={width} height={height} role="img" aria-label="Ιστόγραμμα αξιών απευθείας αναθέσεων">
          <g transform={`translate(${margin.left},${margin.top})`}>
            {ticks.map(tick => (
              <g key={tick} transform={`translate(0,${y(tick)})`}>
                <line className="yr-grid" x1={0} x2={innerW} />
                <text className="yr-axis-label" x={-8} dy="0.32em" textAnchor="end">{tick.toLocaleString('el-GR')}</text>
              </g>
            ))}
            {bins.map(bin => {
              const x0 = x(bin.lo) + 1
              const w = Math.max(x(bin.hi) - x(bin.lo) - 2, 1)
              const by = y(bin.count)
              const content = <><strong>{formatAmount(bin.lo)} – {formatAmount(bin.hi)}</strong><span>{bin.count.toLocaleString('el-GR')} απευθείας αναθέσεις</span></>
              return (
                <g key={bin.lo} className="yr-mark" tabIndex={0} role="graphics-symbol"
                  aria-label={`${formatAmount(bin.lo)} έως ${formatAmount(bin.hi)}: ${bin.count} αναθέσεις`}
                  onPointerEnter={e => show(e.currentTarget, content)} onPointerLeave={hide}
                  onFocus={e => show(e.currentTarget, content)} onBlur={hide}>
                  <rect className="yr-hit" x={x(bin.lo)} y={0} width={Math.max(x(bin.hi) - x(bin.lo), 1)} height={innerH} />
                  <path d={columnPath(x0, by, w, innerH - by)} fill={emphasize(bin) ? color : mutedColor} />
                </g>
              )
            })}
            <line className="yr-threshold" x1={tx} x2={tx} y1={0} y2={innerH} />
            <text className="yr-threshold-label" x={tx - 6} y={-4} textAnchor="end">{thresholdLabel}</text>
            <line className="yr-baseline" x1={0} x2={innerW} y1={innerH} y2={innerH} />
            {axisValues.map(value => (
              <text key={value} className="yr-axis-label" x={x(value)} y={innerH + 18} textAnchor="middle">{formatAmount(value)}</text>
            ))}
          </g>
        </svg>
      )}
      <Tooltip state={tooltip} width={width} />
    </div>
  )
}

// ── Grouped columns (several series per category) ──────────────────────

export type GroupedDatum = { label: string; values: Record<string, number>; tooltip: ReactNode; ariaLabel: string }

export function GroupedColumns({ data, series, formatAxis, band, height = 250 }: {
  data: GroupedDatum[]
  series: Series[]
  formatAxis: (value: number) => string
  band?: { from: number; to: number; label: string }
  height?: number
}) {
  const [ref, width] = useWidth<HTMLDivElement>()
  const { tooltip, show, hide } = useTooltip(ref)
  const margin = { top: 14, right: 8, bottom: 26, left: 40 }
  const innerW = Math.max(width - margin.left - margin.right, 0)
  const innerH = height - margin.top - margin.bottom
  const max = Math.max(...data.flatMap(item => series.map(s => item.values[s.key] ?? 0)), 1)
  const ticks = niceTicks(max)
  const y = scaleLinear().domain([0, ticks[ticks.length - 1] || max]).range([innerH, 0])
  const x = scaleBand<string>().domain(data.map((_, i) => String(i))).range([0, innerW]).paddingInner(0.22).paddingOuter(0.08)
  const inner = scaleBand<string>().domain(series.map(s => s.key)).range([0, x.bandwidth()]).paddingInner(0.12)
  const barW = Math.min(inner.bandwidth(), 14)

  return (
    <div className="yr-plot" ref={ref}>
      {width > 0 && (
        <svg width={width} height={height} role="img" aria-label="Ομαδοποιημένο ραβδόγραμμα">
          <g transform={`translate(${margin.left},${margin.top})`}>
            {band && (() => {
              const x0 = (x(String(band.from)) ?? 0) - (x.step() * x.paddingInner()) / 2
              const x1 = (x(String(band.to)) ?? 0) + x.bandwidth() + (x.step() * x.paddingInner()) / 2
              return (
                <g className="yr-band">
                  <rect x={x0} y={0} width={Math.max(x1 - x0, 0)} height={innerH} />
                  <text x={x0 + 4} y={10}>{band.label}</text>
                </g>
              )
            })()}
            {ticks.map(tick => (
              <g key={tick} transform={`translate(0,${y(tick)})`}>
                <line className="yr-grid" x1={0} x2={innerW} />
                <text className="yr-axis-label" x={-8} dy="0.32em" textAnchor="end">{formatAxis(tick)}</text>
              </g>
            ))}
            {data.map((item, i) => (
              <g key={item.label} className="yr-mark" tabIndex={0} role="graphics-symbol" aria-label={item.ariaLabel}
                onPointerEnter={e => show(e.currentTarget, item.tooltip)} onPointerLeave={hide}
                onFocus={e => show(e.currentTarget, item.tooltip)} onBlur={hide}>
                <rect className="yr-hit" x={x(String(i))} y={0} width={x.bandwidth()} height={innerH} />
                {series.map(s => {
                  const value = item.values[s.key] ?? 0
                  const bx = (x(String(i)) ?? 0) + (inner(s.key) ?? 0) + (inner.bandwidth() - barW) / 2
                  const by = y(value)
                  return <path key={s.key} d={columnPath(bx, by, barW, innerH - by)} fill={s.color} />
                })}
                <text className="yr-axis-label" x={(x(String(i)) ?? 0) + x.bandwidth() / 2} y={innerH + 16} textAnchor="middle">{item.label}</text>
              </g>
            ))}
            <line className="yr-baseline" x1={0} x2={innerW} y1={innerH} y2={innerH} />
          </g>
        </svg>
      )}
      <Tooltip state={tooltip} width={width} />
    </div>
  )
}

// ── Stacked columns (one column per category, segments per series) ──────

export function StackedColumns({ data, series, formatAxis, formatTotal, height = 260 }: {
  data: GroupedDatum[]
  series: Series[]
  formatAxis: (value: number) => string
  formatTotal: (value: number) => string
  height?: number
}) {
  const [ref, width] = useWidth<HTMLDivElement>()
  const { tooltip, show, hide } = useTooltip(ref)
  const margin = { top: 22, right: 8, bottom: 26, left: 48 }
  const innerW = Math.max(width - margin.left - margin.right, 0)
  const innerH = height - margin.top - margin.bottom
  const totals = data.map(item => series.reduce((sum, s) => sum + (item.values[s.key] ?? 0), 0))
  const max = Math.max(...totals, 1)
  const ticks = niceTicks(max)
  const y = scaleLinear().domain([0, ticks[ticks.length - 1] || max]).range([innerH, 0])
  const x = scaleBand<string>().domain(data.map((_, i) => String(i))).range([0, innerW]).paddingInner(0.45).paddingOuter(0.2)
  const barW = Math.min(x.bandwidth(), 56)

  return (
    <div className="yr-plot" ref={ref}>
      {width > 0 && (
        <svg width={width} height={height} role="img" aria-label="Στοιβαγμένο ραβδόγραμμα">
          <g transform={`translate(${margin.left},${margin.top})`}>
            {ticks.map(tick => (
              <g key={tick} transform={`translate(0,${y(tick)})`}>
                <line className="yr-grid" x1={0} x2={innerW} />
                <text className="yr-axis-label" x={-8} dy="0.32em" textAnchor="end">{formatAxis(tick)}</text>
              </g>
            ))}
            {data.map((item, i) => {
              const bx = (x(String(i)) ?? 0) + (x.bandwidth() - barW) / 2
              let cursor = 0
              const visible = series.filter(s => (item.values[s.key] ?? 0) > 0)
              return (
                <g key={item.label} className="yr-mark" tabIndex={0} role="graphics-symbol" aria-label={item.ariaLabel}
                  onPointerEnter={e => show(e.currentTarget, item.tooltip)} onPointerLeave={hide}
                  onFocus={e => show(e.currentTarget, item.tooltip)} onBlur={hide}>
                  <rect className="yr-hit" x={x(String(i))} y={0} width={x.bandwidth()} height={innerH} />
                  {visible.map((s, index) => {
                    const value = item.values[s.key] ?? 0
                    const y0 = y(cursor)
                    cursor += value
                    const y1 = y(cursor)
                    const gap = index < visible.length - 1 ? 2 : 0
                    const h = Math.max(y0 - y1 - gap, 0)
                    const top = y1 + gap
                    return index === visible.length - 1
                      ? <path key={s.key} d={columnPath(bx, top, barW, h)} fill={s.color} />
                      : <rect key={s.key} x={bx} y={top} width={barW} height={h} fill={s.color} />
                  })}
                  <text className="yr-value-label" x={bx + barW / 2} y={y(totals[i]) - 6} textAnchor="middle">{formatTotal(totals[i])}</text>
                  <text className="yr-axis-label" x={(x(String(i)) ?? 0) + x.bandwidth() / 2} y={innerH + 16} textAnchor="middle">{item.label}</text>
                </g>
              )
            })}
            <line className="yr-baseline" x1={0} x2={innerW} y1={innerH} y2={innerH} />
          </g>
        </svg>
      )}
      <Tooltip state={tooltip} width={width} />
    </div>
  )
}

// ── Horizontal stacked bars (rows of segments) ──────────────────────────

export type StackRow = { label: string; segments: { key: string; value: number; tooltip: ReactNode }[] }

export function StackedRows({ rows, series, normalize = false, formatValue }: {
  rows: StackRow[]
  series: Series[]
  normalize?: boolean
  formatValue: (value: number, total: number) => string
}) {
  const [ref, width] = useWidth<HTMLDivElement>()
  const { tooltip, show, hide } = useTooltip(ref)
  const rowH = 22
  const rowGap = 34
  const margin = { top: 6, right: 6, bottom: 4, left: 0 }
  const innerW = Math.max(width - margin.left - margin.right, 0)
  const height = margin.top + rows.length * (rowH + rowGap) + margin.bottom
  const maxTotal = Math.max(...rows.map(row => row.segments.reduce((sum, s) => sum + s.value, 0)), 1)
  const colorOf = (key: string) => series.find(s => s.key === key)?.color ?? '#9d9688'

  return (
    <div className="yr-plot" ref={ref}>
      {width > 0 && (
        <svg width={width} height={height} role="img" aria-label="Οριζόντιο στοιβαγμένο ραβδόγραμμα">
          <g transform={`translate(${margin.left},${margin.top})`}>
            {rows.map((row, rowIndex) => {
              const total = row.segments.reduce((sum, s) => sum + s.value, 0)
              const scale = scaleLinear().domain([0, normalize ? total || 1 : maxTotal]).range([0, innerW])
              const top = rowIndex * (rowH + rowGap) + 18
              let cursor = 0
              const visible = row.segments.filter(s => s.value > 0)
              return (
                <g key={row.label}>
                  <text className="yr-row-label" x={0} y={top - 6}>{row.label}</text>
                  {visible.map((segment, index) => {
                    const x0 = scale(cursor)
                    cursor += segment.value
                    const gap = index < visible.length - 1 ? 2 : 0
                    const w = Math.max(scale(cursor) - x0 - gap, 0)
                    const fill = colorOf(segment.key)
                    const labelText = formatValue(segment.value, total)
                    const fits = w > labelText.length * 6.4 + 10
                    return (
                      <g key={segment.key} className="yr-mark" tabIndex={0} role="graphics-symbol"
                        aria-label={`${row.label}, ${series.find(s => s.key === segment.key)?.label ?? segment.key}: ${labelText}`}
                        onPointerEnter={e => show(e.currentTarget, segment.tooltip)} onPointerLeave={hide}
                        onFocus={e => show(e.currentTarget, segment.tooltip)} onBlur={hide}>
                        {index === visible.length - 1
                          ? <path d={barPath(x0, top, w, rowH)} fill={fill} />
                          : <rect x={x0} y={top} width={w} height={rowH} fill={fill} />}
                        {fits && <text className="yr-inbar-label" fill={textOn(fill)} x={x0 + 6} y={top + rowH / 2} dy="0.34em">{labelText}</text>}
                      </g>
                    )
                  })}
                </g>
              )
            })}
          </g>
        </svg>
      )}
      <Tooltip state={tooltip} width={width} />
    </div>
  )
}

// ── Horizontal bar list (one series, label + value per row) ─────────────

export type BarListItem = { label: string; value: number; valueLabel: string; detail?: string; muted?: boolean }

export function BarList({ items, color, mutedColor }: { items: BarListItem[]; color: string; mutedColor: string }) {
  const max = Math.max(...items.map(item => item.value), 1)
  return (
    <ul className="yr-barlist">
      {items.map((item, index) => (
        <li key={`${index}-${item.label}`}>
          <div className="yr-barlist__head">
            <span className="yr-barlist__label">{item.label}</span>
            <span className="yr-barlist__value">{item.valueLabel}</span>
          </div>
          <div className="yr-barlist__track" aria-hidden="true">
            <div className="yr-barlist__fill" style={{ width: `${Math.max((item.value / max) * 100, item.value > 0 ? 0.6 : 0)}%`, background: item.muted ? mutedColor : color }} />
          </div>
          {item.detail && <span className="yr-barlist__detail">{item.detail}</span>}
        </li>
      ))}
    </ul>
  )
}
