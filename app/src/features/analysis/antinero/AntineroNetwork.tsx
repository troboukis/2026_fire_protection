import { useEffect, useMemo, useRef, useState, type CSSProperties } from 'react'
import { connections, programmeStyles } from './data/antineroGraph'
import { layoutOverview } from './data/antineroLayout'
import { report } from './data/antineroReport'
import { contractMark, shortArea } from './data/antineroLabels'

export function NetworkLegend({ relationKinds }: { relationKinds: string[] }) {
  const usedColors = new Set(report.contracts.map(item => item.nodeContext.programme))
  return <div className="report-network-legends">
    <div className="report-programme-legend" aria-label="Χρώματα προγραμμάτων AntiNERO">
      {programmeStyles.filter(style => usedColors.has(style.label)).map(style => <span key={style.label}>
        <i style={{ background: style.color }} aria-hidden="true" />
        {style.label}
      </span>)}
    </div>
    <div className="report-connection-legend" aria-label="Υπόμνημα συνδέσεων">
      <strong>Συνδέσεις</strong>
      {[
        { id: 'amendment', label: 'Τροποποιεί' },
        { id: 'shared', label: 'Κοινές αποφάσεις Διαύγειας' },
        { id: 'beneficiary', label: 'Κοινός ανάδοχος' },
      ].map(item => <span key={item.id} className={relationKinds.includes(item.id) ? '' : 'is-inactive'}>
        <i className={`report-connection-sample report-connection-sample--${item.id}`} aria-hidden="true" />
        {item.label}
      </span>)}
    </div>
  </div>
}

export default function AntineroNetwork({ contractIds, selectedId, relationKinds, onSelect, onConnection }: {
  contractIds: string[]; selectedId?: string; selectedContractId?: string; relationKinds: string[]
  onSelect: (id: string, context?: string) => void; onConnection: (id: string) => void
}) {
  const svgRef = useRef<SVGSVGElement>(null)
  const contractKey = [...contractIds].sort().join(',')
  const graph = useMemo(() => layoutOverview(contractKey.split(',')), [contractKey])
  const [camera, setCamera] = useState({ x: 0, y: 0, k: 1 })
  const [activeConnectionId, setActiveConnectionId] = useState<string | null>(null)
  const drag = useRef<{ x: number; y: number; pointerId: number } | null>(null)
  const positions = new Map(graph.nodes.map(node => [node.id, node]))
  const cx = graph.width / 2, cy = graph.height / 2
  useEffect(() => {
    if (!graph.nodes.length || graph.nodes.length === report.contracts.length) {
      setCamera({ x: 0, y: 0, k: 1 })
      return
    }
    const xs = graph.nodes.map(node => node.x), ys = graph.nodes.map(node => node.y)
    const left = Math.min(...xs) - 90, right = Math.max(...xs) + 90
    const top = Math.min(...ys) - 65, bottom = Math.max(...ys) + 95
    const k = Math.min(1.8, graph.width / (right - left), graph.height / (bottom - top))
    setCamera({ x: (cx - (left + right) / 2) * k, y: (cy - (top + bottom) / 2) * k, k })
  }, [graph, cx, cy])
  function zoom(factor: number) {
    setCamera(value => {
      const k = Math.max(.6, Math.min(4, value.k * factor))
      return { x: value.x * k / value.k, y: value.y * k / value.k, k }
    })
  }
  useEffect(() => {
    const svg = svgRef.current
    if (!svg) return
    function wheel(event: WheelEvent) {
      // Keep ordinary page scrolling available; explicit controls also work on touch.
      if (!event.ctrlKey && !event.metaKey) return
      event.preventDefault()
      const matrix = svg!.getScreenCTM()
      if (!matrix) return
      const point = new DOMPoint(event.clientX, event.clientY).matrixTransform(matrix.inverse())
      const x = point.x - cx, y = point.y - cy
      setCamera(value => {
        const k = Math.max(.6, Math.min(4, value.k * Math.exp(-event.deltaY * .005)))
        return { x: x - (x - value.x) * k / value.k, y: y - (y - value.y) * k / value.k, k }
      })
    }
    svg.addEventListener('wheel', wheel, { passive: false })
    return () => svg.removeEventListener('wheel', wheel)
  }, [cx, cy])
  function endDrag(event: React.PointerEvent<SVGSVGElement>) {
    if (drag.current?.pointerId !== event.pointerId) return
    drag.current = null
    if (event.currentTarget.hasPointerCapture(event.pointerId)) event.currentTarget.releasePointerCapture(event.pointerId)
  }
  return <div className="report-graph">
    <div className="report-graph-controls" role="group" aria-label="Πλοήγηση δικτύου">
      <span>Κάθε κύκλος, μία σύμβαση</span>
      <button onClick={() => zoom(1.25)} aria-label="Μεγέθυνση">+</button>
      <button onClick={() => zoom(1 / 1.25)} aria-label="Σμίκρυνση">−</button>
      <button onClick={() => setCamera({ x: 0, y: 0, k: 1 })}>Επαναφορά προβολής</button>
    </div>
    <svg ref={svgRef} viewBox={`0 0 ${graph.width} ${graph.height}`} aria-label={`Δίκτυο ${contractIds.length} συμβάσεων AntiNERO`} onPointerDown={event => {
      if (event.button !== 0 || (event.target as Element).closest('[role="button"]')) return
      drag.current = { x: event.clientX, y: event.clientY, pointerId: event.pointerId }
      event.currentTarget.setPointerCapture(event.pointerId)
    }} onPointerMove={event => {
      const start = drag.current
      if (!start || start.pointerId !== event.pointerId) return
      const matrix = event.currentTarget.getScreenCTM()
      if (!matrix) return
      const previous = new DOMPoint(start.x, start.y).matrixTransform(matrix.inverse())
      const next = new DOMPoint(event.clientX, event.clientY).matrixTransform(matrix.inverse())
      setCamera(value => ({ ...value, x: value.x + next.x - previous.x, y: value.y + next.y - previous.y }))
      drag.current = { x: event.clientX, y: event.clientY, pointerId: event.pointerId }
    }} onPointerUp={endDrag} onPointerCancel={endDrag} onLostPointerCapture={() => { drag.current = null }}>
      <defs>
        <marker id="report-amendment-arrow" markerWidth="6" markerHeight="6" refX="5.5" refY="3" orient="auto" markerUnits="strokeWidth">
          <path className="report-connection-arrow" d="M 0 0 L 6 3 L 0 6 Z" />
        </marker>
      </defs>
      <g transform={`translate(${cx + camera.x},${cy + camera.y}) scale(${camera.k}) translate(${-cx},${-cy})`}>
        {graph.edges.map(edge => {
          const connection = connections.find(item => item.id === edge.id)!
          const kind = relationKinds.includes(connection.kind) ? connection.kind : connection.relations.find(item => relationKinds.includes(item))
          if (!kind) return null
          const source = positions.get(edge.source)!, target = positions.get(edge.target)!
          const from = kind === 'amendment' ? target : source
          const to = kind === 'amendment' ? source : target
          const distance = Math.hypot(to.x - from.x, to.y - from.y) || 1
          const ux = (to.x - from.x) / distance, uy = (to.y - from.y) / distance
          const line = `M ${from.x + ux * 38} ${from.y + uy * 38} L ${to.x - ux * 43} ${to.y - uy * 43}`
          const label = kind === 'amendment' ? 'Τροποποιεί' : kind === 'shared' ? 'Κοινές αποφάσεις Διαύγειας' : 'Κοινός ανάδοχος'
          return <g key={edge.id} role="button" tabIndex={0} className="report-connection-action" aria-label={`${label}: ${edge.source} — ${edge.target}`} onPointerEnter={() => setActiveConnectionId(edge.id)} onPointerLeave={() => setActiveConnectionId(value => value === edge.id ? null : value)} onFocus={() => setActiveConnectionId(edge.id)} onBlur={() => setActiveConnectionId(value => value === edge.id ? null : value)} onClick={() => onConnection(edge.id)} onKeyDown={event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); onConnection(edge.id) } }}>
            <path d={line} className={`report-connection report-connection--${kind}`} markerEnd={kind === 'amendment' ? 'url(#report-amendment-arrow)' : undefined} />
            <path d={line} className="report-connection-hit" />
          </g>
        })}
        {graph.nodes.map(node => {
          const contract = report.contracts.find(item => item.id === node.id)!
          return <g key={node.id} data-node-id={node.id} style={{ '--category-color': node.color } as CSSProperties} transform={`translate(${node.x},${node.y})`} role="button" tabIndex={0} aria-label={`${contract.id}, ${contract.nodeContext.programme}, ${contract.label}`} aria-haspopup="dialog" className={`report-graph-contract ${selectedId === node.id ? 'is-selected' : ''}`} onClick={() => onSelect(node.id, node.id)} onKeyDown={event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); onSelect(node.id, node.id) } }}>
            <title>{contract.label} · {contract.nodeContext.area} · {contract.id}</title>
            <circle className="report-node-halo" r={41} />
            <circle className="report-node-surface" r={35} />
            <text textAnchor="middle" y={-2} className="report-node-mark">{contractMark(contract)}</text>
            <text textAnchor="middle" y={16} className="report-node-year">{contract.documentDate.slice(0, 4)}</text>
            <text textAnchor="middle" y={54} className="report-node-area">{shortArea(contract)}</text>
            <text textAnchor="middle" y={70} className="report-node-reference">{contract.id}</text>
          </g>
        })}
        {activeConnectionId && (() => {
          const edge = graph.edges.find(item => item.id === activeConnectionId)
          if (!edge) return null
          const connection = connections.find(item => item.id === edge.id)
          if (!connection) return null
          const kind = relationKinds.includes(connection.kind) ? connection.kind : connection.relations.find(item => relationKinds.includes(item))
          if (!kind) return null
          const source = positions.get(edge.source), target = positions.get(edge.target)
          if (!source || !target) return null
          const label = kind === 'amendment' ? 'Τροποποιεί' : kind === 'shared' ? 'Κοινές αποφάσεις Διαύγειας' : 'Κοινός ανάδοχος'
          const tooltipWidth = Math.max(70, label.length * 5.8 + 10)
          return <g className="report-connection-tooltip is-visible" transform={`translate(${(source.x + target.x) / 2},${(source.y + target.y) / 2})`} aria-hidden="true">
            <rect x={-tooltipWidth / 2} y="-32" width={tooltipWidth} height="24" rx="3" />
            <text textAnchor="middle" y="-16">{label}</text>
          </g>
        })()}
      </g>
    </svg>
    <p className="report-graph-hint">Επιλέξτε μία σύμβαση για να δείτε όλα τα έγγραφα που τη συνοδεύουν · + / − για μεγέθυνση.</p>
    <NetworkLegend relationKinds={relationKinds} />
  </div>
}
