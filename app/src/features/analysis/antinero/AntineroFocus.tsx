import { useEffect, useRef, useState, type CSSProperties, type ReactNode } from 'react'
import { connections, programmeStyle, type ConnectionKind } from './data/antineroGraph'
import { contractDecisions, report } from './data/antineroReport'
import { contractMark, shortArea } from './data/antineroLabels'
import { NetworkLegend } from './AntineroNetwork'

type FocusProps = {
  selectedId?: string
  selectedContractId: string
  connectionId?: string
  relationKinds: string[]
  onSelect: (id: string, context?: string) => void
  onConnection: (id: string) => void
  onClose: () => void
  children: ReactNode
}
type Neighbor = { id: string; label: string; subtitle: string; mark: string; color: string; kind: 'contract' | 'category' | 'decision'; relationKind?: ConnectionKind; amendmentTarget?: 'center' | 'neighbor'; select: () => void }
type SceneProps = FocusProps & {
  initialCategoryId?: string
  onCategorySelect: (contractId: string, categoryId: string) => void
}

function FocusScene({ selectedId, selectedContractId, connectionId, relationKinds, onSelect, onCategorySelect, initialCategoryId, children }: SceneProps) {
  const [categoryId, setCategoryId] = useState<string | null>(initialCategoryId ?? null)
  const centerRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const center = centerRef.current
    // Keep keyboard focus on the new record when its previous node disappears.
    if (center?.closest('dialog')?.open) center.focus({ preventScroll: true })
  }, [categoryId])
  const contract = report.contracts.find(item => item.id === selectedContractId)!
  const decision = report.decisions.find(item => item.id === selectedId)
  const connection = connections.find(item => item.id === connectionId)
  const category = report.categories.find(item => item.id === categoryId)
  const allDocuments = contractDecisions(contract.id)
  const documents = category ? allDocuments.filter(item => item.categoryId === category.id) : []
  const color = category?.color ?? (decision ? report.categories.find(item => item.id === decision.categoryId)?.color : programmeStyle(contract.nodeContext.programme).color)

  function contractNeighbor(id: string, subtitle = 'Σύμβαση', relationKind?: ConnectionKind, amendmentTarget?: 'center' | 'neighbor'): Neighbor {
    const item = report.contracts.find(record => record.id === id)!
    return { id, label: item.label, subtitle: `${subtitle} · ${shortArea(item)}`, mark: contractMark(item), color: programmeStyle(item.nodeContext.programme).color, kind: 'contract', relationKind, amendmentTarget, select: () => onSelect(id, id) }
  }
  function categoryNeighbor(id: string, select: () => void): Neighbor {
    const item = report.categories.find(record => record.id === id)!
    const count = allDocuments.filter(doc => doc.categoryId === id).length
    return { id: `${contract.id}:${id}`, label: item.label, subtitle: `${count} έγγραφα στη Διαύγεια`, mark: String(count), color: item.color, kind: 'category', select }
  }
  let left: Neighbor[] = []
  let right: Neighbor[] = []
  if (category) {
    left = [contractNeighbor(contract.id, 'Σύμβαση προέλευσης')]
    right = documents.map((item, index) => ({
      id: item.id, label: item.label, subtitle: new Date(`${item.date}T12:00:00`).toLocaleDateString('el-GR'), mark: String(index + 1).padStart(2, '0'), color: category.color, kind: 'decision', select: () => onSelect(item.id, contract.id),
    }))
    left[0].select = () => setCategoryId(null)
  } else if (decision) {
    left = [
      categoryNeighbor(decision.categoryId, () => onCategorySelect(contract.id, decision.categoryId)),
      ...decision.contractIds.map(id => contractNeighbor(id, 'Συνδεδεμένη σύμβαση')),
    ]
  } else if (connection) {
    left = [contractNeighbor(connection.source, connection.kind === 'amendment' ? 'Σύμβαση που τροποποιείται' : 'Σύμβαση', connection.kind, connection.kind === 'amendment' ? 'neighbor' : undefined)]
    right = [contractNeighbor(connection.target, connection.kind === 'amendment' ? 'Τροποποιητική πράξη' : 'Σύμβαση', connection.kind)]
  } else {
    left = connections.filter(edge => (edge.source === contract.id || edge.target === contract.id) && edge.relations.some(kind => relationKinds.includes(kind))).map(edge => {
      const activeKinds = edge.relations.filter(kind => relationKinds.includes(kind))
      const label = activeKinds.map(kind => kind === 'amendment' ? edge.source === contract.id ? 'Τροποποίηση' : 'Αρχική σύμβαση' : kind === 'shared' ? 'Κοινές αποφάσεις' : 'Κοινός ανάδοχος').join(' / ')
      const displayedKind = activeKinds.includes(edge.kind) ? edge.kind : activeKinds[0]
      const amendmentTarget = displayedKind === 'amendment' ? edge.source === contract.id ? 'center' : 'neighbor' : undefined
      return contractNeighbor(edge.source === contract.id ? edge.target : edge.source, label, displayedKind, amendmentTarget)
    })
    right = report.categories.filter(item => allDocuments.some(doc => doc.categoryId === item.id)).map(item => categoryNeighbor(item.id, () => setCategoryId(item.id)))
  }
  const origins = left.map((item, index) => ({ ...item, y: (index + 1) / (left.length + 1) * 100 }))
  const branchLabel = connection ? connection.kind === 'amendment' ? 'Τροποποίηση ή συμπλήρωση σύμβασης' : 'Συνδεδεμένη σύμβαση' : 'Συνδεόμενα έγγραφα στη Διαύγεια'
  const originLabel = category ? 'Σύμβαση προέλευσης' : decision ? 'Προέλευση εγγράφου' : connection ? 'Σύμβαση προέλευσης' : 'Συνδεδεμένες συμβάσεις'

  function neighborButton(item: Neighbor, side: 'left' | 'right', y?: number) {
    return <button key={item.id} type="button" data-focus-node-id={item.id} className={`report-focus-neighbor report-focus-neighbor--${side} report-focus-neighbor--${item.kind}${item.relationKind ? ` report-focus-neighbor--relation-${item.relationKind}` : ''}`} style={{ '--neighbor-y': `${y ?? 50}%`, '--neighbor-color': item.color } as CSSProperties} onClick={item.select} aria-label={`${item.label}, ${item.subtitle}`} title={`${item.label} · ${item.subtitle}`}>
      <span className="report-focus-neighbor__circle" aria-hidden="true">{item.mark}</span>
      <span className="report-focus-neighbor__text"><strong>{item.label}</strong><small>{item.subtitle}</small></span>
    </button>
  }

  return <>
    <div className="report-focus-context">
      <div>{category ? <button className="report-focus-back" onClick={() => setCategoryId(null)}>← Πίσω στη σύμβαση</button> : decision ? <button className="report-focus-back" onClick={() => onCategorySelect(contract.id, decision.categoryId)}>← Στην κατηγορία εγγράφων</button> : <span>Επιλεγμένη σύμβασην &amp; έγγραφα από τη Διαύγεια</span>}</div>
      <span>{category ? `${documents.length} έγγραφα από τη Διαύγεια που ανήκουν στην κατηγορία ${category.label}` : decision ? 'Έγγραφο της Διαύγειας' : `${left.length + right.length} συνδεδεμένοι κόμβοι`}</span>
    </div>
    <div className="report-focus-stage" style={{ '--category-color': color } as CSSProperties}>
      <svg className="report-focus-lines" viewBox="0 0 1000 600" preserveAspectRatio="none" aria-hidden="true">
        <defs>
          <marker id="report-focus-amendment-arrow" markerWidth="6" markerHeight="6" refX="5.5" refY="3" orient="auto-start-reverse" markerUnits="strokeWidth">
            <path className="report-connection-arrow" d="M 0 0 L 6 3 L 0 6 Z" />
          </marker>
        </defs>
        {origins.map(item => <path key={item.id} className={item.relationKind ? `report-focus-line--${item.relationKind}` : undefined} d={`M 295 300 H 230 V ${item.y * 6} H 160`} style={{ stroke: item.relationKind === 'amendment' ? 'var(--accent)' : item.color }} markerStart={item.amendmentTarget === 'center' ? 'url(#report-focus-amendment-arrow)' : undefined} markerEnd={item.amendmentTarget === 'neighbor' ? 'url(#report-focus-amendment-arrow)' : undefined} />)}
        {right.length > 0 && <path className={`report-focus-forward-line${connection?.kind === 'amendment' ? ' report-focus-line--amendment' : ''}`} d="M 705 300 H 740" style={{ stroke: connection?.kind === 'amendment' ? 'var(--accent)' : color }} />}
      </svg>
      <div ref={centerRef} tabIndex={-1} className="report-focus-center" aria-label={category ? category.label : decision?.label ?? (connection ? 'Στοιχεία σύνδεσης' : contract.label)}>
        <div className="report-focus-badge" aria-hidden="true">{category ? category.label.slice(0, 2).toLocaleUpperCase('el') : decision ? 'ΑΠ' : connection ? '↔' : contractMark(contract)}</div>
        {category ? <article className="report-detail report-focus-category-detail">
          <div className="eyebrow">Έγγραφα στη Διαύγεια</div>
          <h3>{category.label}</h3>
          <p>{contract.label}</p>
          <p className="report-id">{contract.id}</p>
          <p>Επιλέξτε ένα από τα {documents.length} έγγραφα στα δεξιά για να διαβάσετε το θέμα, τις πληροφορίες και την πηγή του.</p>
          <button className="report-inline" onClick={() => setCategoryId(null)}>Στοιχεία σύμβασης</button>
        </article> : children}
      </div>
      {left.length > 0 && <div className="report-focus-neighbors" aria-label={originLabel}>
        {origins.map(item => neighborButton(item, 'left', item.y))}
      </div>}
      {right.length > 0 && <section className="report-focus-branch" aria-label={branchLabel}>
        <h3 className="report-focus-branch-title">{branchLabel}</h3>
        <div className="report-focus-forward" tabIndex={0} aria-label={category ? `Όλα τα έγγραφα: ${category.label}` : branchLabel}>
          <div className="report-focus-forward-list">{right.map(item => neighborButton(item, 'right'))}</div>
        </div>
      </section>}
    </div>
    <p className="report-focus-hint">Αριστερά η προέλευση · Δεξιά οι κατηγορίες και τα συνδεόμενα έγγραφα.</p>
    <NetworkLegend relationKinds={relationKinds} />
  </>
}

export default function AntineroFocus(props: FocusProps) {
  const dialogRef = useRef<HTMLDialogElement>(null)
  const [categoryRequest, setCategoryRequest] = useState<{ contractId: string; categoryId: string } | null>(null)
  const initialCategoryId = categoryRequest && props.selectedId === categoryRequest.contractId ? categoryRequest.categoryId : undefined
  function selectCategory(contractId: string, categoryId: string) {
    setCategoryRequest({ contractId, categoryId })
    props.onSelect(contractId, contractId)
  }
  function selectNode(id: string, context?: string) {
    setCategoryRequest(null)
    props.onSelect(id, context)
  }
  useEffect(() => {
    const dialog = dialogRef.current!
    const previousFocus = document.activeElement
    const previousOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    dialog.showModal()
    return () => {
      dialog.close()
      document.body.style.overflow = previousOverflow
      if (previousFocus instanceof HTMLElement || previousFocus instanceof SVGElement) previousFocus.focus({ preventScroll: true })
    }
  }, [])
  return <dialog ref={dialogRef} className="report-focus" aria-label="Εστίαση στο δίκτυο AntiNERO" onCancel={event => { event.preventDefault(); props.onClose() }} onKeyDown={event => {
    if (event.key !== 'Tab') return
    const focusable = [...event.currentTarget.querySelectorAll<HTMLElement>('button:not([disabled]), a[href], input:not([disabled]), select:not([disabled]), [tabindex]:not([tabindex="-1"])')].filter(element => element.getClientRects().length > 0)
    const first = focusable[0], last = focusable[focusable.length - 1]
    if (!first || !last) return
    if (event.shiftKey && (document.activeElement === first || document.activeElement === event.currentTarget)) { event.preventDefault(); last.focus() }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus() }
  }}>
    <header className="report-focus-header"><div><span className="eyebrow">AntiNERO / Δυτική Αττική</span><h2>Εξερευνήστε τις συνδέσεις</h2></div><button type="button" autoFocus className="report-focus-close" onClick={props.onClose}>Κλείσιμο <span aria-hidden="true">×</span></button></header>
    <FocusScene key={`${props.selectedId ?? ''}:${props.selectedContractId}:${props.connectionId ?? ''}:${initialCategoryId ?? ''}`} {...props} onSelect={selectNode} initialCategoryId={initialCategoryId} onCategorySelect={selectCategory} />
  </dialog>
}
