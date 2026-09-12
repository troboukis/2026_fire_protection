import { Link, useSearchParams } from 'react-router-dom'
import { report, resolveSelection } from './data/antineroReport'
import { connections } from './data/antineroGraph'
import AntineroNetwork from './AntineroNetwork'
import AntineroFocus from './AntineroFocus'
import '../AnalysisLayout.css'
import './Antinero.css'

const dateLabel = (date: string) => new Date(`${date}T12:00:00`).toLocaleDateString('el-GR')
const money = (amount: string | null) => amount === null ? 'Δεν ορίζεται νέο τίμημα' : new Intl.NumberFormat('el-GR', { style: 'currency', currency: 'EUR' }).format(Number(amount))
const amendmentLabel = (kind: string) => kind === 'supplement' ? 'Συμπληρωματική πράξη' : 'Διορθωτική τροποποίηση'
const relationKinds = ['amendment', 'shared', 'beneficiary']

export default function AntineroPage() {
  const [params, setParams] = useSearchParams()
  const { contract, decision } = resolveSelection(params.get('node'), params.get('contract'))
  const selectedConnection = connections.find(item => item.id === params.get('connection'))
  const connectionSource = selectedConnection && report.contracts.find(item => item.id === selectedConnection.source)
  const connectionTarget = selectedConnection && report.contracts.find(item => item.id === selectedConnection.target)
  const hasNodeSelection = !!params.get('node') && (!!decision || report.contracts.some(item => item.id === params.get('node') || (item.aliases as string[]).includes(params.get('node')!)))
  const originals = report.contracts.filter(item => !item.parentId)

  function select(id: string, context?: string) {
    const updated = new URLSearchParams(params)
    updated.set('node', id)
    updated.set('contract', context ?? contract.id)
    updated.delete('connection')
    setParams(updated)
  }
  function selectContract(id: string) {
    const updated = new URLSearchParams(params)
    updated.set('node', id); updated.set('contract', id); updated.delete('connection')
    setParams(updated)
  }
  function selectConnection(id: string) {
    const updated = new URLSearchParams(params)
    updated.delete('node'); updated.delete('contract'); updated.set('connection', id)
    setParams(updated)
  }
  function closeSelection() {
    const updated = new URLSearchParams(params)
    updated.delete('node'); updated.delete('contract'); updated.delete('connection')
    setParams(updated, { replace: true })
  }
  const documentUrl = decision?.url ?? contract.url

  return <main className="analysis-detail-page">
    <nav aria-label="Πλοήγηση ανάλυσης"><Link className="analysis-back" to="/analysis">← Όλες οι αναλύσεις</Link></nav>
    <header className="section-rule section-head">
      <div className="eyebrow">Δίκτυο συμβάσεων</div>
      <h2>AntiNERO στη Δυτική Αττική</h2>
      <p className="analysis-header-note">Το καλοκαίρι του 2026 σημαδεύτηκε από τη <a href="https://www.fire-watch-app.gr/municipalities?municipality=9230" target="_blank" rel="noopener noreferrer"> μεγάλη πυρκαγιά στη Δυτική Αττική</a>, η οποία κατέκαψε πάνω από 110.000 στρέμματα. Η χαρτογράφηση των δημοσίων συμβάσεων που αφορούν σε έργα αντιπυρικής προστασίας (AntiNero) στη Δυτική Αττική πραγματοποιείται στο πλαίσιο προώθησης της διαφάνειας και λογοδοσίας στα έργα αντιπυρικής προστασίας που υλοποιούνται στα δάση της χώρας. Το δίκτυο συμβάσεων που προκύπτει, καθώς και η σύνδεσή τους με τις αντίστοιχες αποφάσεις στη Διαύγεια, βοηθούν ώστε ευαισθητοποιημένοι πολίτες, οργανισμοί και ερευνητές να εμβαθύνουν και να αξιολογήσουν τις δημόσιες συμβάσεις που αφορούν τα έργα αυτά, αποκτώντας μια πληρέστερη εικόνα.</p>
      <div className="report-meta">Ανάλυση {originals.length} συμβάσεων (ΚΗΜΔΗΣ) και {report.decisions.length} εγγράφων (Διαύγεια) από το 2023 έως το 2026. <br />Ενδέχεται να λείπουν συμβάσεις από την ανάλυση.</div>
    </header>
    <section className="report-workspace section-rule report-workspace--overview" aria-label="Εξερεύνηση συμβάσεων">
      <div className="report-explorer">
        <AntineroNetwork contractIds={report.contracts.map(item => item.id)} relationKinds={relationKinds} onSelect={select} onConnection={selectConnection} />
      </div>
      {(hasNodeSelection || selectedConnection) && <AntineroFocus selectedId={hasNodeSelection ? decision?.id ?? contract.id : undefined} selectedContractId={contract.id} connectionId={selectedConnection?.id} relationKinds={relationKinds} onSelect={select} onConnection={selectConnection} onClose={closeSelection}>
      {selectedConnection && <aside className="report-detail" aria-label="Τεκμηρίωση σύνδεσης">
        <div className="eyebrow">Σύνδεση συμβάσεων</div><h3>{selectedConnection.kind === 'amendment' ? 'Τροποποίηση ή συμπλήρωση σύμβασης' : selectedConnection.kind === 'shared' ? 'Κοινές αποφάσεις Διαύγειας' : 'Κοινός ανάδοχος'}</h3>
        {selectedConnection.kind === 'amendment' && connectionSource && connectionTarget ? <div className="report-relationship">
          <div className="report-relationship__record">
            <div className="eyebrow">Αρχική σύμβαση</div>
            <button className="report-relationship__id" onClick={() => selectContract(connectionSource.id)}>{connectionSource.id}</button>
            <strong>{connectionSource.label}</strong>
            <span>{connectionSource.nodeContext.programme} · {connectionSource.nodeContext.forestry}</span>
            <a className="report-source" href={connectionSource.url} target="_blank" rel="noreferrer">Κατέβασε το έγγραφο <span aria-hidden="true">↗</span></a>
          </div>
          <div className="report-relationship__direction" aria-label="Τροποποιείται από"><span>τροποποιείται από</span><b aria-hidden="true">↓</b></div>
          <div className="report-relationship__record report-relationship__record--amendment">
            <div className="eyebrow">Τροποποιητική πράξη</div>
            <button className="report-relationship__id" onClick={() => selectContract(connectionTarget.id)}>{connectionTarget.id}</button>
            <strong>{amendmentLabel(connectionTarget.kind)}</strong>
            <span>{connectionTarget.label}</span>
            <a className="report-source" href={connectionTarget.url} target="_blank" rel="noreferrer">Κατέβασε το έγγραφο <span aria-hidden="true">↗</span></a>
          </div>
          {selectedConnection.beneficiaryNames?.length && <p><strong>Κοινός ανάδοχος:</strong> {selectedConnection.beneficiaryNames.join(' / ')}</p>}
        </div> : <>
          <div className="report-relationship report-relationship--shared">{[connectionSource, connectionTarget].map((item, index) => item && <div className="report-relationship__record" key={item.id}>
            <div className="eyebrow">Σύμβαση {index + 1}</div>
            <button className="report-relationship__id" onClick={() => selectContract(item.id)}>{item.id}</button>
            <strong>{item.label}</strong>
          </div>)}</div>
          {selectedConnection.kind === 'shared' ? <p>Οι ίδιες αποφάσεις αναφέρονται και στις δύο συμβάσεις. Η σύνδεση δεν δηλώνει ότι πρόκειται για το ίδιο έργο.</p> : <p><strong>Κοινός ανάδοχος:</strong> {selectedConnection.beneficiaryNames?.join(' / ')}</p>}
        </>}
        {(selectedConnection.kind === 'shared' || selectedConnection.evidenceIds.some(id => id !== selectedConnection.target)) && <div className="report-relationship__evidence">
          <h4>{selectedConnection.kind === 'shared' ? 'Κοινά έγγραφα τεκμηρίωσης' : 'Πρόσθετα έγγραφα τεκμηρίωσης'}</h4>
          {selectedConnection.evidenceIds.filter(id => selectedConnection.kind === 'shared' || id !== selectedConnection.target).map(id => { const source = report.decisions.find(item => item.id === id) ?? report.contracts.find(item => item.id === id); return source && <p key={id}><a className="report-source" href={source.url} target="_blank" rel="noreferrer">{id} ↗</a>{'subject' in source && <span className="report-source-note">{source.subject}</span>}</p> })}
        </div>}
      </aside>}
      {hasNodeSelection && <aside className="report-detail" aria-label="Πληροφορίες επιλεγμένου εγγράφου">
        <div className="eyebrow">{decision ? `Απόφαση / ${decision.category}` : <time dateTime={contract.documentDate}>{dateLabel(contract.documentDate)}</time>}</div>
        <h3>{decision?.label ?? contract.label}</h3>
        <p className="report-id" aria-live="polite">{decision?.id ?? contract.id}</p>
        {decision ? <><p>Ημερομηνία έκδοσης: <time dateTime={decision.date}>{dateLabel(decision.date)}</time></p><p>Συνδεδεμένες συμβάσεις:</p>{decision.contractIds.map(id => <p key={id}><button className="report-inline" onClick={() => selectContract(id)}>{id}</button></p>)}</> : <>
          <p>{contract.description || contract.label}</p>
          <dl><dt>Πρόγραμμα</dt><dd>{contract.nodeContext.programme}</dd><dt>Δασική υπηρεσία</dt><dd>{contract.nodeContext.forestry}</dd><dt>Ανάδοχος</dt><dd>{contract.contractors.join(' / ')}</dd><dt>Αναθέτουσα αρχή</dt><dd>{contract.authority}</dd>
            <dt>{contract.amountBasis === 'additional_amount' ? 'Πρόσθετο τίμημα χωρίς ΦΠΑ' : 'Συμβατικό τίμημα χωρίς ΦΠΑ'}</dt><dd>{contract.withoutVat === null ? 'Χωρίς νέο τίμημα στην τροποποίηση' : money(contract.withoutVat)}</dd></dl>
          {contract.kind === 'supplement' && <p className="report-caveat">Το διαθέσιμο έγγραφο εγκρίνει τη συμπληρωματική σύμβαση· δεν τεκμηριώνει αυτοτελώς την υπογραφή της.{contract.id === '26SYMV018978343' ? ' Αφορά τη Χαλκίδα και διατηρείται για τη σύνδεσή του με την αρχική σύμβαση.' : ''}</p>}
        </>}
        {!decision && connections.filter(edge => edge.source === contract.id || edge.target === contract.id).map(edge => <p key={edge.id}><button className="report-inline" onClick={() => selectConnection(edge.id)}>{edge.kind === 'amendment' ? 'Τροποποίηση ή συμπλήρωση' : edge.kind === 'shared' ? 'Κοινές αποφάσεις' : 'Κοινός ανάδοχος'} · {edge.source === contract.id ? edge.target : edge.source} ↗</button></p>)}
        <a className="report-source" href={documentUrl} target="_blank" rel="noreferrer">Κατέβασε το έγγραφο <span aria-hidden="true">↗</span></a>
        {decision && <p className="report-source-note">Πηγή: Διαύγεια. Η ημερομηνία έκδοσης δεν ταυτίζεται απαραίτητα με την ημερομηνία εργασιών ή πληρωμής.</p>}
      </aside>}
      </AntineroFocus>}
    </section>
  </main>
}
