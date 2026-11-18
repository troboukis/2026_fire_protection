import { useOverviewUi } from '../../../i18n/overview'
import { useSearchParams } from 'react-router-dom'
import { LocalizedLink as Link } from '../../../routing/LocalizedLink'
import { report, resolveSelection } from './data/antineroReport'
import { connections } from './data/antineroGraph'
import AntineroNetwork from './AntineroNetwork'
import AntineroFocus from './AntineroFocus'
import '../AnalysisLayout.css'
import './Antinero.css'

const latestDate = (dates: Array<string | null>) => dates.reduce<string | null>((latest, date) => date && (!latest || date > latest) ? date : latest, null)
const amendmentLabel = (kind: string) => kind === 'supplement' ? 'Συμπληρωματική πράξη' : 'Διορθωτική τροποποίηση'
const relationKinds = ['amendment', 'shared', 'beneficiary']

export default function AntineroPage() {
  const ui = useOverviewUi()
  const dateLabel = (date: string) => new Date(`${date}T12:00:00`).toLocaleDateString(ui.locale)
  const money = (amount: string | null) => amount === null ? ui("Δεν ορίζεται νέο τίμημα") : new Intl.NumberFormat(ui.locale, { style: 'currency', currency: 'EUR' }).format(Number(amount))
  const [params, setParams] = useSearchParams()
  const { contract, decision } = resolveSelection(params.get('node'), params.get('contract'))
  const selectedConnection = connections.find(item => item.id === params.get('connection'))
  const connectionSource = selectedConnection && report.contracts.find(item => item.id === selectedConnection.source)
  const connectionTarget = selectedConnection && report.contracts.find(item => item.id === selectedConnection.target)
  const hasNodeSelection = !!params.get('node') && (!!decision || report.contracts.some(item => item.id === params.get('node') || (item.aliases as string[]).includes(params.get('node')!)))
  const originals = report.contracts.filter(item => !item.parentId)
  const initialCompletionDate = latestDate(contract.completionDetails.map(item => item.initialDate))
  const finalCompletionDate = latestDate(contract.completionDetails.map(item => item.finalDate))

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
    <nav aria-label={ui("Πλοήγηση ανάλυσης")}><Link className="analysis-back" to="/analysis">{ui("← Όλες οι αναλύσεις")}</Link></nav>
    <header className="section-rule section-head">
      <div className="eyebrow">{ui("Δίκτυο συμβάσεων")}</div>
      <h2>{ui("Συμβάσεις AntiNERO στη Δυτική Αττική")}</h2>
      <p className="analysis-header-note">{ui("Το καλοκαίρι του 2026 σημαδεύτηκε από τη ")}<Link to="https://www.fire-watch-app.gr/municipalities?municipality=9230" target="_blank" rel="noopener noreferrer"> {ui("μεγάλη πυρκαγιά στη Δυτική Αττική")}</Link>{ui(", η οποία κατέκαψε πάνω από 110.000 στρέμματα. Η χαρτογράφηση των δημοσίων συμβάσεων που αφορούν σε έργα αντιπυρικής προστασίας (AntiNero) στη Δυτική Αττική πραγματοποιείται στο πλαίσιο προώθησης της διαφάνειας και λογοδοσίας στα έργα αντιπυρικής προστασίας που υλοποιούνται στα δάση της χώρας. Το δίκτυο συμβάσεων που προκύπτει, καθώς και η σύνδεσή τους με τις αντίστοιχες αποφάσεις στη Διαύγεια, βοηθούν ώστε ευαισθητοποιημένοι πολίτες, οργανισμοί και ερευνητές να εμβαθύνουν και να αξιολογήσουν τις δημόσιες συμβάσεις που αφορούν τα έργα αυτά, αποκτώντας μια πληρέστερη εικόνα.")}</p>
      <div className="report-meta">{ui("Ανάλυση ")}{ui.language === 'en' ? 'of ' : ''}{originals.length} {ui("συμβάσεων (ΚΗΜΔΗΣ) και ")}{report.decisions.length} {ui("εγγράφων (Διαύγεια) από το 2023 έως το 2026. ")}<br />{ui("Ενδέχεται να λείπουν συμβάσεις από την ανάλυση.")}</div>
    </header>
    <section className="report-workspace section-rule report-workspace--overview" aria-label={ui("Εξερεύνηση συμβάσεων")}>
      <div className="report-explorer">
        <AntineroNetwork contractIds={report.contracts.map(item => item.id)} relationKinds={relationKinds} onSelect={select} onConnection={selectConnection} />
      </div>
      {(hasNodeSelection || selectedConnection) && <AntineroFocus selectedId={hasNodeSelection ? decision?.id ?? contract.id : undefined} selectedContractId={contract.id} connectionId={selectedConnection?.id} relationKinds={relationKinds} onSelect={select} onConnection={selectConnection} onClose={closeSelection}>
      {selectedConnection && <aside className="report-detail" aria-label={ui("Τεκμηρίωση σύνδεσης")}>
        <div className="eyebrow">{ui("Σύνδεση συμβάσεων")}</div><h3>{selectedConnection.kind === 'amendment' ? ui("Τροποποίηση ή συμπλήρωση σύμβασης") : selectedConnection.kind === 'shared' ? ui("Κοινές αποφάσεις Διαύγειας") : ui("Κοινός ανάδοχος")}</h3>
        {selectedConnection.kind === 'amendment' && connectionSource && connectionTarget ? <div className="report-relationship">
          <div className="report-relationship__record">
            <div className="eyebrow">{ui("Αρχική σύμβαση")}</div>
            <button className="report-relationship__id" onClick={() => selectContract(connectionSource.id)}>{connectionSource.id}</button>
            <strong lang="el">{connectionSource.label}</strong>
            <span>{connectionSource.nodeContext.programme} · {ui(connectionSource.nodeContext.forestry)}</span>
            <a className="report-source" href={connectionSource.url} target="_blank" rel="noreferrer">{ui("Κατέβασε το έγγραφο ")}<span aria-hidden="true">↗</span></a>
          </div>
          <div className="report-relationship__direction" aria-label={ui("Τροποποιείται από")}><span>{ui("τροποποιείται από")}</span><b aria-hidden="true">↓</b></div>
          <div className="report-relationship__record report-relationship__record--amendment">
            <div className="eyebrow">{ui("Τροποποιητική πράξη")}</div>
            <button className="report-relationship__id" onClick={() => selectContract(connectionTarget.id)}>{connectionTarget.id}</button>
            <strong>{ui(amendmentLabel(connectionTarget.kind))}</strong>
            <span lang="el">{connectionTarget.label}</span>
            <a className="report-source" href={connectionTarget.url} target="_blank" rel="noreferrer">{ui("Κατέβασε το έγγραφο ")}<span aria-hidden="true">↗</span></a>
          </div>
          {selectedConnection.beneficiaryNames?.length && <p><strong>{ui("Κοινός ανάδοχος:")}</strong> {selectedConnection.beneficiaryNames.join(' / ')}</p>}
        </div> : <>
          <div className="report-relationship report-relationship--shared">{[connectionSource, connectionTarget].map((item, index) => item && <div className="report-relationship__record" key={item.id}>
            <div className="eyebrow">{ui("Σύμβαση ")}{index + 1}</div>
            <button className="report-relationship__id" onClick={() => selectContract(item.id)}>{item.id}</button>
            <strong lang="el">{item.label}</strong>
          </div>)}</div>
          {selectedConnection.kind === 'shared' ? <p>{ui("Οι ίδιες αποφάσεις αναφέρονται και στις δύο συμβάσεις. Η σύνδεση δεν δηλώνει ότι πρόκειται για το ίδιο έργο.")}</p> : <p><strong>{ui("Κοινός ανάδοχος:")}</strong> {selectedConnection.beneficiaryNames?.join(' / ')}</p>}
        </>}
        {(selectedConnection.kind === 'shared' || selectedConnection.evidenceIds.some(id => id !== selectedConnection.target)) && <div className="report-relationship__evidence">
          <h4>{selectedConnection.kind === 'shared' ? ui("Κοινά έγγραφα τεκμηρίωσης") : ui("Πρόσθετα έγγραφα τεκμηρίωσης")}</h4>
          {selectedConnection.evidenceIds.filter(id => selectedConnection.kind === 'shared' || id !== selectedConnection.target).map(id => { const source = report.decisions.find(item => item.id === id) ?? report.contracts.find(item => item.id === id); return source && <p key={id}><a className="report-source" href={source.url} target="_blank" rel="noreferrer">{id} ↗</a>{'subject' in source && <span className="report-source-note" lang="el">{source.subject}</span>}</p> })}
        </div>}
      </aside>}
      {hasNodeSelection && <aside className="report-detail" aria-label={ui("Πληροφορίες επιλεγμένου εγγράφου")}>
        <div className="eyebrow">{decision ? ui("Απόφαση / {{v0}}", { v0: ui(decision.category) }) : <time dateTime={contract.documentDate}>{dateLabel(contract.documentDate)}</time>}</div>
        <h3 lang="el">{decision?.label ?? contract.label}</h3>
        <p className="report-id" aria-live="polite">{decision?.id ?? contract.id}</p>
        {decision ? <><p>{ui("Ημερομηνία έκδοσης: ")}<time dateTime={decision.date}>{dateLabel(decision.date)}</time></p><p>{ui("Συνδεδεμένες συμβάσεις:")}</p>{decision.contractIds.map(id => <p key={id}><button className="report-inline" onClick={() => selectContract(id)}>{id}</button></p>)}</> : <>
          <p lang="el">{contract.description || contract.label}</p>
          <dl><dt>{ui("Πρόγραμμα")}</dt><dd>{contract.nodeContext.programme}</dd><dt>{ui("Δασική υπηρεσία")}</dt><dd>{ui(contract.nodeContext.forestry)}</dd><dt>{ui("Ανάδοχος")}</dt><dd>{contract.contractors.join(' / ')}</dd><dt>{ui("Αναθέτουσα αρχή")}</dt><dd>{ui.organization(contract.authority)}</dd>
            <dt>{contract.amountBasis === 'additional_amount' ? ui("Πρόσθετο τίμημα χωρίς ΦΠΑ") : ui("Συμβατικό τίμημα χωρίς ΦΠΑ")}</dt><dd>{contract.withoutVat === null ? ui("Χωρίς νέο τίμημα στην τροποποίηση") : money(contract.withoutVat)}</dd>
            <dt>{ui("Παράδοση έργου")}</dt><dd>{initialCompletionDate ? <time className="report-completion-value" dateTime={initialCompletionDate}>{dateLabel(initialCompletionDate)}</time> : '—'}</dd>
            <dt>{ui("Τελική ημερομηνία παράδοσης έργου")}</dt><dd>{finalCompletionDate ? <time className="report-completion-value" dateTime={finalCompletionDate}>{dateLabel(finalCompletionDate)}</time> : '—'}</dd>
          </dl>
          {contract.kind === 'supplement' && <p className="report-caveat">{ui("Το διαθέσιμο έγγραφο εγκρίνει τη συμπληρωματική σύμβαση· δεν τεκμηριώνει αυτοτελώς την υπογραφή της.")}{contract.id === '26SYMV018978343' ? ui(" Αφορά τη Χαλκίδα και διατηρείται για τη σύνδεσή του με την αρχική σύμβαση.") : ''}</p>}
        </>}
        {!decision && connections.filter(edge => edge.source === contract.id || edge.target === contract.id).map(edge => <p key={edge.id}><button className="report-inline" onClick={() => selectConnection(edge.id)}>{edge.kind === 'amendment' ? ui("Τροποποίηση ή συμπλήρωση") : edge.kind === 'shared' ? ui("Κοινές αποφάσεις") : ui("Κοινός ανάδοχος")} · {edge.source === contract.id ? edge.target : edge.source} ↗</button></p>)}
        <a className="report-source" href={documentUrl} target="_blank" rel="noreferrer">{ui("Κατέβασε το έγγραφο ")}<span aria-hidden="true">↗</span></a>
        {decision && <p className="report-source-note">{ui("Πηγή: Διαύγεια. Η ημερομηνία έκδοσης δεν ταυτίζεται απαραίτητα με την ημερομηνία εργασιών ή πληρωμής.")}</p>}
      </aside>}
      </AntineroFocus>}
    </section>
  </main>
}
