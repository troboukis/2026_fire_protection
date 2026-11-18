import { useState, type ReactNode } from 'react'
import {
  CODE_EFFIS, CODE_FIRMS, CODE_JOIN, COPY_PAYLOADS, NEGATIVE_GROUPS, POSITIVE_KEYWORDS,
  PRIMARY_CPVS, SECONDARY_CPVS, TESTER_PRESETS, evaluateTitle, type TesterCpv,
} from './guideData'

// The guide is published in Greek only for now; the English route shows the same Greek text.

function Section({ id, eyebrow, title, children }: { id: string; eyebrow: string; title: string; children: ReactNode }) {
  return (
    <section id={id} className="mg-section" aria-labelledby={`${id}-title`}>
      <header className="mg-section__head">
        <div className="eyebrow">{eyebrow}</div>
        <h3 id={`${id}-title`}>{title}</h3>
      </header>
      <div className="mg-section__body">{children}</div>
    </section>
  )
}

function ExternalLink({ href, children }: { href: string; children: ReactNode }) {
  return <a href={href} target="_blank" rel="noopener noreferrer">{children}</a>
}

function CopyButton({ text }: { text: string }) {
  const [state, setState] = useState<'idle' | 'done' | 'failed'>('idle')
  const copy = () => {
    const reset = () => window.setTimeout(() => setState('idle'), 1600)
    try {
      navigator.clipboard.writeText(text).then(() => { setState('done'); reset() }, () => { setState('failed'); reset() })
    } catch {
      setState('failed'); reset()
    }
  }
  return (
    <button type="button" className="mg-copy" onClick={copy} aria-live="polite">
      {state === 'done' ? 'Αντιγράφηκε' : state === 'failed' ? 'Δεν αντιγράφηκε' : 'Αντιγραφή'}
    </button>
  )
}

function CodeBlock({ code, label }: { code: string; label: string }) {
  return (
    <figure className="mg-code">
      <figcaption><span>{label}</span><CopyButton text={code} /></figcaption>
      <pre><code>{code}</code></pre>
    </figure>
  )
}

/* ── Venn diagrams ── */
const P = '#244b67'
const S = '#796f63'
const K = '#b9852f'
const N = '#d3482d'
const KEEP = '#5f8b55'

function VennDefs() {
  return (
    <svg width="0" height="0" className="mg-defs" aria-hidden="true" focusable="false">
      <defs>
        <pattern id="mg-hatch" width="7" height="7" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
          <rect width="7" height="7" className="mg-hatch-bg" />
          <line x1="0" y1="0" x2="0" y2="7" className="mg-hatch-line" strokeWidth="2.5" />
        </pattern>
        <clipPath id="mg-clip-s"><circle cx="215" cy="150" r="68" /></clipPath>
        <clipPath id="mg-clip-ps"><circle cx="135" cy="150" r="68" /><circle cx="215" cy="150" r="68" /></clipPath>
      </defs>
    </svg>
  )
}

function Label({ x, y, color, lines, anchor = 'middle' }: { x: number; y: number; color: string; lines: string[]; anchor?: 'start' | 'middle' | 'end' }) {
  return <>{lines.map((line, index) => <text key={line} x={x} y={y + index * 16} textAnchor={anchor} className="mg-venn__label" fill={color}>{line}</text>)}</>
}

function CpvCircles({ step }: { step: 1 | 2 | 3 }) {
  return (
    <svg viewBox="0 0 360 280" className="mg-venn" role="img" aria-label={[
      'Δύο επικαλυπτόμενοι κύκλοι, Primary CPVs και Secondary CPVs, και οι δύο σημειωμένοι ως κρατάμε',
      'Ο κύκλος των αρνητικών λέξεων κόβει και τους δύο κύκλους CPV, και το κομμάτι που επικαλύπτει πετιέται',
      'Από τα Secondary CPVs μένει μόνο η τομή με τις θετικές λέξεις. Τα Primary CPVs μένουν όλα, εκτός από τις αρνητικές λέξεις',
    ][step - 1]}>
      <rect x="1" y="1" width="358" height="278" className="mg-venn__universe" />
      {step === 3 ? <>
        <circle cx="215" cy="150" r="68" fill="url(#mg-hatch)" />
        <circle cx="135" cy="150" r="68" className="mg-venn__keep" />
        <circle cx="252" cy="212" r="44" className="mg-venn__keep" clipPath="url(#mg-clip-s)" />
      </> : <>
        <circle cx="135" cy="150" r="68" className="mg-venn__keep" />
        <circle cx="215" cy="150" r="68" className="mg-venn__keep" />
      </>}
      {step > 1 && <circle cx="175" cy="78" r="42" fill="url(#mg-hatch)" clipPath="url(#mg-clip-ps)" />}
      <circle cx="135" cy="150" r="68" fill="none" stroke={P} strokeWidth="2.5" />
      <circle cx="215" cy="150" r="68" fill="none" stroke={S} strokeWidth="2.5" />
      {step > 1 && <circle cx="175" cy="78" r="42" fill="none" stroke={N} strokeWidth="2.5" strokeDasharray="6 4" />}
      {step === 3 && <circle cx="252" cy="212" r="44" fill="none" stroke={K} strokeWidth="2.5" strokeDasharray="6 4" />}
      {step > 1 && <Label x={175} y={28} color={N} lines={['Αρνητικές λέξεις']} />}
      <Label x={62} y={146} color={P} lines={['Primary', 'CPVs']} anchor="end" />
      <Label x={289} y={step === 3 ? 126 : 146} color={S} lines={['Secondary', 'CPVs']} anchor="start" />
      {step === 3 && <Label x={320} y={246} color={K} lines={['Θετικές', 'λέξεις']} />}
    </svg>
  )
}

function DiavgeiaCircles() {
  return (
    <svg viewBox="0 0 300 260" className="mg-venn" role="img" aria-label="Μεγάλος κύκλος ευρείας αναζήτησης. Μέσα του μικρότερος κύκλος με θετικές λέξεις στο θέμα, που κρατάμε. Ο κύκλος των αρνητικών λέξεων τέμνει και τους δύο και κόβει ό,τι πιάνει.">
      <rect x="1" y="1" width="298" height="258" className="mg-venn__universe" />
      <circle cx="135" cy="145" r="92" fill="url(#mg-hatch)" />
      <circle cx="135" cy="168" r="57" className="mg-venn__keep" />
      <circle cx="198" cy="125" r="44" fill="url(#mg-hatch)" />
      <circle cx="135" cy="145" r="92" fill="none" stroke={K} strokeWidth="2.5" />
      <circle cx="135" cy="168" r="57" fill="none" stroke={KEEP} strokeWidth="2.5" />
      <circle cx="198" cy="125" r="44" fill="none" stroke={N} strokeWidth="2.5" strokeDasharray="6 4" />
      <Label x={14} y={34} color={K} lines={['Ευρεία αναζήτηση']} anchor="start" />
      <Label x={292} y={56} color={N} lines={['Αρνητικές λέξεις']} anchor="end" />
      <Label x={130} y={190} color={KEEP} lines={['Θετικές λέξεις', 'στο θέμα']} />
    </svg>
  )
}

/* ── Title tester ── */
const CPV_OPTIONS: { value: TesterCpv; label: string; hint: string }[] = [
  { value: 'primary', label: 'Primary CPV', hint: 'κωδικός μόνο για πυρκαγιές, π.χ. υπηρεσίες πρόληψης πυρκαγιών' },
  { value: 'secondary', label: 'Secondary CPV', hint: 'γενικότερος κωδικός, π.χ. συντήρηση οδών' },
  { value: 'none', label: 'Κανέναν από τις λίστες', hint: 'ο κωδικός δεν είναι στις λίστες μας' },
]

type StepState = 'pass' | 'fail' | 'skip'

function TesterStep({ number, title, state, children }: { number: number; title: string; state: StepState; children: ReactNode }) {
  return (
    <li className={`mg-check mg-check--${state}`}>
      <span className="mg-check__icon" aria-hidden="true">{state === 'pass' ? '✓' : state === 'fail' ? '✕' : '–'}</span>
      <div>
        <div className="mg-check__title">Βήμα {number} · {title}</div>
        <p>{children}</p>
      </div>
    </li>
  )
}

function TitleTester() {
  const [title, setTitle] = useState(TESTER_PRESETS[1].title)
  const [organization, setOrganization] = useState('')
  const [cpv, setCpv] = useState<TesterCpv>(TESTER_PRESETS[1].cpv)
  const result = evaluateTitle({ title, organization, cpv })
  const quoted = (items: string[]) => items.map((item, index) => <span key={item}>{index > 0 && ', '}«<mark className="mg-mark">{item}</mark>»</span>)
  const cpvLabel = CPV_OPTIONS.find(option => option.value === cpv)!.label

  const step1: StepState = cpv === 'none' ? 'fail' : 'pass'
  const step2: StepState = result.droppedAt === 1 ? 'skip' : result.negativeHits.length ? 'fail' : 'pass'
  const step3: StepState = result.droppedAt === 1 || result.droppedAt === 2 ? 'skip' : result.droppedAt === 3 ? 'fail' : 'pass'

  return (
    <div className="mg-tester">
      <div className="mg-tester__form">
        <div className="mg-field">
          <span>Δοκιμάστε ένα παράδειγμα</span>
          <div className="mg-presets">
            {TESTER_PRESETS.map(preset => <button key={preset.label} type="button" onClick={() => { setTitle(preset.title); setOrganization(''); setCpv(preset.cpv) }}>{preset.label}</button>)}
          </div>
        </div>
        <label className="mg-field">
          <span>Τίτλος σύμβασης</span>
          <textarea value={title} onChange={event => setTitle(event.target.value)} rows={3} />
        </label>
        <fieldset className="mg-field mg-cpv">
          <legend>Τι κωδικό CPV έχει η σύμβαση;</legend>
          {CPV_OPTIONS.map(option => (
            <label key={option.value}>
              <input type="radio" name="mg-cpv" value={option.value} checked={cpv === option.value} onChange={() => setCpv(option.value)} />
              <span><strong>{option.label}</strong> <small>{option.hint}</small></span>
            </label>
          ))}
        </fieldset>
        <label className="mg-field">
          <span>Αναθέτων φορέας (προαιρετικό)</span>
          <input type="text" value={organization} onChange={event => setOrganization(event.target.value)} placeholder="π.χ. Δήμος Μαραθώνα" />
        </label>
      </div>
      <div className="mg-tester__result" aria-live="polite">
        <div className={`mg-verdict mg-verdict--${result.verdict}`}>
          {result.verdict === 'keep' ? '✓ Η σύμβαση περιλαμβάνεται στην ανάλυση' : '✕ Η σύμβαση δεν περιλαμβάνεται στην ανάλυση'}
        </div>
        <ol className="mg-checks-result">
          <TesterStep number={1} title="Κωδικός CPV" state={step1}>
            {cpv === 'none' ? 'Ο κωδικός δεν είναι στις λίστες μας, οπότε η σύμβαση δεν μπαίνει καν στην αναζήτηση.' : <>Έχει {cpvLabel}, οπότε η σύμβαση μπαίνει στην αναζήτηση.</>}
          </TesterStep>
          <TesterStep number={2} title="Αρνητικές λέξεις" state={step2}>
            {step2 === 'skip' ? 'Δεν εξετάζεται.' : result.negativeHits.length
              ? <>Βρέθηκε αρνητική λέξη: {result.negativeHits.slice(0, 3).map((hit, index) => <span key={`${hit.field}-${hit.words}`}>{index > 0 && ', '}«<mark className="mg-mark mg-mark--negative">{hit.words}</mark>» {hit.field === 'title' ? 'στον τίτλο' : 'στον φορέα'}</span>)}. Η σύμβαση αποκλείεται.</>
              : 'Δεν βρέθηκε καμία αρνητική λέξη.'}
          </TesterStep>
          <TesterStep number={3} title="Θετικές λέξεις" state={step3}>
            {step3 === 'skip' ? 'Δεν εξετάζεται.'
              : !result.requiresPositive ? 'Με Primary CPV δεν χρειάζεται θετική λέξη.'
                : result.positiveHits.length ? <>Βρέθηκε θετική λέξη στον τίτλο: {quoted(result.positiveHits)}.</>
                  : <>Το {cpvLabel} χρειάζεται θετική λέξη στον τίτλο, αλλά δεν βρέθηκε. Η σύμβαση αποκλείεται.</>}
          </TesterStep>
        </ol>
      </div>
    </div>
  )
}

function CpvList({ items }: { items: Record<string, string> }) {
  return (
    <ul className="mg-cpvlist">
      {Object.entries(items).map(([code, label]) => <li key={code}><code>{code}</code><span>{label}</span></li>)}
    </ul>
  )
}

function Roots({ items }: { items: string[] }) {
  return <ul className="mg-roots">{items.map(item => <li key={item}><code>{item}</code></li>)}</ul>
}

function KitBox({ title, color, payload, children }: { title: string; color: string; payload: unknown; children: ReactNode }) {
  return (
    <div className="mg-kit__box" style={{ borderTopColor: color }}>
      <div className="mg-kit__head"><h4 style={{ color }}>{title}</h4><CopyButton text={JSON.stringify(payload, null, 2)} /></div>
      {children}
    </div>
  )
}

const QUESTIONS: { title: string; question: string; method: string; methodLabel?: string; limits?: string; note?: string }[] = [
  {
    title: 'Απευθείας αναθέσεις και κατάτμηση συμβάσεων',
    question: 'Συγκεντρώνονται οι απευθείας αναθέσεις σε ποσά λίγο κάτω από το νόμιμο όριο;',
    method: 'Εξετάζουμε την κατανομή των συμβάσεων ανά ποσό, με έμφαση στις αξίες κοντά στο όριο της απευθείας ανάθεσης. Αναζητούμε επίσης πιθανές ενδείξεις κατάτμησης, εντοπίζοντας συμβάσεις με τον ίδιο αναθέτοντα φορέα, τον ίδιο ανάδοχο, παρόμοιο αντικείμενο και κοντινές ημερομηνίες υπογραφής.',
    limits: 'Τα όρια των απευθείας αναθέσεων διαφέρουν ανά χώρα και είδος σύμβασης. Η συγκέντρωση συμβάσεων κοντά στο όριο δεν αποτελεί από μόνη της απόδειξη παράνομης κατάτμησης.',
  },
  {
    title: 'Ετοιμότητα πριν από την αντιπυρική περίοδο',
    question: 'Υπογράφονται εγκαίρως οι συμβάσεις πρόληψης, πριν από την έναρξη της αντιπυρικής περιόδου;',
    method: 'Αναλύουμε τις συμβάσεις ανά μήνα υπογραφής και εξετάζουμε πόσες υπογράφονται πριν και πόσες μετά την έναρξη της αντιπυρικής περιόδου, η οποία στην Ελλάδα ξεκινά την 1η Μαΐου.',
    limits: 'Οι πολυετείς συμβάσεις υπογράφονται μία φορά, αλλά μπορεί να καλύπτουν περισσότερες αντιπυρικές περιόδους.',
  },
  {
    title: 'Χρηματοδότηση και πραγματικές δαπάνες',
    question: 'Οι δήμοι που λαμβάνουν χρηματοδότηση για πυροπροστασία εμφανίζουν αντίστοιχες συμβάσεις και πληρωμές;',
    method: 'Συγκρίνουμε τη χρηματοδότηση που λαμβάνει κάθε δήμος με τις συμβάσεις που καταγράφονται στο ΚΗΜΔΗΣ και τις σχετικές πληρωμές που εντοπίζονται στη Διαύγεια κατά το ίδιο έτος.',
    limits: 'Η απουσία καταγεγραμμένης σύμβασης ή πληρωμής δεν σημαίνει απαραίτητα ότι τα χρήματα δεν δαπανήθηκαν.',
  },
  {
    title: 'Συγκέντρωση δαπανών σε αναδόχους',
    question: 'Τι ποσοστό της συνολικής αξίας των συμβάσεων συγκεντρώνεται σε περιορισμένο αριθμό αναδόχων;',
    method: 'Υπολογίζουμε το μερίδιο των μεγαλύτερων αναδόχων στη συνολική αξία των συμβάσεων και εξετάζουμε τις σχέσεις μεταξύ αναθετουσών αρχών και αναδόχων.',
    limits: 'Η ταυτοποίηση των αναδόχων γίνεται με βάση τον ΑΦΜ και όχι την επωνυμία, καθώς η ίδια επιχείρηση μπορεί να εμφανίζεται με διαφορετικές ονομασίες.',
  },
  {
    title: 'Δαπάνες πρόληψης και καταστολής',
    question: 'Πόσα δημόσια χρήματα κατευθύνονται στην πρόληψη των πυρκαγιών και πόσα στην καταστολή τους;',
    methodLabel: 'Μεθοδολογική δυσκολία',
    method: 'Οι τίτλοι των συμβάσεων συχνά είναι γενικοί ή περιλαμβάνουν δραστηριότητες τόσο πρόληψης όσο και καταστολής. Επιπλέον, ο ίδιος εξοπλισμός μπορεί να χρησιμοποιείται και για τους δύο σκοπούς.',
    note: 'Με τα διαθέσιμα δεδομένα δεν έχει εντοπιστεί ακόμη αξιόπιστη μέθοδος που να επιτρέπει τον σαφή διαχωρισμό των δύο κατηγοριών.',
  },
  {
    title: 'Σχέση δημόσιων δαπανών και καμένων εκτάσεων',
    question: 'Υπάρχει σχέση μεταξύ των χρημάτων που δαπανώνται για την πυροπροστασία σε μια περιοχή και της έκτασης που τελικά καίγεται;',
    methodLabel: 'Μεθοδολογική δυσκολία',
    method: 'Η έκταση των καταστροφών επηρεάζεται από πολλούς παράγοντες, ιδιαίτερα τις μετεωρολογικές συνθήκες. Λίγες μεγάλες πυρκαγιές μπορούν να επηρεάσουν δυσανάλογα τα συνολικά στατιστικά, ενώ σημαντικό μέρος των δαπανών πραγματοποιείται μετά την εκδήλωση μιας πυρκαγιάς.',
    note: 'Επομένως, μια απλή σύγκριση δαπανών και καμένων εκτάσεων δεν επαρκεί για να αξιολογηθεί η αποτελεσματικότητα των μέτρων πυροπροστασίας.',
  },
]

const SECTIONS: [string, string][] = [
  ['kritiria', 'Τα κριτήρια'],
  ['vimata', 'Τα βήματα'],
  ['paradeigmata', 'Παραδείγματα'],
  ['diavgeia', 'Αποφάσεις και πληρωμές'],
  ['dokimi', 'Δοκιμάστε έναν τίτλο'],
  ['listes', 'Οι λίστες λέξεων'],
  ['piges', 'Πηγές'],
  ['pyrkagies', 'Δεδομένα πυρκαγιών'],
  ['erotimata', 'Ερευνητικά ερωτήματα'],
  ['pagides', 'Παγίδες'],
]

export default function MethodologyGuide() {
  return (
    <article className="mg-root" lang="el">
      <VennDefs />
      <header className="section-head mg-hero">
        <div className="eyebrow">Οδηγός</div>
        <h2>Η μεθοδολογία συλλογής δημόσιων δεδομένων για τις δασικές πυρκαγιές</h2>
        <p className="mg-subtitle">Ένας οδηγός για δημοσιογράφους και μη που θέλουν να ερευήσουν τις δημόσιες συμβάσεις για την πρόληψη και αντιμετώπιση δασικών πυρκαγιών.</p>
      </header>

      <div className="mg-layout">
        <nav className="mg-toc" aria-label="Περιεχόμενα οδηγού">
          <div className="eyebrow">Περιεχόμενα</div>
          <ol>{SECTIONS.map(([id, label]) => <li key={id}><a href={`#${id}`}>{label}</a></li>)}</ol>
        </nav>

        <div className="mg-content">
          <Section id="kritiria" eyebrow="1 · Κριτήρια" title="Τα τέσσερα κριτήρια επιλογής συμβάσεων">
            <p>Η ανάλυση επικεντρώνεται σε δημόσιες συμβάσεις που σχετίζονται με την <strong>πρόληψη, την ετοιμότητα και την αντιμετώπιση δασικών πυρκαγιών</strong>.</p>
            <p>Για τον εντοπισμό των σχετικών συμβάσεων εξετάζονται οι κωδικοί CPV (Κοινό Λεξιλόγιο Δημοσίων Συμβάσεων) και το περιεχόμενο των τίτλων τους. Η διαδικασία βασίζεται σε τέσσερα κριτήρια:</p>
            <div className="mg-criteria">
              <div className="mg-criterion" style={{ borderTopColor: P }}>
                <span className="mg-criterion__num" style={{ color: P }}>1</span>
                <h4>Πρωτεύοντες κωδικοί CPV (Primary CPVs)</h4>
                <p>Κωδικοί που αφορούν αποκλειστικά υπηρεσίες σχετικές με πυρκαγιές.</p>
                <p className="mg-criterion__example">Παράδειγμα: <code>75251120-7</code> Υπηρεσίες καταπολέμησης δασοπυρκαγιών</p>
              </div>
              <div className="mg-criterion" style={{ borderTopColor: S }}>
                <span className="mg-criterion__num" style={{ color: S }}>2</span>
                <h4>Δευτερεύοντες κωδικοί CPV (Secondary CPVs)</h4>
                <p>Κωδικοί που καλύπτουν ευρύτερες κατηγορίες έργων και υπηρεσιών, οι οποίες ενδέχεται να σχετίζονται με την πυροπροστασία.</p>
                <p className="mg-criterion__example">Παράδειγμα: <code>45233141-9</code> Συντήρηση οδών</p>
              </div>
              <div className="mg-criterion" style={{ borderTopColor: K }}>
                <span className="mg-criterion__num" style={{ color: K }}>3</span>
                <h4>Θετικές λέξεις-κλειδιά</h4>
                <p>Όροι στον τίτλο που υποδηλώνουν ότι η σύμβαση σχετίζεται με την πρόληψη ή την αντιμετώπιση πυρκαγιών.</p>
                <p className="mg-criterion__example">Παραδείγματα: πυροπροστασία, αντιπυρική, κατάσβεση.</p>
              </div>
              <div className="mg-criterion" style={{ borderTopColor: N }}>
                <span className="mg-criterion__num" style={{ color: N }}>4</span>
                <h4>Αρνητικές λέξεις-κλειδιά</h4>
                <p>Όροι στον τίτλο, την περιγραφή ή τον φορέα που υποδηλώνουν ότι η σύμβαση αφορά διαφορετικό αντικείμενο και πρέπει να εξαιρεθεί.</p>
                <p className="mg-criterion__example">Παραδείγματα: σχολείο, νοσοκομείο, πυροσβεστήρας.</p>
              </div>
            </div>
          </Section>

          <Section id="vimata" eyebrow="2 · Βήματα" title="Πώς επιλέγουμε τις συμβάσεις: τρία βήματα">
            <div className="mg-legend">
              <span><i className="mg-legend__keep" />κρατάμε</span>
              <span><i className="mg-legend__drop" />πετάμε</span>
              <span>Γκρι φόντο: όλες οι συμβάσεις του μητρώου</span>
            </div>
            <div className="mg-steps">
              <figure className="mg-step">
                <div className="eyebrow">Βήμα 1</div>
                <h4>Εντοπισμός συμβάσεων βάσει CPV</h4>
                <CpvCircles step={1} />
                <figcaption>
                  <p>Αναζητούμε στο μητρώο δημοσίων συμβάσεων όσες διαθέτουν έναν από τους επιλεγμένους κωδικούς <strong>Primary CPV</strong> ή <strong>Secondary CPV</strong>. Συμβάσεις με διαφορετικούς κωδικούς δεν περιλαμβάνονται στην αναζήτηση.</p>
                </figcaption>
              </figure>
              <figure className="mg-step">
                <div className="eyebrow">Βήμα 2</div>
                <h4>Αποκλεισμός άσχετων συμβάσεων</h4>
                <CpvCircles step={2} />
                <figcaption>
                  <p>Ελέγχουμε τον τίτλο, την περιγραφή και τον φορέα κάθε σύμβασης για <strong>αρνητικές λέξεις-κλειδιά</strong>. Εάν εντοπιστεί έστω μία, η σύμβαση αποκλείεται, ανεξάρτητα από τον κωδικό CPV.</p>
                </figcaption>
              </figure>
              <figure className="mg-step">
                <div className="eyebrow">Βήμα 3</div>
                <h4>Έλεγχος συνάφειας των Secondary CPVs</h4>
                <CpvCircles step={3} />
                <figcaption>
                  <p>Οι συμβάσεις με <strong>Primary CPV</strong> διατηρούνται, εφόσον δεν έχουν αποκλειστεί στο προηγούμενο βήμα.</p>
                  <p>Για τις συμβάσεις με <strong>Secondary CPV</strong> εφαρμόζεται ένα επιπλέον κριτήριο: πρέπει να περιλαμβάνουν τουλάχιστον μία <strong>θετική λέξη-κλειδί στον τίτλο</strong>, ώστε να επιβεβαιώνεται η σχέση τους με την πυροπροστασία.</p>
                </figcaption>
              </figure>
            </div>

            <div className="mg-rule">
              <h4>Ο βασικός κανόνας επιλογής</h4>
              <p>Μια σύμβαση περιλαμβάνεται στην ανάλυση όταν διαθέτει <strong style={{ color: P }}>Primary CPV</strong> ή <strong style={{ color: S }}>Secondary CPV</strong> με <strong style={{ color: K }}>θετική λέξη-κλειδί</strong> στον τίτλο, υπό την προϋπόθεση ότι δεν περιέχει καμία <strong style={{ color: N }}>αρνητική λέξη-κλειδί</strong>.</p>
              <p className="mg-math" aria-label="Κρατάμε ίσον, P ένωση με S τομή K, μείον N">Κρατάμε = ( <b style={{ color: P }}>P</b> ∪ ( <b style={{ color: S }}>S</b> ∩ <b style={{ color: K }}>K</b> ) ) ∖ <b style={{ color: N }}>N</b></p>
              <p className="mg-mathkey">Όπου <b style={{ color: P }}>P</b> = Primary CPVs, <b style={{ color: S }}>S</b> = Secondary CPVs, <b style={{ color: K }}>K</b> = θετικές λέξεις-κλειδιά και <b style={{ color: N }}>N</b> = συμβάσεις με αρνητικές λέξεις-κλειδιά.</p>
              <h4>Πρόσθετοι κανόνες</h4>
              <ul className="mg-bullets">
                <li><strong>Οι θετικές λέξεις δεν αρκούν από μόνες τους.</strong> Μια σύμβαση χωρίς επιλεγμένο CPV δεν περιλαμβάνεται στην αναζήτηση, ακόμη κι αν ο τίτλος της περιέχει όρους σχετικούς με την πυροπροστασία.</li>
                <li><strong>Πρόσθετοι αποκλεισμοί:</strong> Εξαιρούνται οι ακυρωμένες συμβάσεις και όσες έχουν ως αναθέτοντα φορέα νοσοκομείο.</li>
              </ul>
            </div>
          </Section>

          <Section id="paradeigmata" eyebrow="3 · Παραδείγματα" title="Παραδείγματα">
            <p>Πώς εφαρμόζεται ο κανόνας σε πέντε τυπικούς τίτλους.</p>
            <div className="mg-table">
              <table>
                <thead><tr><th>Τίτλος</th><th>Κωδικός</th><th>Θετική λέξη</th><th>Αρνητική λέξη</th><th>Αποτέλεσμα</th></tr></thead>
                <tbody>
                  <tr><td>Προληπτικός καθαρισμός βλάστησης οικοπέδων</td><td style={{ color: P }}>Primary</td><td className="mg-muted">όχι</td><td className="mg-muted">όχι</td><td className="mg-keep">✓ κρατάμε</td></tr>
                  <tr><td>Εργασίες συνδρομής στην κατάσβεση πυρκαγιάς</td><td style={{ color: S }}>Secondary</td><td><code>κατασβεσ</code></td><td className="mg-muted">όχι</td><td className="mg-keep">✓ κρατάμε</td></tr>
                  <tr><td>Αποκατάσταση βατότητας δρόμου</td><td style={{ color: S }}>Secondary</td><td className="mg-muted">όχι</td><td className="mg-muted">όχι</td><td className="mg-drop">✕ πετάμε</td></tr>
                  <tr><td>Προμήθεια πυροσβεστικού εξοπλισμού για το δημοτικό γήπεδο</td><td style={{ color: P }}>Primary</td><td className="mg-muted">όχι</td><td><code>γηπεδ</code></td><td className="mg-drop">✕ πετάμε</td></tr>
                  <tr><td>Συντήρηση συστημάτων πυροπροστασίας σχολικών κτιρίων</td><td style={{ color: S }}>Secondary</td><td><code>πυροπροστασ</code></td><td><code>σχολικ</code></td><td className="mg-drop">✕ πετάμε</td></tr>
                </tbody>
              </table>
            </div>
          </Section>

          <Section id="diavgeia" eyebrow="4 · Διαύγεια" title="Αποφάσεις δαπάνης και πληρωμές">
            <p>Οι αποφάσεις δαπάνης και οι πληρωμές των δημόσιων φορέων δημοσιεύονται στη <strong>Διαύγεια</strong>. Η Διαύγεια είναι η επίσημη διαδικτυακή πλατφόρμα στην οποία υπουργεία, δήμοι, περιφέρειες και άλλοι δημόσιοι φορείς δημοσιεύουν υποχρεωτικά τις διοικητικές τους πράξεις. Μεταξύ αυτών περιλαμβάνονται αποφάσεις έγκρισης δαπανών, ανάληψης υποχρεώσεων και πληρωμών. Τα έγγραφα είναι δημόσια προσβάσιμα, επιτρέποντας την παρακολούθηση της διαχείρισης του δημόσιου χρήματος.</p>
            <p>Σε αντίθεση με το ΚΗΜΔΗΣ, στη Διαύγεια δεν υπάρχουν κωδικοί CPV που να επιτρέπουν τον εντοπισμό των σχετικών εγγράφων. Για τον λόγο αυτό, η αναζήτηση βασίζεται αποκλειστικά σε λέξεις-κλειδιά και πραγματοποιείται σε δύο στάδια.</p>
            <div className="mg-split">
              <DiavgeiaCircles />
              <div>
                <h4>1. Εντοπισμός σχετικών αποφάσεων</h4>
                <p>Αρχικά, πραγματοποιούμε ευρεία αναζήτηση χρησιμοποιώντας βασικές ρίζες λέξεων που σχετίζονται με την πυροπροστασία, όπως «πυροπροστ», «αποψιλ», «δασοπροστ» και «αντιπυρ».</p>
                <h4>2. Έλεγχος και επιλογή αποτελεσμάτων</h4>
                <p>Από τα αποτελέσματα της αρχικής αναζήτησης:</p>
                <ul className="mg-bullets">
                  <li><strong>Αποκλείουμε</strong> αποφάσεις που περιέχουν αρνητικές λέξεις-κλειδιά, καθώς και όσες προέρχονται από νοσοκομεία.</li>
                  <li><strong>Διατηρούμε</strong> μόνο τις αποφάσεις που περιλαμβάνουν στο θέμα τους <strong>θετικές λέξεις-κλειδιά</strong>, όπως «δασικών πυρκαγιών», «περιμετρικών ζωνών» ή «πυροφυλακίου».</li>
                </ul>
              </div>
            </div>
            <div className="mg-rule">
              <h4>Ο κανόνας επιλογής στη Διαύγεια</h4>
              <p className="mg-math" aria-label="Κρατάμε ίσον, R τομή K, μείον N">Κρατάμε = ( <b style={{ color: K }}>R</b> ∩ <b style={{ color: KEEP }}>K</b> ) ∖ <b style={{ color: N }}>N</b></p>
              <p className="mg-mathkey">Όπου <b style={{ color: K }}>R</b> = αποφάσεις της ευρείας αναζήτησης με ρίζες λέξεων, <b style={{ color: KEEP }}>K</b> = αποφάσεις με θετική λέξη-κλειδί στο θέμα και <b style={{ color: N }}>N</b> = αποφάσεις με αρνητικές λέξεις-κλειδιά ή από νοσοκομεία.</p>
            </div>
            <h4>Αποφυγή διπλής καταμέτρησης</h4>
            <p>Όταν μια πληρωμή συνδέεται με σύμβαση που έχει ήδη καταγραφεί, συσχετίζουμε τα δύο έγγραφα, ώστε <strong>η ίδια δαπάνη να μην υπολογίζεται δύο φορές</strong>.</p>
          </Section>

          <Section id="dokimi" eyebrow="5 · Δοκιμή" title="Δοκιμάστε έναν τίτλο">
            <p>Γράψτε έναν τίτλο και διαλέξτε τον κωδικό. Το εργαλείο εφαρμόζει τις λίστες και τη μεθοδολογία του FireWatch.</p>
            <TitleTester />
          </Section>

          <Section id="listes" eyebrow="6 · Λίστες" title="Οι λίστες λέξεων-κλειδιών">
            <p>Οι κωδικοί CPV είναι κοινοί για όλες τις χώρες της Ευρωπαϊκής Ένωσης. Αντίθετα, οι λίστες θετικών και αρνητικών λέξεων-κλειδιών πρέπει να προσαρμόζονται στη γλώσσα και στην ορολογία που χρησιμοποιούν οι δημόσιοι φορείς κάθε χώρας.</p>
            <p>Για να εντοπίζουμε όσο το δυνατόν περισσότερες σχετικές συμβάσεις και να περιορίζουμε τα λανθασμένα αποτελέσματα, εφαρμόζουμε τρεις κανόνες:</p>
            <ul className="mg-bullets">
              <li><strong>Αναζήτηση με ρίζες λέξεων:</strong> Αντί για ολόκληρες λέξεις, χρησιμοποιούμε τις ρίζες τους. Για παράδειγμα, η ρίζα «πυροπροστασ» εντοπίζει τόσο την «πυροπροστασία» όσο και την «πυροπροστασίας».</li>
              <li><strong>Ενιαία μορφή κειμένου:</strong> Πριν από κάθε σύγκριση, μετατρέπουμε το κείμενο σε πεζά γράμματα και αφαιρούμε τους τόνους, ώστε διαφορετικές μορφές γραφής να αναγνωρίζονται ως ίδιες.</li>
              <li><strong>Αναγνώριση σύνθετων φράσεων:</strong> Στις αρνητικές λέξεις-κλειδιά επιτρέπουμε έως δύο ενδιάμεσες λέξεις ανάμεσα στους όρους μιας φράσης. Έτσι, η φράση «πυροπροστασ κτιριακ εγκαταστασ» εντοπίζει και την αναφορά «πυροπροστασία των κτιριακών εγκαταστάσεων», ώστε να αποκλείονται συμβάσεις που δεν αφορούν δασικές ή αγροτικές πυρκαγιές.</li>
            </ul>
            <div className="mg-kit">
              <KitBox title="Primary CPVs" color={P} payload={COPY_PAYLOADS.primary}>
                <CpvList items={PRIMARY_CPVS} />
              </KitBox>
              <KitBox title="Secondary CPVs" color={S} payload={COPY_PAYLOADS.secondary}>
                <CpvList items={SECONDARY_CPVS} />
              </KitBox>
              <KitBox title="Θετικές λέξεις" color={K} payload={COPY_PAYLOADS.positive}>
                <p className="mg-note">Για τα Secondary CPVs:</p>
                <Roots items={POSITIVE_KEYWORDS} />
              </KitBox>
              <KitBox title="Αρνητικές λέξεις" color={N} payload={COPY_PAYLOADS.negative}>
                {NEGATIVE_GROUPS.map(([group, items]) => <div key={group} className="mg-neggroup"><h5>{group}</h5><Roots items={items} /></div>)}
              </KitBox>
            </div>
          </Section>

          <Section id="piges" eyebrow="7 · Πηγές" title="Πηγές">
            <div className="mg-table">
              <table>
                <thead><tr><th>Στην Ελλάδα</th><th>Τι δίνει</th><th>Στη χώρα σας</th></tr></thead>
                <tbody>
                  <tr><td><strong>ΚΗΜΔΗΣ</strong></td><td>Συμβάσεις με κωδικό CPV, τίτλο, φορέα, ανάδοχο, ποσό</td><td>Το εθνικό μητρώο δημοσίων συμβάσεων· το TED για τις μεγάλες συμβάσεις</td></tr>
                  <tr><td><strong>Διαύγεια</strong></td><td>Αποφάσεις δαπάνης και πληρωμές</td><td>Πύλη δημοσίευσης αποφάσεων ή πληρωμών, αν υπάρχει</td></tr>
                  <tr><td><strong>Κατανομές υπουργείου</strong></td><td>Χρηματοδότηση πυροπροστασίας ανά δήμο</td><td>Αποφάσεις κατανομής πόρων στην τοπική αυτοδιοίκηση</td></tr>
                  <tr><td><strong>Copernicus EFFIS</strong></td><td>Καμένες εκτάσεις</td><td>Το ίδιο, καλύπτει όλη την Ευρώπη</td></tr>
                  <tr><td><strong>NASA FIRMS</strong></td><td>Ενεργές εστίες από δορυφόρο</td><td>Το ίδιο, παγκόσμια κάλυψη</td></tr>
                  <tr><td><strong>ΓΕΜΗ</strong></td><td>Στοιχεία αναδόχων</td><td>Εθνικό μητρώο επιχειρήσεων, OpenCorporates</td></tr>
                </tbody>
              </table>
            </div>
          </Section>

          <Section id="pyrkagies" eyebrow="8 · Πυρκαγιές" title="Δεδομένα πυρκαγιών">
            <p>Για την καταγραφή και ανάλυση των πυρκαγιών χρησιμοποιούμε δύο ανοιχτές πηγές δεδομένων: το ευρωπαϊκό σύστημα <strong>Copernicus EFFIS</strong> και το <strong>NASA FIRMS</strong>. Και οι δύο παρέχουν δεδομένα για τις ευρωπαϊκές χώρες, χωρίς να απαιτούνται αλλαγές στη βασική μεθοδολογία.</p>
            <p>Τα παραδείγματα κώδικα είναι γραμμένα σε Python και χρησιμοποιούν τις βιβλιοθήκες <code>requests</code> και <code>pandas</code>. Για τη γεωγραφική αντιστοίχιση των πυρκαγιών με τα διοικητικά όρια των δήμων χρησιμοποιείται επιπλέον η <code>geopandas</code>.</p>

            <h4 className="mg-midtitle">Copernicus EFFIS: Καμένες εκτάσεις</h4>
            <p>Το <strong>Ευρωπαϊκό Σύστημα Πληροφοριών για τις Δασικές Πυρκαγιές (EFFIS)</strong> παρέχει γεωγραφικά δεδομένα για τις καταγεγραμμένες πυρκαγιές. Σε αυτά περιλαμβάνονται η ημερομηνία εκδήλωσης, η καμένη έκταση σε εκτάρια, τα γεωγραφικά όρια της πυρκαγιάς, η περιοχή, οι κατηγορίες βλάστησης που επηρεάστηκαν και το ποσοστό της καμένης έκτασης που βρίσκεται εντός περιοχών Natura 2000.</p>
            <p>Η πρόσβαση στα δεδομένα μέσω του API δεν απαιτεί κλειδί.</p>
            <CodeBlock code={CODE_EFFIS} label="Python · EFFIS" />
            <p><strong>Σημεία προσοχής:</strong></p>
            <ul className="mg-bullets">
              <li><strong>Αναθεωρήσεις δεδομένων:</strong> Το EFFIS μπορεί να διορθώνει τα όρια και τις εκτιμήσεις των καμένων εκτάσεων μετά την αρχική καταγραφή. Οι αναθεωρήσεις ενδέχεται να αλλάζουν και το αναγνωριστικό μιας πυρκαγιάς. Για τον λόγο αυτό, σε κάθε ενημέρωση ανακτούμε εκ νέου τα δεδομένα ολόκληρης της εξεταζόμενης περιόδου και δεν βασιζόμαστε αποκλειστικά στην ημερομηνία τελευταίας τροποποίησης.</li>
            </ul>

            <h4 className="mg-midtitle">NASA FIRMS: Δορυφορικές ανιχνεύσεις θερμικών ανωμαλιών</h4>
            <p>Το <strong>Fire Information for Resource Management System (FIRMS) της NASA</strong> παρέχει δεδομένα από δορυφορικούς αισθητήρες που εντοπίζουν θερμικές ανωμαλίες στην επιφάνεια της Γης. Τα δεδομένα διατίθενται συνήθως λίγες ώρες μετά τη διέλευση του δορυφόρου και επιτρέπουν την παρακολούθηση πιθανών ενεργών εστιών.</p>
            <p>Για την πρόσβαση στο API απαιτείται δωρεάν κλειδί (<code>MAP_KEY</code>).</p>
            <CodeBlock code={CODE_FIRMS} label="Python · FIRMS" />
            <p><strong>Σημεία προσοχής:</strong></p>
            <ul className="mg-bullets">
              <li><strong>Θερμική ανωμαλία δεν σημαίνει απαραίτητα πυρκαγιά:</strong> Οι δορυφόροι μπορούν να εντοπίσουν και άλλες πηγές θερμότητας, όπως βιομηχανικές εγκαταστάσεις, διυλιστήρια και χωματερές. Για τον περιορισμό των λανθασμένων αποτελεσμάτων, εξαιρούμε γνωστές σταθερές πηγές θερμότητας.</li>
              <li><strong>Επίπεδο εμπιστοσύνης:</strong> Η μεταβλητή <code>confidence</code> εκφράζει την αξιοπιστία της ανίχνευσης. Στα δεδομένα VIIRS χρησιμοποιούνται οι κατηγορίες χαμηλής, ονομαστικής και υψηλής εμπιστοσύνης (<code>l</code>, <code>n</code>, <code>h</code>), ενώ στα MODIS χρησιμοποιείται αριθμητική κλίμακα από 0 έως 100.</li>
              <li><strong>Ώρα καταγραφής:</strong> Τα πεδία <code>acq_date</code> και <code>acq_time</code> αναφέρονται στην ώρα UTC και πρέπει να μετατρέπονται στην τοπική ώρα για τη χρονική ανάλυση των πυρκαγιών.</li>
            </ul>

            <h4 className="mg-midtitle">Αντιστοίχιση πυρκαγιών με δήμους</h4>
            <p>Για να συγκρίνουμε τις δημόσιες δαπάνες πυροπροστασίας με τις πυρκαγιές που εκδηλώνονται σε μια περιοχή, χρειάζεται να συσχετίσουμε τα γεωγραφικά δεδομένα των καμένων εκτάσεων με τα διοικητικά όρια των δήμων.</p>
            <p>Επειδή μια πυρκαγιά μπορεί να επεκτείνεται σε περισσότερους από έναν δήμους, υπολογίζουμε <strong>το τμήμα της καμένης έκτασης που αντιστοιχεί σε κάθε δήμο</strong>, αντί να αποδίδουμε ολόκληρη την πυρκαγιά σε μία μόνο διοικητική ενότητα.</p>
            <p>Για τους γεωγραφικούς υπολογισμούς χρησιμοποιούμε την προβολή <strong>EPSG:3035</strong>, η οποία είναι κατάλληλη για τον υπολογισμό και τη σύγκριση εκτάσεων σε ευρωπαϊκό επίπεδο.</p>
            <CodeBlock code={CODE_JOIN} label="Python · geopandas" />
          </Section>

          <Section id="erotimata" eyebrow="9 · Ερωτήματα" title="Ερευνητικά ερωτήματα">
            <p>Τα δεδομένα των δημόσιων συμβάσεων, των πληρωμών και της χρηματοδότησης της πυροπροστασίας επιτρέπουν τη διερεύνηση μιας σειράς ερωτημάτων σχετικά με τη διαχείριση των δημόσιων πόρων. Παράλληλα, υπάρχουν ζητήματα που δεν μπορούν να απαντηθούν με αξιοπιστία μόνο από τα διαθέσιμα στοιχεία.</p>
            <div className="mg-questions">
              {QUESTIONS.map((item, index) => (
                <article key={item.title} className="mg-question">
                  <h4>{index + 1}. {item.title}</h4>
                  <dl>
                    <dt>Ερευνητικό ερώτημα</dt><dd>{item.question}</dd>
                    <dt>{item.methodLabel ?? 'Μεθοδολογία'}</dt><dd>{item.method}</dd>
                    {item.limits && <><dt>Περιορισμοί</dt><dd>{item.limits}</dd></>}
                  </dl>
                  {item.note && <p>{item.note}</p>}
                </article>
              ))}
            </div>
          </Section>

          <Section id="pagides" eyebrow="10 · Παγίδες" title="Παγίδες και περιορισμοί των δεδομένων">
            <p>Η ανάλυση των δημόσιων συμβάσεων και πληρωμών απαιτεί ιδιαίτερη προσοχή, καθώς τα διαθέσιμα στοιχεία μπορεί να περιέχουν διπλές εγγραφές, ασυνέπειες και σφάλματα.</p>
            <ul className="mg-bullets">
              <li><strong>Διπλή καταμέτρηση:</strong> Η ίδια δαπάνη μπορεί να εμφανίζεται τόσο ως σύμβαση στο ΚΗΜΔΗΣ όσο και ως πληρωμή στη Διαύγεια. Οι σχετικές εγγραφές πρέπει να συσχετίζονται, ώστε να αποφεύγεται η διπλή καταμέτρηση.</li>
              <li><strong>Τροποποιήσεις συμβάσεων:</strong> Μια σύμβαση μπορεί να τροποποιηθεί και να αποκτήσει νέο αριθμό καταχώρισης. Για να μην υπολογίζεται περισσότερες φορές, λαμβάνουμε υπόψη την τελευταία τροποποιημένη έκδοσή της.</li>
              <li><strong>Φόρος Προστιθέμενης Αξίας (ΦΠΑ):</strong> Τα ποσά δεν καταχωρίζονται πάντα με τον ίδιο τρόπο. Για να είναι συγκρίσιμα, πρέπει να χρησιμοποιείται ενιαία βάση υπολογισμού, με ή χωρίς ΦΠΑ.</li>
              <li><strong>Σφάλματα στα ποσά:</strong> Οι καταχωρίσεις ενδέχεται να περιέχουν λανθασμένες αξίες. Ιδιαίτερα για τις συμβάσεις με τα μεγαλύτερα ποσά, απαιτείται διασταύρωση με τα πρωτότυπα έγγραφα.</li>
              <li><strong>Μισθοδοσία προσωπικού:</strong> Πληρωμές που καταγράφονται ως δαπάνες πυροπροστασίας μπορεί να αφορούν αποδοχές εποχικού προσωπικού και όχι προμήθειες ή έργα. Η διάκριση αυτή είναι απαραίτητη για τη σωστή ερμηνεία των δαπανών.</li>
              <li><strong>Διαφορετικές ονομασίες φορέων:</strong> Ο ίδιος δημόσιος φορέας μπορεί να εμφανίζεται με διαφορετικές επωνυμίες ή συντομογραφίες. Απαιτείται ενοποίηση των καταχωρίσεων, ώστε οι δαπάνες του να υπολογίζονται συγκεντρωτικά.</li>
              <li><strong>Περιορισμοί της αυτοματοποιημένης ταξινόμησης:</strong> Κάθε κανόνας επιλογής ή αποκλεισμού μπορεί να οδηγήσει είτε στην παράλειψη σχετικών συμβάσεων είτε στη συμπερίληψη άσχετων. Για τον λόγο αυτό, πραγματοποιούμε δειγματοληπτικούς ελέγχους τόσο στις συμβάσεις που διατηρούνται όσο και σε εκείνες που αποκλείονται.</li>
            </ul>
            <p className="mg-caveat">Ο κώδικας και η αναλυτική μεθοδολογία: <ExternalLink href="https://github.com/troboukis/2026_fire_protection">GitHub</ExternalLink> · <ExternalLink href="https://github.com/troboukis/2026_fire_protection/blob/main/Methodology.md">Methodology.md</ExternalLink></p>
          </Section>
        </div>
      </div>
    </article>
  )
}
