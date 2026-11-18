import { useState, type ReactNode } from 'react'
import { useOverviewUi } from '../../../i18n/overview'
import { guideCode } from './guideCode'
import {
  CODE_EFFIS, CODE_FIRMS, CODE_JOIN, COPY_PAYLOADS, NEGATIVE_GROUPS, POSITIVE_KEYWORDS,
  PRIMARY_CPVS, SECONDARY_CPVS, TESTER_PRESETS, evaluateTitle, type TesterCpv,
} from './guideData'

// Only editorial copy is translated. Source titles, matching roots and tester state stay unchanged.

function Section({ id, eyebrow, title, children }: { id: string; eyebrow: string; title: string; children: ReactNode }) {
  const ui = useOverviewUi()
  const label = ui(eyebrow)
  const numberedLabel = ui.language === 'en'
    ? label.replace(/^\d+/, number => String(Number(number) > 5 ? Number(number) - 1 : Number(number)))
    : label
  return (
    <section id={id} className="mg-section" aria-labelledby={`${id}-title`}>
      <header className="mg-section__head">
        <div className="eyebrow">{numberedLabel}</div>
        <h3 id={`${id}-title`}>{ui(title)}</h3>
      </header>
      <div className="mg-section__body">{children}</div>
    </section>
  )
}

function ExternalLink({ href, children }: { href: string; children: ReactNode }) {
  return <a href={href} target="_blank" rel="noopener noreferrer">{children}</a>
}

function CopyButton({ text }: { text: string }) {
  const ui = useOverviewUi()
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
      {state === 'done' ? ui("Αντιγράφηκε") : state === 'failed' ? ui("Δεν αντιγράφηκε") : ui("Αντιγραφή")}
    </button>
  )
}

function CodeBlock({ code, label }: { code: string; label: string }) {
  const ui = useOverviewUi()
  return (
    <figure className="mg-code">
      <figcaption><span>{label}</span><CopyButton text={guideCode(code, ui)} /></figcaption>
      <pre><code>{guideCode(code, ui)}</code></pre>
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
  const ui = useOverviewUi()
  return (
    <svg viewBox="0 0 360 280" className="mg-venn" role="img" aria-label={[
      ui("Δύο επικαλυπτόμενοι κύκλοι, Primary CPVs και Secondary CPVs, και οι δύο σημειωμένοι ως κρατάμε"),
      ui("Ο κύκλος των αρνητικών λέξεων κόβει και τους δύο κύκλους CPV, και το κομμάτι που επικαλύπτει πετιέται"),
      ui("Από τα Secondary CPVs μένει μόνο η τομή με τις θετικές λέξεις. Τα Primary CPVs μένουν όλα, εκτός από τις αρνητικές λέξεις"),
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
      {step > 1 && <Label x={175} y={28} color={N} lines={[ui("Αρνητικές λέξεις")]} />}
      <Label x={62} y={146} color={P} lines={['Primary', 'CPVs']} anchor="end" />
      <Label x={289} y={step === 3 ? 126 : 146} color={S} lines={['Secondary', 'CPVs']} anchor="start" />
      {step === 3 && <Label x={320} y={246} color={K} lines={[ui("Θετικές"), ui("λέξεις")]} />}
    </svg>
  )
}

function DiavgeiaCircles() {
  const ui = useOverviewUi()
  return (
    <svg viewBox="0 0 300 260" className="mg-venn" role="img" aria-label={ui("Μεγάλος κύκλος ευρείας αναζήτησης. Μέσα του μικρότερος κύκλος με θετικές λέξεις στο θέμα, που κρατάμε. Ο κύκλος των αρνητικών λέξεων τέμνει και τους δύο και κόβει ό,τι πιάνει.")}>
      <rect x="1" y="1" width="298" height="258" className="mg-venn__universe" />
      <circle cx="135" cy="145" r="92" fill="url(#mg-hatch)" />
      <circle cx="135" cy="168" r="57" className="mg-venn__keep" />
      <circle cx="198" cy="125" r="44" fill="url(#mg-hatch)" />
      <circle cx="135" cy="145" r="92" fill="none" stroke={K} strokeWidth="2.5" />
      <circle cx="135" cy="168" r="57" fill="none" stroke={KEEP} strokeWidth="2.5" />
      <circle cx="198" cy="125" r="44" fill="none" stroke={N} strokeWidth="2.5" strokeDasharray="6 4" />
      <Label x={14} y={34} color={K} lines={[ui("Ευρεία αναζήτηση")]} anchor="start" />
      <Label x={292} y={56} color={N} lines={[ui("Αρνητικές λέξεις")]} anchor="end" />
      <Label x={130} y={190} color={KEEP} lines={[ui("Θετικές λέξεις"), ui("στο θέμα")]} />
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
  const ui = useOverviewUi()
  return (
    <li className={`mg-check mg-check--${state}`}>
      <span className="mg-check__icon" aria-hidden="true">{state === 'pass' ? '✓' : state === 'fail' ? '✕' : '–'}</span>
      <div>
        <div className="mg-check__title">{ui("Βήμα ")}{number} · {title}</div>
        <p>{children}</p>
      </div>
    </li>
  )
}

function TitleTester() {
  const ui = useOverviewUi()
  const [title, setTitle] = useState(TESTER_PRESETS[1].title)
  const [organization, setOrganization] = useState('')
  const [cpv, setCpv] = useState<TesterCpv>(TESTER_PRESETS[1].cpv)
  const result = evaluateTitle({ title, organization, cpv })
  const quoted = (items: string[]) => items.map((item, index) => <span key={item}>{index > 0 && ', '}«<mark className="mg-mark" lang="el">{item}</mark>»</span>)
  const cpvLabel = ui(CPV_OPTIONS.find(option => option.value === cpv)!.label)

  const step1: StepState = cpv === 'none' ? 'fail' : 'pass'
  const step2: StepState = result.droppedAt === 1 ? 'skip' : result.negativeHits.length ? 'fail' : 'pass'
  const step3: StepState = result.droppedAt === 1 || result.droppedAt === 2 ? 'skip' : result.droppedAt === 3 ? 'fail' : 'pass'

  return (
    <div className="mg-tester">
      <div className="mg-tester__form">
        <div className="mg-field">
          <span>{ui("Δοκιμάστε ένα παράδειγμα")}</span>
          <div className="mg-presets">
            {TESTER_PRESETS.map(preset => <button key={preset.label} type="button" onClick={() => { setTitle(preset.title); setOrganization(''); setCpv(preset.cpv) }}>{ui(preset.label)}</button>)}
          </div>
        </div>
        <label className="mg-field">
          <span>{ui("Τίτλος σύμβασης")}</span>
          <textarea lang="el" value={title} onChange={event => setTitle(event.target.value)} rows={3} />
        </label>
        <fieldset className="mg-field mg-cpv">
          <legend>{ui("Τι κωδικό CPV έχει η σύμβαση;")}</legend>
          {CPV_OPTIONS.map(option => (
            <label key={option.value}>
              <input type="radio" name="mg-cpv" value={option.value} checked={cpv === option.value} onChange={() => setCpv(option.value)} />
              <span><strong>{ui(option.label)}</strong> <small>{ui(option.hint)}</small></span>
            </label>
          ))}
        </fieldset>
        <label className="mg-field">
          <span>{ui("Αναθέτων φορέας (προαιρετικό)")}</span>
          <input type="text" lang="el" value={organization} onChange={event => setOrganization(event.target.value)} placeholder={ui("π.χ. Δήμος Μαραθώνα")} />
        </label>
      </div>
      <div className="mg-tester__result" aria-live="polite">
        <div className={`mg-verdict mg-verdict--${result.verdict}`}>
          {result.verdict === 'keep' ? ui("✓ Η σύμβαση περιλαμβάνεται στην ανάλυση") : ui("✕ Η σύμβαση δεν περιλαμβάνεται στην ανάλυση")}
        </div>
        <ol className="mg-checks-result">
          <TesterStep number={1} title={ui("Κωδικός CPV")} state={step1}>
            {cpv === 'none' ? ui("Ο κωδικός δεν είναι στις λίστες μας, οπότε η σύμβαση δεν μπαίνει καν στην αναζήτηση.") : <>{ui("Έχει ")}{cpvLabel}{ui(", οπότε η σύμβαση μπαίνει στην αναζήτηση.")}</>}
          </TesterStep>
          <TesterStep number={2} title={ui("Αρνητικές λέξεις")} state={step2}>
            {step2 === 'skip' ? ui("Δεν εξετάζεται.") : result.negativeHits.length
              ? <>{ui("Βρέθηκε αρνητική λέξη: ")}{result.negativeHits.slice(0, 3).map((hit, index) => <span key={`${hit.field}-${hit.words}`}>{index > 0 && ', '}«<mark className="mg-mark mg-mark--negative" lang="el">{hit.words}</mark>» {hit.field === 'title' ? ui("στον τίτλο") : ui("στον φορέα")}</span>)}{ui(". Η σύμβαση αποκλείεται.")}</>
              : ui("Δεν βρέθηκε καμία αρνητική λέξη.")}
          </TesterStep>
          <TesterStep number={3} title={ui("Θετικές λέξεις")} state={step3}>
            {step3 === 'skip' ? ui("Δεν εξετάζεται.")
              : !result.requiresPositive ? ui("Με Primary CPV δεν χρειάζεται θετική λέξη.")
                : result.positiveHits.length ? <>{ui("Βρέθηκε θετική λέξη στον τίτλο: ")}{quoted(result.positiveHits)}.</>
                  : <>{ui.language === 'el' && ui("Το ")}{cpvLabel}{ui(" χρειάζεται θετική λέξη στον τίτλο, αλλά δεν βρέθηκε. Η σύμβαση αποκλείεται.")}</>}
          </TesterStep>
        </ol>
      </div>
    </div>
  )
}

function CpvList({ items }: { items: Record<string, string> }) {
  const ui = useOverviewUi()
  return (
    <ul className="mg-cpvlist">
      {Object.entries(items).map(([code, label]) => <li key={code}><code>{code}</code><span>{ui.cpv(code, ui(label))}</span></li>)}
    </ul>
  )
}

function Roots({ items }: { items: string[] }) {
  return <ul className="mg-roots">{items.map(item => <li key={item}><code lang="el">{item}</code></li>)}</ul>
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
  const ui = useOverviewUi()
  return (
    <article className="mg-root" lang={ui.language}>
      <VennDefs />
      <header className="section-head mg-hero">
        <div className="eyebrow">{ui("Οδηγός")}</div>
        <h2>{ui("Η μεθοδολογία συλλογής δημόσιων δεδομένων για τις δασικές πυρκαγιές")}</h2>
        <p className="mg-subtitle">{ui("Ένας οδηγός για δημοσιογράφους και μη που θέλουν να ερευήσουν τις δημόσιες συμβάσεις για την πρόληψη και αντιμετώπιση δασικών πυρκαγιών.")}</p>
      </header>

      <div className="mg-layout">
        <nav className="mg-toc" aria-label={ui("Περιεχόμενα οδηγού")}>
          <div className="eyebrow">{ui("Περιεχόμενα")}</div>
          <ol>{SECTIONS.filter(([id]) => ui.language === 'el' || id !== 'dokimi').map(([id, label]) => <li key={id}><a href={`#${id}`}>{ui(label)}</a></li>)}</ol>
        </nav>

        <div className="mg-content">
          <Section id="kritiria" eyebrow={ui("1 · Κριτήρια")} title={ui("Τα τέσσερα κριτήρια επιλογής συμβάσεων")}>
            <p>{ui("Η ανάλυση επικεντρώνεται σε δημόσιες συμβάσεις που σχετίζονται με την ")}<strong>{ui("πρόληψη, την ετοιμότητα και την αντιμετώπιση δασικών πυρκαγιών")}</strong>.</p>
            <p>{ui("Για τον εντοπισμό των σχετικών συμβάσεων εξετάζονται οι κωδικοί CPV (Κοινό Λεξιλόγιο Δημοσίων Συμβάσεων) και το περιεχόμενο των τίτλων τους. Η διαδικασία βασίζεται σε τέσσερα κριτήρια:")}</p>
            <div className="mg-criteria">
              <div className="mg-criterion" style={{ borderTopColor: P }}>
                <span className="mg-criterion__num" style={{ color: P }}>1</span>
                <h4>{ui("Πρωτεύοντες κωδικοί CPV (Primary CPVs)")}</h4>
                <p>{ui("Κωδικοί που αφορούν αποκλειστικά υπηρεσίες σχετικές με πυρκαγιές.")}</p>
                <p className="mg-criterion__example">{ui("Παράδειγμα: ")}<code>75251120-7</code>{' '}{ui.cpv('75251120-7', PRIMARY_CPVS['75251120-7'])}</p>
              </div>
              <div className="mg-criterion" style={{ borderTopColor: S }}>
                <span className="mg-criterion__num" style={{ color: S }}>2</span>
                <h4>{ui("Δευτερεύοντες κωδικοί CPV (Secondary CPVs)")}</h4>
                <p>{ui("Κωδικοί που καλύπτουν ευρύτερες κατηγορίες έργων και υπηρεσιών, οι οποίες ενδέχεται να σχετίζονται με την πυροπροστασία.")}</p>
                <p className="mg-criterion__example">{ui("Παράδειγμα: ")}<code>45233141-9</code>{' '}{ui.cpv('45233141-9', SECONDARY_CPVS['45233141-9'])}</p>
              </div>
              <div className="mg-criterion" style={{ borderTopColor: K }}>
                <span className="mg-criterion__num" style={{ color: K }}>3</span>
                <h4>{ui("Θετικές λέξεις-κλειδιά")}</h4>
                <p>{ui("Όροι στον τίτλο που υποδηλώνουν ότι η σύμβαση σχετίζεται με την πρόληψη ή την αντιμετώπιση πυρκαγιών.")}</p>
                <p className="mg-criterion__example">{ui("Παραδείγματα: ")}<span lang="el">πυροπροστασία, αντιπυρική, κατάσβεση.</span></p>
              </div>
              <div className="mg-criterion" style={{ borderTopColor: N }}>
                <span className="mg-criterion__num" style={{ color: N }}>4</span>
                <h4>{ui("Αρνητικές λέξεις-κλειδιά")}</h4>
                <p>{ui("Όροι στον τίτλο, την περιγραφή ή τον φορέα που υποδηλώνουν ότι η σύμβαση αφορά διαφορετικό αντικείμενο και πρέπει να εξαιρεθεί.")}</p>
                <p className="mg-criterion__example">{ui("Παραδείγματα: ")}<span lang="el">σχολείο, νοσοκομείο, πυροσβεστήρας.</span></p>
              </div>
            </div>
          </Section>

          <Section id="vimata" eyebrow={ui("2 · Βήματα")} title={ui("Πώς επιλέγουμε τις συμβάσεις: τρία βήματα")}>
            <div className="mg-legend">
              <span><i className="mg-legend__keep" />{ui("κρατάμε")}</span>
              <span><i className="mg-legend__drop" />{ui("πετάμε")}</span>
              <span>{ui("Γκρι φόντο: όλες οι συμβάσεις του μητρώου")}</span>
            </div>
            <div className="mg-steps">
              <figure className="mg-step">
                <div className="eyebrow">{ui("Βήμα 1")}</div>
                <h4>{ui("Εντοπισμός συμβάσεων βάσει CPV")}</h4>
                <CpvCircles step={1} />
                <figcaption>
                  <p>{ui("Αναζητούμε στο μητρώο δημοσίων συμβάσεων όσες διαθέτουν έναν από τους επιλεγμένους κωδικούς ")}<strong>Primary CPV</strong>{ui(" ή ")}<strong>Secondary CPV</strong>{ui(". Συμβάσεις με διαφορετικούς κωδικούς δεν περιλαμβάνονται στην αναζήτηση.")}</p>
                </figcaption>
              </figure>
              <figure className="mg-step">
                <div className="eyebrow">{ui("Βήμα 2")}</div>
                <h4>{ui("Αποκλεισμός άσχετων συμβάσεων")}</h4>
                <CpvCircles step={2} />
                <figcaption>
                  <p>{ui("Ελέγχουμε τον τίτλο, την περιγραφή και τον φορέα κάθε σύμβασης για ")}<strong>{ui("αρνητικές λέξεις-κλειδιά")}</strong>{ui(". Εάν εντοπιστεί έστω μία, η σύμβαση αποκλείεται, ανεξάρτητα από τον κωδικό CPV.")}</p>
                </figcaption>
              </figure>
              <figure className="mg-step">
                <div className="eyebrow">{ui("Βήμα 3")}</div>
                <h4>{ui("Έλεγχος συνάφειας των Secondary CPVs")}</h4>
                <CpvCircles step={3} />
                <figcaption>
                  <p>{ui("Οι συμβάσεις με ")}<strong>Primary CPV</strong>{ui(" διατηρούνται, εφόσον δεν έχουν αποκλειστεί στο προηγούμενο βήμα.")}</p>
                  <p>{ui("Για τις συμβάσεις με ")}<strong>Secondary CPV</strong>{ui(" εφαρμόζεται ένα επιπλέον κριτήριο: πρέπει να περιλαμβάνουν τουλάχιστον μία ")}<strong>{ui("θετική λέξη-κλειδί στον τίτλο")}</strong>{ui(", ώστε να επιβεβαιώνεται η σχέση τους με την πυροπροστασία.")}</p>
                </figcaption>
              </figure>
            </div>

            <div className="mg-rule">
              <h4>{ui("Ο βασικός κανόνας επιλογής")}</h4>
              <p>{ui("Μια σύμβαση περιλαμβάνεται στην ανάλυση όταν διαθέτει ")}<strong style={{ color: P }}>Primary CPV</strong>{ui(" ή ")}<strong style={{ color: S }}>Secondary CPV</strong>{ui(" με ")}<strong style={{ color: K }}>{ui("θετική λέξη-κλειδί")}</strong>{ui(" στον τίτλο, υπό την προϋπόθεση ότι δεν περιέχει καμία ")}<strong style={{ color: N }}>{ui("αρνητική λέξη-κλειδί")}</strong>.</p>
              <p className="mg-math" aria-label={ui("Κρατάμε ίσον, P ένωση με S τομή K, μείον N")}>{ui("Κρατάμε = ( ")}<b style={{ color: P }}>P</b> ∪ ( <b style={{ color: S }}>S</b> ∩ <b style={{ color: K }}>K</b> ) ) ∖ <b style={{ color: N }}>N</b></p>
              <p className="mg-mathkey">{ui("Όπου ")}<b style={{ color: P }}>P</b> = Primary CPVs, <b style={{ color: S }}>S</b> = Secondary CPVs, <b style={{ color: K }}>K</b>{ui(" = θετικές λέξεις-κλειδιά και ")}<b style={{ color: N }}>N</b>{ui(" = συμβάσεις με αρνητικές λέξεις-κλειδιά.")}</p>
              <h4>{ui("Πρόσθετοι κανόνες")}</h4>
              <ul className="mg-bullets">
                <li><strong>{ui("Οι θετικές λέξεις δεν αρκούν από μόνες τους.")}</strong>{ui(" Μια σύμβαση χωρίς επιλεγμένο CPV δεν περιλαμβάνεται στην αναζήτηση, ακόμη κι αν ο τίτλος της περιέχει όρους σχετικούς με την πυροπροστασία.")}</li>
                <li><strong>{ui("Πρόσθετοι αποκλεισμοί:")}</strong>{ui(" Εξαιρούνται οι ακυρωμένες συμβάσεις και όσες έχουν ως αναθέτοντα φορέα νοσοκομείο.")}</li>
              </ul>
            </div>
          </Section>

          <Section id="paradeigmata" eyebrow={ui("3 · Παραδείγματα")} title={ui("Παραδείγματα")}>
            <p>{ui("Πώς εφαρμόζεται ο κανόνας σε πέντε τυπικούς τίτλους.")}</p>
            <div className="mg-table">
              <table>
                <thead><tr><th>{ui("Τίτλος")}</th><th>{ui("Κωδικός")}</th><th>{ui("Θετική λέξη")}</th><th>{ui("Αρνητική λέξη")}</th><th>{ui("Αποτέλεσμα")}</th></tr></thead>
                <tbody>
                  <tr><td lang="el">Προληπτικός καθαρισμός βλάστησης οικοπέδων</td><td style={{ color: P }}>Primary</td><td className="mg-muted">{ui("όχι")}</td><td className="mg-muted">{ui("όχι")}</td><td className="mg-keep">{ui("✓ κρατάμε")}</td></tr>
                  <tr><td lang="el">Εργασίες συνδρομής στην κατάσβεση πυρκαγιάς</td><td style={{ color: S }}>Secondary</td><td><code lang="el">κατασβεσ</code></td><td className="mg-muted">{ui("όχι")}</td><td className="mg-keep">{ui("✓ κρατάμε")}</td></tr>
                  <tr><td lang="el">Αποκατάσταση βατότητας δρόμου</td><td style={{ color: S }}>Secondary</td><td className="mg-muted">{ui("όχι")}</td><td className="mg-muted">{ui("όχι")}</td><td className="mg-drop">{ui("✕ πετάμε")}</td></tr>
                  <tr><td lang="el">Προμήθεια πυροσβεστικού εξοπλισμού για το δημοτικό γήπεδο</td><td style={{ color: P }}>Primary</td><td className="mg-muted">{ui("όχι")}</td><td><code lang="el">γηπεδ</code></td><td className="mg-drop">{ui("✕ πετάμε")}</td></tr>
                  <tr><td lang="el">Συντήρηση συστημάτων πυροπροστασίας σχολικών κτιρίων</td><td style={{ color: S }}>Secondary</td><td><code lang="el">πυροπροστασ</code></td><td><code lang="el">σχολικ</code></td><td className="mg-drop">{ui("✕ πετάμε")}</td></tr>
                </tbody>
              </table>
            </div>
          </Section>

          <Section id="diavgeia" eyebrow={ui("4 · Διαύγεια")} title={ui("Αποφάσεις δαπάνης και πληρωμές")}>
            <p>{ui("Οι αποφάσεις δαπάνης και οι πληρωμές των δημόσιων φορέων δημοσιεύονται στη ")}<strong>{ui("Διαύγεια")}</strong>{ui(". Η Διαύγεια είναι η επίσημη διαδικτυακή πλατφόρμα στην οποία υπουργεία, δήμοι, περιφέρειες και άλλοι δημόσιοι φορείς δημοσιεύουν υποχρεωτικά τις διοικητικές τους πράξεις. Μεταξύ αυτών περιλαμβάνονται αποφάσεις έγκρισης δαπανών, ανάληψης υποχρεώσεων και πληρωμών. Τα έγγραφα είναι δημόσια προσβάσιμα, επιτρέποντας την παρακολούθηση της διαχείρισης του δημόσιου χρήματος.")}</p>
            <p>{ui("Σε αντίθεση με το ΚΗΜΔΗΣ, στη Διαύγεια δεν υπάρχουν κωδικοί CPV που να επιτρέπουν τον εντοπισμό των σχετικών εγγράφων. Για τον λόγο αυτό, η αναζήτηση βασίζεται αποκλειστικά σε λέξεις-κλειδιά και πραγματοποιείται σε δύο στάδια.")}</p>
            <div className="mg-split">
              <DiavgeiaCircles />
              <div>
                <h4>{ui("1. Εντοπισμός σχετικών αποφάσεων")}</h4>
                <p>{ui("Αρχικά, πραγματοποιούμε ευρεία αναζήτηση χρησιμοποιώντας βασικές ρίζες λέξεων που σχετίζονται με την πυροπροστασία, όπως ")}<span lang="el">«πυροπροστ»</span>, <span lang="el">«αποψιλ»</span>, <span lang="el">«δασοπροστ»</span>{ui(" και ")}<span lang="el">«αντιπυρ»</span>.</p>
                <h4>{ui("2. Έλεγχος και επιλογή αποτελεσμάτων")}</h4>
                <p>{ui("Από τα αποτελέσματα της αρχικής αναζήτησης:")}</p>
                <ul className="mg-bullets">
                  <li><strong>{ui("Αποκλείουμε")}</strong>{ui(" αποφάσεις που περιέχουν αρνητικές λέξεις-κλειδιά, καθώς και όσες προέρχονται από νοσοκομεία.")}</li>
                  <li><strong>{ui("Διατηρούμε")}</strong>{ui(" μόνο τις αποφάσεις που περιλαμβάνουν στο θέμα τους ")}<strong>{ui("θετικές λέξεις-κλειδιά")}</strong>{ui(", όπως ")}<span lang="el">«δασικών πυρκαγιών»</span>, <span lang="el">«περιμετρικών ζωνών»</span>{ui(" ή ")}<span lang="el">«πυροφυλακίου»</span>.</li>
                </ul>
              </div>
            </div>
            <div className="mg-rule">
              <h4>{ui("Ο κανόνας επιλογής στη Διαύγεια")}</h4>
              <p className="mg-math" aria-label={ui("Κρατάμε ίσον, R τομή K, μείον N")}>{ui("Κρατάμε = ( ")}<b style={{ color: K }}>R</b> ∩ <b style={{ color: KEEP }}>K</b> ) ∖ <b style={{ color: N }}>N</b></p>
              <p className="mg-mathkey">{ui("Όπου ")}<b style={{ color: K }}>R</b>{ui(" = αποφάσεις της ευρείας αναζήτησης με ρίζες λέξεων, ")}<b style={{ color: KEEP }}>K</b>{ui(" = αποφάσεις με θετική λέξη-κλειδί στο θέμα και ")}<b style={{ color: N }}>N</b>{ui(" = αποφάσεις με αρνητικές λέξεις-κλειδιά ή από νοσοκομεία.")}</p>
            </div>
            <h4>{ui("Αποφυγή διπλής καταμέτρησης")}</h4>
            <p>{ui("Όταν μια πληρωμή συνδέεται με σύμβαση που έχει ήδη καταγραφεί, συσχετίζουμε τα δύο έγγραφα, ώστε ")}<strong>{ui("η ίδια δαπάνη να μην υπολογίζεται δύο φορές")}</strong>.</p>
          </Section>

          {ui.language === 'el' && <Section id="dokimi" eyebrow={ui("5 · Δοκιμή")} title={ui("Δοκιμάστε έναν τίτλο")}>
            <p>{ui("Γράψτε έναν τίτλο και διαλέξτε τον κωδικό. Το εργαλείο εφαρμόζει τις λίστες και τη μεθοδολογία του FireWatch.")}</p>
            <TitleTester />
          </Section>}

          <Section id="listes" eyebrow={ui("6 · Λίστες")} title={ui("Οι λίστες λέξεων-κλειδιών")}>
            <p>{ui("Οι κωδικοί CPV είναι κοινοί για όλες τις χώρες της Ευρωπαϊκής Ένωσης. Αντίθετα, οι λίστες θετικών και αρνητικών λέξεων-κλειδιών πρέπει να προσαρμόζονται στη γλώσσα και στην ορολογία που χρησιμοποιούν οι δημόσιοι φορείς κάθε χώρας.")}</p>
            <p>{ui("Για να εντοπίζουμε όσο το δυνατόν περισσότερες σχετικές συμβάσεις και να περιορίζουμε τα λανθασμένα αποτελέσματα, εφαρμόζουμε τρεις κανόνες:")}</p>
            <ul className="mg-bullets">
              <li><strong>{ui("Αναζήτηση με ρίζες λέξεων:")}</strong>{ui(" Αντί για ολόκληρες λέξεις, χρησιμοποιούμε τις ρίζες τους. Για παράδειγμα, η ρίζα ")}<span lang="el">«πυροπροστασ»</span>{ui(" εντοπίζει τόσο την ")}<span lang="el">«πυροπροστασία»</span>{ui(" όσο και την ")}<span lang="el">«πυροπροστασίας»</span>.</li>
              <li><strong>{ui("Ενιαία μορφή κειμένου:")}</strong>{ui(" Πριν από κάθε σύγκριση, μετατρέπουμε το κείμενο σε πεζά γράμματα και αφαιρούμε τους τόνους, ώστε διαφορετικές μορφές γραφής να αναγνωρίζονται ως ίδιες.")}</li>
              <li><strong>{ui("Αναγνώριση σύνθετων φράσεων:")}</strong>{ui(" Στις αρνητικές λέξεις-κλειδιά επιτρέπουμε έως δύο ενδιάμεσες λέξεις ανάμεσα στους όρους μιας φράσης. Έτσι, η φράση ")}<span lang="el">«πυροπροστασ κτιριακ εγκαταστασ»</span>{ui(" εντοπίζει και την αναφορά ")}<span lang="el">«πυροπροστασία των κτιριακών εγκαταστάσεων»</span>{ui(", ώστε να αποκλείονται συμβάσεις που δεν αφορούν δασικές ή αγροτικές πυρκαγιές.")}</li>
            </ul>
            <div className="mg-kit">
              <KitBox title="Primary CPVs" color={P} payload={COPY_PAYLOADS.primary}>
                <CpvList items={PRIMARY_CPVS} />
              </KitBox>
              <KitBox title="Secondary CPVs" color={S} payload={COPY_PAYLOADS.secondary}>
                <CpvList items={SECONDARY_CPVS} />
              </KitBox>
              {ui.language === 'el' && <><KitBox title={ui("Θετικές λέξεις")} color={K} payload={COPY_PAYLOADS.positive}>
                <p className="mg-note">{ui("Για τα Secondary CPVs:")}</p>
                <Roots items={POSITIVE_KEYWORDS} />
              </KitBox>
              <KitBox title={ui("Αρνητικές λέξεις")} color={N} payload={COPY_PAYLOADS.negative}>
                {NEGATIVE_GROUPS.map(([group, items]) => <div key={group} className="mg-neggroup"><h5>{ui(group)}</h5><Roots items={items} /></div>)}
              </KitBox></>}
            </div>
          </Section>

          <Section id="piges" eyebrow={ui("7 · Πηγές")} title={ui("Πηγές")}>
            <div className="mg-table">
              <table>
                <thead><tr><th>{ui("Στην Ελλάδα")}</th><th>{ui("Τι δίνει")}</th><th>{ui("Στη χώρα σας")}</th></tr></thead>
                <tbody>
                  <tr><td><strong>{ui("ΚΗΜΔΗΣ")}</strong></td><td>{ui("Συμβάσεις με κωδικό CPV, τίτλο, φορέα, ανάδοχο, ποσό")}</td><td>{ui("Το εθνικό μητρώο δημοσίων συμβάσεων· το TED για τις μεγάλες συμβάσεις")}</td></tr>
                  <tr><td><strong>{ui("Διαύγεια")}</strong></td><td>{ui("Αποφάσεις δαπάνης και πληρωμές")}</td><td>{ui("Πύλη δημοσίευσης αποφάσεων ή πληρωμών, αν υπάρχει")}</td></tr>
                  <tr><td><strong>{ui("Κατανομές υπουργείου")}</strong></td><td>{ui("Χρηματοδότηση πυροπροστασίας ανά δήμο")}</td><td>{ui("Αποφάσεις κατανομής πόρων στην τοπική αυτοδιοίκηση")}</td></tr>
                  <tr><td><strong>Copernicus EFFIS</strong></td><td>{ui("Καμένες εκτάσεις")}</td><td>{ui("Το ίδιο, καλύπτει όλη την Ευρώπη")}</td></tr>
                  <tr><td><strong>NASA FIRMS</strong></td><td>{ui("Ενεργές εστίες από δορυφόρο")}</td><td>{ui("Το ίδιο, παγκόσμια κάλυψη")}</td></tr>
                  <tr><td><strong>{ui("ΓΕΜΗ")}</strong></td><td>{ui("Στοιχεία αναδόχων")}</td><td>{ui("Εθνικό μητρώο επιχειρήσεων, OpenCorporates")}</td></tr>
                </tbody>
              </table>
            </div>
          </Section>

          <Section id="pyrkagies" eyebrow={ui("8 · Πυρκαγιές")} title={ui("Δεδομένα πυρκαγιών")}>
            <p>{ui("Για την καταγραφή και ανάλυση των πυρκαγιών χρησιμοποιούμε δύο ανοιχτές πηγές δεδομένων: το ευρωπαϊκό σύστημα ")}<strong>Copernicus EFFIS</strong>{ui(ui.language === 'el' ? " και το " : " και ")}<strong>NASA FIRMS</strong>{ui(". Και οι δύο παρέχουν δεδομένα για τις ευρωπαϊκές χώρες, χωρίς να απαιτούνται αλλαγές στη βασική μεθοδολογία.")}</p>
            <p>{ui("Τα παραδείγματα κώδικα είναι γραμμένα σε Python και χρησιμοποιούν τις βιβλιοθήκες ")}<code>requests</code>{ui(" και ")}<code>pandas</code>{ui(". Για τη γεωγραφική αντιστοίχιση των πυρκαγιών με τα διοικητικά όρια των δήμων χρησιμοποιείται επιπλέον η ")}<code>geopandas</code>.</p>

            <h4 className="mg-midtitle">{ui("Copernicus EFFIS: Καμένες εκτάσεις")}</h4>
            <p>{ui.language === 'el' && ui("Το ")}<strong>{ui("Ευρωπαϊκό Σύστημα Πληροφοριών για τις Δασικές Πυρκαγιές (EFFIS)")}</strong>{ui(" παρέχει γεωγραφικά δεδομένα για τις καταγεγραμμένες πυρκαγιές. Σε αυτά περιλαμβάνονται η ημερομηνία εκδήλωσης, η καμένη έκταση σε εκτάρια, τα γεωγραφικά όρια της πυρκαγιάς, η περιοχή, οι κατηγορίες βλάστησης που επηρεάστηκαν και το ποσοστό της καμένης έκτασης που βρίσκεται εντός περιοχών Natura 2000.")}</p>
            <p>{ui("Η πρόσβαση στα δεδομένα μέσω του API δεν απαιτεί κλειδί.")}</p>
            <CodeBlock code={CODE_EFFIS} label="Python · EFFIS" />
            <p><strong>{ui("Σημεία προσοχής:")}</strong></p>
            <ul className="mg-bullets">
              <li><strong>{ui("Αναθεωρήσεις δεδομένων:")}</strong>{ui(" Το EFFIS μπορεί να διορθώνει τα όρια και τις εκτιμήσεις των καμένων εκτάσεων μετά την αρχική καταγραφή. Οι αναθεωρήσεις ενδέχεται να αλλάζουν και το αναγνωριστικό μιας πυρκαγιάς. Για τον λόγο αυτό, σε κάθε ενημέρωση ανακτούμε εκ νέου τα δεδομένα ολόκληρης της εξεταζόμενης περιόδου και δεν βασιζόμαστε αποκλειστικά στην ημερομηνία τελευταίας τροποποίησης.")}</li>
            </ul>

            <h4 className="mg-midtitle">{ui("NASA FIRMS: Δορυφορικές ανιχνεύσεις θερμικών ανωμαλιών")}</h4>
            <p>{ui.language === 'el' && ui("Το ")}<strong>{ui("Fire Information for Resource Management System (FIRMS) της NASA")}</strong>{ui(" παρέχει δεδομένα από δορυφορικούς αισθητήρες που εντοπίζουν θερμικές ανωμαλίες στην επιφάνεια της Γης. Τα δεδομένα διατίθενται συνήθως λίγες ώρες μετά τη διέλευση του δορυφόρου και επιτρέπουν την παρακολούθηση πιθανών ενεργών εστιών.")}</p>
            <p>{ui("Για την πρόσβαση στο API απαιτείται δωρεάν κλειδί (")}<code>MAP_KEY</code>).</p>
            <CodeBlock code={CODE_FIRMS} label="Python · FIRMS" />
            <p><strong>{ui("Σημεία προσοχής:")}</strong></p>
            <ul className="mg-bullets">
              <li><strong>{ui("Θερμική ανωμαλία δεν σημαίνει απαραίτητα πυρκαγιά:")}</strong>{ui(" Οι δορυφόροι μπορούν να εντοπίσουν και άλλες πηγές θερμότητας, όπως βιομηχανικές εγκαταστάσεις, διυλιστήρια και χωματερές. Για τον περιορισμό των λανθασμένων αποτελεσμάτων, εξαιρούμε γνωστές σταθερές πηγές θερμότητας.")}</li>
              <li><strong>{ui("Επίπεδο εμπιστοσύνης:")}</strong>{ui(" Η μεταβλητή ")}<code>confidence</code>{ui(" εκφράζει την αξιοπιστία της ανίχνευσης. Στα δεδομένα VIIRS χρησιμοποιούνται οι κατηγορίες χαμηλής, ονομαστικής και υψηλής εμπιστοσύνης (")}<code>l</code>, <code>n</code>, <code>h</code>{ui("), ενώ στα MODIS χρησιμοποιείται αριθμητική κλίμακα από 0 έως 100.")}</li>
              <li><strong>{ui("Ώρα καταγραφής:")}</strong>{ui(" Τα πεδία ")}<code>acq_date</code>{ui(" και ")}<code>acq_time</code>{ui(" αναφέρονται στην ώρα UTC και πρέπει να μετατρέπονται στην τοπική ώρα για τη χρονική ανάλυση των πυρκαγιών.")}</li>
            </ul>

            <h4 className="mg-midtitle">{ui("Αντιστοίχιση πυρκαγιών με δήμους")}</h4>
            <p>{ui("Για να συγκρίνουμε τις δημόσιες δαπάνες πυροπροστασίας με τις πυρκαγιές που εκδηλώνονται σε μια περιοχή, χρειάζεται να συσχετίσουμε τα γεωγραφικά δεδομένα των καμένων εκτάσεων με τα διοικητικά όρια των δήμων.")}</p>
            <p>{ui("Επειδή μια πυρκαγιά μπορεί να επεκτείνεται σε περισσότερους από έναν δήμους, υπολογίζουμε ")}<strong>{ui("το τμήμα της καμένης έκτασης που αντιστοιχεί σε κάθε δήμο")}</strong>{ui(", αντί να αποδίδουμε ολόκληρη την πυρκαγιά σε μία μόνο διοικητική ενότητα.")}</p>
            <p>{ui("Για τους γεωγραφικούς υπολογισμούς χρησιμοποιούμε την προβολή ")}<strong>EPSG:3035</strong>{ui(", η οποία είναι κατάλληλη για τον υπολογισμό και τη σύγκριση εκτάσεων σε ευρωπαϊκό επίπεδο.")}</p>
            <CodeBlock code={CODE_JOIN} label="Python · geopandas" />
          </Section>

          <Section id="erotimata" eyebrow={ui("9 · Ερωτήματα")} title={ui("Ερευνητικά ερωτήματα")}>
            <p>{ui("Τα δεδομένα των δημόσιων συμβάσεων, των πληρωμών και της χρηματοδότησης της πυροπροστασίας επιτρέπουν τη διερεύνηση μιας σειράς ερωτημάτων σχετικά με τη διαχείριση των δημόσιων πόρων. Παράλληλα, υπάρχουν ζητήματα που δεν μπορούν να απαντηθούν με αξιοπιστία μόνο από τα διαθέσιμα στοιχεία.")}</p>
            <div className="mg-questions">
              {QUESTIONS.map((item, index) => (
                <article key={ui(item.title)} className="mg-question">
                  <h4>{index + 1}. {ui(item.title)}</h4>
                  <dl>
                    <dt>{ui("Ερευνητικό ερώτημα")}</dt><dd>{ui(item.question)}</dd>
                    <dt>{ui(item.methodLabel ?? ui("Μεθοδολογία"))}</dt><dd>{ui(item.method)}</dd>
                    {item.limits && <><dt>{ui("Περιορισμοί")}</dt><dd>{ui(item.limits)}</dd></>}
                  </dl>
                  {item.note && <p>{ui(item.note)}</p>}
                </article>
              ))}
            </div>
          </Section>

          <Section id="pagides" eyebrow={ui("10 · Παγίδες")} title={ui("Παγίδες και περιορισμοί των δεδομένων")}>
            <p>{ui("Η ανάλυση των δημόσιων συμβάσεων και πληρωμών απαιτεί ιδιαίτερη προσοχή, καθώς τα διαθέσιμα στοιχεία μπορεί να περιέχουν διπλές εγγραφές, ασυνέπειες και σφάλματα.")}</p>
            <ul className="mg-bullets">
              <li><strong>{ui("Διπλή καταμέτρηση:")}</strong>{ui(" Η ίδια δαπάνη μπορεί να εμφανίζεται τόσο ως σύμβαση στο ΚΗΜΔΗΣ όσο και ως πληρωμή στη Διαύγεια. Οι σχετικές εγγραφές πρέπει να συσχετίζονται, ώστε να αποφεύγεται η διπλή καταμέτρηση.")}</li>
              <li><strong>{ui("Τροποποιήσεις συμβάσεων:")}</strong>{ui(" Μια σύμβαση μπορεί να τροποποιηθεί και να αποκτήσει νέο αριθμό καταχώρισης. Για να μην υπολογίζεται περισσότερες φορές, λαμβάνουμε υπόψη την τελευταία τροποποιημένη έκδοσή της.")}</li>
              <li><strong>{ui("Φόρος Προστιθέμενης Αξίας (ΦΠΑ):")}</strong>{ui(" Τα ποσά δεν καταχωρίζονται πάντα με τον ίδιο τρόπο. Για να είναι συγκρίσιμα, πρέπει να χρησιμοποιείται ενιαία βάση υπολογισμού, με ή χωρίς ΦΠΑ.")}</li>
              <li><strong>{ui("Σφάλματα στα ποσά:")}</strong>{ui(" Οι καταχωρίσεις ενδέχεται να περιέχουν λανθασμένες αξίες. Ιδιαίτερα για τις συμβάσεις με τα μεγαλύτερα ποσά, απαιτείται διασταύρωση με τα πρωτότυπα έγγραφα.")}</li>
              <li><strong>{ui("Μισθοδοσία προσωπικού:")}</strong>{ui(" Πληρωμές που καταγράφονται ως δαπάνες πυροπροστασίας μπορεί να αφορούν αποδοχές εποχικού προσωπικού και όχι προμήθειες ή έργα. Η διάκριση αυτή είναι απαραίτητη για τη σωστή ερμηνεία των δαπανών.")}</li>
              <li><strong>{ui("Διαφορετικές ονομασίες φορέων:")}</strong>{ui(" Ο ίδιος δημόσιος φορέας μπορεί να εμφανίζεται με διαφορετικές επωνυμίες ή συντομογραφίες. Απαιτείται ενοποίηση των καταχωρίσεων, ώστε οι δαπάνες του να υπολογίζονται συγκεντρωτικά.")}</li>
              <li><strong>{ui("Περιορισμοί της αυτοματοποιημένης ταξινόμησης:")}</strong>{ui(" Κάθε κανόνας επιλογής ή αποκλεισμού μπορεί να οδηγήσει είτε στην παράλειψη σχετικών συμβάσεων είτε στη συμπερίληψη άσχετων. Για τον λόγο αυτό, πραγματοποιούμε δειγματοληπτικούς ελέγχους τόσο στις συμβάσεις που διατηρούνται όσο και σε εκείνες που αποκλείονται.")}</li>
            </ul>
            <p className="mg-caveat">{ui("Ο κώδικας και η αναλυτική μεθοδολογία: ")}<ExternalLink href="https://github.com/troboukis/2026_fire_protection">GitHub</ExternalLink> · <ExternalLink href="https://github.com/troboukis/2026_fire_protection/blob/main/Methodology.md">Methodology.md</ExternalLink></p>
          </Section>
        </div>
      </div>
    </article>
  )
}
