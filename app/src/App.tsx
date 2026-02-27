type ProcurementRecord = {
  id: string
  authority: string
  title: string
  amount: string
  stage: string
  cpv: string
  date: string
  supplier: string
}

type Kpi = {
  label: string
  value: string
  note: string
}

const featuredRecords: ProcurementRecord[] = [
  {
    id: 'ΠΥΡ-2026-0142',
    authority: 'ΠΕΡΙΦΕΡΕΙΑ ΑΤΤΙΚΗΣ / ΠΟΛΙΤΙΚΗ ΠΡΟΣΤΑΣΙΑ',
    title: 'Καθαρισμοί δασικών ζωνών και αντιπυρικών λωρίδων, φάση Α',
    amount: '€ 3.200.000',
    stage: 'Ανατέθηκε',
    cpv: 'CPV 77312000-0',
    date: '18 Φεβ 2026',
    supplier: 'Κοινοπραξία Δασικών Έργων ΑΕ',
  },
  {
    id: 'ΠΥΡ-2026-0097',
    authority: 'ΔΗΜΟΣ ΜΑΡΑΘΩΝΑ',
    title: 'Μίσθωση μηχανημάτων έργου για προληπτική αποψίλωση',
    amount: '€ 842.500',
    stage: 'Διαγωνισμός',
    cpv: 'CPV 45500000-2',
    date: '03 Φεβ 2026',
    supplier: 'Ανοικτή διαδικασία',
  },
  {
    id: 'ΠΥΡ-2025-2218',
    authority: 'ΥΠΟΥΡΓΕΙΟ ΚΛΙΜΑΤΙΚΗΣ ΚΡΙΣΗΣ',
    title: 'Προμήθεια δεξαμενών νερού και κινητών μονάδων υποστήριξης',
    amount: '€ 12.400.000',
    stage: 'Ολοκληρώθηκε',
    cpv: 'CPV 44611500-1',
    date: '12 Νοε 2025',
    supplier: 'Helios Emergency Systems',
  },
]

const kpis: Kpi[] = [
  { label: 'Καταγεγραμμένες Αποφάσεις', value: '4,821', note: 'ευρετηριασμένες αποφάσεις προμηθειών' },
  { label: 'Δήμοι', value: '332', note: 'διασταύρωση με δεδομένα έκθεσης σε πυρκαγιές' },
  { label: 'Εκτιμώμενη Δαπάνη', value: '€ 214.7M', note: 'πρόληψη πυρκαγιών + ετοιμότητα' },
  { label: 'Τελευταία Ενημέρωση', value: '25 Φεβ 2026', note: 'συγχρονισμένο στιγμιότυπο δημόσιου αρχείου' },
]

const orgKpis: Kpi[] = [
  { label: 'Συνολική Δαπάνη 2025-2026', value: '€ 24.3M', note: 'περιφερειακές + δημοτικές αποφάσεις' },
  { label: 'Συμβάσεις', value: '118', note: 'μοναδικές αποφάσεις προμηθειών' },
  { label: 'Προμηθευτές', value: '43', note: 'διακριτοί αντισυμβαλλόμενοι' },
  { label: 'Ζώνες Υψηλού Κινδύνου', value: '17', note: 'δήμοι προτεραιότητας παρέμβασης' },
]

const timelineItems = [
  { month: 'ΜΑΡ', year: '2025', text: 'Δημοσίευση κατανομής προϋπολογισμού / έγκριση πακέτου πρόληψης' },
  { month: 'ΜΑΪ', year: '2025', text: 'Άνοιγμα συμβάσεων για διαχείριση καύσιμης ύλης και καθαρισμούς' },
  { month: 'ΙΟΥΛ', year: '2025', text: 'Κορύφωση έκτακτων μισθώσεων στη διάρκεια της αντιπυρικής περιόδου' },
  { month: 'ΝΟΕ', year: '2025', text: 'Καταγραφή αποφάσεων αναπλήρωσης εξοπλισμού μετά την περίοδο' },
  { month: 'ΦΕΒ', year: '2026', text: 'Επανεκκίνηση κύκλου με δημοτικές προμήθειες συντήρησης' },
]

const newsUpdates = [
  {
    slug: 'Κύριο',
    title: 'Οι συμβάσεις καθαρισμών επιταχύνονται πριν από το ανοιξιάτικο παράθυρο υψηλού κινδύνου',
    meta: 'Σύνταξη Αθήνας / 25 Φεβ 2026 / παρατηρητήριο προμηθειών',
  },
  {
    slug: 'Χάρτης',
    title: 'Η συγκέντρωση δαπανών παραμένει σε Αττική, Πελοπόννησο και Στερεά Ελλάδα',
    meta: 'Εθνική κάλυψη / γεωχωρική σύγκριση',
  },
  {
    slug: 'Έλεγχος',
    title: 'Αύξηση επαναλαμβανόμενων προμηθευτών σε μισθώσεις μηχανημάτων σε δήμους μέσου κινδύνου',
    meta: 'Έλεγχος συμβάσεων / αντισυμβαλλόμενοι',
  },
]

const mapSignals = [
  { region: 'Αττική', value: '€ 24.3M', note: 'υψηλότερος καταγεγραμμένος όγκος προμηθειών', tone: 'high' },
  { region: 'Πελοπόννησος', value: '€ 18.7M', note: 'αντιπυρικές λωρίδες + συντήρηση δρόμων πρόσβασης', tone: 'mid' },
  { region: 'Στερεά Ελλάδα', value: '€ 16.2M', note: 'εξοπλισμός και πακέτα τοπικών έργων', tone: 'mid' },
  { region: 'Κρήτη', value: '€ 9.4M', note: 'εποχική ετοιμότητα και κινητές μονάδες νερού', tone: 'low' },
]

const regionalMapCards = [
  {
    title: 'Χάρτης Εθνικής Δαπάνης',
    metric: 'Χωροπλεθικός / €',
    value: '332 δήμοι',
    note: 'Η ένταση χρώματος αποτυπώνει τον όγκο προμηθειών σε σχέση με την εθνικά καταγεγραμμένη δαπάνη.',
  },
  {
    title: 'Επικάλυψη Καμένων Εκτάσεων',
    metric: 'Συμβάντα / ha',
    value: 'Διασταύρωση ιστορικού πυρκαγιών',
    note: 'Σύγκριση συμβάσεων πρόληψης με πρόσφατη έκθεση σε πυρκαγιές και επαναλαμβανόμενα μοτίβα καύσης.',
  },
  {
    title: 'Αρχές Κάλυψης',
    metric: 'Περιφερειακό / Αποκεντρωμένο',
    value: 'Πολυεπίπεδη χαρτογραφική όψη',
    note: 'Ιχνηλάτηση των αρχών που καλύπτουν κάθε δήμο και της προέλευσης των αποφάσεων.',
  },
]

function formatCommitDateTimeEl(iso: string): string {
  const dt = new Date(iso)
  if (Number.isNaN(dt.getTime())) return '—'
  return new Intl.DateTimeFormat('el-GR', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(dt)
}

import ContractAnalysis from './components/ContractAnalysis'

export default function App() {
  const lastCommitLabel = formatCommitDateTimeEl(__LAST_COMMIT_ISO__)

  return (
    <div className="pyro-app">
      <div className="page-grid" aria-hidden="true" />

      <header className="site-header">
        <div className="brand-block">
          <div className="eyebrow">παρατηρητηριο για τις δασικές πυρκαγιές</div>
          <div className="brand-line">
            <h1>PROJECT ΠΥΡ</h1>
            <span className="brand-mark">Τελευταία ενημέρωση / {lastCommitLabel}</span>
          </div>
        </div>
        <nav className="top-nav" aria-label="Κύρια πλοήγηση">
          <a href="#latest">Τελευταία</a>
          <a href="#mapdesk">Χάρτες</a>
          <a href="#analysis">Ανάλυση Συμβάσεων</a>
          <a href="#organizations">Φορείς</a>
          <a href="#documents">Διαύγεια</a>
          <a href="#about">About</a>
        </nav>
        <button className="menu-button" type="button" aria-label="Άνοιγμα μενού">
          <span />
          <span />
          <span />
        </button>
      </header>

      <main>
        <section id="latest" className="news-wire section-rule" aria-label="Τελευταία ρεπορτάζ">
          <div className="news-wire__label">
            <span className="eyebrow">News Desk</span>
            <strong>Οι πιο πρόσφατες αποφάσεις που έχουν δημοσιευτεί και αφορούν στην πυροπροστασία.</strong>
          </div>
          <div className="news-wire__items">
            {newsUpdates.map((item) => (
              <article className="wire-item" key={item.title}>
                <span className="wire-item__slug">{item.slug}</span>
                <h2>{item.title}</h2>
                <p>{item.meta}</p>
              </article>
            ))}
          </div>
        </section>

        <section className="hero section-rule">
          <div className="hero-left">
            <div className="hero-meta">
              <span>Δημοσιογραφική πλατφόρμα προμηθειών δασοπροστασίας / χαρτοκεντρικό ρεπορτάζ</span>
              <span>Εθνικός δείκτης / αρχεία / γεωχωρικά τεκμήρια</span>
            </div>
            <p className="hero-summary">
              Κάθε προμήθεια αντιμετωπίζεται ως φύλλο ενός εθνικού φακέλου πρόληψης πυρκαγιών:
              ανιχνεύσιμη, συγκρίσιμη και άμεσα αναγνώσιμη, με τους χάρτες να δείχνουν πού
              συναντιούνται η δαπάνη και ο κίνδυνος.
            </p>
            <div className="hero-actions">
              <a href="#records" className="link-cta">
                Προβολή εγγραφών
              </a>
              <a href="#documents" className="link-cta">
                Άνοιγμα αρχείου
              </a>
            </div>
          </div>

          <div className="hero-right">
            <div className="hero-background-year" aria-hidden="true">
              2026
            </div>
            <div className="hero-amount-card">
              <div className="eyebrow">Καταγεγραμμένος Προϋπολογισμός Πρόληψης</div>
              <div className="hero-amount">€ 214.7M</div>
              <div className="hero-subgrid">
                <div>
                  <span className="label">Κύκλος</span>
                  <strong>2025 / 2026</strong>
                </div>
                <div>
                  <span className="label">Εστίαση</span>
                  <strong>καύσιμη ύλη / πρόσβαση / εξοπλισμός</strong>
                </div>
                <div>
                  <span className="label">Πηγή</span>
                  <strong>δημόσιες αποφάσεις προμηθειών</strong>
                </div>
                <div>
                  <span className="label">Λειτουργία</span>
                  <strong>ευρετήριο αρχείου</strong>
                </div>
              </div>
            </div>
          </div>
        </section>

        <section id="mapdesk" className="map-desk section-rule">
          <div className="section-head">
            <div className="eyebrow">Γραφείο Χαρτών</div>
            <h2>Χαρτοκεντρικά πάνελ ρεπορτάζ για προμήθειες, κίνδυνο και λογοδοσία κάλυψης</h2>
          </div>

          <div className="map-desk__layout">
            <div className="map-board">
              <div className="map-board__header">
                <span className="eyebrow">Εθνική Συγκέντρωση Προμηθειών</span>
                <div className="map-board__legend" aria-label="Υπόμνημα">
                  <span>Χαμηλή</span>
                  <div aria-hidden="true" />
                  <span>Υψηλή</span>
                </div>
              </div>

              <div className="map-board__frame">
                <svg
                  className="news-map-svg"
                  viewBox="0 0 780 460"
                  role="img"
                  aria-label="Στυλιζαρισμένος εθνικός χάρτης προμηθειών με επισημασμένες περιοχές"
                >
                  <defs>
                    <pattern id="mapGrid" width="28" height="28" patternUnits="userSpaceOnUse">
                      <path d="M 28 0 L 0 0 0 28" fill="none" stroke="rgba(17,17,17,0.06)" strokeWidth="1" />
                    </pattern>
                    <linearGradient id="heatScale" x1="0%" y1="0%" x2="100%" y2="0%">
                      <stop offset="0%" stopColor="#efece2" />
                      <stop offset="60%" stopColor="#e4d8c7" />
                      <stop offset="100%" stopColor="#d3482d" />
                    </linearGradient>
                  </defs>

                  <rect x="0" y="0" width="780" height="460" fill="url(#mapGrid)" />

                  <path
                    d="M130 116 L198 76 L286 94 L324 132 L372 146 L426 192 L432 228 L406 252 L356 268 L330 306 L282 320 L238 308 L210 286 L188 248 L160 236 L126 196 L112 158 Z"
                    fill="#f2efe6"
                    stroke="rgba(17,17,17,0.25)"
                    strokeWidth="2"
                  />
                  <path
                    d="M468 248 L508 232 L554 240 L586 262 L574 294 L536 306 L494 296 L474 274 Z"
                    fill="#f2efe6"
                    stroke="rgba(17,17,17,0.2)"
                    strokeWidth="2"
                  />
                  <path
                    d="M596 184 L630 170 L662 184 L670 212 L648 232 L616 224 L600 204 Z"
                    fill="#f2efe6"
                    stroke="rgba(17,17,17,0.2)"
                    strokeWidth="2"
                  />
                  <path
                    d="M544 332 L594 324 L632 344 L620 372 L572 380 L544 360 Z"
                    fill="#f2efe6"
                    stroke="rgba(17,17,17,0.2)"
                    strokeWidth="2"
                  />

                  <path
                    d="M160 172 L218 122 L296 136 L344 176 L350 224 L312 258 L250 272 L202 248 L176 212 Z"
                    fill="rgba(211,72,45,0.22)"
                    stroke="rgba(17,17,17,0.08)"
                  />
                  <path
                    d="M238 150 L286 154 L318 184 L306 214 L268 230 L232 214 L218 184 Z"
                    fill="rgba(211,72,45,0.4)"
                    stroke="rgba(17,17,17,0.08)"
                  />
                  <path
                    d="M486 252 L532 250 L562 272 L548 292 L512 294 L490 278 Z"
                    fill="rgba(211,72,45,0.28)"
                    stroke="rgba(17,17,17,0.08)"
                  />
                  <path
                    d="M607 189 L642 194 L648 216 L620 222 L606 207 Z"
                    fill="rgba(211,72,45,0.24)"
                    stroke="rgba(17,17,17,0.08)"
                  />

                  <g fill="none" stroke="rgba(17,17,17,0.14)" strokeWidth="1">
                    <path d="M102 94 C182 132, 286 104, 366 168" />
                    <path d="M152 330 C270 286, 406 302, 522 258" />
                    <path d="M382 132 C462 164, 548 164, 664 180" />
                  </g>

                  <g>
                    <circle cx="252" cy="194" r="7" fill="#d3482d" stroke="#111" strokeWidth="1.5" />
                    <circle cx="252" cy="194" r="18" fill="none" stroke="rgba(211,72,45,0.35)" strokeWidth="1.5" />
                    <text x="272" y="188" className="map-anno">ΑΤΤΙΚΗ</text>
                    <text x="272" y="208" className="map-anno-sub">€ 24.3M καταγεγραμμένα</text>

                    <circle cx="520" cy="274" r="6" fill="#d3482d" stroke="#111" strokeWidth="1.5" />
                    <text x="538" y="270" className="map-anno">ΠΕΛΟΠΟΝΝΗΣΟΣ</text>
                    <text x="538" y="290" className="map-anno-sub">€ 18.7M</text>

                    <circle cx="622" cy="206" r="5.5" fill="#d3482d" stroke="#111" strokeWidth="1.5" />
                    <text x="640" y="203" className="map-anno">ΚΡΗΤΗ</text>
                    <text x="640" y="222" className="map-anno-sub">€ 9.4M</text>
                  </g>

                  <g transform="translate(26 334)">
                    <rect width="206" height="94" fill="#f7f5ee" stroke="rgba(17,17,17,0.18)" />
                    <text x="14" y="22" className="map-inset-title">Ένθετο Αιγαίου / νησιά</text>
                    <g fill="rgba(211,72,45,0.22)" stroke="rgba(17,17,17,0.2)" strokeWidth="1">
                      <circle cx="34" cy="50" r="7" />
                      <circle cx="68" cy="44" r="5" />
                      <circle cx="98" cy="60" r="6" />
                      <circle cx="130" cy="47" r="5" />
                      <circle cx="160" cy="58" r="7" />
                    </g>
                    <text x="14" y="80" className="map-inset-note">Μικρές κατανομές, κατακερματισμένη γεωγραφία, απαιτητικές μεταφορές.</text>
                  </g>
                </svg>

                <div className="map-board__scale" aria-hidden="true">
                  <span>€</span>
                  <div />
                  <span>Ένταση</span>
                </div>
              </div>
            </div>

            <aside className="map-sidebar">
              <div className="map-sidebar__panel">
                <div className="eyebrow">Περιφερειακά Σήματα</div>
                <ul className="signal-list">
                  {mapSignals.map((signal) => (
                    <li key={signal.region}>
                      <div className={`signal-dot signal-dot--${signal.tone}`} aria-hidden="true" />
                      <div className="signal-copy">
                        <strong>{signal.region}</strong>
                        <span>{signal.value}</span>
                        <p>{signal.note}</p>
                      </div>
                    </li>
                  ))}
                </ul>
              </div>

              <div className="map-sidebar__panel">
                <div className="eyebrow">Δημοσιογραφική Οπτική</div>
                <div className="map-questions">
                  <p>Πού συγκεντρώνεται η δαπάνη πρόληψης σε σχέση με τις πρόσφατες καμένες εκτάσεις;</p>
                  <p>Ποιοι δήμοι εξαρτώνται από περιφερειακή ή αποκεντρωμένη κάλυψη φορέων;</p>
                  <p>Συγκεντρώνονται οι ίδιοι προμηθευτές στις ίδιες ζώνες υψηλού κινδύνου κάθε χρόνο;</p>
                </div>
              </div>
            </aside>
          </div>

          <div className="map-inset-grid">
            {regionalMapCards.map((card, i) => (
              <article className="map-inset-card" key={card.title}>
                <div className="map-inset-card__head">
                  <div className="eyebrow">{card.metric}</div>
                  <h3>{card.title}</h3>
                </div>
                <div className="mini-map" aria-hidden="true">
                  <svg viewBox="0 0 320 128">
                    <rect x="0" y="0" width="320" height="128" fill="#f7f5ee" />
                    <g stroke="rgba(17,17,17,0.1)" strokeWidth="1" fill="none">
                      <path d="M0 20 H320" />
                      <path d="M0 54 H320" />
                      <path d="M0 88 H320" />
                      <path d="M80 0 V128" />
                      <path d="M160 0 V128" />
                      <path d="M240 0 V128" />
                    </g>
                    <path
                      d={
                        i === 0
                          ? 'M16 92 L62 78 L100 86 L142 56 L184 62 L228 42 L270 56 L304 28'
                          : i === 1
                            ? 'M16 78 L54 82 L96 52 L138 72 L176 38 L214 46 L256 64 L304 44'
                            : 'M16 84 L58 62 L96 66 L136 42 L178 78 L216 54 L260 58 L304 36'
                      }
                      fill="none"
                      stroke="rgba(17,17,17,0.75)"
                      strokeWidth="2"
                    />
                    <path
                      d={
                        i === 0
                          ? 'M16 104 L62 92 L100 98 L142 72 L184 76 L228 58 L270 70 L304 46'
                          : i === 1
                            ? 'M16 94 L54 98 L96 66 L138 86 L176 54 L214 62 L256 78 L304 60'
                            : 'M16 102 L58 80 L96 84 L136 58 L178 92 L216 70 L260 72 L304 54'
                      }
                      fill="none"
                      stroke="rgba(211,72,45,0.45)"
                      strokeWidth="2"
                    />
                    <g fill="#d3482d" stroke="#111" strokeWidth="1">
                      <circle cx={i === 0 ? 142 : i === 1 ? 176 : 136} cy={i === 0 ? 56 : i === 1 ? 38 : 42} r="4.5" />
                      <circle cx={i === 0 ? 228 : i === 1 ? 96 : 216} cy={i === 0 ? 42 : i === 1 ? 52 : 54} r="3.5" />
                    </g>
                  </svg>
                </div>
                <div className="map-inset-card__foot">
                  <strong>{card.value}</strong>
                  <p>{card.note}</p>
                </div>
              </article>
            ))}
          </div>
        </section>

        <ContractAnalysis />

        <section className="kpi-rail section-rule" aria-label="Βασικοί δείκτες">
          {kpis.map((kpi) => (
            <article className="kpi-tile" key={kpi.label}>
              <div className="eyebrow">{kpi.label}</div>
              <div className="kpi-value">{kpi.value}</div>
              <p>{kpi.note}</p>
            </article>
          ))}
        </section>

        <section id="records" className="records section-rule">
          <div className="section-head">
            <div className="eyebrow">Επιλεγμένες Εγγραφές Προμηθειών</div>
            <h2>Φύλλα εγγραφών τύπου αφίσας με ιεράρχηση που ξεκινά από τη δαπάνη</h2>
          </div>

          <div className="records-grid">
            {featuredRecords.map((record, idx) => (
              <article className="record-card" key={record.id}>
                <div className="record-card__year" aria-hidden="true">
                  {idx === 2 ? '2025' : '2026'}
                </div>
                <div className="record-card__header">
                  <div className="record-card__authority">{record.authority}</div>
                  <div className="record-card__id">{record.id}</div>
                </div>

                <h3>{record.title}</h3>

                <div className="record-card__amount">{record.amount}</div>

                <div className="record-card__tags" aria-label="Μεταδεδομένα εγγραφής">
                  <span>{record.stage}</span>
                  <span>{record.cpv}</span>
                  <span>{record.date}</span>
                </div>

                <div className="record-card__footer">
                  <div>
                    <span className="label">Προμηθευτής</span>
                    <strong>{record.supplier}</strong>
                  </div>
                  <a href="/" onClick={(e) => e.preventDefault()}>
                    Προβολή λεπτομερειών
                  </a>
                </div>
              </article>
            ))}
          </div>
        </section>

        <section id="organizations" className="organization section-rule">
          <div className="organization__header">
            <div className="eyebrow">Σελίδα Φορέα</div>
            <h2>Περιφέρεια Αττικής</h2>
            <p>
              Συντακτική διάταξη για προφίλ φορέα: μεγάλες συνολικές τιμές, λεπτοί κανόνες και
              πυκνά μπλοκ μεταδεδομένων που διαβάζονται σαν δημόσιο καθολικό.
            </p>
          </div>

          <div className="organization__hero">
            <div className="org-year" aria-hidden="true">
              2025
            </div>
            <div className="org-total">
              <span className="eyebrow">Συνολική Δαπάνη</span>
              <div className="org-total__value">€ 24.3M</div>
              <div className="org-total__note">
                Περιφέρεια + εποπτευόμενοι δήμοι / πρόληψη πυρκαγιών και ετοιμότητα
              </div>
            </div>
            <div className="org-codes">
              <div className="eyebrow">Κατηγορίες Υψηλού Όγκου</div>
              <div className="cpv-wall">
                <span>CPV 77312000-0</span>
                <span>CPV 45500000-2</span>
                <span>CPV 34144210-3</span>
                <span>CPV 44611500-1</span>
              </div>
            </div>
          </div>

          <div className="organization__grid">
            <div className="organization__kpis">
              {orgKpis.map((kpi) => (
                <article className="org-kpi" key={kpi.label}>
                  <div className="eyebrow">{kpi.label}</div>
                  <div className="org-kpi__value">{kpi.value}</div>
                  <p>{kpi.note}</p>
                </article>
              ))}
            </div>

            <div className="organization__timeline">
              <div className="eyebrow">Χρονολόγιο</div>
              <ul>
                {timelineItems.map((item) => (
                  <li key={`${item.month}-${item.year}`}>
                    <div className="timeline-date">
                      <span>{item.month}</span>
                      <strong>{item.year}</strong>
                    </div>
                    <p>{item.text}</p>
                  </li>
                ))}
              </ul>
            </div>
          </div>
        </section>

        <section id="documents" className="document-archive section-rule">
          <div className="section-head">
            <div className="eyebrow">Προβολή Εγγράφου</div>
            <h2>Πλαίσιο δημόσιου αρχείου με πλευρικά μεταδεδομένα και κυρίαρχη προεπισκόπηση σελίδας</h2>
          </div>

          <div className="document-shell" role="region" aria-label="Προεπισκόπηση εγγράφου">
            <aside className="document-meta">
              <div className="meta-group">
                <span className="label">Κωδικός Εγγραφής</span>
                <strong>ΠΥΡ-2026-0142</strong>
              </div>
              <div className="meta-group">
                <span className="label">Φορέας</span>
                <strong>Περιφέρεια Αττικής</strong>
              </div>
              <div className="meta-group">
                <span className="label">Ημερομηνία Έκδοσης</span>
                <strong>18 Φεβ 2026</strong>
              </div>
              <div className="meta-group">
                <span className="label">Ποσό</span>
                <strong className="accent-text">€ 3.200.000</strong>
              </div>
              <div className="meta-group">
                <span className="label">Κατάσταση</span>
                <strong>Ανατέθηκε</strong>
              </div>
              <button className="doc-action" type="button">
                Άνοιγμα PDF
              </button>
            </aside>

            <div className="document-page" aria-hidden="true">
              <div className="document-page__header">
                <span>ΕΛΛΗΝΙΚΗ ΔΗΜΟΚΡΑΤΙΑ</span>
                <span>ΑΠΟΦΑΣΗ ΑΝΑΘΕΣΗΣ</span>
                <span>Σελίδα 1 / 8</span>
              </div>

              <div className="document-title-block">
                <div className="document-watermark">0142</div>
                <h3>ΠΡΟΛΗΠΤΙΚΟΙ ΚΑΘΑΡΙΣΜΟΙ ΚΑΙ ΔΙΑΝΟΙΞΗ ΑΝΤΙΠΥΡΙΚΩΝ ΖΩΝΩΝ</h3>
                <p>
                  Απόφαση για την ανάθεση εργασιών πρόληψης πυρκαγιών σε ζώνες υψηλής
                  επικινδυνότητας με σκοπό τη μείωση καύσιμης ύλης και τη βελτίωση πρόσβασης.
                </p>
              </div>

              <div className="document-lines">
                {Array.from({ length: 12 }).map((_, i) => (
                  <div className="doc-line" key={i}>
                    <span>{String(i + 1).padStart(2, '0')}</span>
                    <div />
                  </div>
                ))}
              </div>
            </div>
          </div>
        </section>

        <section id="about" className="about-panel section-rule">
          <div className="about-panel__left">
            <div className="eyebrow">Σχεδιαστική Σύλληψη</div>
            <h2>Δημόσιο αρχείο / πολιτικό αποθετήριο / ερευνητική πλατφόρμα</h2>
            <p>
              Μονόχρωμη δομή, υπερμεγέθη μεγέθη, ορατή λογική πλέγματος και περιορισμένη χρήση
              έμφασης δημιουργούν ένα περιβάλλον πιο θεσμικό παρά «προϊοντικό».
            </p>
          </div>
          <div className="about-panel__right">
            <div className="poster-motif" aria-hidden="true">
              <div className="poster-motif__sun" />
              <div className="poster-motif__terrain" />
            </div>
            <div className="about-stats">
              <div>
                <span className="label">Θέμα</span>
                <strong>φωτιά / γραφειοκρατία / τεκμήρια</strong>
              </div>
              <div>
                <span className="label">Γλώσσα UI</span>
                <strong>ελβετική συντακτική λογική + μπρουταλιστική εγκράτεια</strong>
              </div>
              <div>
                <span className="label">Χρήση Έμφασης</span>
                <strong>σύνδεσμοι / ενεργές καταστάσεις / κρίσιμες σημάνσεις δαπάνης</strong>
              </div>
            </div>
          </div>
        </section>
      </main>
    </div>
  )
}
