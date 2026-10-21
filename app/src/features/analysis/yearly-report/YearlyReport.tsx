import type { ReactNode } from 'react'
import {
  BarList,
  ChartFrame,
  ColumnChart,
  DataTable,
  GroupedColumns,
  StackedRows,
  type Series,
} from './ReportCharts'
import {
  MONTHS,
  REPORT_YEAR,
  amount,
  contracts,
  count,
  dayMonth,
  derived,
  eur,
  fullYear,
  listJoin,
  longDate,
  mil,
  milText,
  municipalFunding,
  num,
  paymentCategory,
  pct,
  pctText,
  procedure,
  report,
  share,
  shortDate,
  signedPct,
  timing,
  wholeMillions,
  ytd,
  ytdGroup,
} from './reportData'

const COLORS = {
  blue: '#2a78d6',
  accent: '#d3482d',
  aqua: '#1baf7a',
  yellow: '#eda100',
  gray: '#b9b2a5',
  grayDark: '#8f897e',
  grayLight: '#ddd7cb',
}

const SOURCES = {
  fireSeason: 'https://www.thessaly.gov.gr/enimerosi/anakoinosi/6594',
  kimdis: 'https://cerpp.eprocurement.gov.gr/khmdhs-opendata',
  diavgeia: 'https://diavgeia.gov.gr',
  directAwardLaw: 'https://www.taxheaven.gr/law/4412/2016/arthro/118',
}


const AUTHORITY_NAMES: Record<string, string> = {
  'ΥΠΟΥΡΓΕΙΟ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΕΝΕΡΓΕΙΑΣ': 'το Υπουργείο Περιβάλλοντος και Ενέργειας',
  'ΥΠΟΥΡΓΕΙΟ ΕΘΝΙΚΗΣ ΑΜΥΝΑΣ': 'το Υπουργείο Εθνικής Άμυνας',
  'ΥΠΟΥΡΓΕΙΟ ΠΟΛΙΤΙΣΜΟΥ': 'το Υπουργείο Πολιτισμού',
  'ΥΠΟΥΡΓΕΙΟ ΚΛΙΜΑΤΙΚΗΣ ΚΡΙΣΗΣ ΚΑΙ ΠΟΛΙΤΙΚΗΣ ΠΡΟΣΤΑΣΙΑΣ': 'το Υπουργείο Κλιματικής Κρίσης και Πολιτικής Προστασίας',
  'ΥΠΟΥΡΓΕΙΟ ΥΠΟΔΟΜΩΝ ΚΑΙ ΜΕΤΑΦΟΡΩΝ': 'το Υπουργείο Υποδομών και Μεταφορών',
  'ΥΠΟΥΡΓΕΙΟ ΠΡΟΣΤΑΣΙΑΣ ΤΟΥ ΠΟΛΙΤΗ': 'το Υπουργείο Προστασίας του Πολίτη',
  'ΥΠΟΥΡΓΕΙΟ ΝΑΥΤΙΛΙΑΣ ΚΑΙ ΝΗΣΙΩΤΙΚΗΣ ΠΟΛΙΤΙΚΗΣ': 'το Υπουργείο Ναυτιλίας και Νησιωτικής Πολιτικής',
  'ΥΠΟΥΡΓΕΙΟ ΑΓΡΟΤΙΚΗΣ ΑΝΑΠΤΥΞΗΣ ΚΑΙ ΤΡΟΦΙΜΩΝ': 'το Υπουργείο Αγροτικής Ανάπτυξης και Τροφίμων',
  'ΥΠΟΥΡΓΕΙΟ ΕΣΩΤΕΡΙΚΩΝ': 'το Υπουργείο Εσωτερικών',
  'ΠΕΡΙΦΕΡΕΙΑ ΑΤΤΙΚΗΣ': 'η Περιφέρεια Αττικής',
  'ΠΕΡΙΦΕΡΕΙΑ ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ ΚΑΙ ΘΡΑΚΗΣ': 'η Περιφέρεια Ανατολικής Μακεδονίας και Θράκης',
  'ΠΕΡΙΦΕΡΕΙΑ ΚΕΝΤΡΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ': 'η Περιφέρεια Κεντρικής Μακεδονίας',
  'ΠΕΡΙΦΕΡΕΙΑ ΔΥΤΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ': 'η Περιφέρεια Δυτικής Μακεδονίας',
  'ΠΕΡΙΦΕΡΕΙΑ ΗΠΕΙΡΟΥ': 'η Περιφέρεια Ηπείρου',
  'ΠΕΡΙΦΕΡΕΙΑ ΘΕΣΣΑΛΙΑΣ': 'η Περιφέρεια Θεσσαλίας',
  'ΠΕΡΙΦΕΡΕΙΑ ΙΟΝΙΩΝ ΝΗΣΩΝ': 'η Περιφέρεια Ιονίων Νήσων',
  'ΠΕΡΙΦΕΡΕΙΑ ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ': 'η Περιφέρεια Δυτικής Ελλάδας',
  'ΠΕΡΙΦΕΡΕΙΑ ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ': 'η Περιφέρεια Στερεάς Ελλάδας',
  'ΠΕΡΙΦΕΡΕΙΑ ΠΕΛΟΠΟΝΝΗΣΟΥ': 'η Περιφέρεια Πελοποννήσου',
  'ΠΕΡΙΦΕΡΕΙΑ ΒΟΡΕΙΟΥ ΑΙΓΑΙΟΥ': 'η Περιφέρεια Βορείου Αιγαίου',
  'ΠΕΡΙΦΕΡΕΙΑ ΝΟΤΙΟΥ ΑΙΓΑΙΟΥ': 'η Περιφέρεια Νοτίου Αιγαίου',
  'ΠΕΡΙΦΕΡΕΙΑ ΚΡΗΤΗΣ': 'η Περιφέρεια Κρήτης',
  'ΑΝΕΞΑΡΤΗΤΟΣ ΔΙΑΧΕΙΡΙΣΤΗΣ ΜΕΤΑΦΟΡΑΣ ΗΛΕΚΤΡΙΚΗΣ ΕΝΕΡΓΕΙΑΣ ΑΔΜΗΕ ΑΕ': 'ο ΑΔΜΗΕ',
  'ΟΡΓΑΝΙΣΜΟΣ ΣΙΔΗΡΟΔΡΟΜΩΝ ΕΛΛΑΔΑΣ ΟΣΕ': 'ο ΟΣΕ',
  'ΓΑΙΑ ΟΣΕ ΑΕ': 'η ΓΑΙΑΟΣΕ',
  'ΚΟΙΝΩΝΙΑ ΤΗΣ ΠΛΗΡΟΦΟΡΙΑΣ ΑΕ': 'η Κοινωνία της Πληροφορίας',
  'ΙΕΡΑ ΚΟΙΝΟΤΗΤΑ ΑΓΙΟΥ ΟΡΟΥΣ': 'η Ιερά Κοινότητα Αγίου Όρους',
  'ΟΡΓΑΝΙΣΜΟΣ ΦΥΣΙΚΟΥ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΚΛΙΜΑΤΙΚΗΣ ΑΛΛΑΓΗΣ': 'ο Οργανισμός Φυσικού Περιβάλλοντος και Κλιματικής Αλλαγής',
  'ΕΥΔΑΠ ΑΕ': 'η ΕΥΔΑΠ',
}

const PROCEDURE_NAMES: Record<string, string> = {
  'Απευθείας Ανάθεση': 'απευθείας ανάθεση',
  'Ανοιχτή Διαδικασία': 'ανοιχτή διαδικασία',
  'Διαπραγμάτευση': 'διαπραγμάτευση',
  'Άλλη': 'άλλες διαδικασίες',
}

const TYPE_NAMES: Record<string, { name: string; object: string }> = {
  'Υπηρεσίες': { name: 'οι υπηρεσίες', object: 'υπηρεσίες' },
  'Έργα': { name: 'τα έργα', object: 'έργα' },
  'Προμήθειες': { name: 'οι προμήθειες', object: 'προμήθειες' },
  'Λοιπές': { name: 'οι λοιπές συμβάσεις', object: 'λοιπές συμβάσεις' },
}

const MONTHS_ACCUSATIVE = ['τον Ιανουάριο', 'τον Φεβρουάριο', 'τον Μάρτιο', 'τον Απρίλιο', 'τον Μάιο', 'τον Ιούνιο', 'τον Ιούλιο', 'τον Αύγουστο', 'τον Σεπτέμβριο', 'τον Οκτώβριο', 'τον Νοέμβριο', 'τον Δεκέμβριο']
const MONTHS_NOMINATIVE = ['Ιανουάριος', 'Φεβρουάριος', 'Μάρτιος', 'Απρίλιος', 'Μάιος', 'Ιούνιος', 'Ιούλιος', 'Αύγουστος', 'Σεπτέμβριος', 'Οκτώβριος', 'Νοέμβριος', 'Δεκέμβριος']

function capitalize(text: string) {
  return text.charAt(0).toUpperCase() + text.slice(1)
}

/** After «έως»: «τις 19 Οκτωβρίου 2026», «την 1η Νοεμβρίου 2026». */
function untilDate(iso: string) {
  const text = longDate(iso)
  return text.startsWith('1 ') ? `την 1η ${text.slice(2)}` : `τις ${text}`
}

function authorityName(label: string) {
  const known = AUTHORITY_NAMES[label]
  if (known) return known
  if (label.startsWith('ΔΗΜΟΣ ')) return `ο «${label}»`
  if (label.startsWith('ΠΕΡΙΦΕΡΕΙΑ ')) return `η «${label}»`
  if (label.startsWith('ΥΠΟΥΡΓΕΙΟ ')) return `το «${label}»`
  return `ο φορέας «${label}»`
}

/** Rows with a share above zero, largest first. */
function ranked<T extends { amount: number }>(rows: T[], key: (row: T) => number | null) {
  return rows.filter(row => row.amount > 0 && (key(row) ?? 0) > 0).sort((a, b) => (key(b) ?? 0) - (key(a) ?? 0))
}

function Tip({ title, lines }: { title: string; lines: string[] }) {
  return <><strong>{title}</strong>{lines.map(line => <span key={line}>{line}</span>)}</>
}

function Section({ id, eyebrow, title, children }: { id: string; eyebrow: string; title: string; children: ReactNode }) {
  return (
    <section id={id} className="section-rule yr-section" aria-labelledby={`${id}-title`}>
      <header className="yr-section__head">
        <div className="eyebrow">{eyebrow}</div>
        <h3 id={`${id}-title`}>{title}</h3>
      </header>
      <div className="yr-section__body">{children}</div>
    </section>
  )
}

function ExternalLink({ href, children }: { href: string; children: ReactNode }) {
  return <a href={href} target="_blank" rel="noopener noreferrer">{children}</a>
}

export default function YearlyReport() {
  const t = report.total
  const p = report.payments
  const fx = report.facts
  const m = report.municipalities
  const periodEnd = shortDate(report.meta.period_end)
  const periodDayMonth = dayMonth(report.meta.period_end)
  const updatedLabel = `${longDate(report.meta.snapshot_date)}, ${report.meta.snapshot_time}`
  const cur = ytd(REPORT_YEAR)
  const prev = ytd(REPORT_YEAR - 1)
  const fullPrev = fullYear(REPORT_YEAR - 1)
  const fullPrev2 = fullYear(REPORT_YEAR - 2)
  const direct = procedure('Απευθείας Ανάθεση')
  const da = contracts.direct_awards
  const munTiming = timing(REPORT_YEAR, 'municipal')
  const munTimingPrev = timing(REPORT_YEAR - 1, 'municipal')
  const munTimingPrev2 = timing(REPORT_YEAR - 2, 'municipal')
  const munTopMonths = munTiming.by_month.filter(row => row.contracts > 0).sort((a, b) => b.contracts - a.contracts || a.month - b.month).slice(0, 3)
  const beforeMayShares = [munTimingPrev2, munTimingPrev, munTiming].map(row => share(row.before_may_contracts, row.contracts))
  const procedures = ranked(contracts.procedures, row => row.share_amount_pct)
  const typesByAmount = ranked(contracts.contract_types, row => row.share_amount_pct)
  const topType = typesByAmount[0]
  const topAuthorities = contracts.top_authorities
  const municipalities = contracts.by_group.find(item => item.group === 'Δήμοι')
  const bandsPrev = fullPrev?.direct_awards
  const bandsPrev2 = fullPrev2?.direct_awards
  // «Ακριβώς κάτω από το όριο»: every sentence below depends on the current counts.
  const lowBins = da.bins.filter(bin => bin.hi !== null && bin.hi <= 25_000)
  const belowLimitDecreasing = lowBins.length > 1 && lowBins.every((bin, i) => i === 0 || bin.count <= lowBins[i - 1].count)
  const bandRatio = derived.directBandRatio
  const bandMultiple = bandRatio > 4 ? 'υπερτετραπλάσιο' : bandRatio > 3 ? 'υπερτριπλάσιο' : bandRatio > 2 ? 'υπερδιπλάσιο' : bandRatio > 1 ? 'μεγαλύτερο' : null
  const ratioOf = (bands?: { from_25k_to_30k: number; from_20k_to_25k: number }) => bands && bands.from_20k_to_25k > 0 ? bands.from_25k_to_30k / bands.from_20k_to_25k : 0
  const patternRepeats = ratioOf(bandsPrev) > 1 && ratioOf(bandsPrev2) > 1
  const patternWeaker = patternRepeats && ratioOf(bandsPrev) < bandRatio && ratioOf(bandsPrev2) < bandRatio
  const funding = municipalFunding()
  const fundingCur = funding.find(item => item.year === REPORT_YEAR)
  const fundingPrev = funding.find(item => item.year === REPORT_YEAR - 1)
  const fundingPrev2 = funding.find(item => item.year === REPORT_YEAR - 2)
  // Municipalities: Diavgeia payments of the year by month, and contracts against earlier years.
  const munPay = p.municipal
  const munPayMonths = munPay.by_month.filter(row => row.month <= new Date(`${report.meta.period_end}T12:00:00`).getMonth() + 1)
  const munPayPeak = munPayMonths.filter(row => row.payments > 0).sort((a, b) => b.payments - a.payments).slice(0, 3).sort((a, b) => a.month - b.month)
  const munPayPeakShare = share(munPayPeak.reduce((sum, row) => sum + row.payments, 0), munPay.payments)
  const munContracts = [REPORT_YEAR, REPORT_YEAR - 1, REPORT_YEAR - 2].map(year => ytdGroup(year, 'Δήμοι').amount)
  // The sentences about fewer contracts than last year appear only while the daily snapshot supports them.
  const munContractsLower = munContracts[0] < munContracts[1]
  const unionsFunding = report.funding.find(item => item.year === REPORT_YEAR && item.recipient_type === 'σύνδεσμος')
  const payroll = paymentCategory('payroll')
  const vegetation = paymentCategory('vegetation')
  const equipment = paymentCategory('equipment')
  const machinery = paymentCategory('machinery')
  const fuel = paymentCategory('fuel')
  const rrf = paymentCategory('rrf_ypen')
  const unclear = paymentCategory('unclear')
  const other = paymentCategory('other')
  const municipalPayments = p.by_org_type.find(item => item.org_type === 'ΔΗΜΟΣ')?.amount ?? 0
  const payrollOnlyMunicipal = Object.keys(fx.payroll_org_types).length > 0 && Object.keys(fx.payroll_org_types).every(type => type === 'ΔΗΜΟΣ')
  const finalizations = (year: number) => p.finalizations_by_year[String(year)] ?? 0
  const positiveCancellations = Object.values(p.positive_cancellations_by_org).reduce((sum, value) => sum + value, 0)

  // ── Chart data ──
  const procedureSeries: Series[] = [
    { key: 'Απευθείας Ανάθεση', label: 'Απευθείας ανάθεση', color: COLORS.accent },
    { key: 'Ανοιχτή Διαδικασία', label: 'Ανοιχτή διαδικασία', color: COLORS.blue },
    { key: 'Διαπραγμάτευση', label: 'Διαπραγμάτευση', color: COLORS.aqua },
    { key: 'Άλλη', label: 'Άλλη διαδικασία', color: COLORS.gray },
  ]
  const procedureRows = [
    {
      label: 'Αριθμός συμβάσεων',
      segments: contracts.procedures.map(item => ({ key: item.label, value: item.count, tooltip: <Tip title={item.label} lines={[`${num(item.count)} συμβάσεις`, `${pct(item.share_count_pct)} του συνόλου`]} /> })),
    },
    {
      label: 'Αξία χωρίς ΦΠΑ',
      segments: contracts.procedures.map(item => ({ key: item.label, value: item.amount, tooltip: <Tip title={item.label} lines={[mil(item.amount), `${pct(item.share_amount_pct)} της αξίας`]} /> })),
    },
  ]

  const binRange = (bin: { lo: number; hi: number | null }) => bin.hi === null ? `πάνω από ${eur(bin.lo)}` : `${eur(bin.lo)} – ${eur(bin.hi)}`
  const awardBins = da.bins.map(bin => ({
    label: bin.hi === null ? `>${bin.lo / 1_000}` : `${bin.lo / 1_000}–${bin.hi / 1_000}`,
    value: bin.count,
    emphasis: bin.lo === 25_000,
    ariaLabel: `${binRange(bin)}: ${num(bin.count)} αναθέσεις`,
    tooltip: <Tip title={binRange(bin)} lines={[`${num(bin.count)} ${bin.count === 1 ? 'ανάθεση' : 'αναθέσεις'}`]} />,
  }))

  const yearSeries: Series[] = [
    { key: String(REPORT_YEAR - 2), label: String(REPORT_YEAR - 2), color: COLORS.grayLight },
    { key: String(REPORT_YEAR - 1), label: String(REPORT_YEAR - 1), color: COLORS.grayDark },
    { key: String(REPORT_YEAR), label: String(REPORT_YEAR), color: COLORS.accent },
  ]
  const timingColumns = MONTHS.map((label, index) => {
    const values = Object.fromEntries([REPORT_YEAR - 2, REPORT_YEAR - 1, REPORT_YEAR].map(year => [String(year), timing(year, 'municipal').by_month[index]?.contracts ?? 0]))
    return {
      label,
      values,
      ariaLabel: `${label}: ${[REPORT_YEAR - 2, REPORT_YEAR - 1, REPORT_YEAR].map(year => `${year} ${values[String(year)]}`).join(', ')}`,
      tooltip: <Tip title={`${label} · συμβάσεις δήμων`} lines={[REPORT_YEAR - 2, REPORT_YEAR - 1, REPORT_YEAR].map(year => `${year}: ${num(values[String(year)])}`)} />,
    }
  })

  const categoryItems = [...p.by_category].filter(row => row.payments > 0).sort((a, b) => b.amount - a.amount).map(row => ({
    label: row.label,
    value: row.amount,
    valueLabel: mil(row.amount, 2),
    detail: `${num(row.payments)} ${row.payments === 1 ? 'πληρωμή' : 'πληρωμές'}`,
    muted: row.category === 'unclear',
  }))

  return (
    <article className="yr-root">
      <header className="section-rule section-head yr-hero">
        <div className="eyebrow">Ετήσια αναφορά {REPORT_YEAR}</div>
        <h2>Τουλάχιστον {wholeMillions(t.amount)} εκατομμύρια ευρώ για πυροπροστασία το {REPORT_YEAR}</h2>
        <p className="yr-lead">
          Η ετήσια οικονομική αναφορά του FireWatch επιχειρεί να αποτυπώσει όσο το δυνατόν πληρέστερα τις δαπάνες για την πρόληψη και την καταστολή δασικών πυρκαγιών στην Ελλάδα.
          Η ανάλυση βασίζεται σε δύο πηγές: τις δημόσιες συμβάσεις που καταγράφονται στο Κεντρικό Ηλεκτρονικό Μητρώο Δημοσίων Συμβάσεων (ΚΗΜΔΗΣ) και τις οριστικοποιήσεις πληρωμών που δημοσιεύονται στη Διαύγεια.
          Για την αποφυγή διπλής καταμέτρησης, εξαιρούνται οι πληρωμές που έχει διαπιστωθεί ότι συνδέονται με συμβάσεις οι οποίες έχουν ήδη συμπεριληφθεί στην ανάλυση.
        </p>
        <p className="yr-meta">
          Τα δεδομένα αφορούν το διάστημα {shortDate(report.meta.period_start)} έως {periodEnd}. Τα ποσά ενδέχεται να αλλάξουν.
        </p>
      </header>

      <section className="section-rule yr-section yr-findings" aria-labelledby="yr-findings-title">
        <header className="yr-section__head">
          <div className="eyebrow">Με μια ματιά</div>
          <h3 id="yr-findings-title">{pct(direct.share_amount_pct, 0)} των δημόσιων συμβάσεων με απευθείας ανάθεση το {REPORT_YEAR}</h3>
        </header>
        <div className="yr-findings__grid">
          <article className="yr-finding">
            <div className="yr-finding__value">{mil(t.amount)}</div>
            <p>(χωρίς ΦΠΑ) για την πυροπροστασία το {REPORT_YEAR} : {mil(t.contracts_amount, 2)} από συμβάσεις και {mil(t.payments_amount, 2)} από πληρωμές (Διαύγεια).</p>
          </article>
          {topType && (
            <article className="yr-finding">
              <div className="yr-finding__value">{pct(topType.share_amount_pct)}</div>
              <p>
                της δαπάνης των δημόσιων συμβάσεων αφορά {TYPE_NAMES[topType.label]?.object ?? topType.label.toLowerCase()}.
                {typesByAmount.length > 1 && <> {capitalize(listJoin(typesByAmount.slice(1).map((row, index) => `${TYPE_NAMES[row.label]?.name ?? row.label}${index === 0 ? ' είναι' : ''} το ${pct(row.share_amount_pct)}`)))}.</>}
              </p>
            </article>
          )}
          <article className="yr-finding">
            <div className="yr-finding__value">{signedPct(derived.ytdChangePct)}</div>
            <p>δαπανήθηκε σε σχέση με το ίδιο διάστημα του {REPORT_YEAR - 1} (1/1–{periodDayMonth}): {mil(cur.amount)} έναντι {mil(prev.amount)}.</p>
          </article>
        </div>
      </section>

      <nav className="section-rule yr-toc" aria-label="Περιεχόμενα αναφοράς">
        <div className="eyebrow">Περιεχόμενα</div>
        <ol>
          <li><a href="#symvaseis">Οι συμβάσεις του {REPORT_YEAR}</a></li>
          <li><a href="#xronos">Πότε υπογράφονται οι συμβάσεις</a></li>
          <li><a href="#plirwmes">Οι πληρωμές στη Διαύγεια</a></li>
          <li><a href="#periorismoi">Τι πρέπει να έχετε υπόψη</a></li>
          <li><a href="#methodologia">Μεθοδολογία και πηγές</a></li>
        </ol>
      </nav>

      <Section id="symvaseis" eyebrow="1 · Συμβάσεις" title={`Οι συμβάσεις του ${REPORT_YEAR}`}>
        <p>
          Από {shortDate(report.meta.period_start)} έως {periodEnd}, έχουν δημοσιευτεί στο ΚΗΜΔΗΣ <strong>{num(contracts.contracts)} συμβάσεις που αφορούν σε αντιμετώπιση ή πρόληψη δασικών πυρκαγιών, συνολικής αξίας {mil(contracts.amount)} χωρίς ΦΠΑ</strong>.
        </p>
        {procedures.length > 0 && (
          <p>
            Επί αυτού του ποσού, {listJoin(procedures.map((row, index) => `το ${pct(row.share_amount_pct)}${index === 0 ? ' δόθηκε' : ''} με ${PROCEDURE_NAMES[row.label] ?? row.label.toLowerCase()}`))}.
            {direct.count > 0 && <> Συγκεντρωτικά, οι απευθείας αναθέσεις αποτελούν το {pct(direct.share_count_pct)} του συνόλου των συμβάσεων.</>}
          </p>
        )}
        
        {contracts.top_cpvs.length > 0 && (
          <p>
            {contracts.top_cpvs.length === 5 ? 'Οι πέντε συχνότεροι' : 'Οι συχνότεροι'} κωδικοί CPV (Common Procurement Vocabulary) είναι {listJoin(contracts.top_cpvs.map((row, index) => `«${row.cpv}» (${pct(row.share_contracts_pct)}${index === 0 ? ' των συμβάσεων' : ''})`))}.
            Αξίζει να σημειωθεί όμως ότι μια σύμβαση μπορεί να έχει περισσότερους από έναν κωδικούς CPV.
          </p>
        )}
        <ChartFrame
          title="Διαδικασία ανάθεσης"
          subtitle={`Συμβάσεις που υπογράφηκαν από ${shortDate(report.meta.period_start)} έως ${periodEnd}. Κάθε γραμμή αναπαριστά το 100%.`}
          legend={procedureSeries}
          table={<DataTable caption={`Διαδικασίες ανάθεσης ${REPORT_YEAR}`} columns={[{ key: 'label', label: 'Διαδικασία' }, { key: 'count', label: 'Συμβάσεις', numeric: true }, { key: 'countPct', label: '% πλήθους', numeric: true }, { key: 'amount', label: 'Αξία χωρίς ΦΠΑ', numeric: true }, { key: 'amountPct', label: '% αξίας', numeric: true }]}
            rows={contracts.procedures.map(item => ({ label: item.label, count: num(item.count), countPct: pct(item.share_count_pct), amount: mil(item.amount, 2), amountPct: pct(item.share_amount_pct) }))} />}
        >
          <StackedRows rows={procedureRows} series={procedureSeries} normalize formatValue={(value, sum) => pct(share(value, sum), 0)} />
        </ChartFrame>

        <h4>Ακριβώς κάτω από το νόμιμο όριο</h4>
        <p>
          {da.median !== null
            ? <>Οι μισές από τις {num(da.count)} απευθείας αναθέσεις δεν ξεπερνούν τις {num(Math.round(da.median))} ευρώ, ενώ {num(da.up_to_30k)} ({pct(derived.directUpTo30kPct)}) έχουν αξία έως 30.000 ευρώ.</>
            : <>Από τις {num(da.count)} απευθείας αναθέσεις, {num(da.up_to_30k)} ({pct(derived.directUpTo30kPct)}) έχουν αξία έως 30.000 ευρώ.</>}
          {' '}Το ποσό αυτό αποτελεί το γενικό ανώτατο όριο για την απευθείας ανάθεση δημόσιων συμβάσεων, σύμφωνα με το <ExternalLink href={SOURCES.directAwardLaw}>άρθρο 118 του ν. 4412/2016</ExternalLink>, όπως τροποποιήθηκε με τον ν. 4782/2021.
          Για τις συμβάσεις έργων και ορισμένες ειδικές υπηρεσίες, το όριο ανέρχεται στις 60.000 ευρώ.
        </p>
        <p>
          Το γράφημα παρουσιάζει τον αριθμό των απευθείας αναθέσεων ανά 5.000 ευρώ αξίας σύμβασης, χωρίς ΦΠΑ.
          {belowLimitDecreasing && <> Όπως είναι αναμενόμενο, οι συμβάσεις μικρότερης αξίας είναι περισσότερες και ο αριθμός τους μειώνεται σταδιακά όσο αυξάνεται το ποσό.</>}
          {bandRatio > 1.5 && (belowLimitDecreasing
            ? <> Η τάση αυτή, ωστόσο, ανατρέπεται απότομα λίγο πριν από το όριο των 30.000 ευρώ.</>
            : <> Λίγο πριν από το όριο των 30.000 ευρώ, ωστόσο, ο αριθμός τους αυξάνεται απότομα.</>)}
        </p>
        <p>
          Στο κλιμάκιο των 25.000–30.000 ευρώ καταγράφονται {num(da.from_25k_to_30k)} απευθείας αναθέσεις, έναντι {bandRatio > 2 ? 'μόλις ' : ''}{num(da.from_20k_to_25k)} στο αμέσως προηγούμενο κλιμάκιο των 20.000–25.000 ευρώ.
          {bandMultiple && <> Πρόκειται για {bandMultiple} αριθμό συμβάσεων, παρά το γεγονός ότι τα δύο διαστήματα καλύπτουν ακριβώς το ίδιο εύρος αξιών.</>}
        </p>
        {bandsPrev && bandsPrev2 && (
          <p>
            {patternRepeats ? `Το ίδιο μοτίβο παρατηρείται και τα προηγούμενα χρόνια${patternWeaker ? ', αν και λιγότερο έντονα' : ''}` : 'Τα προηγούμενα χρόνια οι αντίστοιχοι αριθμοί ήταν διαφορετικοί'}:
            {' '}το {REPORT_YEAR - 1} καταγράφηκαν {num(bandsPrev.from_25k_to_30k)} απευθείας αναθέσεις στο κλιμάκιο των 25.000–30.000 ευρώ, έναντι {num(bandsPrev.from_20k_to_25k)} στο κλιμάκιο των 20.000–25.000 ευρώ,
            {' '}και το {REPORT_YEAR - 2} {num(bandsPrev2.from_25k_to_30k)} έναντι {num(bandsPrev2.from_20k_to_25k)}.
          </p>
        )}
        {bandRatio > 1 && (
          <p>
            Η συγκέντρωση αυτή δεν αποδεικνύει από μόνη της παρατυπία. Αναδεικνύει, ωστόσο, {patternRepeats ? 'ένα επαναλαμβανόμενο μοτίβο' : 'ένα μοτίβο'}: σημαντικός αριθμός συμβάσεων ανατίθεται σε ποσά που πλησιάζουν το ανώτατο νομοθετικό όριο της γενικής διαδικασίας απευθείας ανάθεσης.
          </p>
        )}
        <ChartFrame
          title={`Διασπορά απευθείας αναθέσεων ${REPORT_YEAR}`}
          subtitle="Πλήθος απευθείας αναθέσεων ανά κλιμάκιο 5.000 €."
          table={<DataTable caption="Κλιμάκια αξίας απευθείας αναθέσεων" columns={[{ key: 'range', label: 'Κλιμάκιο' }, { key: 'count', label: 'Αναθέσεις', numeric: true }]}
            rows={da.bins.map(bin => ({ range: binRange(bin), count: num(bin.count) }))} />}
          note="Κάθε κλιμάκιο περιλαμβάνει και το ανώτερο ποσό του. Για παράδειγμα, μια ανάθεση ακριβώς 30.000 € μετρά στο κλιμάκιο 25.000–30.000 €."
        >
          <ColumnChart data={awardBins} formatAxis={value => value.toLocaleString('el-GR')} color={COLORS.accent} mutedColor={COLORS.gray}
            axisTitle="Αξία ανάθεσης χωρίς ΦΠΑ, σε χιλιάδες ευρώ" labelAnchor={Math.max(awardBins.findIndex(bin => bin.emphasis), 0)} />
        </ChartFrame>

        <h4>Οι πέντε φορείς με τη μεγαλύτερη αξία συμβάσεων</h4>
        {topAuthorities.length > 0 && (
          <p>
            Τη μεγαλύτερη αξία συμβάσεων έχει {authorityName(topAuthorities[0].authority)}, με {num(topAuthorities[0].contracts)} {topAuthorities[0].contracts === 1 ? 'σύμβαση' : 'συμβάσεις'} και {mil(topAuthorities[0].amount)} ({pct(topAuthorities[0].share_pct)}).
            {topAuthorities.length > 1 && <> Ακολουθούν {listJoin(topAuthorities.slice(1, 5).map(item => `${authorityName(item.authority)} (${mil(item.amount)})`))}.</>}
            {municipalities && municipalities.contracts > 0 && <> Οι δήμοι μαζί υπέγραψαν {num(municipalities.contracts)} συμβάσεις αξίας {mil(municipalities.amount)}.</>}
          </p>
        )}
        <p>
          Οι συμβάσεις του {REPORT_YEAR} μοιράστηκαν σε {num(report.contractors.distinct)} διαφορετικούς αναδόχους ή κοινοπραξίες, με βάση τον ΑΦΜ.
          Οι δέκα μεγαλύτεροι συγκέντρωσαν το {pct(report.contractors.top10_share_pct)} της αξίας.
        </p>
        <ChartFrame
          title={`Οι δέκα ανάδοχοι με τη μεγαλύτερη αξία συμβάσεων, ${REPORT_YEAR}`}
          subtitle={`Αξία χωρίς ΦΠΑ και μερίδιο του αναδόχου στη συνολική αξία των συμβάσεων του ${REPORT_YEAR}. Κάτω από κάθε μπάρα, οι συμβάσεις και οι φορείς που τις ανέθεσαν. Οι ανάδοχοι ομαδοποιούνται με βάση τον ΑΦΜ.`}
        >
          <BarList color={COLORS.accent} mutedColor={COLORS.gray} items={report.contractors.top10.map(item => ({
            label: item.name,
            value: item.amount,
            valueLabel: `${mil(item.amount)} · ${pct(item.share_pct)}`,
            detail: `${num(item.contracts)} ${item.contracts === 1 ? 'σύμβαση' : 'συμβάσεις'} · ${item.authorities.length === 1 ? 'Φορέας' : 'Φορείς'}: ${
              item.authorities.slice(0, 3).map(row => item.authorities.length > 1 ? `${row.authority} (${num(row.contracts)})` : row.authority).join(', ')
            }${item.authorities.length > 3 ? ` και ${num(item.authorities.length - 3)} ακόμη` : ''}`,
          }))} />
        </ChartFrame>
        <details className="yr-list">
          <summary>Λεξιλόγιο: τύποι συμβάσεων και διαδικασίες ανάθεσης</summary>
          <dl className="yr-glossary">
            <dt>Υπηρεσίες</dt><dd>Συμβάσεις παροχής υπηρεσιών από τρίτους προς την αναθέτουσα αρχή. Δεν παράγουν υλικό αποτέλεσμα (έργο) ούτε μεταβιβάζουν κυριότητα αγαθού.</dd>
            <dt>Προμήθειες</dt><dd>Συμβάσεις αγοράς, χρηματοδοτικής μίσθωσης ή μίσθωσης αγαθών και εξοπλισμού που μεταβιβάζονται ή παραχωρούνται στην αναθέτουσα αρχή.</dd>
            <dt>Έργα</dt><dd>Συμβάσεις εκτέλεσης οικοδομικών ή τεχνικών εργασιών (κατασκευή, ανακαίνιση, αποκατάσταση), με αποτέλεσμα ακίνητο τεχνικό έργο.</dd>
            <dt>Απευθείας ανάθεση</dt><dd>Η πιο απλοποιημένη διαδικασία, χωρίς πλήρη διαγωνιστική προκήρυξη. Η αναθέτουσα αρχή επιλέγει απευθείας ανάδοχο.</dd>
            <dt>Ανοιχτή διαδικασία</dt><dd>Κάθε ενδιαφερόμενος οικονομικός φορέας μπορεί να υποβάλει προσφορά μετά από δημοσίευση στο ΚΗΜΔΗΣ και, όπου απαιτείται, στην ΕΕ.</dd>
            <dt>Διαπραγμάτευση</dt><dd>Η αναθέτουσα αρχή διαπραγματεύεται με έναν ή περισσότερους φορείς, μόνο σε ειδικές περιπτώσεις που προβλέπει ο νόμος.</dd>
          </dl>
        </details>
      </Section>

      <Section id="xronos" eyebrow="2 · Χρόνος" title={derived.municipalAfterMayPct > 50 ? 'Οι δήμοι υπογράφουν τις συμβάσεις πυροπροστασίας μέσα στην αντιπυρική περίοδο' : 'Πότε υπογράφονται οι συμβάσεις πυροπροστασίας'}>
        <p>
          Η χρηματοδότηση των δήμων για την πυροπροστασία αυξήθηκε σημαντικά το {REPORT_YEAR}, φτάνοντας στο υψηλότερο επίπεδο των τελευταίων ετών.
          {munContractsLower && <> Ωστόσο, η συνολική αξία των συμβάσεων που εντοπίστηκαν στο Κεντρικό Ηλεκτρονικό Μητρώο Δημοσίων Συμβάσεων (ΚΗΜΔΗΣ) είναι μικρότερη από πέρυσι.</>}
          {' '}Παράλληλα, η μεγάλη πλειονότητα των συμβάσεων υπογράφηκε αφού είχε ήδη ξεκινήσει η αντιπυρική περίοδος.
        </p>

        <h4>Αυξημένη χρηματοδότηση{munContractsLower ? ', λιγότερα χρήματα σε συμβάσεις' : ''}</h4>
        {fundingCur && fundingPrev && fundingPrev2 && (
          <p>
            Το Υπουργείο Εσωτερικών κατένειμε το {REPORT_YEAR} στους δήμους {milText(fundingCur.total, 2)} για δράσεις πυροπροστασίας, έναντι {milText(fundingPrev.total, 2)} το {REPORT_YEAR - 1}.
            {fundingCur.total > fundingPrev.total && <> Πρόκειται για αύξηση περίπου {pctText(share(fundingCur.total - fundingPrev.total, fundingPrev.total), 0)} μέσα σε έναν χρόνο.</>}
            {' '}Το {REPORT_YEAR - 2} η αντίστοιχη χρηματοδότηση είχε φτάσει τα {milText(fundingPrev2.total, 2)}{fundingPrev2.extra > 0 ? `, συμπεριλαμβανομένης έκτακτης ενίσχυσης ύψους ${milText(fundingPrev2.extra, 2)}` : ''}.
            {unionsFunding && <> Επιπλέον, το {REPORT_YEAR} διατέθηκαν {milText(unionsFunding.amount, 2)} σε συνδέσμους δήμων.</>}
          </p>
        )}
        <p>
          {munContractsLower && <>Η αύξηση της χρηματοδότησης, ωστόσο, δεν συνοδεύεται από αντίστοιχη αύξηση της αξίας των συμβάσεων που έχουν καταγραφεί. </>}
          Από την 1η Ιανουαρίου έως {untilDate(report.meta.period_end)}, το FireWatch εντόπισε συμβάσεις πυροπροστασίας των δήμων συνολικής αξίας {milText(munContracts[0])} χωρίς ΦΠΑ, έναντι {milText(munContracts[1])} το αντίστοιχο διάστημα του {REPORT_YEAR - 1} και {milText(munContracts[2])} το {REPORT_YEAR - 2}.
        </p>
        <p>
          {munContractsLower && <>Η διαφορά δεν σημαίνει απαραίτητα ότι οι δήμοι δαπάνησαν λιγότερα χρήματα. </>}
          Η χρηματοδότηση που λαμβάνουν από το Υπουργείο Εσωτερικών και η αξία των συμβάσεων που δημοσιεύονται στο ΚΗΜΔΗΣ είναι δύο διαφορετικά μεγέθη.
          Οι δήμοι μπορούν να καλύπτουν και άλλες δαπάνες πυροπροστασίας, όπως τη μισθοδοσία προσωπικού, ενώ δεν είναι βέβαιο ότι όλες οι σχετικές δαπάνες εντοπίζονται με τα κριτήρια της ανάλυσης.
        </p>

        <h4>Οι συμβάσεις υπογράφονται αφού αρχίσει η αντιπυρική περίοδος</h4>
        <p>
          Η αντιπυρική περίοδος διαρκεί από την 1η Μαΐου έως τις 31 Οκτωβρίου. Θα περίμενε κανείς ότι στο πλαίσιο της πρόληψης των δασικών πυρκαγιών, οι δήμοι θα προγραμμάτιζαν τις εργασίες πρόληψης πριν την έναρξη της αντιπυρικής περιόδου. 
          Παρ' όλα αυτά, μόλις {num(munTiming.before_may_contracts)} από τις {num(munTiming.contracts)} συμβάσεις πυροπροστασίας των δήμων το {REPORT_YEAR} ({pctText(beforeMayShares[2])}) είχαν υπογραφεί πριν από την έναρξή της.
        </p>
        <p>
          Αντίθετα, {num(munTiming.may_to_august_contracts)} συμβάσεις ({pctText(share(munTiming.may_to_august_contracts, munTiming.contracts))}) υπογράφηκαν από τον Μάιο έως και τον Αύγουστο, δηλαδή μέσα στους τέσσερις πρώτους μήνες της αντιπυρικής περιόδου.
          {munTopMonths.length === 3 && munTopMonths[0].contracts > munTopMonths[1].contracts && <> Ο {MONTHS_NOMINATIVE[munTopMonths[0].month - 1]} ήταν ο μήνας με τις περισσότερες υπογραφές, συνολικά {num(munTopMonths[0].contracts)}, ενώ ακολούθησαν ο {MONTHS_NOMINATIVE[munTopMonths[1].month - 1]} με {num(munTopMonths[1].contracts)} και ο {MONTHS_NOMINATIVE[munTopMonths[2].month - 1]} με {num(munTopMonths[2].contracts)}.</>}
        </p>
        <p>
          Το ίδιο φαινόμενο καταγράφεται και τα προηγούμενα χρόνια. Πριν από την 1η Μαΐου είχε υπογραφεί μόλις το {pctText(beforeMayShares[0])} των συμβάσεων του {REPORT_YEAR - 2} και το {pctText(beforeMayShares[1])} του {REPORT_YEAR - 1}.
        </p>
        <p>
          Τα στοιχεία αναδεικνύουν ένα επαναλαμβανόμενο χαρακτηριστικό στον προγραμματισμό της πυροπροστασίας σε τοπικό επίπεδο: η συντριπτική πλειονότητα των συμβάσεων υπογράφεται όταν η περίοδος αυξημένου κινδύνου δασικών πυρκαγιών έχει ήδη ξεκινήσει.
        </p>
        <p>
          Αυτό δεν σημαίνει ότι οι αντίστοιχες εργασίες εκτελέστηκαν απαραίτητα τότε. Η ημερομηνία υπογραφής δείχνει πότε αναλήφθηκε η συμβατική δέσμευση, όχι πότε ξεκίνησαν ή ολοκληρώθηκαν οι εργασίες.
        </p>
        <ChartFrame
          title="Συμβάσεις πυροπροστασίας των δήμων ανά μήνα υπογραφής"
          subtitle="Πλήθος συμβάσεων. Η σκίαση δείχνει την αντιπυρική περίοδο (Μάιος–Οκτώβριος)."
          legend={yearSeries}
          table={<DataTable caption="Συμβάσεις πυροπροστασίας δήμων ανά μήνα" columns={[{ key: 'month', label: 'Μήνας' }, ...yearSeries.map(s => ({ key: s.key, label: s.label, numeric: true }))]}
            rows={timingColumns.map(row => ({ month: row.label, ...Object.fromEntries(Object.entries(row.values).map(([key, value]) => [key, num(value)])) }))} />}
          note={`Τα στοιχεία του ${REPORT_YEAR} φτάνουν έως τις ${periodEnd}.`}
        >
          <GroupedColumns data={timingColumns} series={yearSeries} formatAxis={value => value.toLocaleString('el-GR')} band={{ from: 4, to: 9, label: 'Αντιπυρική περίοδος' }} />
        </ChartFrame>

        {munPay.payments > 0 && (
          <>
            <h4>Τι δείχνουν οι πληρωμές στη Διαύγεια</h4>
            <p>
              Εκτός από τις συμβάσεις στο ΚΗΜΔΗΣ, το FireWatch εντόπισε στη Διαύγεια {num(munPay.payments)} οριστικοποιήσεις πληρωμών για την πυροπροστασία των δήμων το {REPORT_YEAR}, συνολικής αξίας {milText(munPay.amount)} χωρίς ΦΠΑ, οι οποίες δεν ταυτοποιήθηκαν με συμβάσεις που έχουν ήδη καταμετρηθεί.
            </p>
            <p>
              Από αυτές, οι {num(munPay.from_may_1)} ({pctText(share(munPay.from_may_1, munPay.payments))}) αναρτήθηκαν από την 1η Μαΐου και μετά{munPayPeakShare > 50 ? `, με τις περισσότερες αναρτήσεις να συγκεντρώνονται ${listJoin(munPayPeak.map(row => MONTHS_ACCUSATIVE[row.month - 1]))}` : ''}.
              {munPay.payroll > munPay.payments / 2 && <> Περισσότερες από τις μισές ({num(munPay.payroll)}) αφορούν τη μισθοδοσία προσωπικού πυροπροστασίας.</>}
            </p>
            <p>
              Οι ημερομηνίες των πληρωμών δεν μπορούν να ερμηνευτούν με τον ίδιο τρόπο όπως οι ημερομηνίες υπογραφής των συμβάσεων. Οι πληρωμές καταγράφονται συνήθως σε μεταγενέστερο στάδιο και δεν αποτυπώνουν πότε ανατέθηκε ή εκτελέστηκε μια εργασία.
              Συνεπώς, η συγκέντρωσή τους κατά τη διάρκεια της αντιπυρικής περιόδου δεν συνιστά από μόνη της ένδειξη καθυστερημένου προγραμματισμού.
            </p>
          </>
        )}

        <h4>Δήμοι χωρίς καταγεγραμμένες συμβάσεις ή πληρωμές</h4>
        <p>
          Για τις ανάγκες της ανάλυσης, τα δεδομένα των δήμων οργανώθηκαν σε {num(m.funded)} δημοτικές μονάδες. Ορισμένοι δήμοι που σήμερα λειτουργούν χωριστά, όπως εκείνοι της Κέρκυρας, της Κεφαλονιάς, της Λέσβου και της Σάμου, εμφανίζονται συγκεντρωτικά.
        </p>
        <p>
          Από τις {num(m.funded)} μονάδες, οι {num(m.with_contracts)} εμφανίζουν τουλάχιστον μία σύμβαση πυροπροστασίας το {REPORT_YEAR}, ενώ για {num(m.with_payments)} εντοπίζεται τουλάχιστον μία σχετική πληρωμή στη Διαύγεια.
        </p>
        {m.funded_without_contract > 0 && (
          <p>
            Αυτό σημαίνει ότι για {num(m.funded_without_contract)} δημοτικές μονάδες που έλαβαν χρηματοδότηση δεν εντοπίστηκε καμία σύμβαση πυροπροστασίας το {REPORT_YEAR}.
            {m.funded_without_contract_or_payment > 0 && <> Σε {num(m.funded_without_contract_or_payment)} από αυτές δεν βρέθηκε ούτε σύμβαση στο ΚΗΜΔΗΣ ούτε σχετική πληρωμή στη Διαύγεια, παρότι είχαν λάβει συνολικά {milText(m.funding_of_no_trace)} από τη χρηματοδότηση του Υπουργείου Εσωτερικών.</>}
          </p>
        )}
        {m.funded_without_contract_or_payment > 0 && (
          <>
            <p>
              Η απουσία καταγραφών δεν αποδεικνύει ότι τα συγκεκριμένα ποσά δεν δαπανήθηκαν. Ενδέχεται οι δήμοι να πραγματοποίησαν δαπάνες που δεν εντοπίζονται από τη μεθοδολογία συλλογής, να χρησιμοποίησαν διαφορετικές περιγραφές ή κωδικούς ή να μην έχουν ακόμη δημοσιευτεί όλα τα σχετικά έγγραφα.
            </p>
            <p>
              Αναδεικνύεται, ωστόσο, ένα σημαντικό κενό στην καταγραφή των δαπανών: για μέρος της κρατικής χρηματοδότησης δεν είναι δυνατόν, με βάση τα διαθέσιμα στοιχεία και τα κριτήρια της έρευνας, να προσδιοριστεί σε ποιες συγκεκριμένες δράσεις πυροπροστασίας κατευθύνθηκε.
            </p>
          </>
        )}
      </Section>

      <Section id="plirwmes" eyebrow="3 · Πληρωμές" title="Τι δείχνει η Διαύγεια">
        <p>
          Η «οριστικοποίηση πληρωμής» είναι ο τύπος πράξης με τον οποίο ένας φορέας δημοσιεύει στη Διαύγεια την πληρωμή ενός ή περισσότερων δικαιούχων. Οι πράξεις συλλέγονται με αναζήτηση σειράς λέξεων-κλειδιών, και στη συνέχεια επιβεβαιώνουμε οτι πράγματι τα έγγραφα που έχουμε συγκεντρώσει αφορούν σε πληρωμές που σχετίζονται με την αντιμετώπιση ή πρόληψη δασικών πυρκαγιών. 
        </p>
        <p>
          Στη Διαύγεια εντοπίζονται για το ίδιο διάστημα <strong>{num(p.counted)} πληρωμές πυροπροστασίας που δεν συνδέονται με κάποια σύμβαση, συνολικά {mil(p.amount)} χωρίς ΦΠΑ</strong>. Αυτές προστίθενται στο σύνολο της αναφοράς.
          Τα ποσά της Διαύγειας περιλαμβάνουν ΦΠΑ. Εδώ εμφανίζονται χωρίς ΦΠΑ 24%, εκτός από τη μισθοδοσία και τις άλλες δαπάνες που δεν επιβαρύνονται με ΦΠΑ.
        </p>
        <p>
          {municipalPayments > 0 && <>Από αυτά, τα {mil(municipalPayments)} είναι πληρωμές δήμων. </>}
          {payroll.payments > 0 && <>Η μισθοδοσία προσωπικού είναι {num(payroll.payments)} πληρωμές από {num(payroll.bodies)} {payrollOnlyMunicipal ? 'δήμους' : 'φορείς'} ({amount(payroll.amount)}).</>}
          {derived.payrollTotal > 0 && derived.payrollAugSep > derived.payrollTotal / 2 && <> Οι περισσότερες αναρτήθηκαν τον Αύγουστο και τον Σεπτέμβριο ({num(derived.payrollAugSep)} από τις {num(derived.payrollTotal)}).</>}
          {vegetation.payments > 0 && <> Οι αντιπυρικές ζώνες, οι καθαρισμοί και τα κλαδέματα είναι {num(vegetation.payments)} πληρωμές ({amount(vegetation.amount)}).</>}
          {rrf.payments > 0 && <> {rrf.payments === 1 ? 'Μία πληρωμή' : `${capitalize(count(rrf.payments, true))} πληρωμές`} του Υπουργείου Περιβάλλοντος και Ενέργειας για {rrf.payments === 1 ? 'έργο' : 'έργα'} του Ταμείου Ανάκαμψης {rrf.payments === 1 ? 'αντιστοιχεί' : 'αντιστοιχούν'} σε {amount(rrf.amount)}.</>}
        </p>
        <p>
          {equipment.payments > 0 && <>Ο εξοπλισμός και η συντήρηση μέσων είναι {num(equipment.payments)} πληρωμές ({amount(equipment.amount)}){machinery.payments > 0 ? ` και οι μισθώσεις μηχανημάτων ${num(machinery.payments)} (${amount(machinery.amount)})` : ''}.</>}
          {fuel.payments > 0 && <> Τα καύσιμα και τα λιπαντικά είναι μόνο {num(fuel.payments)} πληρωμές από {num(fuel.bodies)} φορείς ({amount(fuel.amount)}). Πληρωμές καυσίμων που δεν περιέχουν τις λέξεις-κλειδιά δεν συλλέγονται, οπότε το ποσό δεν είναι η συνολική δαπάνη για καύσιμα.</>}
          {unclear.payments > 0 && <> Σε {num(unclear.payments)} πληρωμές ({amount(unclear.amount)}) το θέμα δεν περιγράφει τη δαπάνη, π.χ. «ΕΝΤΑΛΜΑ ΠΛΗΡΩΜΗΣ».</>}
          {other.payments > 0 && <> Άλλες {num(other.payments)} ({amount(other.amount)}) αφορούν λοιπές δαπάνες πυροπροστασίας.</>}
        </p>
        <ChartFrame
          title={`Πληρωμές ${REPORT_YEAR} ανά κατηγορία`}
          subtitle="Πληρωμές χωρίς σύνδεση με σύμβαση, ποσά χωρίς ΦΠΑ. Οι κατηγορίες προκύπτουν από το θέμα κάθε πληρωμής."
          table={<DataTable caption="Πληρωμές ανά κατηγορία" columns={[{ key: 'label', label: 'Κατηγορία' }, { key: 'payments', label: 'Πληρωμές', numeric: true }, { key: 'amount', label: 'Ποσό χωρίς ΦΠΑ', numeric: true }, { key: 'rule', label: 'Κανόνας' }]}
            rows={p.by_category.map(item => ({ label: item.label, payments: num(item.payments), amount: mil(item.amount, 2), rule: item.rule }))} />}
          note="Με γκρι: πληρωμές των οποίων το θέμα δεν περιγράφει τη δαπάνη."
        >
          <BarList items={categoryItems} color={COLORS.accent} mutedColor={COLORS.gray} />
        </ChartFrame>
        <p className="yr-caveat">
          Η Διαύγεια περιέχει πολύ λιγότερες πληρωμές για τα προηγούμενα έτη ({num(finalizations(REPORT_YEAR - 2))} το {REPORT_YEAR - 2} και {num(finalizations(REPORT_YEAR - 1))} το {REPORT_YEAR - 1}, έναντι {num(finalizations(REPORT_YEAR))} το {REPORT_YEAR}). Γι' αυτό δεν συγκρίνουμε τις πληρωμές μεταξύ ετών.
        </p>
      </Section>

      <Section id="periorismoi" eyebrow="4 · Περιορισμοί" title="Τι πρέπει να έχετε υπόψη">
        <ul className="yr-bullets">
          <li><strong>Τα στοιχεία είναι μερικά.</strong> Οι συμβάσεις συλλέγονται με κωδικούς CPV και οι πληρωμές με λέξεις-κλειδιά. Δαπάνες με άλλους κωδικούς ή άλλη διατύπωση δεν περιλαμβάνονται.</li>
          <li><strong>Οι συνδέσεις πληρωμών και συμβάσεων είναι ελλιπείς στις πηγές.</strong> Μόνο για {num(p.report_year_contracts_with_linked_payment)} από τις {num(contracts.contracts)} συμβάσεις του {REPORT_YEAR} υπάρχει πληρωμή που συνδέεται ρητά μαζί τους ({pct(p.report_year_contracts_with_linked_payment_value_pct)} της αξίας).{p.subject_refs_not_in_db > 0 && ` Ακόμη, ${num(p.subject_refs_not_in_db)} πληρωμές αναφέρουν κωδικούς συμβάσεων που δεν υπάρχουν στα δεδομένα.`}</li>
          {p.cancellations > 0 && (
            <li><strong>Ακυρωτικές πράξεις.</strong> Αφαιρέσαμε {count(p.cancellations, true)} ακυρωτικές πράξεις ή αντιλογισμούς{positiveCancellations > 0 ? `, ανάμεσά τους ${count(positiveCancellations, true)} με θετικό ποσό (${Object.entries(p.positive_cancellations_by_org).map(([org, n]) => `${org}: ${n}`).join(', ')})` : ''}. Αν προστεθούν στο σύνολο, μια ακυρωμένη πληρωμή μετριέται σαν να έγινε.
              {p.cancelled_originals.map(item => <span key={item.ada}> Αφαιρέσαμε και την πληρωμή <ExternalLink href={`https://diavgeia.gov.gr/doc/${item.ada}`}>{item.ada}</ExternalLink> ({eur(item.amount, 2)}), που ακυρώθηκε με την πράξη <ExternalLink href={`https://diavgeia.gov.gr/doc/${item.cancelled_by}`}>{item.cancelled_by}</ExternalLink>.</span>)}
            </li>
          )}
          {p.exact_duplicate_groups > 0 && <li><strong>Πιθανές διπλοεγγραφές.</strong> Σε {count(p.exact_duplicate_groups, true)} {p.exact_duplicate_groups === 1 ? 'περίπτωση' : 'περιπτώσεις'} υπάρχουν πληρωμές με ίδιο φορέα, ίδιο ποσό και ίδιο θέμα (επιπλέον ποσό {eur(p.exact_duplicates_extra_amount, 2)} χωρίς ΦΠΑ). Τις κρατήσαμε, γιατί μπορεί να είναι διαφορετικές δόσεις.</li>}
          <li><strong>Τροποποιήσεις συμβάσεων.</strong> Όταν μια σύμβαση τροποποιείται, μετράμε μόνο την τελευταία εκδοχή της. Το {REPORT_YEAR} εξαιρέθηκαν έτσι {num(report.dataQuality.superseded_or_duplicate_report_year_contract_rows)} εγγραφές.</li>
          <li><strong>Συμφωνίες-πλαίσιο.</strong> Η αξία τους είναι το ανώτατο ποσό που μπορεί να δαπανηθεί και όχι απαραίτητα η τελική δαπάνη.</li>
          <li><strong>ΦΠΑ.</strong> Από τις πληρωμές αφαιρείται ο γενικός συντελεστής 24%. Όπου ισχύει μειωμένος συντελεστής, το ποσό χωρίς ΦΠΑ υποεκτιμάται ελαφρά.</li>
          <li><strong>Αυτόματη κατηγοριοποίηση.</strong> Οι κατηγορίες των πληρωμών προκύπτουν αυτόματα από τα θέματά τους. Μπορεί να περιέχουν λάθη σε μεμονωμένες εγγραφές.</li>
        </ul>
      </Section>

      <Section id="methodologia" eyebrow="5 · Μεθοδολογία" title="Πώς έγινε η ανάλυση">
        <ul className="yr-bullets">
          <li><strong>Συμβάσεις:</strong> <ExternalLink href={SOURCES.kimdis}>ΚΗΜΔΗΣ</ExternalLink>. Μετράμε τις ενεργές συμβάσεις που δεν έχουν ακυρωθεί, μία φορά η καθεμία, με την ημερομηνία υπογραφής τους. Η αξία είναι η συμβατική αξία χωρίς ΦΠΑ της τελευταίας εκδοχής.</li>
          <li><strong>Πληρωμές:</strong> <ExternalLink href={SOURCES.diavgeia}>Διαύγεια</ExternalLink>, πράξεις «Οριστικοποίηση πληρωμής» με ημερομηνία ανάρτησης από {shortDate(report.meta.period_start)} έως {periodEnd}. Όταν μια πράξη έχει περισσότερους δικαιούχους, αθροίζουμε τα ποσά τους. Αφαιρούμε τον ΦΠΑ όπως περιγράφεται στην ενότητα 3.</li>
          <li><strong>Χωρίς διπλομέτρηση:</strong> αξία συμβάσεων {REPORT_YEAR}, συν τις πληρωμές του {REPORT_YEAR} που δεν συνδέονται με καμία σύμβαση των δεδομένων.</li>
          <li><strong>Πεδίο:</strong> η αναφορά αφορά την πρόληψη και την καταστολή δασικών πυρκαγιών.</li>
          <li><strong>Χρηματοδότηση δήμων:</strong> αποφάσεις κατανομής του Υπουργείου Εσωτερικών από τους Κεντρικούς Αυτοτελείς Πόρους.</li>
          <li><strong>Ενημέρωση:</strong> {report.meta.year_complete ? `η αναφορά καλύπτει ολόκληρο το ${REPORT_YEAR}.` : `η αναφορά ενημερώνεται αυτόματα κάθε μέρα έως τις 31/12/${REPORT_YEAR}.`} Τελευταία ενημέρωση: {updatedLabel}. Κάθε ενημέρωση χρησιμοποιεί τα στοιχεία της βάσης εκείνης της στιγμής.</li>
        </ul>
      </Section>
    </article>
  )
}
