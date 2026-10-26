import type { ReactNode } from 'react'
import { municipalSpending } from '../../../data/municipalSpending'
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
  contractValueChangePct,
  count,
  decimal,
  derived,
  eur,
  fullYear,
  listJoin,
  longDate,
  mil,
  milText,
  minimumMillions,
  municipalFunding,
  num,
  paymentCategory,
  pct,
  pctText,
  procedure,
  report,
  share,
  shortDate,
  timing,
  ytdGroup,
} from './reportData'
import { CURRENT_YEAR } from '../statistics/analysisData'

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
    <section id={id} className="yr-section" aria-labelledby={`${id}-title`}>
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
  const previousValueChangePct = contractValueChangePct(REPORT_YEAR - 1)
  const twoYearValueChangePct = contractValueChangePct(REPORT_YEAR - 2)
  const updatedLabel = `${longDate(report.meta.snapshot_date)}, ${report.meta.snapshot_time}`
  const fullPrev = fullYear(REPORT_YEAR - 1)
  const fullPrev2 = fullYear(REPORT_YEAR - 2)
  const direct = procedure('Απευθείας Ανάθεση')
  const directCountShare = share(direct.count, contracts.contracts)
  const da = contracts.direct_awards
  const munTiming = timing(REPORT_YEAR, 'municipal')
  const regionalTiming = timing(REPORT_YEAR, 'regional')
  const environmentTiming = timing(REPORT_YEAR, 'environment_ministry')
  const civilProtectionTiming = timing(REPORT_YEAR, 'civil_protection_ministry')
  const munTimingPrev = timing(REPORT_YEAR - 1, 'municipal')
  const munTimingPrev2 = timing(REPORT_YEAR - 2, 'municipal')
  const munTopMonths = munTiming.by_month.filter(row => row.contracts > 0).sort((a, b) => b.contracts - a.contracts || a.month - b.month).slice(0, 3)
  const beforeMayShares = [munTimingPrev2, munTimingPrev, munTiming].map(row => share(row.before_may_contracts, row.contracts))
  const procedures = ranked(contracts.procedures, row => row.share_amount_pct)
  const topAuthorities = contracts.top_authorities
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
  const municipalPayments = p.by_org_type.find(item => item.org_type === 'ΔΗΜΟΣ')?.amount ?? 0
  const payrollOnlyMunicipal = Object.keys(fx.payroll_org_types).length > 0 && Object.keys(fx.payroll_org_types).every(type => type === 'ΔΗΜΟΣ')

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
      segments: contracts.procedures.map(item => ({ key: item.label, value: item.count, tooltip: <Tip title={item.label} lines={[`${num(item.count)} συμβάσεις`, `${pct(share(item.count, contracts.contracts))} του συνόλου`]} /> })),
    },
    {
      label: 'Αξία χωρίς ΦΠΑ',
      segments: contracts.procedures.map(item => ({ key: item.label, value: item.amount, tooltip: <Tip title={item.label} lines={[mil(item.amount), `${pct(item.share_amount_pct)} της αξίας`]} /> })),
    },
  ]

  const binRange = (bin: { lo: number; hi: number | null }) => bin.hi === null ? `πάνω από ${eur(bin.lo)}` : `${eur(bin.lo)} – ${eur(bin.hi)}`
  const awardBins = da.bins.map(bin => ({
    label: bin.hi === null ? `>${num(bin.lo / 1_000)}` : num(bin.hi / 1_000),
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
    valueLabel: mil(row.amount),
    detail: `${num(row.payments)} ${row.payments === 1 ? 'πληρωμή' : 'πληρωμές'}`,
    muted: row.category === 'unclear',
  }))

  return (
    <article className="yr-root">
      <header className="section-head yr-hero">
        <div className="eyebrow">Ετήσια αναφορά {REPORT_YEAR}</div>
        <h2>Τουλάχιστον {minimumMillions(t.amount)} εκατομμύρια ευρώ για πυροπροστασία το {REPORT_YEAR}</h2>
        <p className="yr-subtitle">
          Η ετήσια αναφορά του FireWatch επιχειρεί να αποτυπώσει όσο το δυνατόν πληρέστερα τις δαπάνες για την πρόληψη και την καταστολή δασικών πυρκαγιών στην Ελλάδα.
          Τα οικονομικά δεδομένα προέρχονται από δύο πηγές: τις δημόσιες συμβάσεις που δημοσιεύονται στο Κεντρικό Ηλεκτρονικό Μητρώο Δημοσίων Συμβάσεων (ΚΗΜΔΗΣ) και τις οριστικοποιήσεις πληρωμών που αναρτώνται στη Διαύγεια.
        </p>
        <p className="yr-meta">
          Τα δεδομένα αφορούν το διάστημα {shortDate(report.meta.period_start)} έως {periodEnd}. Για την αποφυγή διπλής καταμέτρησης, εξαιρούνται οι πληρωμές που έχει διαπιστωθεί ότι συνδέονται με συμβάσεις. Τα ποσά ενδέχεται να αλλάξουν.
        </p>
      </header>

      <div className="yr-layout">
        <nav className="yr-toc" aria-label="Περιεχόμενα αναφοράς">
          <div className="eyebrow">Περιεχόμενα</div>
          <ol>
            <li><a href="#symvaseis">Οι συμβάσεις του {REPORT_YEAR}</a></li>
            <li><a href="#xronos">Ο προγραμματισμός των εργασιών</a></li>
            <li><a href="#plirwmes">Οι πληρωμές</a></li>
            <li><a href="#methodologia">Μεθοδολογία</a></li>
          </ol>
        </nav>

        <div className="yr-content">
          <Section id="symvaseis" eyebrow="1 · Συμβάσεις" title={`Οι συμβάσεις του ${REPORT_YEAR}`}>
            <p>
              Από {shortDate(report.meta.period_start)} έως {periodEnd}, έχουν δημοσιευτεί στο ΚΗΜΔΗΣ <strong>{num(contracts.contracts)} συμβάσεις που αφορούν σε αντιμετώπιση ή πρόληψη δασικών πυρκαγιών, συνολικής αξίας {mil(contracts.amount)} χωρίς ΦΠΑ</strong>.
            </p>
            <p>
              Η συνολική αξία των δημόσιων συμβάσεων παρουσιάζει <strong>{previousValueChangePct < 0 ? 'μείωση' : 'αύξηση'} {pct(Math.abs(previousValueChangePct))}</strong> σε σύγκριση με το ίδιο διάστημα του {REPORT_YEAR - 1} και <strong>{twoYearValueChangePct < 0 ? 'μείωση' : 'αύξηση'} {pct(Math.abs(twoYearValueChangePct))}</strong> σε σύγκριση με το ίδιο διάστημα του {REPORT_YEAR - 2}. Η μείωση αυτή συνδέεται, σε σημαντικό βαθμό, με το πρόγραμμα προστασίας δασών «AntiNERO», το οποίο χρηματοδοτήθηκε από το Ταμείο Ανάκαμψης και Ανθεκτικότητας, στο πλαίσιο του σχεδίου «Ελλάδα 2.0».
            </p>
            <p>
              Το πρόγραμμα ξεκίνησε το 2022 και υλοποιήθηκε σε διαδοχικές φάσεις, με αντικείμενο τον καθαρισμό δασικών εκτάσεων, τη δημιουργία αντιπυρικών ζωνών και τη βελτίωση του δασικού οδικού δικτύου. Ο μεγαλύτερος όγκος των σχετικών συμβάσεων ανατέθηκε κατά τα προηγούμενα έτη, αυξάνοντας σημαντικά τη συνολική αξία των δημόσιων συμβάσεων εκείνης της περιόδου. Κατά συνέπεια, η μείωση που καταγράφεται το 2026 αντανακλά, τουλάχιστον εν μέρει, την ολοκλήρωση του προγράμματος AntiNERO.
            </p>
            <h4 className="yr-midtitle">Από τα {mil(contracts.amount)} των δημόσιων συμβάσεων, το {pct(direct.share_amount_pct)} δόθηκε με απευθείας ανάθεση το {REPORT_YEAR}</h4>
            {procedures.length > 0 && (
              <p>
                Ως προς τη συνολική αξία των συμβάσεων, {listJoin(procedures.map((row, index) => `το ${pct(row.share_amount_pct)}${index === 0 ? ' δόθηκε' : ''} με ${PROCEDURE_NAMES[row.label] ?? row.label.toLowerCase()}`))}.
                {direct.count > 0 && <> Ως προς το πλήθος, οι απευθείας αναθέσεις είναι {num(direct.count)} από τις {num(contracts.contracts)} συμβάσεις ({pct(directCountShare)} του συνόλου).</>}
              </p>
            )}

            {contracts.top_cpvs.length > 0 && (
              <p>
                {contracts.top_cpvs.length === 5 ? 'Οι πέντε συχνότεροι' : 'Οι συχνότεροι'} κωδικοί εργασιών που περιγράφονται στις συμβάσεις (<ExternalLink href="https://cerpp.eprocurement.gov.gr/cpv/main/">CPV</ExternalLink> - Common Procurement Vocabulary) είναι {listJoin(contracts.top_cpvs.map((row, index) => `«${row.cpv}» (${pct(row.share_contracts_pct)}${index === 0 ? ' των συμβάσεων' : ''})`))}.
                Αξίζει να σημειωθεί όμως ότι μια σύμβαση μπορεί να έχει περισσότερους από έναν κωδικούς CPV.
              </p>
            )}
            <ChartFrame
              title="Διαδικασία ανάθεσης"
              subtitle={`Συμβάσεις που υπογράφηκαν από ${shortDate(report.meta.period_start)} έως ${periodEnd}. Κάθε γραμμή αναπαριστά το ${pct(100)}.`}
              legend={procedureSeries}
              table={
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
              }
            >
              <StackedRows rows={procedureRows} series={procedureSeries} normalize formatValue={(value, sum) => pct(share(value, sum))} />
            </ChartFrame>

            <h4>Ακριβώς κάτω από το νόμιμο όριο</h4>
            <p>
              {da.median !== null
                ? <>Οι μισές από τις {num(da.count)} απευθείας αναθέσεις δεν ξεπερνούν τις {decimal(da.median)} ευρώ, ενώ {num(da.up_to_30k)} ({pct(derived.directUpTo30kPct)}) έχουν αξία έως 30.000 ευρώ.</>
                : <>Από τις {num(da.count)} απευθείας αναθέσεις, {num(da.up_to_30k)} ({pct(derived.directUpTo30kPct)}) έχουν αξία έως 30.000 ευρώ.</>}
              {' '}Το ποσό αυτό αποτελεί το γενικό ανώτατο όριο για την απευθείας ανάθεση δημόσιων συμβάσεων, σύμφωνα με το <ExternalLink href={SOURCES.directAwardLaw}>άρθρο 118 του ν. 4412/2016</ExternalLink>, όπως τροποποιήθηκε με τον ν. 4782/2021.
              Για τις συμβάσεις έργων και ορισμένες ειδικές υπηρεσίες, το όριο ανέρχεται στις 60.000 ευρώ.
            </p>
            <p>
              Το παρακάτω γράφημα παρουσιάζει τον αριθμό των απευθείας αναθέσεων ανά 5.000 ευρώ αξίας σύμβασης, χωρίς ΦΠΑ.
              {belowLimitDecreasing && <> Όπως είναι αναμενόμενο, οι συμβάσεις μικρότερης αξίας είναι περισσότερες και ο αριθμός τους μειώνεται σταδιακά όσο αυξάνεται το ποσό.</>}
              {bandRatio > 1.5 && (belowLimitDecreasing
                ? <> Η τάση αυτή, ωστόσο, ανατρέπεται απότομα λίγο πριν από το όριο των 30.000 ευρώ.</>
                : <> Λίγο πριν από το όριο των 30.000 ευρώ, ωστόσο, ο αριθμός τους αυξάνεται απότομα.</>)}
            </p>
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
            <p>
              Στο κλιμάκιο των 25.000–30.000 ευρώ καταγράφονται {num(da.from_25k_to_30k)} απευθείας αναθέσεις, έναντι {bandRatio > 2 ? 'μόλις ' : ''}{num(da.from_20k_to_25k)} στο αμέσως προηγούμενο κλιμάκιο των 20.000–25.000 ευρώ.
              {bandMultiple && <> Πρόκειται για {bandMultiple} αριθμό συμβάσεων, παρά το γεγονός ότι τα δύο διαστήματα καλύπτουν το ίδιο εύρος αξίας (κάθε κλιμάκιο προσαυξάνεται κατά 5.000 €).</>}
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
            <p>
              Η συγκέντρωση συμβάσεων κοντά στο όριο των 30.000 ευρώ εγείρει το ερώτημα κατά πόσον ορισμένες αναθέσεις ενδέχεται να συνδέονται με πρακτικές κατάτμησης, δηλαδή τη διάσπαση μιας ενιαίας προμήθειας ή υπηρεσίας σε περισσότερες συμβάσεις μικρότερης αξίας, με αποτέλεσμα να μπορούν να ανατεθούν απευθείας, χωρίς τη διενέργεια διαγωνισμού.
            </p>
            <p>
              Το ζήτημα της κατάτμησης των δημόσιων συμβάσεων έχει επισημανθεί και από το Ελεγκτικό Συνέδριο. Στην <ExternalLink href="https://www.elsyn.gr/index.php/el">έκθεσή του για τον Απολογισμό και τον Ισολογισμό του Κράτους του 2024</ExternalLink>, το Συνέδριο καταγράφει περιπτώσεις απευθείας αναθέσεων για όμοια ή ομοειδή αγαθά, αντί της διενέργειας ενιαίου ανοικτού διαγωνισμού, καθώς και περιπτώσεις κατάτμησης προμηθειών σε μικρότερες συμβάσεις. Μεταξύ των φορέων στους οποίους εντοπίστηκαν τέτοιες πρακτικές περιλαμβάνεται και το Υπουργείο Κλιματικής Κρίσης και Πολιτικής Προστασίας. Οι διαπιστώσεις αυτές αφορούν συγκεκριμένες συμβάσεις που εξετάστηκαν από το Ελεγκτικό Συνέδριο και δεν συνιστούν απόδειξη παρατυπιών στις αναθέσεις της παρούσας έρευνας.
            </p>

            <h4>Οι πέντε φορείς με τη μεγαλύτερη αξία συμβάσεων</h4>
            {topAuthorities.length > 0 && (
              <p>
                <strong>{capitalize(authorityName(topAuthorities[0].authority))}</strong> συγκεντρώνει τη μεγαλύτερη αξία συμβάσεων για το {REPORT_YEAR}, με {num(topAuthorities[0].contracts)} {topAuthorities[0].contracts === 1 ? 'σύμβαση' : 'συμβάσεις'} συνολικού ύψους <strong>{milText(topAuthorities[0].amount)}</strong>, που αντιστοιχούν στο {pct(share(topAuthorities[0].amount, contracts.amount))} της συνολικής αξίας.
                {topAuthorities.length > 1 && <> Ακολουθούν {listJoin(topAuthorities.slice(1, 5).map(item => `${authorityName(item.authority)} με ${milText(item.amount)}`))}.</>}
              </p>
            )}
            <p>
              Παράλληλα, καταγράφεται σημαντική συγκέντρωση της αξίας των συμβάσεων σε περιορισμένο αριθμό αναδόχων. Οι συμβάσεις του {REPORT_YEAR} κατανέμονται σε <strong>{num(report.contractors.distinct)} διαφορετικούς αναδόχους ή κοινοπραξίες</strong>, βάσει ΑΦΜ, με τους δέκα μεγαλύτερους να συγκεντρώνουν το <strong>{pct(report.contractors.top10_share_pct)} της συνολικής αξίας</strong>.
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
          </Section>

          <Section id="xronos" eyebrow="2 · Προγραμματισμός" title="Με καθυστέρηση οι εργασίες πρόληψης δασικών πυρκαγιών">
            <p>
              Η χρηματοδότηση των δήμων για την πυροπροστασία αυξήθηκε σημαντικά το {REPORT_YEAR}, φτάνοντας στο υψηλότερο επίπεδο των τελευταίων ετών.
              {munContractsLower && <> Ωστόσο, η συνολική αξία των συμβάσεων που εντοπίστηκαν στο Κεντρικό Ηλεκτρονικό Μητρώο Δημοσίων Συμβάσεων (ΚΗΜΔΗΣ) είναι μικρότερη από πέρυσι.</>}
              {' '}Παράλληλα, η πλειονότητα των συμβάσεων υπογράφηκε αφού είχε ήδη ξεκινήσει η αντιπυρική περίοδος.
            </p>

            {fundingCur && fundingPrev && fundingPrev2 && (
              <p>
                Το Υπουργείο Εσωτερικών κατένειμε το {REPORT_YEAR} στους δήμους {milText(fundingCur.total)} για δράσεις πυροπροστασίας, έναντι {milText(fundingPrev.total)} το {REPORT_YEAR - 1}.
                {fundingCur.total > fundingPrev.total && <> Πρόκειται για αύξηση περίπου {pctText(share(fundingCur.total - fundingPrev.total, fundingPrev.total))} μέσα σε έναν χρόνο.</>}
                {' '}Το {REPORT_YEAR - 2} η αντίστοιχη χρηματοδότηση είχε φτάσει τα {milText(fundingPrev2.total)}{fundingPrev2.extra > 0 ? `, συμπεριλαμβανομένης έκτακτης ενίσχυσης ύψους ${milText(fundingPrev2.extra)}` : ''}.
                {unionsFunding && <> Επιπλέον, το {REPORT_YEAR} διατέθηκαν {milText(unionsFunding.amount)} σε συνδέσμους δήμων.</>}
              </p>
            )}
            <p>
              Από την 1η Ιανουαρίου έως {untilDate(report.meta.period_end)}, το FireWatch εντόπισε συμβάσεις πυροπροστασίας των δήμων συνολικής αξίας {milText(munContracts[0])} χωρίς ΦΠΑ, έναντι {milText(munContracts[1])} το αντίστοιχο διάστημα του {REPORT_YEAR - 1} και {milText(munContracts[2])} το {REPORT_YEAR - 2}.
            </p>
            <p>
              Ωστόσο, συνυπολογίζοντας {num(municipalSpending.unlinkedPaymentsCount)} οριστικοποιήσεις πληρωμής δήμων στη Διαύγεια το {REPORT_YEAR}, ύψους {milText(municipalSpending.unlinkedPaymentsAmount)}, η εκτίμηση δαπανών των δήμων για το ίδιο διάστημα φτάνει τα <strong>{milText(municipalSpending.totalAmount)} χωρίς ΦΠΑ</strong>, δηλαδή {(municipalSpending.totalAmount - 50000000)>0 ? 'υπερβαίνει την ετήσια χρηματοδότηση κατά' : 'λιγότερα από την ετήσια χρηματοδότηση κατά'} {milText(municipalSpending.totalAmount - 50000000)}.
            </p>
            <p>
              Σε αυτό το σημέιο αξίζει να αναφερθεί ότι ο τρόπος που οι δήμοι και οι υπόλοιποι δημόσιοι φορείς καταχωρούν τις δημόσιες συμβάσεις στο Κεντρικό Μητρώο Δημοσίων Συμβάσεων (ΚΗΜΔΗΣ) και τις αποφάσεις πληρωμών στη Διαύγεια, καθιστά σχεδόν αδύνατη την παρακολούθηση της ροής δαπανών που αφορούν στην πυροπροστασία. Λάθη στην καταχώρηση ποσών, λάθη στην κωδικοποίηση των εργασιών ή και καθυστέρηση ανάρτησης των εγγράφων δεν μας επιτρέπουν να υπολογίσουμε με ακρίβεια πώς δαπανά κάθε δήμος την ετήσια κρατική χρηματοδότηση για δράσεις πρόληψης και καταπολέμησης δασικών πυρκαγιών.
            </p>

            <h4>Εργασίες πρόληψης μετά την έναρξη της αντιπυρικής περιόδου</h4>
            <p>
             Η αντιπυρική περίοδος διαρκεί κάθε χρόνο από την 1η Μαΐου έως τις 31 Οκτωβρίου. Θα περίμενε κανείς ότι οι δήμοι θα είχαν μεριμνήσει εγκαίρως για τον προγραμματισμό των απαραίτητων εργασιών πρόληψης των δασικών πυρκαγιών, συνάπτοντας, για παράδειγμα, συμβάσεις με ιδιώτες αναδόχους πριν από την έναρξη της αντιπυρικής περιόδου, ώστε να διασφαλίζεται η έγκαιρη εκτέλεσή τους.
              Παρ' όλα αυτά, μόλις {num(munTiming.before_may_contracts)} από τις {num(munTiming.contracts)} συμβάσεις πυροπροστασίας των δήμων το {REPORT_YEAR} ({pctText(beforeMayShares[2])}) είχαν υπογραφεί πριν από την έναρξή της.
              {' '}Η αξία αυτών των συμβάσεων αντιστοιχούσε στο {pctText(share(munTiming.before_may_amount, munTiming.amount))} της συνολικής αξίας των συμβάσεων πυροπροστασίας των δήμων για το {REPORT_YEAR}, έως την ημερομηνία ενημέρωσης της αναφοράς.
            </p>
            <p>
              Στις περιφέρειες, πριν από την 1η Μαΐου είχαν υπογραφεί {num(regionalTiming.before_may_contracts)} από τις {num(regionalTiming.contracts)} συμβάσεις πυροπροστασίας του {REPORT_YEAR} ({pctText(share(regionalTiming.before_may_contracts, regionalTiming.contracts))}).
              {' '}Η αξία τους αντιστοιχούσε στο {pctText(share(regionalTiming.before_may_amount, regionalTiming.amount))} της συνολικής αξίας των συμβάσεων πυροπροστασίας των περιφερειών για το {REPORT_YEAR}, έως την ημερομηνία ενημέρωσης της αναφοράς.
            </p>
            <p>
              Στο Υπουργείο Περιβάλλοντος και Ενέργειας, πριν από την 1η Μαΐου είχαν υπογραφεί {num(environmentTiming.before_may_contracts)} από τις {num(environmentTiming.contracts)} συμβάσεις ({pctText(share(environmentTiming.before_may_contracts, environmentTiming.contracts))}), που αντιστοιχούσαν στο {pctText(share(environmentTiming.before_may_amount, environmentTiming.amount))} της συνολικής αξίας των συμβάσεων πυροπροστασίας του υπουργείου για το {REPORT_YEAR}, έως την ημερομηνία ενημέρωσης της αναφοράς.
            </p>
            <p>
              Στο Υπουργείο Κλιματικής Κρίσης και Πολιτικής Προστασίας, πριν από την 1η Μαΐου είχαν υπογραφεί {num(civilProtectionTiming.before_may_contracts)} από τις {num(civilProtectionTiming.contracts)} συμβάσεις ({pctText(share(civilProtectionTiming.before_may_contracts, civilProtectionTiming.contracts))}), που αντιστοιχούσαν στο {pctText(share(civilProtectionTiming.before_may_amount, civilProtectionTiming.amount))} της συνολικής αξίας των συμβάσεων πυροπροστασίας του υπουργείου για το {REPORT_YEAR}, έως την ημερομηνία ενημέρωσης της αναφοράς.
            </p>
            <p>
              Στους δήμους, {num(munTiming.may_to_august_contracts)} συμβάσεις ({pctText(share(munTiming.may_to_august_contracts, munTiming.contracts))}) υπογράφηκαν από τον Μάιο έως και τον Αύγουστο, δηλαδή μέσα στους τέσσερις πρώτους μήνες της αντιπυρικής περιόδου.
              {munTopMonths.length === 3 && munTopMonths[0].contracts > munTopMonths[1].contracts && <> Ο {MONTHS_NOMINATIVE[munTopMonths[0].month - 1]} ήταν ο μήνας με τις περισσότερες υπογραφές, συνολικά {num(munTopMonths[0].contracts)}, ενώ ακολούθησαν ο {MONTHS_NOMINATIVE[munTopMonths[1].month - 1]} με {num(munTopMonths[1].contracts)} και ο {MONTHS_NOMINATIVE[munTopMonths[2].month - 1]} με {num(munTopMonths[2].contracts)}.</>}
            </p>
            <p>
              Το ίδιο φαινόμενο καταγράφεται και τα προηγούμενα χρόνια. Πριν από την 1η Μαΐου είχε υπογραφεί μόλις το {pctText(beforeMayShares[0])} των συμβάσεων του {REPORT_YEAR - 2} και το {pctText(beforeMayShares[1])} του {REPORT_YEAR - 1}.
            </p>
            <p>
              Τα στοιχεία αναδεικνύουν ένα επαναλαμβανόμενο χαρακτηριστικό στην πρόληψη πυρκαγιών σε τοπικό επίπεδο: η συντριπτική πλειονότητα των συμβάσεων υπογράφεται όταν η περίοδος αυξημένου κινδύνου δασικών πυρκαγιών έχει ήδη ξεκινήσει.
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
                  Εκτός από τις συμβάσεις στο ΚΗΜΔΗΣ, το FireWatch εντόπισε στη Διαύγεια {num(munPay.payments)} οριστικοποιήσεις πληρωμών για την πυροπροστασία των δήμων το {REPORT_YEAR}, συνολικής αξίας {milText(munPay.amount)} χωρίς ΦΠΑ.
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
              Για τις ανάγκες της ανάλυσης, τα δεδομένα των δήμων ομαδοποιήθηκαν σε <strong>{num(m.funded)} δημοτικές μονάδες</strong>. Δήμοι που σήμερα αποτελούν ξεχωριστές διοικητικές οντότητες, όπως εκείνοι της Κέρκυρας, της Κεφαλονιάς, της Λέσβου και της Σάμου, εξετάστηκαν συγκεντρωτικά.
            </p>
            <p>
              Από τις {num(m.funded)} δημοτικές μονάδες, στις <strong>{num(m.with_contracts)} εντοπίστηκε τουλάχιστον μία σύμβαση πυροπροστασίας το {REPORT_YEAR}</strong>, ενώ σε {num(m.with_payments)} καταγράφηκε τουλάχιστον μία σχετική πληρωμή στη Διαύγεια.
            </p>
            {m.funded_without_contract > 0 && (
              <p>
                Αντίστοιχα, για <strong>{num(m.funded_without_contract)} δημοτικές μονάδες που έλαβαν κρατική χρηματοδότηση δεν εντοπίστηκε καμία σύμβαση πυροπροστασίας το {REPORT_YEAR}</strong>.
                {' '}Σε {num(m.funded_without_contract - m.funded_without_contract_or_payment)} από αυτές καταγράφηκαν σχετικές πληρωμές στη Διαύγεια{m.funded_without_contract_or_payment > 0 && <>, ενώ για τις υπόλοιπες <strong>{num(m.funded_without_contract_or_payment)} δεν εντοπίστηκε καμία σύμβαση στο ΚΗΜΔΗΣ ούτε σχετική οριστικοποίηση πληρωμής στη Διαύγεια</strong>, παρότι είχαν λάβει συνολικά <strong>{milText(m.funding_of_no_trace)}</strong> από το Υπουργείο Εσωτερικών για δράσεις πυροπροστασίας</>}.
              </p>
            )}
            {m.funded_without_contract_or_payment > 0 && (
              <>
                <p>
                  Η απουσία καταγραφών δεν σημαίνει απαραίτητα ότι δεν έγιναν έργα πυροπροστασίας. Είναι πιθανό ορισμένες δαπάνες να μην εντοπίστηκαν λόγω διαφορετικών περιγραφών ή κωδικών καταχώρισης, να μην καλύπτονται από τη μεθοδολογία της έρευνας να μην έχουν ακόμη δημοσιευτεί τα σχετικά έγγραφα ή να έχουν υλοποιηθεί από προσωπικό των δήμων.
                </p>
                <p>
                  <strong>Το εύρημα αναδεικνύει, ωστόσο, ένα κενό στην ιχνηλασιμότητα της δημόσιας χρηματοδότησης:</strong> με βάση τα διαθέσιμα στοιχεία και τα κριτήρια της έρευνας, δεν είναι δυνατόν να προσδιοριστεί σε ποιες συγκεκριμένες δράσεις πυροπροστασίας κατευθύνθηκε μέρος των χρημάτων που διατέθηκαν στους δήμους. Επομένως δεν μπορεί να ασκηθεί ουσιαστικός έλεγχος για το κόστος των δράσεων ή απολογισμός για την αποτελεσματικότητα των έργων.
                </p>
              </>
            )}
          </Section>

          <Section id="plirwmes" eyebrow="3 · Πληρωμές" title="Μια ανάλυση των εγγράφων που αναρτώνται στη Διαύγεια">
            <p>
              Η «οριστικοποίηση πληρωμής» είναι ο τύπος πράξης με τον οποίο ένας φορέας δημοσιεύει στη Διαύγεια την πληρωμή ενός ή περισσότερων δικαιούχων. Οι πράξεις συλλέγονται με αναζήτηση σειράς λέξεων-κλειδιών που σχετίζονται με την πυροπροστασία, και στη συνέχεια εξειδικευμένοι αλγόριθμοι διασταυρώνουν εάν πράγματι τα έγγραφα που έχουμε συγκεντρώσει αφορούν σε πληρωμές που σχετίζονται με την αντιμετώπιση ή πρόληψη δασικών πυρκαγιών.
            </p>
            <p>
              Στη Διαύγεια εντοπίζονται <strong>{num(p.counted)} πληρωμές του {CURRENT_YEAR} που σχετίζονται με την πυροπροστασία, αξίας {mil(p.amount)}</strong> χωρίς ΦΠΑ. Αυτές οι πληρωμές αφορούν όλους τους δημόσιους φορείς και δεν συνδέονται με κάποια σύμβαση.
            </p>
            <p>
              {municipalPayments > 0 && <>Από αυτά, τα {mil(municipalPayments)} αφορούν σε πληρωμές δήμων. </>}
              {payroll.payments > 0 && <>Η μισθοδοσία προσωπικού είναι {num(payroll.payments)} πληρωμές από {num(payroll.bodies)} {payrollOnlyMunicipal ? 'δήμους' : 'φορείς'} ({amount(payroll.amount)}).</>}
              {derived.payrollTotal > 0 && derived.payrollAugSep > derived.payrollTotal / 2 && <> Οι περισσότερες αναρτήθηκαν τον Αύγουστο και τον Σεπτέμβριο ({num(derived.payrollAugSep)} από τις {num(derived.payrollTotal)}).</>}
              {vegetation.payments > 0 && <> Οι αντιπυρικές ζώνες, οι καθαρισμοί και τα κλαδέματα είναι {num(vegetation.payments)} πληρωμές ({amount(vegetation.amount)}).</>}
              {rrf.payments > 0 && <> {rrf.payments === 1 ? 'Μία πληρωμή' : `${capitalize(count(rrf.payments, true))} πληρωμές`} του Υπουργείου Περιβάλλοντος και Ενέργειας για {rrf.payments === 1 ? 'έργο' : 'έργα'} του Ταμείου Ανάκαμψης {rrf.payments === 1 ? 'αντιστοιχεί' : 'αντιστοιχούν'} σε {amount(rrf.amount)}.</>}
            </p>
            <p>
              {equipment.payments > 0 && <>Ο εξοπλισμός και η συντήρηση μέσων είναι {num(equipment.payments)} πληρωμές ({amount(equipment.amount)}){machinery.payments > 0 ? ` και οι μισθώσεις μηχανημάτων ${num(machinery.payments)} (${amount(machinery.amount)})` : ''}.</>}
              {fuel.payments > 0 && <> Τα καύσιμα και τα λιπαντικά είναι μόνο {num(fuel.payments)} πληρωμές από {num(fuel.bodies)} φορείς ({amount(fuel.amount)}).</>}

            </p>
            <ChartFrame
              title={`Πληρωμές ${REPORT_YEAR} ανά κατηγορία`}
              subtitle="Πληρωμές χωρίς σύνδεση με σύμβαση, ποσά χωρίς ΦΠΑ. Οι κατηγορίες προκύπτουν από το θέμα κάθε πληρωμής."
              table={<DataTable caption="Πληρωμές ανά κατηγορία" columns={[{ key: 'label', label: 'Κατηγορία' }, { key: 'payments', label: 'Πληρωμές', numeric: true }, { key: 'amount', label: 'Ποσό χωρίς ΦΠΑ', numeric: true }, { key: 'rule', label: 'Κανόνας' }]}
                rows={p.by_category.map(item => ({ label: item.label, payments: num(item.payments), amount: mil(item.amount), rule: item.rule }))} />}
              note="Με γκρι: πληρωμές των οποίων το θέμα δεν περιγράφει τη δαπάνη."
            >
              <BarList items={categoryItems} color={COLORS.accent} mutedColor={COLORS.gray} />
            </ChartFrame>
          </Section>

          <Section id="methodologia" eyebrow="Μεθοδολογία" title="Περιορισμοί και παραδοχές στην ανάλυση">
            <p>
              Η ανάλυση για το διάστημα {shortDate(report.meta.period_start)}–{periodEnd} στηρίζεται σε {num(contracts.contracts)} ενεργές συμβάσεις και {num(p.records)} πράξεις «Οριστικοποίηση πληρωμής» της Διαύγειας που έχουν εντοπιστεί στη συλλογή μας.
              {' '}Μετά τους ελέγχους ακυρώσεων και συνάφειας, καθώς και την αποφυγή διπλής καταμέτρησης με τις συμβάσεις, {num(p.counted)} πληρωμές προστίθενται στην εκτίμηση δαπανών.
              {' '}Τα μεγέθη αυτά περιγράφουν τα στοιχεία που συγκεντρώσαμε και δεν αποτελούν το σύνολο των αναρτήσεων των δημόσιων φορέων.
            </p>

            <h4>Πηγές και συλλογή στοιχείων</h4>
            <p>
              Συλλέγουμε συμβάσεις από το <ExternalLink href={SOURCES.kimdis}>Κεντρικό Ηλεκτρονικό Μητρώο Δημοσίων Συμβάσεων (ΚΗΜΔΗΣ)</ExternalLink>, με αναζητήσεις βάσει κωδικών CPV, δηλαδή της κοινής ταξινόμησης του αντικειμένου των δημόσιων συμβάσεων.
              {' '}Για ευρείς κωδικούς, που καλύπτουν και άσχετες δραστηριότητες, εφαρμόζουμε επιπλέον έλεγχο συνάφειας στον τίτλο.
              {' '}Στη <ExternalLink href={SOURCES.diavgeia}>Διαύγεια</ExternalLink> αναζητούμε λέξεις και παραλλαγές τους, όπως «πυροπροστασία», «αποψίλωση», «δασοπροστασία» και «αντιπυρική», και εξετάζουμε τους τίτλους και, όπου έχει εξαχθεί αναγνώσιμο κείμενο, τα συνοδευτικά έγγραφα.
              {' '}Εφαρμόζουμε επίσης κανόνες αποκλεισμού για άσχετους φορείς και αντικείμενα, όπως νοσοκομεία, σχολεία και δαπάνες πυροσβεστήρων.
            </p>
            <p>
              Στην ανάλυση πληρωμών χρησιμοποιούμε τις πράξεις «Οριστικοποίηση πληρωμής». Εγκρίσεις δαπανών, αναλήψεις υποχρέωσης και αποφάσεις ανάθεσης δεν λογίζονται από μόνες τους ως πληρωμές.
              {' '}Τα ποσά χρηματοδότησης των δήμων προέρχονται χωριστά από τις αποφάσεις κατανομής του Υπουργείου Εσωτερικών μέσω των Κεντρικών Αυτοτελών Πόρων.
            </p>

            <h4>Πώς μετράμε τις συμβάσεις</h4>
            <p>
              Εξαιρούμε τις ακυρωμένες συμβάσεις και κρατάμε μία εγγραφή ανά σύμβαση, με βάση τον μοναδικό κωδικό της (ΑΔΑΜ).
              {' '}Όταν υπάρχουν διαδοχικές εκδοχές ή τροποποιήσεις, κρατάμε την τελευταία ενεργή εκδοχή και δεν αθροίζουμε την αρχική με εκείνη που την αντικαθιστά.
              {' '}Για την περίοδο της αναφοράς, εξαιρούνται {num(report.dataQuality.superseded_or_duplicate_report_year_contract_rows)} συμβάσεις που είτε είχαν τροποποιηθεί από νεότερη σύμβαση, είτε αποτελούσαν διπλές καταχωρήσεις.
            </p>
            <p>
              Η ένταξη μιας σύμβασης στο έτος και στον μήνα γίνεται με την ημερομηνία υπογραφής της. Ως αξία μιας σύμβασης εννούμε τη συμβατική αξία χωρίς ΦΠΑ. Η υπογραφή μιας σύμβασης δεν συνεπάγεται οτι η εργασία που περιγράφει έχει ολοκληρωθεί ή οτι το ποσό έχει πράγματι καταβληθεί.
              {' '}Στις συμφωνίες-πλαίσιο, η αξία μιας σύμβασης μπορεί να εκφράζει το ανώτατο διαθέσιμο ποσό που μπορεί να δαπανηθεί.
              {' '}Στις συγκρίσεις με προηγούμενα χρόνια χρησιμοποιούμε το ίδιο ημερολογιακό διάστημα (π.χ. εάν εξετάζουμε το διάστημα 1/1/2026 έως 23/10/2026, η σύγκριση με το προηγούμενο έτος θα αφορά το διάστημα 1/1/2025 - 23/10/2025). Η υπογραφή πριν ή μετά την 1η Μαΐου δείχνει τον χρόνο της συμβατικής δέσμευσης και δεν προσδιορίζει από μόνη της πότε εκτελέστηκαν οι εργασίες.
            </p>

            <h4>Πληρωμές, ακυρώσεις και κατηγοριοποίηση</h4>
            <p>
              Στις πληρωμές που εντοπίζουμε στη Διαύγεια, θεωρούμε ως ημερομηνία πληρωμής την ημερομηνία ανάρτησης της απόφασης στη Διαύγεια, η οποία μπορεί να απέχει από την ημερομηνία των εργασιών ή του ίδιου του εντάλματος.
              {' '}Όταν μία πράξη περιλαμβάνει περισσότερα ποσά δικαιούχων, κατά κανόνα τα αθροίζουμε. Οι συγκεντρωτικές καταστάσεις χρειάζονται ιδιαίτερο έλεγχο, επειδή μπορεί να περιλαμβάνουν και άσχετες πληρωμές. Όταν εντοπίζουμε τέτοια έγγραφα, διορθώνουμε το ποσό της πληρωμής ώστε να αντανακλά μόνο την πληρωμή για εργασία που σχετίζεται με την πυροπροστασία (π.χ. 9ΥΡ77Λ7-ΥΚ9).
            </p>
            <p>
              Δεν προσμετράμε στην ανάλυση ακυρωτικές πράξεις, ενώ αφαιρούμε και την αρχική πληρωμή όταν εντοπίζεται ότι ακυρώθηκε.
              {' '}Η αντιστοίχιση γίνεται από τον ΑΔΑ που αναφέρει η ακυρωτική πράξη ή, όταν δεν υπάρχει τέτοια αναφορά, από προγενέστερη πληρωμή του ίδιου φορέα με το ίδιο απόλυτο ποσό. Η δεύτερη περίπτωση αποτελεί παραδοχή της ανάλυσης.
            </p>
            <p>
              Οι επιμέρους κατηγορίες δαπανών προκύπτουν από λέξεις στο θέμα της απόφασης, με ειδικές διορθώσεις όπου έχει ελεγχθεί το έγγραφο.
              {' '}Οι «Λοιπές δαπάνες πυροπροστασίας» συγκεντρώνουν όσες δεν κατατάσσονται στις ειδικότερες κατηγορίες· η ονομασία αυτή δεν περιγράφει ένα ενιαίο είδος εργασίας.
              {' '}Για να προσμετρηθεί μισθοδοσία στην αναφορά απαιτούνται ταυτόχρονα ενδείξεις μισθολογικής δαπάνης και αναφορά σε πυροπροστασία, δασοπροστασία, πυρασφάλεια, πολιτική προστασία ή έκτακτες ανάγκες στον τίτλο/περιγραφή.
              {' '}Με αυτό το πρόσθετο κριτήριο εξαιρούνται από την αναφορά {num(p.report_excluded_payroll.payments)} πράξεις.
            </p>

            <h4>Συνδέσεις Διαύγειας–συμβάσεων και διπλή καταμέτρηση</h4>
            <p>
              Συνδέουμε πράξεις της Διαύγειας με συμβάσεις του ΚΗΜΔΗΣ μέσω των αναγνωριστικών τους, του ΑΔΑ και του ΑΔΑΜ, όταν προκύπτει τεκμηριωμένη αναφορά από τα στοιχεία ή τα έγγραφα και έχει καταγραφεί η αντίστοιχη σύνδεση.
              {' '}Η αναφορά χρησιμοποιεί αυτές τις καταγεγραμμένες συνδέσεις και δεν συμπεραίνει ότι δύο εγγραφές συνδέονται μόνο επειδή έχουν ίδιο ανάδοχο ή ίδιο ποσό.
              {' '}Οι {num(p.linked)} έγκυρες πληρωμές με σύνδεση σε σύμβαση δεν προστίθενται ξανά στο σύνολο.
              {' '}Η εκτίμηση προκύπτει έτσι από την αξία των συμβάσεων της περιόδου συν τις {num(p.counted)} πληρωμές χωρίς καταγεγραμμένη σύνδεση.
            </p>
            <p>
              Η αντιστοίχιση παραμένει ελλιπής: για {num(p.report_year_contracts_with_linked_payment)} από τις {num(contracts.contracts)} συμβάσεις του {REPORT_YEAR} έχει εντοπιστεί συνδεδεμένη πληρωμή. Οι συμβάσεις αυτές αντιπροσωπεύουν το {pct(p.report_year_contracts_with_linked_payment_value_pct)} της συμβατικής αξίας.
              {p.subject_refs_not_in_db > 0 && <> Επιπλέον, {num(p.subject_refs_not_in_db)} πληρωμές αναφέρουν κωδικούς συμβάσεων που δεν υπάρχουν στη συλλογή μας.</>}
              {' '}Η απουσία σύνδεσης δεν αποδεικνύει ότι μια σύμβαση δεν πληρώθηκε ή ότι μια πληρωμή αφορά διαφορετική δαπάνη. Συνεπώς, παραμένει ενδεχόμενο διπλής καταμέτρησης όπου η σύνδεση δεν έχει εντοπιστεί.
            </p>

            <h4>Ποσά και ΦΠΑ</h4>
            <p>
              Παρουσιάζουμε τα συνολικά ποσά χωρίς ΦΠΑ. Για τις συμβάσεις χρησιμοποιούμε τη δηλωμένη αξία χωρίς ΦΠΑ του ΚΗΜΔΗΣ.
              {' '}Για τις πληρωμές της Διαύγειας θεωρούμε κατά κανόνα ότι το καταχωρισμένο ποσό περιλαμβάνει ΦΠΑ {pct(report.meta.vat_rate * 100)} και το διαιρούμε με {(1 + report.meta.vat_rate).toLocaleString('el-GR', { maximumFractionDigits: 2 })}.
              {' '}Δεν αφαιρούμε ΦΠΑ από μισθοδοσία και από δαπάνες που αναγνωρίζονται ως εισφορές, κρατήσεις, αποδόσεις φόρων, επιχορηγήσεις, παράβολα, ανταποδοτικά τέλη, οδοιπορικά ή ημερήσιες αποζημιώσεις. Η αναφορά σε επιχορήγηση μαζί με προμήθεια αντιμετωπίζεται ως προμήθεια με ΦΠΑ.
              {' '}Η αναγνώριση βασίζεται στο θέμα της απόφασης. Ο γενικός συντελεστής είναι παραδοχή και δεν επιβεβαιώνεται ανά παραστατικό· αν ισχύει μειωμένος συντελεστής ή απαλλαγή που δεν αναγνωρίστηκε, η καθαρή αξία υποεκτιμάται.
            </p>

            <h4>Έλεγχοι και μόνιμες διορθώσεις (overrides)</h4>
            <p>
              Όταν εντοπίζουμε ασυμφωνία, συγκρίνουμε τα διαθέσιμα πεδία με το επίσημο έγγραφο και καταγράφουμε την ελεγμένη διόρθωση ανά ΑΔΑ ή ΑΔΑΜ.
              {' '}Αυτές οι μόνιμες εξαιρέσεις διορθώνουν συγκεκριμένα ποσά ή κατηγορίες και υπερισχύουν των αντίστοιχων αυτόματων τιμών, ώστε μια νέα εισαγωγή να μην επαναφέρει το ίδιο σφάλμα.
              {' '}Έχουν διορθωθεί λανθασμένες τάξεις μεγέθους στα ποσά και περιπτώσεις παρατάσεων συμβάσεων που δεν δημιουργούσαν πρόσθετη δαπάνη.
            </p>
            <p>
              Ενδεικτικά παραδείγματα ελέγχων της συλλογής, συμπεριλαμβανομένων παλαιότερων ετών:
            </p>
            <ul className="yr-bullets">
              <li>
                <strong>Ποσό περίπου 100 φορές μεγαλύτερο στη Διαύγεια.</strong> Στα δομημένα δεδομένα της απόφασης <ExternalLink href="https://diavgeia.gov.gr/doc/Ψ94Σ4653Π8-ΦΚΕ">Ψ94Σ4653Π8-ΦΚΕ</ExternalLink> του Υπουργείου Περιβάλλοντος και Ενέργειας εμφανιζόταν ποσό 17.882.628,00 €, ενώ το επίσημο έγγραφο αναφέρει εντολή πληρωμής 178.826,27 €. Κρατήσαμε το ποσό του εγγράφου, πριν από την αφαίρεση ΦΠΑ για την ανάλυση.
              </li>
              <li>
                <strong>Δεύτερη αντίστοιχη διόρθωση στη Διαύγεια.</strong> Στην απόφαση <ExternalLink href="https://diavgeia.gov.gr/doc/ΨΞΩΥ4653Π8-ΡΓΘ">ΨΞΩΥ4653Π8-ΡΓΘ</ExternalLink> του ίδιου υπουργείου, το πεδίο ποσού έδινε 5.489.497,00 €, αντί των 54.894,97 € της εντολής πληρωμής στο έγγραφο — επίσης περίπου 100 φορές μεγαλύτερο ποσό.
              </li>
              <li>
                <strong>Ποσό 1.000 φορές μεγαλύτερο στο ΚΗΜΔΗΣ.</strong> Για τη σύμβαση <ExternalLink href="https://cerpp.eprocurement.gov.gr/khmdhs-opendata/contract/attachment/26SYMV019592516">26SYMV019592516</ExternalLink> του Δήμου Μεγαρέων, για απομάκρυνση υπολειμμάτων βλάστησης στη Νέα Πέραμο, τα αρχικά δεδομένα έδιναν 29.920.000,00 € χωρίς ΦΠΑ. Η υπογεγραμμένη σύμβαση ορίζει 29.920,00 € χωρίς ΦΠΑ, ποσό που χρησιμοποιούμε μετά τη διόρθωση.
              </li>
              <li>
                <strong>Συνολική χρηματοδότηση αντί για αξία σύμβασης.</strong> Στη σύμβαση <ExternalLink href="https://cerpp.eprocurement.gov.gr/khmdhs-opendata/contract/attachment/24SYMV015544651#page=6">24SYMV015544651</ExternalLink>, για το τμήμα της Σπάρτης, είχαν καταχωριστεί 31.024.697,04 € χωρίς ΦΠΑ. Το ποσό αυτό αφορά τη συνεισφορά του Ταμείου Ανάκαμψης στον συνολικό προϋπολογισμό του έργου. Το άρθρο 5.1 της σύμβασης ορίζει την αμοιβή του συγκεκριμένου αναδόχου σε 4.003.194,80 € χωρίς ΦΠΑ, την οποία και προσμετράμε.
              </li>
              <li>
                <strong>Παρατάσεις χωρίς πρόσθετο κόστος.</strong> Οι πράξεις <ExternalLink href="https://cerpp.eprocurement.gov.gr/khmdhs-opendata/contract/attachment/25SYMV017249656">25SYMV017249656</ExternalLink> και <ExternalLink href="https://cerpp.eprocurement.gov.gr/khmdhs-opendata/contract/attachment/25SYMV017327213">25SYMV017327213</ExternalLink> αφορούν παρατάσεις της ίδιας σύμβασης χωρίς αύξηση της αξίας της. Στις διορθώσεις καταγράφεται μηδενική πρόσθετη δαπάνη, ώστε η παράταση να μη δημιουργεί δεύτερη χρέωση του αρχικού ποσού.
              </li>
              <li>
                <strong>Κωδικός δαπάνης αντί για ποσό.</strong> Στην απόφαση <ExternalLink href="https://diavgeia.gov.gr/doc/Ψ86Ε46Ψ844-ΓΥ4">Ψ86Ε46Ψ844-ΓΥ4</ExternalLink> του Πράσινου Ταμείου είχε μεταφερθεί ως ποσό ένας κωδικός δαπάνης (ΚΑΕ), αντί για το ποσό του εντάλματος.
              </li>
              <li>
                <strong>Επιλογή της σχετικής πληρωμής από συγκεντρωτική κατάσταση.</strong> Στην απόφαση <ExternalLink href="https://diavgeia.gov.gr/doc/9ΥΡ77Λ7-ΥΚ9">9ΥΡ77Λ7-ΥΚ9</ExternalLink> της Περιφέρειας Αττικής κρατήσαμε από τη συγκεντρωτική κατάσταση μόνο την πληρωμή που αφορά αποψίλωση για πρόληψη πυρκαγιών και τη μεταφέραμε στην αντίστοιχη κατηγορία.
              </li>
            </ul>
            <p>
              Εγγραφές που διαπιστώνεται ότι βρίσκονται εκτός του αντικειμένου αφαιρούνται και αποκλείονται από επόμενες εισαγωγές. Ο έλεγχος των εγγράφων δεν έχει ολοκληρωθεί για όλο το υλικό.
            </p>

            <h4>Τι δεν μπορούν να δείξουν με ακρίβεια τα δεδομένα</h4>
            <p>
              Λανθασμένα ποσά, ελλιπής κωδικοποίηση των εργασιών, ασαφείς τίτλοι και καθυστερημένες αναρτήσεις δυσκολεύουν την παρακολούθηση της ροής δαπανών κάθε φορέα.
              {' '}Οι αναζητήσεις με κωδικούς και λέξεις μπορεί να παραλείπουν σχετικές δαπάνες με διαφορετική περιγραφή ή να περιλαμβάνουν δαπάνες που χρειάζονται περαιτέρω έλεγχο συνάφειας.
              {' '}Δεν είναι επομένως δυνατό να πιστοποιήσουμε από αυτή τη συλλογή πώς δαπανήθηκε το σύνολο της χρηματοδότησης κάθε δήμου ή το πλήρες κόστος της δασικής πυροπροστασίας.
              {' '}Η εκτίμηση συνδυάζει συμβατικές δεσμεύσεις και καταχωρισμένες πληρωμές και δεν αποτελεί πλήρη ταμειακό απολογισμό του έτους.
              {' '}Όπου λείπει το ποσό, μια εγγραφή μπορεί να περιλαμβάνεται στο πλήθος χωρίς να προσθέτει αξία στο σύνολο.
            </p>
            <p>
              Πρόσθετη δυσκολία είναι η ασυνέπεια στις ονομασίες και στη γεωγραφική αντιστοίχιση των φορέων. Οι πηγές χρησιμοποιούν άλλοτε νομούς και άλλοτε περιφερειακές ενότητες, ενώ οι δήμοι δεν εμφανίζονται πάντα με την ίδια ονομασία ή γεωγραφική κάλυψη.
              {' '}Για παράδειγμα, στοιχεία για τον Δήμο Δυτικής Λέσβου και τον Δήμο Μυτιλήνης χρειάζεται να αντιστοιχιστούν με την ενιαία καταχώριση «Λέσβος» που υπάρχει σε άλλο μέρος των δεδομένων.
              {' '}Για να συγκρίνουμε χρηματοδοτήσεις, συμβάσεις και πληρωμές, αντιστοιχίζουμε τις διαφορετικές ονομασίες σε κοινές γεωγραφικές ενότητες. Όπου τα στοιχεία παραμένουν ενοποιημένα, η κατανομή των ποσών ανά σημερινό δήμο δεν μπορεί να γίνει με ασφάλεια χωρίς πρόσθετη τεκμηρίωση.
            </p>
            <p>
              {report.meta.year_complete ? `Η αναφορά καλύπτει ολόκληρο το ${REPORT_YEAR}.` : `Η αναφορά ενημερώνεται καθημερινά έως το τέλος του ${REPORT_YEAR}.`}
              {' '}Τελευταία ενημέρωση: {updatedLabel}. Κάθε έκδοση αποτυπώνει τη συλλογή εκείνης της στιγμής και μπορεί να αναθεωρηθεί με νέες αναρτήσεις, συνδέσεις ή ελεγμένες διορθώσεις.
            </p>
          </Section>
        </div>
      </div>
    </article>
  )
}
