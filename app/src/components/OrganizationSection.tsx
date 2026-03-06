import type { ContractModalContract } from './ContractModal'

type OrganizationTimelineItem = {
  month: string
  year: string
  text: string
  contract: ContractModalContract | null
}

export type OrganizationSectionData = {
  name: string
  yearLabel: string
  totalSpend: number
  totalSpendNote: string
  cpvCodes: string[]
  topCpvValue: string | null
  contractCount: number
  beneficiaryCount: number
  latestSignedAt: string | null
  timeline: OrganizationTimelineItem[]
}

type OrganizationSectionProps = {
  data: OrganizationSectionData
  loading: boolean
  formatEurCompact: (n: number) => string
  formatDateEl: (iso: string | null) => string
  onOpenContract?: (contract: ContractModalContract) => void
}

export default function OrganizationSection({
  data,
  loading,
  formatEurCompact,
  formatDateEl,
  onOpenContract,
}: OrganizationSectionProps) {
  const timelineItems = data.timeline.length
    ? data.timeline
    : [{ month: '—', year: '—', text: loading ? 'Φόρτωση στοιχείων φορέα…' : 'Δεν βρέθηκαν συμβάσεις για τον φορέα.', contract: null }]
  const kpis = [
    {
      label: 'Η ΠΙΟ ΣΥΧΝΗ ΕΡΓΑΣΙΑ',
      value: data.topCpvValue ?? '—',
      note: 'η υπηρεσία με τις περισσότερες εμφανίσεις',
    },
    {
      label: 'ΣΥΜΒΑΣΕΙΣ',
      value: data.contractCount.toLocaleString('el-GR'),
      note: 'μοναδικές εγγραφές στη Βάση Δεδομένων',
    },
    {
      label: 'ΠΡΟΜΗΘΕΥΤΕΣ',
      value: data.beneficiaryCount.toLocaleString('el-GR'),
      note: 'δικαιούχοι',
    },
    {
      label: 'τελευταία',
      value: formatDateEl(data.latestSignedAt),
      note: 'η ημερομηνία υπογραφής της πιο πρόσφατης σύμβασης',
    },
  ]

  return (
    <section id="organizations" className="organization section-rule">
      <div className="organization__header">
        <div className="eyebrow">ΟΡΓΑΝΙΣΜΟΣ</div>
        <h2>{data.name}</h2>
        <p>
          Δυναμική ενημέρωση δεδομένων από το Kεντρικό Ηλεκτρονικό Μητρώο Δημοσίων Συμβάσεων.
        </p>
      </div>

      <div className="organization__hero">
        <div className="org-year" aria-hidden="true">
          {data.yearLabel}
        </div>
        <div className="org-total">
          <span className="eyebrow">Συνολική Δαπάνη</span>
          <div className="org-total__value">{formatEurCompact(data.totalSpend)}</div>
          <div className="org-total__note">
            {data.totalSpendNote}
          </div>
        </div>
        <div className="org-codes">
          <div className="eyebrow">Οι πιο συχνές κατηγορίες εργασιών</div>
          <div className="cpv-wall">
            {(data.cpvCodes.length ? data.cpvCodes : ['—']).map((code) => (
              <span key={code}>{code}</span>
            ))}
          </div>
        </div>
      </div>

      <div className="organization__grid">
        <div className="organization__kpis">
          {kpis.map((kpi) => (
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
              <li key={`${item.month}-${item.year}-${item.text}`}>
                <div className="timeline-date">
                  <span>{item.month}</span>
                  <strong>{item.year}</strong>
                </div>
                {item.contract && onOpenContract ? (
                  <button
                    type="button"
                    className="timeline-contract-link"
                    onClick={() => onOpenContract(item.contract!)}
                  >
                    {item.text}
                  </button>
                ) : (
                  <p>{item.text}</p>
                )}
              </li>
            ))}
          </ul>
        </div>
      </div>
    </section>
  )
}
