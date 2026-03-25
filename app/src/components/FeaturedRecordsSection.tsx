import type { ContractModalContract } from './ContractModal'
import DataLoadingCard from './DataLoadingCard'
import type { LatestContractCardView } from './LatestContractCard'

export type FeaturedRecordContract = LatestContractCardView & ContractModalContract & {
  id: string
  what: string
  withoutVatAmount: string
}

export type BeneficiaryInsightRow = {
  beneficiary: string
  organization: string
  totalAmount: number
  contractCount: number
  cpv: string
  startDate: string
  endDate: string
  duration: string
  progressPct: number | null
  signer: string
  relevantContracts: FeaturedRecordContract[]
}

type Props = {
  year: string
  rows: BeneficiaryInsightRow[]
  loading: boolean
  formatEur: (n: number | null) => string
  onOpenContract: (contract: FeaturedRecordContract) => void
}

export default function FeaturedRecordsSection({
  year,
  rows,
  loading,
  formatEur,
  onOpenContract,
}: Props) {
  return (
    <section id="records" className="records section-rule">
      <div className="section-head">
        <div className="eyebrow">{`Δικαιούχοι / ${year}`}</div>
        <h2>Εταιρείες με συμβάσεις έργων πυροπροστασίας</h2>
        <section className='ca-header-note'>Οι ανάδοχοι ταξινομούνται με βάση το συνολικό ποσό των συμβάσεων που έχουν λάβει από δήμους, περιφέρειες και άλλους δημόσιους φορείς.</section>
      </div>

      <div className="records-grid records-grid--horizontal">
        {loading && (
          <DataLoadingCard
            className="records-grid__loading-card"
            message={`Ανακτώνται οι δικαιούχοι και οι συμβάσεις τους για το ${year}.`}
          />
        )}

        {!loading && rows.map((row, idx) => (
          <article
            className="record-card"
            key={`${row.beneficiary}-${idx}`}
          >
            <div className="record-card__year" aria-hidden="true">{year}</div>
            <div className="record-card__header">
              <div className="record-card__authority">#{idx + 1}</div>
              <div className="record-card__id">Συμβάσεις: {row.contractCount.toLocaleString('el-GR')}</div>
            </div>
            <h3>{row.beneficiary}</h3>
            <div className="record-card__amount">{formatEur(row.totalAmount)}</div>
            <div className="record-card__tags" aria-label="Μεταδεδομένα δικαιούχου">
              <span>CPV: {row.cpv}</span>
              <span>Έναρξη: {row.startDate}</span>
            </div>
            <div className="record-duration">
              <div className="record-duration__head">
                <span>Διάρκεια: {row.duration}</span>
                <span>Λήξη: {row.endDate}</span>
              </div>
              <div className="record-duration__track" aria-label="Πρόοδος διάρκειας έργου">
                <div
                  className="record-duration__fill"
                  style={{ width: `${row.progressPct == null ? 0 : row.progressPct}%` }}
                />
                <div
                  className="record-duration__today"
                  style={{ left: `${row.progressPct == null ? 0 : row.progressPct}%` }}
                  title="Σήμερα"
                />
              </div>
            </div>
            {row.relevantContracts.length > 0 && (
              <div className="record-contract-amounts" aria-label="Σχετικές συμβάσεις">
                <div className="record-contract-amounts__title">Σχετικές συμβάσεις</div>
                <ul>
                  {row.relevantContracts.map((contract) => (
                    <li key={`${row.beneficiary}-contract-${contract.id}`}>
                      <button
                        type="button"
                        className="record-contract-link"
                        onClick={() => onOpenContract(contract)}
                      >
                        {contract.what} - {contract.withoutVatAmount}
                      </button>
                    </li>
                  ))}
                </ul>
              </div>
            )}
            <div className="record-card__footer">
              <div>
                <span className="label">Οργανισμός</span>
                <strong>{row.organization}</strong>
              </div>
              <div>
                <span className="label">Υπογράφων</span>
                <strong>{row.signer}</strong>
              </div>
            </div>
          </article>
        ))}
      </div>
    </section>
  )
}
