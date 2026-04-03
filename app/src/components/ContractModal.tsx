import BeneficiaryLink from './BeneficiaryLink'
import { useDevViewEnabled } from '../lib/devView'

type ContractModalContract = {
  id: string
  who: string
  what: string
  when: string
  why: string
  beneficiary: string
  contractType: string
  howMuch: string
  withoutVatAmount: string
  withVatAmount: string
  referenceNumber: string
  contractNumber: string
  cpv: string
  cpvCode: string
  cpvItems?: Array<{ code: string; label: string }>
  signedAt: string
  startDate: string
  endDate: string
  organizationVat: string
  beneficiaryVat: string
  signers: string
  assignCriteria: string
  contractKind: string
  awardProcedure: string
  unitsOperator: string
  fundingCofund: string
  fundingSelf: string
  fundingEspa: string
  fundingRegular: string
  auctionRefNo: string
  paymentRefNo: string
  shortDescription: string
  rawBudget: string
  contractBudget: string
  contractRelatedAda: string
  previousReferenceNumber: string
  nextReferenceNumber: string
  documentUrl: string | null
}

type Props = {
  contract: ContractModalContract
  onClose: () => void
  onDownloadPdf?: () => void
}

export type { ContractModalContract }

export default function ContractModal({ contract, onClose, onDownloadPdf }: Props) {
  const [devViewEnabled] = useDevViewEnabled()
  const cpvItems = (contract.cpvItems ?? []).filter((x) => x.code || x.label)
  const fallbackCpv = `${contract.cpv} (${contract.cpvCode})`
  const gridItems = [
    { label: 'Υπεβλήθη', value: contract.when },
    { label: 'Μοναδικός Κωδικός - ΑΔΑΜ', value: contract.referenceNumber },
    { label: 'Τύπος σύμβασης', value: contract.contractKind },
    { label: 'Υπεγράφη', value: contract.signedAt },
    { label: 'Έναρξη', value: contract.startDate },
    { label: 'Λήξη', value: contract.endDate },
    { label: 'Διαδικασία ανάθεσης', value: contract.awardProcedure },
    { label: 'Υπογραφή', value: contract.signers },
    { label: 'Τύπος διαδικασίας', value: contract.contractType },
    {
      label: 'Δικαιούχος',
      value: (
        <BeneficiaryLink
          name={contract.beneficiary}
          afm={contract.beneficiaryVat}
          className="beneficiary-link"
        />
      ),
    },
    { label: 'ΑΦΜ Δικαιούχου', value: contract.beneficiaryVat },
    { label: 'Ποσό με ΦΠΑ', value: contract.withVatAmount },
    { label: 'Τμήμα', value: contract.unitsOperator },
    {
      label: 'CPV',
      value: cpvItems.length > 0 ? (
        cpvItems.map((item, idx) => (
          <span key={`${item.code}-${item.label}-${idx}`} style={{ display: 'block' }}>
            {item.label} ({item.code})
          </span>
        ))
      ) : (
        fallbackCpv
      ),
    },
    { label: 'ΑΔΑ Εγγράφου στη Διαύγεια', value: contract.contractRelatedAda },
    { label: 'Κωδικός δημοπρασίας', value: contract.auctionRefNo },
    { label: 'Τροποποιεί τη σύμβαση', value: contract.previousReferenceNumber },
    { label: 'Τροποποιείται από τη σύμβαση', value: contract.nextReferenceNumber },
  ]

  return (
    <div className="contract-modal-backdrop" onClick={onClose}>
      <article className="contract-modal" onClick={(e) => e.stopPropagation()}>
        <header className="contract-modal__header">
          <div>
            <span className="eyebrow">{contract.who}</span>
            <h2>{contract.what}</h2>
          </div>
          <div className="contract-modal__actions">
            {onDownloadPdf && (
              <button
                type="button"
                className="contract-modal__icon-button"
                onClick={onDownloadPdf}
                aria-label="Κατέβασμα σύμβασης"
                title="Κατέβασμα σύμβασης"
              >
                <svg viewBox="0 0 20 20" aria-hidden="true">
                  <path d="M10 2.5v8.2" />
                  <path d="M6.8 8.9 10 12.1l3.2-3.2" />
                  <path d="M4 14.5h12" />
                </svg>
              </button>
            )}
            <button
              type="button"
              className="contract-modal__icon-button"
              onClick={onClose}
              aria-label="Κλείσιμο"
              title="Κλείσιμο"
            >
              ✕
            </button>
          </div>
        </header>

        <p className="contract-modal__subtitle">{contract.why}</p>

        <div className="contract-modal__highlight">
          <span className="contract-modal__amount">{contract.withoutVatAmount}</span>
          <span className="contract-modal__arrow">→</span>
          <BeneficiaryLink
            name={contract.beneficiary}
            afm={contract.beneficiaryVat}
            className="contract-modal__beneficiary beneficiary-link"
          />
        </div>

        <div className="contract-modal__grid">
          {gridItems.map((item, index) => (
            <div key={item.label}>
              <span>{devViewEnabled ? `${index + 1}. ${item.label}` : item.label}</span>
              <strong>{item.value}</strong>
            </div>
          ))}
        </div>
      </article>
    </div>
  )
}
