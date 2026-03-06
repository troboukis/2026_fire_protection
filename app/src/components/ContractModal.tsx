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
  documentUrl: string | null
}

type Props = {
  contract: ContractModalContract
  onClose: () => void
  onDownloadPdf?: () => void
}

export type { ContractModalContract }

export default function ContractModal({ contract, onClose, onDownloadPdf }: Props) {
  const cpvItems = (contract.cpvItems ?? []).filter((x) => x.code || x.label)
  const fallbackCpv = `${contract.cpv} (${contract.cpvCode})`
  return (
    <div className="contract-modal-backdrop" onClick={onClose}>
      <article className="contract-modal" onClick={(e) => e.stopPropagation()}>
        <header className="contract-modal__header">
          <div>
            <span className="eyebrow">{contract.who}</span>
            <h2>{contract.what}</h2>
          </div>
          <button type="button" onClick={onClose} aria-label="Κλείσιμο">
            ✕
          </button>
        </header>

        <p className="contract-modal__subtitle">{contract.why}</p>

        <div className="contract-modal__highlight">
          <span className="contract-modal__amount">{contract.withoutVatAmount}</span>
          <span className="contract-modal__arrow">→</span>
          <span className="contract-modal__beneficiary">{contract.beneficiary}</span>
        </div>

        <div className="contract-modal__grid">
          <div><span>Ημερομηνία</span><strong>{contract.when}</strong></div>
          <div><span>Τύπος Διαδικασίας</span><strong>{contract.contractType}</strong></div>
          <div><span>Κωδ. Αναφοράς</span><strong>{contract.referenceNumber}</strong></div>
          <div><span>Κωδ. Σύμβασης</span><strong>{contract.contractNumber}</strong></div>
          <div>
            <span>CPV</span>
            <strong>
              {cpvItems.length > 0 ? (
                cpvItems.map((item, idx) => (
                  <span key={`${item.code}-${item.label}-${idx}`} style={{ display: 'block' }}>
                    {item.label} ({item.code})
                  </span>
                ))
              ) : (
                fallbackCpv
              )}
            </strong>
          </div>
          <div><span>Δικαιούχος ΑΦΜ</span><strong>{contract.beneficiaryVat}</strong></div>
          <div><span>Φορέας ΑΦΜ</span><strong>{contract.organizationVat}</strong></div>
          <div><span>Κριτήριο Ανάθεσης</span><strong>{contract.assignCriteria}</strong></div>
          <div><span>Τύπος Σύμβασης</span><strong>{contract.contractKind}</strong></div>
          <div><span>Υπογράφοντες</span><strong>{contract.signers}</strong></div>
          <div><span>Υπεύθυνη Μονάδα</span><strong>{contract.unitsOperator}</strong></div>
          <div><span>Περιγραφή</span><strong>{contract.shortDescription}</strong></div>
          <div><span>Υπογραφή</span><strong>{contract.signedAt}</strong></div>
          <div><span>Έναρξη</span><strong>{contract.startDate}</strong></div>
          <div><span>Λήξη</span><strong>{contract.endDate}</strong></div>
          <div><span>Ποσό με ΦΠΑ</span><strong>{contract.withVatAmount}</strong></div>
          <div><span>Προϋπολογισμός</span><strong>{contract.rawBudget}</strong></div>
          <div><span>Contract Budget</span><strong>{contract.contractBudget}</strong></div>
          <div><span>Cofund</span><strong>{contract.fundingCofund}</strong></div>
          <div><span>Self Fund</span><strong>{contract.fundingSelf}</strong></div>
          <div><span>ESPA</span><strong>{contract.fundingEspa}</strong></div>
          <div><span>Regular Budget</span><strong>{contract.fundingRegular}</strong></div>
          <div><span>Auction Ref</span><strong>{contract.auctionRefNo}</strong></div>
          <div><span>Payment Ref</span><strong>{contract.paymentRefNo}</strong></div>
        </div>

        <footer className="contract-modal__footer">
          {onDownloadPdf && (
            <button
              type="button"
              className="contract-modal__pdf-button"
              onClick={onDownloadPdf}
            >
              Κατέβασε το
            </button>
          )}
          {contract.documentUrl && (
            <a href={contract.documentUrl} target="_blank" rel="noreferrer">
              Άνοιγμα εγγράφου στη Διαύγεια
            </a>
          )}
        </footer>
      </article>
    </div>
  )
}
