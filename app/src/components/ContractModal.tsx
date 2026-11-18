import { useProcurementI18n } from '../i18n/useProcurementI18n'
import { useOverviewUi } from '../i18n/overview'
import BeneficiaryLink from './BeneficiaryLink'
import { useDevViewEnabled } from '../lib/devView'

type ContractModalContract = {
  sourceValues?: {
    // Some timeline cards display a signed date; this is never a substitute
    // for a missing submission date in the source record.
    displayDate?: string | null
    submittedAt: string | null
    signedAt: string | null
    startDate: string | null
    endDate: string | null
    noEndDate?: boolean
    withoutVat: number | null
    withVat: number | null
  }
  id: string
  who: string
  what: string
  when: string
  why: string
  beneficiary: string
  beneficiaryGemi?: string | null
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
  const { p, format, category, description, cpv, organization } = useProcurementI18n()
  const ui = useOverviewUi()
  const source = contract.sourceValues
  const dates = source ? {
    when: format.date(source.submittedAt), signedAt: format.date(source.signedAt),
    startDate: format.date(source.startDate), endDate: source.noEndDate ? ui('Χωρίς λήξη') : format.date(source.endDate),
  } : contract
  const withoutVat = source ? format.currency(source.withoutVat, { maximumFractionDigits: 0 }) : contract.withoutVatAmount
  const withVat = source ? format.currency(source.withVat, { maximumFractionDigits: 0 }) : contract.withVatAmount
  const [devViewEnabled] = useDevViewEnabled()
  const cpvItems = (contract.cpvItems ?? []).filter((x) => x.code || x.label)
  const fallbackCpv = `${cpv(contract.cpvCode, contract.cpv)} (${contract.cpvCode})`
  const gridItems = [
    { label: p('submitted'), value: dates.when },
    { label: p('uniqueAdam'), value: contract.referenceNumber },
    { label: p('contractKind'), value: category(contract.contractKind) },
    { label: p('signed'), value: dates.signedAt },
    { label: p('start'), value: dates.startDate },
    { label: p('end'), value: dates.endDate },
    { label: p('awardProcedure'), value: category(contract.awardProcedure) },
    { label: p('signature'), value: contract.signers },
    { label: p('procedureType'), value: category(contract.contractType) },
    {
      label: p('beneficiary'),
      value: (
        <BeneficiaryLink
          name={contract.beneficiary}
          gemi={contract.beneficiaryGemi}
          className="beneficiary-link"
        />
      ),
    },
    { label: p('beneficiaryTaxId'), value: contract.beneficiaryVat },
    { label: p('amountWithVat'), value: withVat },
    { label: p('department'), value: contract.unitsOperator },
    {
      label: 'CPV',
      value: cpvItems.length > 0 ? (
        cpvItems.map((item, idx) => (
          <span key={`${item.code}-${item.label}-${idx}`} style={{ display: 'block' }}>
            {cpv(item.code, item.label)} ({item.code})
          </span>
        ))
      ) : (
        fallbackCpv
      ),
    },
    { label: p('relatedAda'), value: contract.contractRelatedAda },
    { label: p('auctionReference'), value: contract.auctionRefNo },
    { label: p('amendsContract'), value: contract.previousReferenceNumber },
    { label: p('amendedByContract'), value: contract.nextReferenceNumber },
  ]

  return (
    <div className="contract-modal-backdrop" onClick={onClose}>
      <article className="contract-modal" onClick={(e) => e.stopPropagation()}>
        <header className="contract-modal__header">
          <div>
            <span className="eyebrow">{organization(contract.who)}</span>
            <h2 lang="el">{contract.what}</h2>
          </div>
          <div className="contract-modal__actions">
            {onDownloadPdf && (
              <button
                type="button"
                className="contract-modal__icon-button"
                onClick={onDownloadPdf}
                aria-label={p('downloadContract')}
                title={p('downloadContract')}
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
              aria-label={p('close')}
              title={p('close')}
            >
              ✕
            </button>
          </div>
        </header>

        <p className="contract-modal__subtitle">{description(contract.why)}</p>

        <div className="contract-modal__highlight">
          <span className="contract-modal__amount">{withoutVat}</span>
          <span className="contract-modal__arrow">→</span>
          <BeneficiaryLink
            name={contract.beneficiary}
            gemi={contract.beneficiaryGemi}
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
