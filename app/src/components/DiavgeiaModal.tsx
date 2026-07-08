import type { ReactNode } from 'react'
import { createPortal } from 'react-dom'
import { Link } from 'react-router-dom'
import type { DiavgeiaDecisionCardView } from './DiavgeiaDecisionCard'

type Props = {
  decision: DiavgeiaDecisionCardView
  onClose: () => void
}

export default function DiavgeiaModal({ decision, onClose }: Props) {
  const organization = [decision.orgType, decision.orgNameClean]
    .filter((part) => part && part !== '—')
    .join(' · ') || '—'
  const documentUrl = decision.documentUrl ?? (decision.ada && decision.ada !== '—'
    ? `https://diavgeia.gov.gr/doc/${decision.ada}`
    : null)
  const isMunicipality = !!decision.municipalityKey
  const organizationValue = isMunicipality ? (
    <Link
      className="contract-modal__text-link"
      to={`/municipalities?municipality=${encodeURIComponent(decision.municipalityKey!)}`}
    >
      {organization}
    </Link>
  ) : organization
  const gridItems: Array<{ label: string; value: ReactNode }> = [
    { label: 'Ημερομηνία απόφασης', value: decision.when },
    { label: 'Φορέας', value: organizationValue },
    { label: 'Ποσό', value: decision.amount ?? '—' },
    { label: 'Αριθμός πρωτοκόλλου', value: decision.protocolNumber ?? '—' },
    { label: 'Υπογράφοντες', value: decision.spendingSigners ?? '—' },
    { label: 'Ανάδοχος / δικαιούχος', value: decision.spendingContractorsName ?? '—' },
    { label: 'ΑΦΜ', value: decision.spendingContractorsAfm ?? '—' },
    { label: 'Θεματικές κατηγορίες', value: decision.thematicCategories ?? '—' },
  ]

  const modal = (
    <div className="contract-modal-backdrop" onClick={onClose}>
      <article className="contract-modal contract-modal--diavgeia" onClick={(e) => e.stopPropagation()}>
        <header className="contract-modal__header">
          <div>
            <span className="eyebrow">Διαύγεια</span>
            <h2>{decision.subject}</h2>
          </div>
          <div className="contract-modal__actions">
            {documentUrl && (
              <a
                className="contract-modal__icon-button"
                href={documentUrl}
                target="_blank"
                rel="noreferrer"
                aria-label="Άνοιγμα απόφασης στη Διαύγεια"
                title="Άνοιγμα απόφασης στη Διαύγεια"
              >
                <svg viewBox="0 0 20 20" aria-hidden="true">
                  <path d="M7.5 5H5a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2v-2.5" />
                  <path d="M11 3h6v6" />
                  <path d="M10 10 17 3" />
                </svg>
              </a>
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

        <div className="contract-modal__highlight">
          <span className="contract-modal__amount">{decision.ada}</span>
          <span className="contract-modal__arrow">→</span>
          <span className="contract-modal__beneficiary">{decision.decisionTypeUid}</span>
        </div>

        <div className="contract-modal__grid">
          {gridItems.map((item) => (
            <div key={item.label}>
              <span>{item.label}</span>
              <strong>{item.value}</strong>
            </div>
          ))}
        </div>

        {documentUrl && (
          <footer className="contract-modal__footer">
            <a href={documentUrl} target="_blank" rel="noreferrer">
              Κατέβασε το έγγραφο
            </a>
          </footer>
        )}
      </article>
    </div>
  )

  return createPortal(modal, document.body)
}
