import { useState, type KeyboardEvent } from 'react'
import { Link } from 'react-router-dom'
import DiavgeiaModal from './DiavgeiaModal'

export type DiavgeiaDecisionCardView = {
  id: string
  orgType: string
  orgNameClean: string
  subject: string
  when: string
  ada: string
  decisionTypeUid: string
  amount?: string | null
  documentUrl?: string | null
  sortDate?: string | null
  municipalityKey?: string | null
  protocolNumber?: string | null
  thematicCategories?: string | null
  spendingSigners?: string | null
  spendingContractorsName?: string | null
  spendingContractorsAfm?: string | null
}

type Props = {
  item: DiavgeiaDecisionCardView
}

function isMunicipalityOrgType(value: string): boolean {
  return value.trim().toLocaleUpperCase('el-GR') === 'ΔΗΜΟΣ'
}

export default function DiavgeiaDecisionCard({ item }: Props) {
  const [modalOpen, setModalOpen] = useState(false)
  const organization = [item.orgType, item.orgNameClean].filter((part) => part && part !== '—').join(' · ') || '—'
  const documentUrl = item.documentUrl ?? (item.ada && item.ada !== '—' ? `https://diavgeia.gov.gr/doc/${item.ada}` : null)
  const visibleSubject = item.subject.trim() || '—'
  const isMunicipality = isMunicipalityOrgType(item.orgType) && !!item.municipalityKey

  const handleKeyDown = (e: KeyboardEvent<HTMLElement>) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault()
      setModalOpen(true)
    }
  }

  return (
    <>
      <article
        className="wire-item diavgeia-decision-card"
        role="button"
        tabIndex={0}
        onClick={() => setModalOpen(true)}
        onKeyDown={handleKeyDown}
      >
        <div className="wire-item__head">
          <span className="eyebrow wire-item__org">Διαύγεια</span>
          <span className="wire-item__date">{item.when}</span>
        </div>
        <h2 className="wire-item__title--four-lines" title={item.subject}>{visibleSubject}</h2>
        <div className="wire-item__rule" aria-hidden="true" />
        <p className="wire-item__subtitle">
          {isMunicipality ? (
            <Link
              className="wire-item__text-link"
              to={`/municipalities?municipality=${encodeURIComponent(item.municipalityKey!)}`}
              onClick={(e) => e.stopPropagation()}
              onKeyDown={(e) => e.stopPropagation()}
            >
              {organization}
            </Link>
          ) : organization}
        </p>
        <div className="wire-item__footer">
          <p className="wire-item__type">{item.ada}</p>
          <p className="wire-item__type">{item.decisionTypeUid}</p>
          {item.amount && <p className="diavgeia-decision-card__amount">{item.amount}</p>}
        </div>
        {documentUrl && (
          <p className="wire-item__link">
            <a
              href={documentUrl}
              target="_blank"
              rel="noreferrer"
              onClick={(e) => e.stopPropagation()}
              onKeyDown={(e) => e.stopPropagation()}
            >
              Άνοιγμα απόφασης
            </a>
          </p>
        )}
      </article>
      {modalOpen && (
        <DiavgeiaModal
          decision={item}
          onClose={() => setModalOpen(false)}
        />
      )}
    </>
  )
}
