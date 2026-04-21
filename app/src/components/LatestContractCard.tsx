import type { KeyboardEvent } from 'react'
import BeneficiaryLink from './BeneficiaryLink'

export type LatestContractCardView = {
  id: string
  who: string
  what: string
  when: string
  why: string
  signedAt?: string
  beneficiary: string
  beneficiaryVat?: string | null
  contractType: string
  howMuch: string
  documentUrl?: string | null
  municipalityKey?: string | null
}

type Props = {
  item: LatestContractCardView
  onOpen?: (id: string) => void
  onMunicipalityClick?: (key: string) => void
  contractTypeTransform?: (value: string) => string
}

export default function LatestContractCard({ item, onOpen, onMunicipalityClick, contractTypeTransform }: Props) {
  const clickable = typeof onOpen === 'function'
  const transformedContractType = contractTypeTransform ? contractTypeTransform(item.contractType) : item.contractType
  const municipalityClickable = typeof onMunicipalityClick === 'function' && !!item.municipalityKey

  const handleKeyDown = (e: KeyboardEvent<HTMLElement>) => {
    if (!clickable) return
    if (e.key === 'Enter' || e.key === ' ') onOpen(item.id)
  }

  return (
    <article
      className="wire-item"
      role={clickable ? 'button' : undefined}
      tabIndex={clickable ? 0 : undefined}
      onClick={clickable ? () => onOpen(item.id) : undefined}
      onKeyDown={handleKeyDown}
    >
      <div className="wire-item__head">
        <span
          className={`eyebrow wire-item__org${municipalityClickable ? ' wire-item__org--clickable' : ''}`}
          onClick={municipalityClickable ? (e) => { e.stopPropagation(); onMunicipalityClick(item.municipalityKey!) } : undefined}
        >{item.who}</span>
        <span className="wire-item__date">{item.when}</span>
      </div>
      <h2>{item.what}</h2>
      <div className="wire-item__rule" aria-hidden="true" />
      <p className="wire-item__subtitle">{item.why}</p>
      <div className="wire-item__footer">
        <p className="wire-item__amount">
          <span>{item.howMuch}</span>
          <span className="wire-item__arrow">→</span>
          <BeneficiaryLink
            name={item.beneficiary}
            afm={item.beneficiaryVat}
            className="wire-item__beneficiary beneficiary-link"
            stopPropagation
          />
        </p>
        <p className="wire-item__type">{transformedContractType}</p>
      </div>
      <p className="wire-item__date">Υπεγράφη: {item.signedAt ?? '—'}</p>
      {item.documentUrl && (
        <p className="wire-item__link">
          <a
            href={item.documentUrl}
            target="_blank"
            rel="noreferrer"
            onClick={(e) => e.stopPropagation()}
          >
            Άνοιγμα εγγράφου
          </a>
        </p>
      )}
    </article>
  )
}
