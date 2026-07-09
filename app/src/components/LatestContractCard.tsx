import type { KeyboardEvent } from 'react'
import { useNavigate } from 'react-router-dom'
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
  beneficiaryGemi?: string | null
  contractType: string
  howMuch: string
  documentUrl?: string | null
  municipalityKey?: string | null
  orgIsMunicipality?: boolean
  sortDate?: string | null
}

type Props = {
  item: LatestContractCardView
  onOpen?: (id: string) => void
  onMunicipalityClick?: (key: string) => void
  contractTypeTransform?: (value: string) => string
}

function truncateWords(value: string, maxWords: number): string {
  const text = value.trim()
  if (!text) return '—'
  const words = text.split(/\s+/).filter(Boolean)
  if (words.length <= maxWords) return text
  return `${words.slice(0, maxWords).join(' ')} ...`
}

function isMunicipalityOrgLabel(value: string): boolean {
  return value.trim().toLocaleUpperCase('el-GR').startsWith('ΔΗΜΟΣ ')
}

export default function LatestContractCard({ item, onOpen, onMunicipalityClick, contractTypeTransform }: Props) {
  const navigate = useNavigate()
  const clickable = typeof onOpen === 'function'
  const transformedContractType = contractTypeTransform ? contractTypeTransform(item.contractType) : item.contractType
  const municipalityClickable = (item.orgIsMunicipality === true || isMunicipalityOrgLabel(item.who)) && !!item.municipalityKey
  const visibleTitle = truncateWords(item.what, 15)

  const openMunicipality = () => {
    if (!item.municipalityKey) return
    if (onMunicipalityClick) {
      onMunicipalityClick(item.municipalityKey)
      return
    }
    navigate(`/municipalities?municipality=${encodeURIComponent(item.municipalityKey)}`)
  }

  const handleKeyDown = (e: KeyboardEvent<HTMLElement>) => {
    if (!clickable) return
    if (e.key === 'Enter' || e.key === ' ') onOpen(item.id)
  }

  const handleHeaderKeyDown = (e: KeyboardEvent<HTMLDivElement>) => {
    if (!municipalityClickable) return
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault()
      e.stopPropagation()
      openMunicipality()
    }
  }

  return (
    <article
      className="wire-item"
      role={clickable ? 'button' : undefined}
      tabIndex={clickable ? 0 : undefined}
      onClick={clickable ? () => onOpen(item.id) : undefined}
      onKeyDown={handleKeyDown}
    >
      <div
        className={`wire-item__head${municipalityClickable ? ' wire-item__head--clickable' : ''}`}
        role={municipalityClickable ? 'link' : undefined}
        tabIndex={municipalityClickable ? 0 : undefined}
        onClick={municipalityClickable ? (e) => { e.stopPropagation(); openMunicipality() } : undefined}
        onKeyDown={handleHeaderKeyDown}
      >
        {municipalityClickable ? (
          <span className="eyebrow wire-item__org">{item.who}</span>
        ) : (
          <span className="eyebrow wire-item__org">{item.who}</span>
        )}
        <span className="wire-item__date">{item.when}</span>
      </div>
      <h2 title={item.what}>{visibleTitle}</h2>
      <div className="wire-item__rule" aria-hidden="true" />
      <p className="wire-item__subtitle">{item.why}</p>
      <div className="wire-item__footer">
        <p className="wire-item__amount">
          <span>{item.howMuch}</span>
          <span className="wire-item__arrow">→</span>
          <BeneficiaryLink
            name={item.beneficiary}
            gemi={item.beneficiaryGemi}
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
