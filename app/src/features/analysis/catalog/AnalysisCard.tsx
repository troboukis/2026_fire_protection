import type { ReactNode } from 'react'
import { Link } from 'react-router-dom'
import './AnalysisCard.css'

type AnalysisCardProps = {
  to: string
  title: string
  description: string
  metadata?: ReactNode
  imageSrc?: string
  imageAlt?: string
}

export default function AnalysisCard({ to, title, description, metadata, imageSrc, imageAlt = '' }: AnalysisCardProps) {
  return (
    <Link className="analysis-card section-rule" to={to}>
      <div className="analysis-card__body">
        <div className="analysis-card__media" aria-hidden={imageSrc ? undefined : true}>
          {imageSrc ? <img src={imageSrc} alt={imageAlt} /> : null}
        </div>
        <div className="analysis-card__copy">
          <h2>{title}</h2>
          <p>{description}</p>
        </div>
      </div>
      <div className="analysis-card__footer">
        {metadata && <div className="analysis-card__meta">{metadata}</div>}
        <span className="analysis-card__action">
          <span className="analysis-card__action-label">Περισσότερα</span>
          <span className="analysis-card__action-arrow" aria-hidden="true">→</span>
        </span>
      </div>
    </Link>
  )
}
