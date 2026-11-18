import { useOverviewUi } from '../../../i18n/overview'
import type { ReactNode } from 'react'
import { LocalizedLink as Link } from '../../../routing/LocalizedLink'
import './AnalysisCard.css'

type AnalysisCardProps = {
  to: string
  title: string
  description: string
  metadata?: ReactNode
  imageSrc?: string
  imageAlt?: string
  imageFit?: 'cover' | 'contain'
  publishedAt?: string
  reloadDocument?: boolean
}

export default function AnalysisCard({ to, title, description, metadata, imageSrc, imageAlt = '', imageFit = 'cover', publishedAt, reloadDocument = false }: AnalysisCardProps) {
  const ui = useOverviewUi()
  return (
    <Link className="analysis-card section-rule" to={to} reloadDocument={reloadDocument}>
      <div className="analysis-card__body">
        <div className="analysis-card__media" aria-hidden={imageSrc ? undefined : true}>
          {imageSrc ? <img src={imageSrc} alt={ui(imageAlt)} style={{ objectFit: imageFit }} /> : null}
        </div>
        <div className="analysis-card__copy">
          <h2>{ui(title)}</h2>
          <p>{ui(description)}</p>
        </div>
      </div>
      <div className="analysis-card__footer">
        {publishedAt && (
          <time className="analysis-card__meta" dateTime={publishedAt}>
            {ui.format.date(publishedAt, { day: 'numeric', month: 'long', year: 'numeric' })}
          </time>
        )}
        {metadata && <div className="analysis-card__meta">{typeof metadata === 'string' ? ui(metadata) : metadata}</div>}
        <span className="analysis-card__action">
          <span className="analysis-card__action-label">{ui('Περισσότερα')}</span>
          <span className="analysis-card__action-arrow" aria-hidden="true">→</span>
        </span>
      </div>
    </Link>
  )
}
