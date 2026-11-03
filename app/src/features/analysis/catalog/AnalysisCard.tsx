import type { ReactNode } from 'react'
import { Link } from 'react-router-dom'
import './AnalysisCard.css'

const publicationDateFormat = new Intl.DateTimeFormat('el-GR', {
  day: 'numeric',
  month: 'long',
  year: 'numeric',
  timeZone: 'Europe/Athens',
})

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
  return (
    <Link className="analysis-card section-rule" to={to} reloadDocument={reloadDocument}>
      <div className="analysis-card__body">
        <div className="analysis-card__media" aria-hidden={imageSrc ? undefined : true}>
          {imageSrc ? <img src={imageSrc} alt={imageAlt} style={{ objectFit: imageFit }} /> : null}
        </div>
        <div className="analysis-card__copy">
          <h2>{title}</h2>
          <p>{description}</p>
        </div>
      </div>
      <div className="analysis-card__footer">
        {publishedAt && (
          <time className="analysis-card__meta" dateTime={publishedAt}>
            {publicationDateFormat.format(new Date(`${publishedAt}T12:00:00Z`))}
          </time>
        )}
        {metadata && <div className="analysis-card__meta">{metadata}</div>}
        <span className="analysis-card__action">
          <span className="analysis-card__action-label">Περισσότερα</span>
          <span className="analysis-card__action-arrow" aria-hidden="true">→</span>
        </span>
      </div>
    </Link>
  )
}
