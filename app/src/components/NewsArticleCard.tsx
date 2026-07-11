import type { KeyboardEvent } from 'react'
import { dispatchCurrentFireHover } from '../lib/currentFireHover'

export type NewsArticleCardView = {
  id: string
  title: string
  source: string
  articleUrl: string
  imageUrl?: string | null
  publishedAt: string
  publishedTime: string
  municipalityName?: string | null
  area?: string | null
  activeFire?: NewsArticleActiveFire | null
}

export type NewsArticleActiveFire = {
  incidentKey: string
}

type Props = {
  item: NewsArticleCardView
}

function truncateWords(value: string, maxWords: number): string {
  const text = value.trim()
  if (!text) return '—'
  const words = text.split(/\s+/).filter(Boolean)
  if (words.length <= maxWords) return text
  return `${words.slice(0, maxWords).join(' ')} ...`
}

export default function NewsArticleCard({ item }: Props) {
  const visibleTitle = truncateWords(item.title, 18)
  const locationLabel = item.area ?? item.municipalityName ?? '—'

  const openArticle = () => {
    window.open(item.articleUrl, '_blank', 'noopener,noreferrer')
  }

  const handleKeyDown = (e: KeyboardEvent<HTMLElement>) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault()
      openArticle()
    }
  }

  const handleFireHover = (hovered: boolean) => {
    if (!item.activeFire) return
    dispatchCurrentFireHover(hovered ? item.activeFire.incidentKey : null)
  }

  return (
    <article
      className="wire-item news-article-card"
      role="button"
      tabIndex={0}
      onClick={openArticle}
      onKeyDown={handleKeyDown}
      onMouseEnter={item.activeFire ? () => handleFireHover(true) : undefined}
      onMouseLeave={item.activeFire ? () => handleFireHover(false) : undefined}
      onFocus={item.activeFire ? () => handleFireHover(true) : undefined}
      onBlur={item.activeFire ? () => handleFireHover(false) : undefined}
    >
      {item.imageUrl ? (
        <div className="news-article-card__media">
          <img src={item.imageUrl} alt="" loading="lazy" />
        </div>
      ) : null}
      <div className="wire-item__head">
        <span className="eyebrow wire-item__org">{item.source}</span>
        <span className="wire-item__date">{item.publishedAt}</span>
      </div>
      <h2 className="news-article-card__title" title={item.title}>{visibleTitle}</h2>
      <div className="wire-item__rule" aria-hidden="true" />
      <p className="wire-item__date news-article-card__published">Δημοσιεύτηκε: {item.publishedTime}</p>
      <p className="wire-item__subtitle">{locationLabel}</p>
      {item.activeFire ? (
        <div className="news-article-card__active-fire" aria-label="Ενεργή πυρκαγιά">
          <span className="news-article-card__active-fire-label">ενεργή πυρκαγιά</span>
        </div>
      ) : null}
    </article>
  )
}
