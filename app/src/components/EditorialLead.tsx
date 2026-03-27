import { Link } from 'react-router-dom'
import ComponentTag from './ComponentTag'

type Props = {
  eyebrow: string
  title: string
  subtitle: string
  ctaLabel?: string
  ctaTo?: string
}

export default function EditorialLead({ eyebrow, title, subtitle, ctaLabel, ctaTo }: Props) {
  return (
    <header className="editorial-lead">
      <ComponentTag name="EditorialLead" />
      <div className="editorial-lead__eyebrow">{eyebrow}</div>
      <h3 className="editorial-lead__title">{title}</h3>
      <p className="ca-header-note editorial-lead__subtitle">{subtitle}</p>
      {ctaLabel && ctaTo && (
        <Link className="editorial-lead__cta" to={ctaTo}>
          {ctaLabel}
        </Link>
      )}
    </header>
  )
}
