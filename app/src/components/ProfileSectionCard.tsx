import type { ReactNode } from 'react'

type Props = {
  eyebrow: string
  title: string
  subtitle?: string
  children: ReactNode
  className?: string
}

export default function ProfileSectionCard({
  eyebrow,
  title,
  subtitle,
  children,
  className,
}: Props) {
  return (
    <section className={`profile-section-card${className ? ` ${className}` : ''}`}>
      <header className="profile-section-card__head">
        <div className="eyebrow">{eyebrow}</div>
        <h2>{title}</h2>
        {subtitle && <p>{subtitle}</p>}
      </header>
      {children}
    </section>
  )
}
