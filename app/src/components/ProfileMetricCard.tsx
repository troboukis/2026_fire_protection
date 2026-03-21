type Props = {
  eyebrow?: string
  label: string
  value: string
  note?: string
  tone?: 'default' | 'accent' | 'ink'
}

export default function ProfileMetricCard({
  eyebrow,
  label,
  value,
  note,
  tone = 'default',
}: Props) {
  return (
    <article className={`profile-metric-card profile-metric-card--${tone}`}>
      {eyebrow && <div className="profile-metric-card__eyebrow eyebrow">{eyebrow}</div>}
      <div className="profile-metric-card__value">{value}</div>
      <div className="profile-metric-card__label">{label}</div>
      {note && <p className="profile-metric-card__note">{note}</p>}
    </article>
  )
}
