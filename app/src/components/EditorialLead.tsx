import ComponentTag from './ComponentTag'

type Props = {
  eyebrow: string
  title: string
  subtitle: string
}

export default function EditorialLead({ eyebrow, title, subtitle }: Props) {
  return (
    <header className="editorial-lead">
      <ComponentTag name="EditorialLead" />
      <div className="editorial-lead__eyebrow">{eyebrow}</div>
      <h3 className="editorial-lead__title">{title}</h3>
      <p className="editorial-lead__subtitle">{subtitle}</p>
    </header>
  )
}
