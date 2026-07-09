import { buildGemiCompanyUrl, normalizeGemiNumber } from '../lib/gemiCompany'

type Props = {
  name: string
  gemi?: string | null
  className?: string
  stopPropagation?: boolean
}

export default function BeneficiaryLink({
  name,
  gemi,
  className,
  stopPropagation = false,
}: Props) {
  const normalizedGemi = normalizeGemiNumber(gemi)
  const inactiveClassName = className
    ?.replace(/\bbeneficiary-link\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()

  if (!normalizedGemi) {
    return <span className={inactiveClassName}>{name}</span>
  }

  return (
    <a
      href={buildGemiCompanyUrl(normalizedGemi)}
      className={className}
      target="_blank"
      rel="noreferrer"
      onClick={stopPropagation ? (event) => event.stopPropagation() : undefined}
      title={`Άνοιγμα ΓΕΜΗ για ${name}`}
      aria-label={`Άνοιγμα ΓΕΜΗ για ${name}`}
    >
      {name}
    </a>
  )
}
