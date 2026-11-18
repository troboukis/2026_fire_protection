import { buildGemiCompanyUrl, normalizeGemiNumber } from '../lib/gemiCompany'
import { useProcurementI18n } from '../i18n/useProcurementI18n'

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
  const { p, language } = useProcurementI18n()
  const linkLabel = p('openGemi', { registry: language === 'el' ? 'ΓΕΜΗ' : 'GEMI', name })
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
      title={linkLabel}
      aria-label={linkLabel}
    >
      {name}
    </a>
  )
}
