import type { MouseEvent } from 'react'
import { normalizeAfm, openGemiCompanyPageByAfm } from '../lib/gemiCompany'

type Props = {
  name: string
  afm?: string | null
  className?: string
  stopPropagation?: boolean
}

export default function BeneficiaryLink({
  name,
  afm,
  className,
  stopPropagation = false,
}: Props) {
  const normalizedAfm = normalizeAfm(afm)
  const inactiveClassName = className
    ?.replace(/\bbeneficiary-link\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()

  const handleClick = async (event: MouseEvent<HTMLButtonElement>) => {
    if (stopPropagation) event.stopPropagation()
    if (!normalizedAfm) return
    await openGemiCompanyPageByAfm(normalizedAfm, name)
  }

  if (!normalizedAfm) {
    return <span className={inactiveClassName}>{name}</span>
  }

  return (
    <button
      type="button"
      className={className}
      onClick={(event) => { void handleClick(event) }}
      title={`Άνοιγμα ΓΕΜΗ για ${name}`}
      aria-label={`Άνοιγμα ΓΕΜΗ για ${name}`}
    >
      {name}
    </button>
  )
}
