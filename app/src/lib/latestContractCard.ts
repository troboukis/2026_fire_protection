import type { LatestContractCardView } from '../components/LatestContractCard'

export type AuthorityScope = 'municipality' | 'region' | 'decentralized' | 'national' | 'other'

type BuildLatestContractCardViewArgs = {
  id: string
  organizationName: string | null
  authorityScope: AuthorityScope
  municipalityLabel?: string | null
  what: string
  when: string
  why: string
  beneficiary: string
  beneficiaryVat?: string | null
  beneficiaryGemi?: string | null
  contractType: string
  howMuch: string
  signedAt?: string
  documentUrl?: string | null
  municipalityKey?: string | null
  orgIsMunicipality?: boolean
  sortDate?: string | null
}

export function resolveLatestContractWho({
  organizationName,
  authorityScope,
  municipalityLabel,
}: {
  organizationName: string | null
  authorityScope: AuthorityScope
  municipalityLabel?: string | null
}): string {
  const canonicalMunicipality = String(municipalityLabel ?? '').trim()
  if (authorityScope === 'municipality' && canonicalMunicipality) {
    return `ΔΗΜΟΣ ${canonicalMunicipality}`
  }
  return String(organizationName ?? '').trim() || '—'
}

export function buildLatestContractCardView(args: BuildLatestContractCardViewArgs): LatestContractCardView {
  return {
    id: args.id,
    who: resolveLatestContractWho(args),
    what: args.what,
    when: args.when,
    why: args.why,
    beneficiary: args.beneficiary,
    beneficiaryVat: args.beneficiaryVat ?? null,
    beneficiaryGemi: args.beneficiaryGemi ?? null,
    contractType: args.contractType,
    howMuch: args.howMuch,
    signedAt: args.signedAt,
    documentUrl: args.documentUrl ?? null,
    municipalityKey: args.municipalityKey ?? null,
    orgIsMunicipality: args.orgIsMunicipality ?? args.authorityScope === 'municipality',
    sortDate: args.sortDate ?? null,
  }
}
