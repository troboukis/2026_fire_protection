import type { LatestContractCardView } from '../components/LatestContractCard'

export type AuthorityScope = 'municipality' | 'region' | 'decentralized' | 'other'

type BuildLatestContractCardViewArgs = {
  id: string
  organizationName: string | null
  authorityScope: AuthorityScope
  municipalityLabel?: string | null
  what: string
  when: string
  why: string
  beneficiary: string
  contractType: string
  howMuch: string
  signedAt?: string
  documentUrl?: string | null
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
    contractType: args.contractType,
    howMuch: args.howMuch,
    signedAt: args.signedAt,
    documentUrl: args.documentUrl ?? null,
  }
}
