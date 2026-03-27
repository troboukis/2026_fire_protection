import { describe, expect, it } from 'vitest'
import { buildContractAuthorityLabel } from './contractAuthority'

describe('buildContractAuthorityLabel', () => {
  it('uses municipality labels for municipality-owned contracts', () => {
    expect(buildContractAuthorityLabel({
      canonicalOwnerScope: 'municipality',
      organizationScope: 'municipality',
      organizationName: 'ΔΗΜΟΣ ΑΓΡΙΝΙΟΥ',
      municipalityLabel: 'Αγρινίου',
      regionLabel: 'Δυτικής Ελλάδας',
    })).toBe('ΔΗΜΟΣ Αγρινίου')
  })

  it('uses region labels for region-scoped contracts', () => {
    expect(buildContractAuthorityLabel({
      canonicalOwnerScope: 'organization',
      organizationScope: 'region',
      organizationName: 'ΠΕΡΙΦΕΡΕΙΑ ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ',
      municipalityLabel: 'Διρφύων - Μεσσαπίων',
      regionLabel: 'Στερεάς Ελλάδας',
    })).toBe('ΠΕΡΙΦΕΡΕΙΑ Στερεάς Ελλάδας')
  })

  it('falls back to organization name when no geography exists', () => {
    expect(buildContractAuthorityLabel({
      canonicalOwnerScope: 'organization',
      organizationScope: 'other',
      organizationName: 'ΠΡΑΣΙΝΟ ΤΑΜΕΙΟ',
      municipalityLabel: null,
      regionLabel: null,
    })).toBe('ΠΡΑΣΙΝΟ ΤΑΜΕΙΟ')
  })
})
