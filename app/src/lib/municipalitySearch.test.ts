import { describe, expect, it } from 'vitest'
import {
  findBestMunicipalityMatch,
  normalizeMunicipalityKey,
  normalizeMunicipalitySearch,
  rankMunicipalityMatches,
} from './municipalitySearch'

describe('municipalitySearch helpers', () => {
  const municipalities = [
    {
      municipality_key: '9179',
      dhmos: 'Αγίας Βαρβάρας',
      municipality_normalized_name: 'Αγίας Βαρβάρας',
      kpi_politikis_prostasias: 0.99,
    },
    {
      municipality_key: '9305',
      dhmos: 'Αιγάλεω',
      municipality_normalized_name: 'Αιγάλεω',
      kpi_politikis_prostasias: 0.8,
    },
  ]

  it('normalizes accents, case, and keyboard-layout mistakes', () => {
    expect(normalizeMunicipalitySearch('ΑΓΙΆΣ ΒΑΡΒΆΡΑΣ')).toBe('αγιασ βαρβαρασ')
    expect(normalizeMunicipalitySearch('Agias Barbaras')).toBe('αγιασ βαρβαρασ')
  })

  it('normalizes municipality keys', () => {
    expect(normalizeMunicipalityKey('09179.0')).toBe('9179')
    expect(normalizeMunicipalityKey('9179')).toBe('9179')
  })

  it('ranks municipality matches by name only', () => {
    const ranked = rankMunicipalityMatches(municipalities, 'Agias Barbaras')
    expect(ranked[0]?.municipality.municipality_key).toBe('9179')
    expect(findBestMunicipalityMatch(municipalities, 'ΑΙΓΑΛΕΩ')?.municipality_key).toBe('9305')
  })

  it('does not treat municipality key as a name search term', () => {
    expect(findBestMunicipalityMatch(municipalities, '9179')).toBeNull()
  })
})
