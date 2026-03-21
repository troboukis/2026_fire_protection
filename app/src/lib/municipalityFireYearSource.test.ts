import { describe, expect, it } from 'vitest'
import {
  getMunicipalityFireYearEmptyState,
  getMunicipalityFireYearOptionLabel,
  getMunicipalityFireYearSource,
  getMunicipalityFireYearSourceLabel,
} from './municipalityFireYearSource'

describe('municipalityFireYearSource helpers', () => {
  it('maps 2000-2024 to forest_fire', () => {
    expect(getMunicipalityFireYearSource(2000, 2026)).toBe('forest_fire')
    expect(getMunicipalityFireYearSource(2024, 2026)).toBe('forest_fire')
  })

  it('maps 2025 through latest available year to copernicus', () => {
    expect(getMunicipalityFireYearSource(2025, 2026)).toBe('copernicus')
    expect(getMunicipalityFireYearSource(2026, 2026)).toBe('copernicus')
  })

  it('leaves only out-of-range years without an active source', () => {
    expect(getMunicipalityFireYearSource(1999, 2026)).toBeNull()
    expect(getMunicipalityFireYearSource(2027, 2026)).toBeNull()
  })

  it('builds clear labels and empty-state copy', () => {
    expect(getMunicipalityFireYearSourceLabel('forest_fire')).toBe('Πυροσβεστική Υπηρεσία')
    expect(getMunicipalityFireYearOptionLabel(2002, 2026)).toBe('2002 · Πυροσβεστική Υπηρεσία')
    expect(getMunicipalityFireYearEmptyState(2015, null, 2026)).toContain('Δεν υπάρχει διαθέσιμη πηγή')
  })
})
