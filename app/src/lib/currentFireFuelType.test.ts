import { describe, expect, it } from 'vitest'
import { normalizeCurrentFireFuelType } from './currentFireFuelType'

describe('normalizeCurrentFireFuelType', () => {
  it('uses one label for forest-fire variants', () => {
    expect(normalizeCurrentFireFuelType('δασική')).toBe('ΔΑΣΙΚΗ ΕΚΤΑΣΗ')
    expect(normalizeCurrentFireFuelType('Δασική έκταση')).toBe('ΔΑΣΙΚΗ ΕΚΤΑΣΗ')
  })

  it('normalizes the remaining vegetation categories', () => {
    expect(normalizeCurrentFireFuelType('αγροτοδασική')).toBe('ΑΓΡΟΤΟΔΑΣΙΚΗ ΕΚΤΑΣΗ')
    expect(normalizeCurrentFireFuelType('χορτολιβαδική')).toBe('ΧΟΡΤΟΛΙΒΑΔΙΚΗ ΕΚΤΑΣΗ')
    expect(normalizeCurrentFireFuelType('Χαμηλή βλάστηση')).toBe('ΧΑΜΗΛΗ ΒΛΑΣΤΗΣΗ')
  })

  it('keeps unknown values in a consistent uppercase form', () => {
    expect(normalizeCurrentFireFuelType('Καλάμια - βάλτοι')).toBe('ΚΑΛΑΜΙΑ - ΒΑΛΤΟΙ')
    expect(normalizeCurrentFireFuelType(null)).toBeNull()
  })
})

