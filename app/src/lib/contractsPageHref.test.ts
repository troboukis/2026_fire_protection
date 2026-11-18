import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { buildContractsPageHref } from './contractsPageHref'

describe('published contract links', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-11-17T12:00:00Z'))
  })
  afterEach(() => vi.useRealTimers())

  it('keeps the existing path, date parameter names and start of coverage', () => {
    const link = new URL(buildContractsPageHref({}), 'https://www.fire-watch-app.gr')
    expect(link.pathname).toBe('/contracts')
    expect(link.searchParams.get('dateFrom')).toBe('2024-01-01')
    expect(link.searchParams.get('dateTo')).toBe('2026-11-17')
  })

  it('retains repeated organizationKey parameters without duplicating a key', () => {
    const link = new URL(buildContractsPageHref({ organizationKeys: [' 100 ', '200', '100', ''] }), 'https://www.fire-watch-app.gr')
    expect(link.searchParams.getAll('organizationKey')).toEqual(['100', '200'])
  })

  it('preserves region and municipality identifiers', () => {
    const link = new URL(buildContractsPageHref({ regionKey: ' 4 ', municipalityKey: ' 9230 ' }), 'https://www.fire-watch-app.gr')
    expect(link.searchParams.get('regionKey')).toBe('4')
    expect(link.searchParams.get('municipalityKey')).toBe('9230')
  })

  it('round-trips Greek parameter values and reserved characters', () => {
    const key = 'ΔΗΜΟΣ Α & Β/Γ'
    const link = new URL(buildContractsPageHref({ organizationKeys: [key] }), 'https://www.fire-watch-app.gr')
    expect(link.searchParams.getAll('organizationKey')).toEqual([key])
  })

  it('omits absent scope parameters', () => {
    const link = new URL(buildContractsPageHref({ organizationKeys: [], regionKey: ' ', municipalityKey: null }), 'https://www.fire-watch-app.gr')
    expect(Array.from(link.searchParams.keys())).toEqual(['dateFrom', 'dateTo'])
  })
})
