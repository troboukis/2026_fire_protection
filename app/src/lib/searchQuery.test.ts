import { describe, expect, it } from 'vitest'
import {
  MAX_SEARCH_QUERY_LENGTH,
  MAX_SEARCH_TERM_LENGTH,
  MAX_SEARCH_TERMS,
  matchesSearchQuery,
  parseSearchQuery,
} from './searchQuery'

describe('search query syntax', () => {
  it('keeps a plain query as one phrase for backwards compatibility', () => {
    expect(parseSearchQuery('δασική προστασία')).toEqual([
      { value: 'ΔΑΣΙΚΗ ΠΡΟΣΤΑΣΙΑ', excluded: false },
    ])
  })

  it('parses required and excluded terms', () => {
    expect(parseSearchQuery('πυροπροστασία & προμήθεια ! καύσιμα')).toEqual([
      { value: 'ΠΥΡΟΠΡΟΣΤΑΣΙΑ', excluded: false },
      { value: 'ΠΡΟΜΗΘΕΙΑ', excluded: false },
      { value: 'ΚΑΥΣΙΜΑ', excluded: true },
    ])
  })

  it('requires every positive term and rejects every excluded term', () => {
    const text = 'Προμήθεια εξοπλισμού πυροπροστασίας για τον Δήμο'
    expect(matchesSearchQuery(text, 'πυροπροστασια & προμηθεια !καυσιμα')).toBe(true)
    expect(matchesSearchQuery(`${text} και καύσιμα`, 'πυροπροστασια & προμηθεια !καυσιμα')).toBe(false)
    expect(matchesSearchQuery(text, 'πυροπροστασια & συντήρηση')).toBe(false)
  })

  it('supports an exclusion-only query', () => {
    expect(matchesSearchQuery('Εξοπλισμός πυρόσβεσης', '!καύσιμα')).toBe(true)
    expect(matchesSearchQuery('Προμήθεια καύσιμα', '!καύσιμα')).toBe(false)
  })

  it('treats SQL metacharacters as ordinary search text', () => {
    const attack = "'; DROP TABLE procurement; --"
    expect(parseSearchQuery(attack)).toEqual([{ value: attack.toUpperCase(), excluded: false }])
    expect(matchesSearchQuery('ordinary contract title', attack)).toBe(false)
  })

  it('caps query size, term count and individual term size', () => {
    const oversizedTerms = Array.from({ length: MAX_SEARCH_TERMS + 5 }, (_, index) => `${index}${'x'.repeat(MAX_SEARCH_TERM_LENGTH + 20)}`)
    const parsed = parseSearchQuery(oversizedTerms.join(' & ').slice(0, MAX_SEARCH_QUERY_LENGTH))
    expect(parsed.length).toBeLessThanOrEqual(MAX_SEARCH_TERMS)
    expect(parsed.every((term) => term.value.length <= MAX_SEARCH_TERM_LENGTH)).toBe(true)
  })
})
