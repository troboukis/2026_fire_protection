import { describe, expect, it } from 'vitest'
import {
  MAX_SEARCH_QUERY_LENGTH,
  MAX_SEARCH_TERM_LENGTH,
  MAX_SEARCH_TERMS,
  matchesSearchQuery,
  normalizeSearchText,
  parseSearchQuery,
} from './searchQuery'

describe('search query syntax', () => {
  it.each(['μάνδρα', 'μανδρα', 'ΜΑΝΔΡΑ', 'ΜάνΔρΑ', '  μάνδρα  '])('matches the Mandra organization label with query %s', (query) => {
    expect(normalizeSearchText('ΔΗΜΟΣ · ΜΑΝΔΡΑΣ - ΕΙΔΥΛΛΙΑΣ').includes(normalizeSearchText(query))).toBe(true)
  })

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

  it('supports AND NOT keywords and mixed symbolic operators', () => {
    for (const query of [
      'πυροπροστασία AND προμήθεια AND NOT καύσιμα',
      'πυροπροστασία and προμήθεια and not καύσιμα',
      'πυροπροστασία & προμήθεια NOT καύσιμα',
      'πυροπροστασία AND προμήθεια !καύσιμα',
      'πυροπροστασία & προμήθεια &NOT καύσιμα',
    ]) {
      expect(matchesSearchQuery('Προμήθεια πυροπροστασίας', query)).toBe(true)
      expect(matchesSearchQuery('Προμήθεια πυροπροστασίας και καύσιμα', query)).toBe(false)
      expect(matchesSearchQuery('Πυροπροστασία', query)).toBe(false)
    }
    expect(matchesSearchQuery('Πυροπροστασία', 'πυροπροστασία AND NOT καύσιμα')).toBe(true)
    expect(matchesSearchQuery('Πυροπροστασία και καύσιμα', 'πυροπροστασία AND NOT καύσιμα')).toBe(false)
    expect(matchesSearchQuery('Εξοπλισμός', 'NOT καύσιμα')).toBe(true)
    expect(matchesSearchQuery('Καύσιμα', 'NOT καύσιμα')).toBe(false)
  })

  it('combines OR with AND NOT within each alternative', () => {
    const query = 'πυροπροστασία AND NOT καύσιμα OR πυρόσβεση AND NOT οχήματα'
    expect(matchesSearchQuery('Πυροπροστασία', query)).toBe(true)
    expect(matchesSearchQuery('Πυρόσβεση', query)).toBe(true)
    expect(matchesSearchQuery('Πυροπροστασία και καύσιμα', query)).toBe(false)
    expect(matchesSearchQuery('Πυρόσβεση για οχήματα', query)).toBe(false)
    expect(matchesSearchQuery('Πυροπροστασία και καύσιμα και πυρόσβεση', query)).toBe(true)
  })

  it('preserves words containing AND or NOT', () => {
    expect(parseSearchQuery('STANDARD NOTICE')).toEqual([
      { value: 'STANDARD NOTICE', excluded: false },
    ])
  })

  it('matches either OR alternative with a pipe or the OR keyword', () => {
    for (const separator of ['|', 'OR', 'or', 'Or']) {
      const query = `πυροπροστασία ${separator} πυρόσβεση`
      expect(matchesSearchQuery('Εξοπλισμός πυροπροστασίας', query)).toBe(true)
      expect(matchesSearchQuery('Εξοπλισμός πυρόσβεσης', query)).toBe(true)
      expect(matchesSearchQuery('Συντήρηση οχημάτων', query)).toBe(false)
    }
    expect(matchesSearchQuery('Πυρόσβεση', 'πυροπροστασία|πυρόσβεση')).toBe(true)
  })

  it('applies AND and exclusions within each OR alternative', () => {
    const query = 'πυροπροστασία & προμήθεια !καύσιμα | πυρόσβεση & συντήρηση !οχήματα'
    expect(matchesSearchQuery('Προμήθεια πυροπροστασίας', query)).toBe(true)
    expect(matchesSearchQuery('Συντήρηση πυρόσβεσης', query)).toBe(true)
    expect(matchesSearchQuery('Πυροπροστασία', query)).toBe(false)
    expect(matchesSearchQuery('Πυρόσβεση', query)).toBe(false)
    expect(matchesSearchQuery('Προμήθεια πυροπροστασίας και καύσιμα', query)).toBe(false)
    expect(matchesSearchQuery('Συντήρηση πυρόσβεσης για οχήματα', query)).toBe(false)
  })

  it('ignores empty alternatives without making all rows match', () => {
    expect(matchesSearchQuery('Πυρόσβεση', '| πυρόσβεση |')).toBe(true)
    expect(matchesSearchQuery('Συντήρηση οχημάτων', '| πυρόσβεση |')).toBe(false)
    expect(matchesSearchQuery('Οποιοδήποτε κείμενο', '')).toBe(true)
  })

  it('keeps phrases and words containing or intact', () => {
    expect(matchesSearchQuery('δασική προστασία', 'δασική προστασία | πυρόσβεση')).toBe(true)
    expect(matchesSearchQuery('δασική οδός προστασία', 'δασική προστασία | πυρόσβεση')).toBe(false)
    expect(matchesSearchQuery('FOREST', 'forest')).toBe(true)
  })

  it('caps the total term count across OR alternatives', () => {
    const query = Array.from({ length: MAX_SEARCH_TERMS + 2 }, (_, index) => `[term${index}]`).join('|')
    expect(parseSearchQuery(query)).toHaveLength(MAX_SEARCH_TERMS)
    expect(matchesSearchQuery('[term11]', query)).toBe(false)
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
