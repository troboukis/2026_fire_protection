export type SearchQueryTerm = {
  value: string
  excluded: boolean
}

export const MAX_SEARCH_QUERY_LENGTH = 500
export const MAX_SEARCH_TERMS = 10
export const MAX_SEARCH_TERM_LENGTH = 100

export function normalizeSearchText(value: unknown): string {
  return String(value ?? '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/ς/g, 'σ')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase()
}

export function parseSearchQuery(query: string): SearchQueryTerm[] {
  return query
    .slice(0, MAX_SEARCH_QUERY_LENGTH)
    .replace(/\s*!\s*/g, '&!')
    .split(/\s*&\s*/)
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => {
      const excluded = part.startsWith('!')
      return {
        excluded,
        value: normalizeSearchText(excluded ? part.slice(1) : part).slice(0, MAX_SEARCH_TERM_LENGTH),
      }
    })
    .filter((term) => Boolean(term.value))
    .slice(0, MAX_SEARCH_TERMS)
}

export function matchesSearchQuery(haystack: unknown, query: string): boolean {
  const normalizedHaystack = normalizeSearchText(haystack)
  return parseSearchQuery(query).every((term) => (
    term.excluded
      ? !normalizedHaystack.includes(term.value)
      : normalizedHaystack.includes(term.value)
  ))
}
