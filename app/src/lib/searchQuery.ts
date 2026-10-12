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
  return parseSearchQueryGroups(query).flat()
}

function parseSearchQueryGroups(query: string): SearchQueryTerm[][] {
  let remainingTerms = MAX_SEARCH_TERMS
  return query
    .slice(0, MAX_SEARCH_QUERY_LENGTH)
    .split(/\s*\|\s*|\s+OR\s+/i)
    .map((group) => {
      const terms = parseSearchGroup(group).slice(0, remainingTerms)
      remainingTerms -= terms.length
      return terms
    })
    .filter((group) => group.length > 0)
}

function parseSearchGroup(group: string): SearchQueryTerm[] {
  return group
    .replace(/\s+AND(?=\s|$)/gi, '&')
    .replace(/(^|[\s&])NOT(?=\s|$)/gi, '$1!')
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
  const groups = parseSearchQueryGroups(query)
  // AND and exclusions bind within each OR alternative.
  return groups.length === 0 || groups.some((group) => group.every((term) => (
    term.excluded
      ? !normalizedHaystack.includes(term.value)
      : normalizedHaystack.includes(term.value)
  )))
}
