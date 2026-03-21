export interface MunicipalitySearchEntry {
  municipality_key: string
  dhmos: string | null
  municipality_normalized_name: string | null
  kpi_politikis_prostasias?: number | string | null
}

export interface RankedMunicipalityMatch<T extends MunicipalitySearchEntry> {
  municipality: T
  name: string
  score: number
  kpi: number
}

const latinKeyboardToGreek: Record<string, string> = {
  a: 'α',
  b: 'β',
  c: 'ψ',
  d: 'δ',
  e: 'ε',
  f: 'φ',
  g: 'γ',
  h: 'η',
  i: 'ι',
  j: 'ξ',
  k: 'κ',
  l: 'λ',
  m: 'μ',
  n: 'ν',
  o: 'ο',
  p: 'π',
  q: ';',
  r: 'ρ',
  s: 'σ',
  t: 'τ',
  u: 'θ',
  v: 'ω',
  w: 'ς',
  x: 'χ',
  y: 'υ',
  z: 'ζ',
}

export function translateLatinKeyboardToGreek(value: string): string {
  return value.replace(/[a-z]/g, (character) => latinKeyboardToGreek[character] ?? character)
}

export function normalizeMunicipalitySearch(value: string | null): string {
  const lower = String(value ?? '')
    .trim()
    .toLowerCase()

  return translateLatinKeyboardToGreek(lower)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/ς/g, 'σ')
    .replace(/[^a-zα-ω0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
}

export function normalizeMunicipalityKey(value: unknown): string {
  const text = String(value ?? '').trim()
  if (!text) return ''
  const noDecimal = text.replace(/\.0+$/, '')
  if (/^\d+$/.test(noDecimal)) return String(Number(noDecimal))
  return noDecimal
}

export function rankMunicipalityMatches<T extends MunicipalitySearchEntry>(
  municipalities: T[],
  query: string | null,
): RankedMunicipalityMatch<T>[] {
  const normalizedQuery = normalizeMunicipalitySearch(query)
  if (!normalizedQuery) return []

  const scored = municipalities.map((municipality) => {
    const name = String(
      municipality.dhmos
        ?? municipality.municipality_normalized_name
        ?? municipality.municipality_key,
    )
    const searchable = [
      municipality.dhmos,
      municipality.municipality_normalized_name,
    ]
      .filter(Boolean)
      .map((value) => normalizeMunicipalitySearch(value))

    const score = searchable.some((value) => value === normalizedQuery)
      ? 0
      : searchable.some((value) => value.startsWith(normalizedQuery))
        ? 1
        : searchable.some((value) => value.includes(normalizedQuery))
          ? 2
          : 3

    return {
      municipality,
      name,
      score,
      kpi: Number(municipality.kpi_politikis_prostasias ?? -1),
    }
  })

  return scored
    .filter((entry) => entry.score < 3)
    .sort((a, b) => a.score - b.score || b.kpi - a.kpi || a.name.localeCompare(b.name, 'el'))
}

export function findBestMunicipalityMatch<T extends MunicipalitySearchEntry>(
  municipalities: T[],
  query: string | null,
): T | null {
  return rankMunicipalityMatches(municipalities, query)[0]?.municipality ?? null
}
