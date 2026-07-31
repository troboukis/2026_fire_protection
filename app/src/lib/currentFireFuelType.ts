function foldGreek(value: string): string {
  return value
    .normalize('NFD')
    .replace(/\p{Diacritic}/gu, '')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase()
}

export function normalizeCurrentFireFuelType(value: string | null): string | null {
  const normalized = value ? foldGreek(value) : ''
  if (!normalized) return null

  if (normalized.includes('ΑΓΡΟΤΟΔΑΣΙΚ')) return 'ΑΓΡΟΤΟΔΑΣΙΚΗ ΕΚΤΑΣΗ'
  if (normalized.includes('ΧΟΡΤΟΛΙΒΑΔ')) return 'ΧΟΡΤΟΛΙΒΑΔΙΚΗ ΕΚΤΑΣΗ'
  if (normalized.includes('ΧΑΜΗΛΗ ΒΛΑΣΤΗΣΗ')) return 'ΧΑΜΗΛΗ ΒΛΑΣΤΗΣΗ'
  if (normalized.includes('ΔΑΣΙΚ')) return 'ΔΑΣΙΚΗ ΕΚΤΑΣΗ'
  if (normalized.includes('ΓΕΩΡΓΙΚ')) return 'ΓΕΩΡΓΙΚΗ ΕΚΤΑΣΗ'

  return normalized
}

