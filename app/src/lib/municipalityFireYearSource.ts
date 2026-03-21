export type MunicipalityFireYearSource = 'forest_fire' | 'copernicus' | null

export function getMunicipalityFireYearSource(year: number, latestAvailableYear: number): MunicipalityFireYearSource {
  if (year >= 2000 && year <= 2024) return 'forest_fire'
  if (year >= 2025 && year <= Math.min(latestAvailableYear, 2026)) return 'copernicus'
  return null
}

export function getMunicipalityFireYearSourceLabel(source: MunicipalityFireYearSource): string {
  if (source === 'forest_fire') return 'Πυροσβεστική Υπηρεσία'
  if (source === 'copernicus') return 'Copernicus'
  return 'χωρίς πηγή'
}

export function getMunicipalityFireYearOptionLabel(year: number, latestAvailableYear: number): string {
  const source = getMunicipalityFireYearSource(year, latestAvailableYear)
  return `${year} · ${getMunicipalityFireYearSourceLabel(source)}`
}

export function getMunicipalityFireYearEmptyState(year: number, source: MunicipalityFireYearSource, latestAvailableYear = 2026): string {
  if (source === 'forest_fire') return `Δεν βρέθηκαν δεδομένα της Πυροσβεστικής Υπηρεσίας για το ${year}.`
  if (source === 'copernicus') return `Δεν βρέθηκαν Copernicus δεδομένα για το ${year}.`
  return `Δεν υπάρχει διαθέσιμη πηγή για το ${year}. Διαθέσιμες πηγές: 2000-2024 Πυροσβεστική Υπηρεσία και 2025-${Math.min(latestAvailableYear, 2026)} Copernicus.`
}
