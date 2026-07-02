const EARTH_RADIUS_M = 6_371_000
const FIRMS_MAP_EXCLUSION_RADIUS_M = 1_000

const FIRMS_MAP_EXCLUSION_CENTERS = [
  { label: 'Elefsina', latitude: 38.1301, longitude: 23.52739 },
  { label: 'Kymi-Aliveri', latitude: 38.37497, longitude: 24.06412 },
  { label: 'Pavlou Mela', latitude: 40.70065, longitude: 22.95211 },
  { label: 'Polygyros', latitude: 40.28647, longitude: 23.44398 },
  { label: 'Loutraki', latitude: 37.91736, longitude: 23.07091 },
]

function toFiniteNumber(value: unknown): number | null {
  if (value == null || value === '') return null
  const numberValue = Number(value)
  return Number.isFinite(numberValue) ? numberValue : null
}

function distanceMeters(
  latitudeA: number,
  longitudeA: number,
  latitudeB: number,
  longitudeB: number,
): number {
  const latA = latitudeA * Math.PI / 180
  const latB = latitudeB * Math.PI / 180
  const deltaLat = latB - latA
  const deltaLon = (longitudeB - longitudeA) * Math.PI / 180
  const haversine = Math.sin(deltaLat / 2) ** 2
    + Math.cos(latA) * Math.cos(latB) * Math.sin(deltaLon / 2) ** 2

  return 2 * EARTH_RADIUS_M * Math.asin(Math.sqrt(haversine))
}

export function isExcludedFirmsMapHotspot(latitude: unknown, longitude: unknown): boolean {
  const lat = toFiniteNumber(latitude)
  const lon = toFiniteNumber(longitude)
  if (lat == null || lon == null) return false

  return FIRMS_MAP_EXCLUSION_CENTERS.some((center) => (
    distanceMeters(lat, lon, center.latitude, center.longitude) <= FIRMS_MAP_EXCLUSION_RADIUS_M
  ))
}

export function excludeFirmsMapHotspots<Row extends { latitude: unknown; longitude: unknown }>(rows: Row[]): Row[] {
  return rows.filter((row) => !isExcludedFirmsMapHotspot(row.latitude, row.longitude))
}
