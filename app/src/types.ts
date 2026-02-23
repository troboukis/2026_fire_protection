export interface Municipality {
  id: string
  name: string
  forest_ha: number | null
}

export interface GeoFeatureProperties {
  name: string
  municipality_code: string
}

export interface GeoFeature {
  type: 'Feature'
  properties: GeoFeatureProperties
  geometry: object
}

export interface GeoData {
  type: 'FeatureCollection'
  features: GeoFeature[]
}

export interface FirePoint {
  lat: number
  lon: number
}

// Per-year national totals — from v_global_fire_summary view
export interface GlobalFireYear {
  year: number
  incident_count: number
  total_burned_stremata: number
  total_burned_ha: number
}

// Per-municipality per-year stats — from v_municipality_fire_summary view
export interface MuniFireYear {
  year: number
  incident_count: number
  total_burned_ha: number
  max_single_fire_ha: number | null
}

// Per-municipality all-time totals — from v_municipality_fire_totals view
export interface MuniFireTotals {
  municipality_id: string
  total_incidents: number
  total_burned_ha: number
  last_fire_year: number | null
}
