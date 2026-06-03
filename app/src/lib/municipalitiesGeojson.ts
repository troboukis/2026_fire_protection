import type { GeoData } from '../types'

let cachedMunicipalitiesGeojson: GeoData | null = null
let municipalitiesGeojsonPromise: Promise<GeoData> | null = null

export function loadMunicipalitiesGeojson(): Promise<GeoData> {
  if (cachedMunicipalitiesGeojson) return Promise.resolve(cachedMunicipalitiesGeojson)
  if (municipalitiesGeojsonPromise) return municipalitiesGeojsonPromise

  municipalitiesGeojsonPromise = fetch(`${import.meta.env.BASE_URL}municipalities.geojson`)
    .then(async (response) => {
      if (!response.ok) throw new Error(`municipalities.geojson failed with ${response.status}`)
      const data = (await response.json()) as GeoData
      cachedMunicipalitiesGeojson = data
      return data
    })
    .catch((error) => {
      municipalitiesGeojsonPromise = null
      throw error
    })

  return municipalitiesGeojsonPromise
}
