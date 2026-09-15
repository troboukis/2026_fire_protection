import { supabase } from './supabase'

export type CopernicusRow = {
  copernicus_id: number
  centroid: { coordinates?: [number, number] } | string | null
  shape: GeoJSON.Geometry | string | null
  area_ha: number | string | null
  firedate: string | null
  commune: string | null
  province: string | null
  municipality_key: string | null
}

const PAGE_SIZE = 500

export async function loadHistoricalCopernicus(
  start: string,
  end: string,
  signal: AbortSignal,
): Promise<CopernicusRow[]> {
  const rows: CopernicusRow[] = []
  let total: number | null = null

  while (true) {
    signal.throwIfAborted()
    const { data, error, count } = await supabase
      .from('copernicus')
      .select('copernicus_id, centroid, shape, area_ha, firedate, commune, province, municipality_key', {
        count: 'exact',
      })
      .gte('firedate', start)
      .lte('firedate', end)
      .order('firedate', { ascending: false })
      .order('copernicus_id', { ascending: false })
      .range(rows.length, rows.length + PAGE_SIZE - 1)
      .abortSignal(signal)

    signal.throwIfAborted()
    if (error) throw error
    const page = (data ?? []) as CopernicusRow[]
    total = count ?? total
    rows.push(...page)

    if (total != null && rows.length >= total) return rows
    if (page.length === 0) {
      if (total != null && rows.length < total) {
        throw new Error('Incomplete historical Copernicus response')
      }
      return rows
    }
    if (total == null && page.length < PAGE_SIZE) return rows
  }
}
