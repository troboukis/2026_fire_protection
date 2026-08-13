import { describe, expect, it } from 'vitest'
import { excludeFirmsMapHotspots, isExcludedFirmsMapHotspot } from './firmsMapExclusions'

describe('FIRMS map hotspot exclusions', () => {
  it('excludes the recurring Volos industrial hotspot', () => {
    expect(isExcludedFirmsMapHotspot(39.35443, 22.98489)).toBe(true)
    expect(isExcludedFirmsMapHotspot(39.35591, 22.98446)).toBe(true)
    expect(isExcludedFirmsMapHotspot(39.35419, 22.98681)).toBe(true)
  })

  it('does not exclude the current Kileler, Larissa, or Amyntaio detections', () => {
    const rows = [
      { area: 'Kileler', latitude: 39.49898, longitude: 22.38703 },
      { area: 'Larissa', latitude: 39.66146, longitude: 22.36858 },
      { area: 'Amyntaio', latitude: 40.59059, longitude: 21.60364 },
    ]

    expect(excludeFirmsMapHotspots(rows)).toEqual(rows)
  })
})
