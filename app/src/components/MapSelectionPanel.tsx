import { useMemo } from 'react'
import * as d3 from 'd3'
import ComponentTag from './ComponentTag'
import EditorialLead from './EditorialLead'
import type { GeoFeature } from '../types'

export type SelectionSource = 'dropdown' | 'map' | 'search'
export type SelectionKind = 'region' | 'municipality'

type Props = {
  source: SelectionSource | null
  kind: SelectionKind | null
  label: string
  municipalityFeature?: GeoFeature | null
  currentYear: number
  municipalityCurrentYearCount?: number | null
}

export default function MapSelectionPanel({
  source,
  kind,
  label,
  municipalityFeature,
  currentYear,
  municipalityCurrentYearCount,
}: Props) {
  const previewPath = useMemo(() => {
    if (!municipalityFeature) return null
    const width = 520
    const height = 320
    const pad = 12
    const projection = d3.geoMercator().fitExtent(
      [[pad, pad], [width - pad, height - pad]],
      municipalityFeature as unknown as d3.GeoPermissibleObjects,
    )
    const path = d3.geoPath().projection(projection)
    return path(municipalityFeature as unknown as d3.GeoPermissibleObjects)
  }, [municipalityFeature])

  const title =
    kind === 'municipality' && label
      ? `Δήμος ${label}`
      : kind === 'region' && label
        ? `Περιφέρεια ${label}`
        : 'Επιλογή περιοχής'

  const subtitle =
    !kind
      ? 'Επιλέξτε Δήμο ή Περιφέρεια στον χάρτη'
      : kind === 'municipality'
        ? `Το ${currentYear} έχουν δημοσιευθεί ${(municipalityCurrentYearCount ?? 0).toLocaleString('el-GR')} συμβάσεις που αφορούν στην πρόληψη ή αντιμετώπιση δασικών πυρκαγιών.`
        : 'Στον χάρτη επισημαίνονται οι δήμοι που ανήκουν στην επιλεγμένη περιφέρεια.'

  return (
    <aside className="maps-selection-panel" data-selection-source={source ?? 'none'}>
      <ComponentTag name="MapSelectionPanel" />
      <EditorialLead eyebrow="Ανάλυση" title={title} subtitle={subtitle} />

      {kind === 'municipality' && previewPath && (
        <div className="maps-selection-panel__shape" aria-label="Polygon Δήμου">
          <svg viewBox="0 0 520 320" role="img" aria-label={`Polygon ${label}`}>
            <path d={previewPath ?? ''} fill="#1a3a5c" fillOpacity="0.92" stroke="#0f2237" strokeWidth="1.3" />
          </svg>
        </div>
      )}
    </aside>
  )
}
