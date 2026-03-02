import { useMemo } from 'react'
import * as d3 from 'd3'
import ComponentTag from './ComponentTag'
import EditorialLead from './EditorialLead'
import LatestContractCard, { type LatestContractCardView } from './LatestContractCard'
import type { GeoFeature } from '../types'

export type SelectionSource = 'dropdown' | 'map' | 'search'
export type SelectionKind = 'region' | 'municipality'

type Props = {
  source: SelectionSource | null
  kind: SelectionKind | null
  label: string
  municipalityLatestContracts?: LatestContractCardView[]
  municipalityLatestLoading?: boolean
  regionLatestContracts?: LatestContractCardView[]
  regionLatestLoading?: boolean
  municipalityFeature?: GeoFeature | null
  currentYear: number
  regionCurrentYearCount?: number | null
  municipalityCurrentYearCount?: number | null
  onContractOpen?: (id: string) => void
}

export default function MapSelectionPanel({
  source,
  kind,
  label,
  municipalityLatestContracts = [],
  municipalityLatestLoading = false,
  regionLatestContracts = [],
  regionLatestLoading = false,
  municipalityFeature,
  currentYear,
  regionCurrentYearCount,
  municipalityCurrentYearCount,
  onContractOpen,
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
        : `Το ${currentYear} έχουν δημοσιευθεί ${(regionCurrentYearCount ?? 0).toLocaleString('el-GR')} συμβάσεις που αφορούν στην πρόληψη ή αντιμετώπιση δασικών πυρκαγιών στην επιλεγμένη περιφέρεια.`

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

      {kind === 'municipality' && (
        <section className="maps-selection-panel__latest" aria-label="Τελευταίες συμβάσεις δήμου">
          <div className="maps-selection-panel__latest-label">
            <span className="eyebrow">τελευταίες συμβάσεις</span>
          </div>
          <div className="maps-selection-panel__latest-items">
            {municipalityLatestLoading && (
              <article className="wire-item">
                <h2>Φόρτωση συμβάσεων…</h2>
              </article>
            )}
            {!municipalityLatestLoading && municipalityLatestContracts.map((item) => (
              <LatestContractCard key={item.id} item={item} onOpen={onContractOpen} />
            ))}
            {!municipalityLatestLoading && municipalityLatestContracts.length === 0 && (
              <article className="wire-item">
                <h2>Δεν βρέθηκαν συμβάσεις για τον επιλεγμένο δήμο.</h2>
              </article>
            )}
          </div>
        </section>
      )}

      {kind === 'region' && (
        <section className="maps-selection-panel__latest" aria-label="Τελευταίες συμβάσεις περιφέρειας">
          <div className="maps-selection-panel__latest-label">
            <span className="eyebrow">τελευταίες συμβάσεις περιφέρειας</span>
          </div>
          <div className="maps-selection-panel__latest-items">
            {regionLatestLoading && (
              <article className="wire-item">
                <h2>Φόρτωση συμβάσεων…</h2>
              </article>
            )}
            {!regionLatestLoading && regionLatestContracts.map((item) => (
              <LatestContractCard key={item.id} item={item} onOpen={onContractOpen} />
            ))}
            {!regionLatestLoading && regionLatestContracts.length === 0 && (
              <article className="wire-item">
                <h2>Δεν βρέθηκαν συμβάσεις για την επιλεγμένη περιφέρεια.</h2>
              </article>
            )}
          </div>
        </section>
      )}
    </aside>
  )
}
