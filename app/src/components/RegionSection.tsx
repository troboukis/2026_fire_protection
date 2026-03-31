import { useEffect, useMemo, useState } from 'react'
import * as d3 from 'd3'
import type { ContractModalContract } from './ContractModal'
import DataLoadingCard from './DataLoadingCard'
import type { OrganizationSectionData } from './OrganizationSection'
import type { GeoData } from '../types'

type RegionTimelineItem = {
  month: string
  year: string
  text: string
  contract: ContractModalContract | null
}

export type RegionSectionData = OrganizationSectionData

type RegionSectionProps = {
  data: RegionSectionData
  loading: boolean
  formatEurCompact: (n: number) => string
  formatDateEl: (iso: string | null) => string
  onOpenContract?: (contract: ContractModalContract) => void
  anchorId?: string
}

function normalizeMunicipalityId(input: unknown): string {
  const raw = String(input ?? '').trim()
  if (!raw) return ''
  const noDecimal = raw.replace(/\.0+$/, '')
  if (/^\d+$/.test(noDecimal)) return String(Number(noDecimal))
  return noDecimal
}

function RegionActivityMap({
  workPoints,
  yearLabel,
  regionName,
}: {
  workPoints: Array<{ lat: number; lon: number; work: string; pointName: string }>
  yearLabel: string
  regionName: string
}) {
  const [geojson, setGeojson] = useState<GeoData | null>(null)

  useEffect(() => {
    let cancelled = false

    const loadGeojson = async () => {
      const res = await fetch(`${import.meta.env.BASE_URL}municipalities.geojson`)
      const data = await res.json() as GeoData
      if (!cancelled) setGeojson(data)
    }

    loadGeojson().catch(() => {
      if (!cancelled) setGeojson(null)
    })

    return () => { cancelled = true }
  }, [])

  const mapData = useMemo(() => {
    if (!geojson) return null

    const width = 360
    const height = 360
    const projection = d3.geoMercator().fitExtent(
      [[12, 5], [width - 18, height - 40]],
      geojson as unknown as d3.ExtendedFeatureCollection,
    )
    const [tx, ty] = projection.translate()
    projection.translate([tx - 22, ty - 20])
    const path = d3.geoPath().projection(projection)
    const points: Array<{ x: number; y: number; key: string }> = []

    for (const point of workPoints) {
      const projected = projection([point.lon, point.lat])
      if (!projected) continue
      const [x, y] = projected
      if (!Number.isFinite(x) || !Number.isFinite(y)) continue
      points.push({ x, y, key: `${point.lat.toFixed(6)}|${point.lon.toFixed(6)}|${point.work}|${point.pointName}` })
    }

    return {
      paths: geojson.features.map((feature, idx) => ({
        key: `${normalizeMunicipalityId((feature.properties as { municipality_code?: string | null }).municipality_code)}-${idx}`,
        d: path(feature as unknown as d3.GeoPermissibleObjects) ?? '',
      })),
      points,
    }
  }, [geojson, workPoints])

  if (!mapData) {
    return <DataLoadingCard className="organization-map organization-map--empty" compact message="Προετοιμάζεται ο χάρτης δραστηριότητας της περιφέρειας." />
  }

  return (
    <div className="organization-map">
      <svg viewBox="0 0 360 280" role="img" aria-label="Χάρτης δραστηριότητας περιφέρειας" preserveAspectRatio="xMinYMin meet">
        <g className="organization-map__base">
          {mapData.paths.map((feature) => (
            <path key={feature.key} d={feature.d} fill="none" stroke="#000000" />
          ))}
        </g>
        <g className="organization-map__points">
          {mapData.points.map((point) => (
            <circle key={point.key} cx={point.x} cy={point.y} r={2.6} fill="#ff4d3d" stroke="#000000" />
          ))}
        </g>
      </svg>
      {mapData.points.length > 0 ? (
        <div className="organization-map__legend">
          <span className="organization-map__legend-dot" aria-hidden="true" />
          <span>{`Εργασίες στην ${regionName} το ${yearLabel}`}</span>
        </div>
      ) : null}
    </div>
  )
}

export default function RegionSection({
  data,
  loading,
  formatEurCompact,
  formatDateEl,
  onOpenContract,
  anchorId,
}: RegionSectionProps) {
  if (loading) {
    return (
      <section id={anchorId} className="organization section-rule">
        <div className="organization__header">
          <div className="eyebrow">ΠΕΡΙΦΕΡΕΙΑ</div>
          <h2>{data.name}</h2>
          <p>
            Δυναμική ενημέρωση δεδομένων από το Κεντρικό Ηλεκτρονικό Μητρώο Δημοσίων Συμβάσεων.
          </p>
        </div>
        <DataLoadingCard message={`Ανακτώνται οι συμβάσεις και οι δείκτες για την ${data.name}.`} />
      </section>
    )
  }

  const totalSpendNote = `Συνολικό ποσό χωρίς ΦΠΑ από τις καταγεγραμμένες πληρωμές του ${data.yearLabel}.`
  const topCpvNote = data.previousYearTopCpvValue
    ? `${data.previousYearLabel}: ${data.previousYearTopCpvValue}`
    : `Δεν υπάρχουν στοιχεία για το ${data.previousYearLabel}.`
  const contractCountNote = `Το ${data.previousYearLabel} είχαν υπογραφεί ${data.previousYearContractCount.toLocaleString('el-GR')} συμβάσεις με ${data.previousYearBeneficiaryCount.toLocaleString('el-GR')} προμηθευτές`
  const contractLabel = data.contractCount === 1 ? 'σύμβαση' : 'συμβάσεις'
  const beneficiaryLabel = data.beneficiaryCount === 1 ? 'προμηθευτή' : 'προμηθευτές'
  const timelineItems: RegionTimelineItem[] = data.timeline.length
    ? data.timeline
    : [{ month: '—', year: '—', text: loading ? 'Φόρτωση στοιχείων περιφέρειας…' : 'Δεν βρέθηκαν συμβάσεις για την περιφέρεια.', contract: null }]
  const kpis = [
    {
      label: `Η ΠΙΟ ΣΥΧΝΗ ΕΡΓΑΣΙΑ ΤΟ ${data.yearLabel}`,
      value: data.topCpvValue ?? '—',
      note: topCpvNote,
    },
    {
      label: 'ΤΕΛΕΥΤΑΙΑ',
      value: formatDateEl(data.latestSignedAt),
      note: 'υπεγράφη η πιο πρόσφατης σύμβαση',
    },
  ]

  return (
    <section id={anchorId} className="organization section-rule">
      <div className="organization__header">
        <div className="eyebrow">ΠΕΡΙΦΕΡΕΙΑ</div>
        <h2>{data.name}</h2>
        <p>
          Δυναμική ενημέρωση δεδομένων από το Κεντρικό Ηλεκτρονικό Μητρώο Δημοσίων Συμβάσεων.
        </p>
      </div>

      <div className="organization__hero">
        <div className="org-year" aria-hidden="true">
          {data.yearLabel}
        </div>
        <div className="org-total">
          <span className="eyebrow">Συνολική Δαπάνη </span>
          <div className="org-total__value">{formatEurCompact(data.totalSpend)}</div>
          <div className="org-total__note">
            {totalSpendNote}
          </div>
        </div>
        <div className="org-codes">
          <RegionActivityMap
            workPoints={data.activityWorkPoints}
            yearLabel={data.yearLabel}
            regionName={data.name}
          />
        </div>
      </div>

      <div className="organization__grid">
        <div className="organization__kpis">
          {kpis.map((kpi) => (
            <article className="org-kpi" key={kpi.label}>
              <div className="eyebrow">{kpi.label}</div>
              <div className="org-kpi__value">{kpi.value}</div>
              <p>{kpi.note}</p>
            </article>
          ))}
          <article className="org-kpi org-kpi--split" key="contracts-beneficiaries">
            <div className="org-kpi-split">
              <section className="org-kpi-split__section">
                <div className="eyebrow">{data.yearLabel}</div>
                <div className="org-kpi__value">{data.contractCount.toLocaleString('el-GR')} {contractLabel} με {data.beneficiaryCount.toLocaleString('el-GR')} {beneficiaryLabel}</div>
                <p>{contractCountNote}</p>
              </section>
            </div>
          </article>
          <article className="org-kpi org-kpi--cpv" key="cpv-wall">
            <div className="eyebrow">Οι πιο συχνές κατηγορίες εργασιών</div>
            <div className="cpv-wall">
              {(data.cpvCodes.length ? data.cpvCodes : ['—']).map((code) => (
                <span key={code}>{code}</span>
              ))}
            </div>
          </article>
        </div>

        <div className="organization__timeline">
          <div className="eyebrow">Χρονολόγιο</div>
          <ul>
            {timelineItems.map((item, index) => (
              <li key={item.contract?.id ?? `${item.month}-${item.year}-${item.text}-${index}`}>
                <div className="timeline-date">
                  <span>{item.month}</span>
                  <strong>{item.year}</strong>
                </div>
                {item.contract && onOpenContract ? (
                  <button
                    type="button"
                    className="timeline-contract-link"
                    onClick={() => onOpenContract(item.contract!)}
                  >
                    {item.text}
                  </button>
                ) : (
                  <p>{item.text}</p>
                )}
              </li>
            ))}
          </ul>
        </div>
      </div>
    </section>
  )
}
