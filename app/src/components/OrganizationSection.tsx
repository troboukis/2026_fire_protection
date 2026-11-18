import { useOverviewUi } from '../i18n/overview'
import { useEffect, useMemo, useState } from 'react'
import * as d3 from 'd3'
import { LocalizedLink as Link } from '../routing/LocalizedLink'
import ComponentTag from './ComponentTag'
import type { ContractModalContract } from './ContractModal'
import DataLoadingCard from './DataLoadingCard'
import MapLegendToggle from './MapLegendToggle'
import { loadMunicipalitiesGeojson } from '../lib/municipalitiesGeojson'
import type { GeoData } from '../types'

type OrganizationTimelineItem = {
  month: string
  year: string
  text: string
  contract: ContractModalContract | null
}

export type OrganizationSectionData = {
  name: string
  yearLabel: string
  previousYearLabel: string
  totalSpend: number
  cpvCodes: string[]
  cpvCodeItems: Array<{ code: string; label: string }>
  topCpvValue: string | null
  previousYearTopCpvValue: string | null
  contractCount: number
  previousYearContractCount: number
  beneficiaryCount: number
  previousYearBeneficiaryCount: number
  latestSignedAt: string | null
  activityWorkPoints: Array<{ lat: number; lon: number; work: string; pointName: string }>
  timeline: OrganizationTimelineItem[]
  contractsPageHref: string | null
}

type OrganizationSectionProps = {
  data: OrganizationSectionData
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

function OrganizationActivityMap({
  workPoints,
  yearLabel,
  organizationName,
}: {
  workPoints: Array<{ lat: number; lon: number; work: string; pointName: string }>
  yearLabel: string
  organizationName: string
}) {
  const ui = useOverviewUi()
  const [geojson, setGeojson] = useState<GeoData | null>(null)
  const [showWorkPoints, setShowWorkPoints] = useState(true)

  useEffect(() => {
    let cancelled = false

    const loadGeojson = async () => {
      const data = await loadMunicipalitiesGeojson()
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
    return <DataLoadingCard className="organization-map organization-map--empty" compact message={ui("Προετοιμάζεται ο χάρτης δραστηριότητας του φορέα.")} />
  }

  if (mapData.points.length === 0) {
    return <div className="organization-map organization-map--empty">{ui("Δεν βρέθηκαν σημεία δραστηριότητας.")}</div>
  }

  return (
    <div className="organization-map">
      <svg viewBox="0 0 360 280" role="img" aria-label={ui("Χάρτης δραστηριότητας φορέα")} preserveAspectRatio="xMinYMin meet">
        <g className="organization-map__base">
          {mapData.paths.map((feature) => (
            <path key={feature.key} d={feature.d} fill="none" stroke="#000000" />
          ))}
        </g>
        <g className="organization-map__points">
          {showWorkPoints && mapData.points.map((point) => (
            <circle key={point.key} cx={point.x} cy={point.y} r={2.6} fill="#ff4d3d" stroke="#000000" />
          ))}
        </g>
      </svg>
      <div className="organization-map__legend">
        <MapLegendToggle
          visible={showWorkPoints}
          onToggle={() => setShowWorkPoints((visible) => !visible)}
          label={ui("Εργασίες του {{v0}} το {{v1}}", { v0: ui.organization(organizationName), v1: yearLabel })}
        >
          <span className="organization-map__legend-dot" aria-hidden="true" />
          <span>{ui("Εργασίες του {{v0}} το {{v1}}", { v0: ui.organization(organizationName), v1: yearLabel })}</span>
        </MapLegendToggle>
      </div>
    </div>
  )
}

export default function OrganizationSection({
  data,
  loading,
  formatEurCompact,
  formatDateEl,
  onOpenContract,
  anchorId,
}: OrganizationSectionProps) {
  const ui = useOverviewUi()
  if (loading) {
    return (
      <section id={anchorId} className="organization section-rule dev-tag-anchor">
        <div className="dev-tag-stack dev-tag-stack--right">
          <ComponentTag name={`OrganizationSection: ${data.name}`} />
          <ComponentTag name="organization section-rule" kind="CLASS" />
        </div>
        <div className="organization__header dev-tag-anchor">
          <ComponentTag name="organization__header" kind="CLASS" className="component-tag--overlay" />
          <div className="eyebrow">{ui("ΟΡΓΑΝΙΣΜΟΣ")}</div>
          <h2>{ui.organization(data.name)}</h2>
          <p>
            {ui("Δυναμική ενημέρωση δεδομένων από το Kεντρικό Ηλεκτρονικό Μητρώο Δημοσίων Συμβάσεων.")}</p>
        </div>
        <DataLoadingCard message={ui("Ανακτώνται οι συμβάσεις και οι δείκτες για τον φορέα {{v0}}.", { v0: ui.organization(data.name) })} />
      </section>
    )
  }

  const totalSpendNote = ui("Συνολικό ποσό χωρίς ΦΠΑ από τις καταγεγραμμένες πληρωμές του {{v0}}.", { v0: data.yearLabel })
  const topCpvNote = data.previousYearTopCpvValue
    ? `${data.previousYearLabel}: ${ui.description(data.previousYearTopCpvValue)}`
    : ui("Δεν υπάρχουν στοιχεία για το {{v0}}.", { v0: data.previousYearLabel })
  const contractCountNote = ui("Το {{v0}} είχαν υπογραφεί {{v1}} συμβάσεις με {{v2}} εταιρείες", { v0: data.previousYearLabel, v1: data.previousYearContractCount.toLocaleString(ui.locale), v2: data.previousYearBeneficiaryCount.toLocaleString(ui.locale) })
  const contractLabel = data.contractCount === 1 ? ui("σύμβαση") : ui("συμβάσεις")
  const beneficiaryLabel = data.beneficiaryCount === 1 ? ui("εταιρεία") : ui("εταιρείες")
  const timelineItems = data.timeline.length
    ? data.timeline
    : [{ month: '—', year: '—', text: loading ? ui("Φόρτωση στοιχείων φορέα…") : ui("Δεν βρέθηκαν συμβάσεις για τον φορέα."), contract: null }]
  const kpis = [
    {
      label: ui("Η ΠΙΟ ΣΥΧΝΗ ΕΡΓΑΣΙΑ ΤΟ {{v0}}", { v0: data.yearLabel }),
      value: ui.description(data.topCpvValue ?? '—'),
      note: topCpvNote,
    },
    {
      label: ui("ΤΕΛΕΥΤΑΙΑ"),
      value: formatDateEl(data.latestSignedAt),
      note: ui("υπεγράφη η πιο πρόσφατης σύμβαση"),
    },
  ]
  const cpvWallItems = data.cpvCodeItems.length
    ? data.cpvCodeItems
    : data.cpvCodes.map((code) => ({ code, label: code }))

  return (
    <section id={anchorId} className="organization section-rule dev-tag-anchor">
      <div className="dev-tag-stack dev-tag-stack--right">
        <ComponentTag name={`OrganizationSection: ${data.name}`} />
        <ComponentTag name="organization section-rule" kind="CLASS" />
      </div>
      <div className="organization__header dev-tag-anchor">
        <ComponentTag name="organization__header" kind="CLASS" className="component-tag--overlay" />
        <div className="eyebrow">{ui("ΟΡΓΑΝΙΣΜΟΣ")}</div>
        <h2>{ui.organization(data.name)}</h2>
        <p>
          {ui("Δυναμική ενημέρωση δεδομένων από το Kεντρικό Ηλεκτρονικό Μητρώο Δημοσίων Συμβάσεων.")}</p>
      </div>

      <div className="organization__hero dev-tag-anchor">
        <ComponentTag name="organization__hero" kind="CLASS" className="component-tag--overlay" />
        <div className="org-year" aria-hidden="true">
          {data.yearLabel}
        </div>
        <div className="org-total dev-tag-anchor">
          <ComponentTag name="org-total" kind="CLASS" className="component-tag--overlay" />
          <span className="eyebrow">{ui("Συνολική Δαπάνη ")}</span>
          <div className="org-total__value">{formatEurCompact(data.totalSpend)}</div>
          <div className="org-total__note">
            {totalSpendNote}
          </div>
        </div>
        <div className="org-codes dev-tag-anchor">
          <ComponentTag
            name="org-codes"
            kind="CLASS"
            className="component-tag--overlay"
            style={{ left: 'auto', right: '0.45rem' }}
          />
          <OrganizationActivityMap
            workPoints={data.activityWorkPoints}
            yearLabel={data.yearLabel}
            organizationName={data.name}
          />
        </div>
      </div>

      <div className="organization__grid dev-tag-anchor">
        <ComponentTag name="organization__grid" kind="CLASS" className="component-tag--overlay" />
        <div className="organization__kpis dev-tag-anchor">
          <ComponentTag name="organization__kpis" kind="CLASS" className="component-tag--overlay" />
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
                <div className="org-kpi__value">{data.contractCount.toLocaleString(ui.locale)} {contractLabel} {ui("με ")}{data.beneficiaryCount.toLocaleString(ui.locale)} {beneficiaryLabel}</div>
                <p>{contractCountNote}</p>
              </section>
            </div>
          </article>
          <article className="org-kpi org-kpi--cpv" key="cpv-wall">
            <div className="eyebrow">{ui("Οι πιο συχνές κατηγορίες εργασιών")}</div>
            <div className="cpv-wall">
              {(cpvWallItems.length ? cpvWallItems : [{ code: '—', label: '—' }]).map((item) => (
                <span
                  key={item.code}
                  className="cpv-wall__item"
                  tabIndex={0}
                  title={ui.cpv(item.code, item.label)}
                  aria-label={`${item.code}: ${ui.cpv(item.code, item.label)}`}
                  data-tooltip={ui.cpv(item.code, item.label)}
                >
                  {item.code}
                </span>
              ))}
            </div>
          </article>
        </div>

        <div className="organization__timeline dev-tag-anchor">
          <ComponentTag
            name="organization__timeline"
            kind="CLASS"
            className="component-tag--overlay"
            style={{ left: 'auto', right: '0.45rem' }}
          />
          <div className="eyebrow">{ui("Χρονολόγιο")}</div>
          <ul>
            {timelineItems.map((item) => (
              <li key={`${item.month}-${item.year}-${item.text}`}>
                <div className="timeline-date">
                  <span>{item.contract?.sourceValues?.signedAt ? ui.format.date(item.contract.sourceValues.signedAt, { day: undefined, year: undefined, month: 'short' }) : item.month}</span>
                  <strong>{item.year}</strong>
                </div>
                {item.contract && onOpenContract ? (
                  <button
                    type="button"
                    className="timeline-contract-link"
                    lang="el"
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
          {data.contractsPageHref ? (
            <Link className="organization__timeline-all-link" to={data.contractsPageHref}>
              {ui("Όλες οι συμβάσεις")}</Link>
          ) : null}
        </div>
      </div>
    </section>
  )
}
