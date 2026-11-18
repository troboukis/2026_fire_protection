import { useOverviewUi, greekOverviewUi, type OverviewUi } from '../i18n/overview'
import { useMemo, useRef, useState } from 'react'
import ComponentTag from './ComponentTag'
import { fundingSummary, type FundingSummary } from '../data/fundingSummary'
import { municipalSpending } from '../data/municipalSpending'

type FundingChartHoverState = {
  year: number
  leftPct: number
}

type FundingProps = {
  currentYear: number
  anchorId?: string
}

function formatNumber(value: number, maximumFractionDigits = 0, ui: OverviewUi = greekOverviewUi): string {
  return value.toLocaleString(ui.locale, { maximumFractionDigits })
}

function formatPct(value: number | null, ui: OverviewUi = greekOverviewUi): string {
  if (value == null || Number.isNaN(value)) return '—'
  return `${value > 0 ? '+' : ''}${formatNumber(value, 0, ui)}%`
}

function formatEur(value: number | null, ui: OverviewUi = greekOverviewUi): string {
  if (value == null || Number.isNaN(value)) return '—'
  return value.toLocaleString(ui.locale, {
    style: 'currency',
    currency: 'EUR',
    maximumFractionDigits: 0,
  })
}

function formatEurCompact(value: number, ui: OverviewUi = greekOverviewUi): string {
  if (Number.isNaN(value)) return '—'
  const abs = Math.abs(value)
  if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}B €`
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M €`
  if (abs >= 1_000) return `${(value / 1_000).toFixed(0)}k €`
  return formatEur(value, ui)
}

function formatEurCompactMillions(value: number | null, ui: OverviewUi = greekOverviewUi): string {
  if (value == null || Number.isNaN(value)) return '—'
  const millionsRounded = Math.round((value / 1_000_000) * 10) / 10
  const millionsText = Number.isInteger(millionsRounded)
    ? String(millionsRounded.toFixed(0))
    : String(millionsRounded.toFixed(1))
  return ui("{{v0}}Μ €", { v0: millionsText })
}

function pctColor(value: number | null): string {
  if (value == null || Number.isNaN(value) || value === 0) return 'var(--ink)'
  return value > 0 ? '#1f8f55' : '#c63b32'
}

export default function Funding({ currentYear, anchorId = 'funding' }: FundingProps) {
  const ui = useOverviewUi()
  const fundingData: FundingSummary = fundingSummary
  const renderedYear = fundingData.yearMain || currentYear
  const [fundingChartHover, setFundingChartHover] = useState<FundingChartHoverState | null>(null)
  const fundingChartFrameRef = useRef<HTMLDivElement | null>(null)

  const fundingHistory = useMemo(() => fundingData.history, [fundingData.history])
  const fundingChartMax = useMemo(() => {
    const values = fundingHistory.map((entry) => entry.totalAmount).filter((value) => Number.isFinite(value))
    return values.length > 0 ? Math.max(1, ...values) : 1
  }, [fundingHistory])
  const fundingProgressPct = useMemo(() => {
    if (!Number.isFinite(fundingData.currentTotal) || fundingData.currentTotal <= 0) return 0
    return Math.max(0, Math.min(100, (fundingData.currentSpendAmount / fundingData.currentTotal) * 100))
  }, [fundingData])
  const fundingChartTicks = [1, 0.5, 0]
  const fundingDeltaPct = useMemo(() => {
    if (!Number.isFinite(fundingData.previousTotal) || fundingData.previousTotal <= 0) return null

    const pct = ((fundingData.currentTotal - fundingData.previousTotal) / fundingData.previousTotal) * 100
    return Number.isFinite(pct) && pct !== 0 ? pct : null
  }, [fundingData])
  const fundingChartTooltip = useMemo(() => {
    if (!fundingChartHover) return null
    const entry = fundingHistory.find((item) => item.year === fundingChartHover.year) ?? null
    if (!entry) return null

    return {
      year: entry.year,
      regularAmount: entry.regularAmount,
      emergencyAmount: entry.emergencyAmount,
      municipalityAmount: entry.municipalityAmount,
      syndesmosAmount: entry.syndesmosAmount,
      totalAmount: entry.totalAmount,
      leftPct: fundingChartHover.leftPct,
    }
  }, [fundingChartHover, fundingHistory])

  return (
    <section id={anchorId} className="funding-hero hero section-rule dev-tag-anchor">
      <div className="dev-tag-stack dev-tag-stack--right">
        <ComponentTag name="Funding" />
        <ComponentTag name="funding-hero hero section-rule" kind="CLASS" />
      </div>

      <div className="hero-left funding-hero__left dev-tag-anchor">
        <ComponentTag name="funding-hero__left" kind="CLASS" className="component-tag--overlay" />
        <div className="hero-amount-card funding-hero__amount-card dev-tag-anchor">
          <ComponentTag name="funding-hero__amount-card" kind="CLASS" className="component-tag--overlay" />
          <div className="eyebrow">{ui("ΧΡΗΜΑΤΟΔΟΤΗΣΗ - {{v0}}", { v0: renderedYear })}</div>
          <div className="funding-hero__headline">
            <div className="hero-amount">{formatEurCompact(fundingData.currentTotal, ui)}</div>
            <div className="funding-hero__delta" style={{ color: pctColor(fundingDeltaPct) }}>
              {fundingDeltaPct == null ? '—' : formatPct(fundingDeltaPct, ui)}
            </div>
          </div>
          <p className="funding-hero__lede">
            {ui("Ετήσια κρατική χρηματοδότηση για δράσεις πυροπροστασίας προς δήμους και συνδέσμους δήμων.")}</p>
          <div
            className="funding-hero__progress"
            aria-label={ui("Δαπάνες {{v0}} σε σχέση με χρηματοδότηση {{v1}} για το {{v2}}", { v0: formatEur(fundingData.currentSpendAmount, ui), v1: formatEur(fundingData.currentTotal, ui), v2: fundingData.currentSpendYear })}
            title={ui("Συμβάσεις ΚΗΜΔΗΣ: {{v0}}. Οριστικοποιήσεις πληρωμής δήμων στη Διαύγεια χωρίς σύνδεση με σύμβαση: {{v1}}. Ποσά χωρίς ΦΠΑ, έως {{v2}}.", { v0: formatEur(municipalSpending.contractsAmount, ui), v1: formatEur(municipalSpending.unlinkedPaymentsAmount, ui), v2: new Date(`${municipalSpending.periodEnd}T12:00:00`).toLocaleDateString(ui.locale) })}
          >
            <div className="funding-hero__progress-divider" aria-hidden="true" />
            <p className="funding-hero__progress-value">{ui("Εκτίμηση δαπανών Δήμων μέχρι σήμερα: {{v0}}", { v0: formatEurCompactMillions(fundingData.currentSpendAmount, ui) })}</p>
            <div className="funding-hero__progress-track" aria-hidden="true">
              <div
                className="funding-hero__progress-fill"
                style={{ width: `${fundingProgressPct}%` }}
              />
            </div>
            <div className="funding-hero__progress-labels" aria-hidden="true">
              <span>0 €</span>
              <span>{formatEurCompactMillions(fundingData.currentTotal, ui)}</span>
            </div>
          </div>

          <div className="hero-subgrid funding-hero__subgrid">
            <div>
              <span className="label">{ui("Δήμοι")}</span>
              <strong>{formatEur(fundingData.currentMunicipalityAmount, ui)}</strong>
            </div>
            <div>
              <span className="label">{ui("Σύνδεσμοι δήμων")}</span>
              <strong>{formatEur(fundingData.currentSyndesmosAmount, ui)}</strong>
            </div>
          </div>
        </div>
      </div>

      <div className="hero-right funding-hero__right dev-tag-anchor">
        <ComponentTag name="funding-hero__right" kind="CLASS" className="component-tag--overlay" />
        <div className="funding-hero__chart-card dev-tag-anchor">
          <ComponentTag name="funding-hero__chart-card" kind="CLASS" className="component-tag--overlay" />
          <div className="hero-chart funding-hero__chart">
            <div className="hero-chart__head">
              <span className="eyebrow">{ui("Χρηματοδότηση ανά έτος")}</span>
            </div>
            <div className="funding-hero__chart-body">
              {fundingHistory.length > 0 ? (
                <>
                <div className="municipality-funding-chart-wrap funding-hero__chart-wrap">
                  <div className="municipality-funding-chart__y-axis" aria-hidden="true">
                    <div className="funding-hero__y-axis-scale">
                      {fundingChartTicks.map((tick) => (
                        <span
                          key={`funding-hero-tick-${tick}`}
                          style={{ top: `${(1 - tick) * 100}%` }}
                        >
                          {tick === 0 ? '0 €' : formatEurCompact(fundingChartMax * tick, ui)}
                        </span>
                      ))}
                    </div>
                  </div>
                  <div
                    ref={fundingChartFrameRef}
                    className="municipality-funding-chart"
                    aria-label={ui("Ετήσια χρηματοδότηση ΚΑΠ προς δήμους και συνδέσμους δήμων")}
                    onMouseLeave={() => setFundingChartHover(null)}
                  >
                    {fundingHistory.map((entry) => {
                      const isCurrentYear = entry.year === renderedYear
                      const regularHeight = (entry.regularAmount / fundingChartMax) * 100
                      const emergencyHeight = (entry.emergencyAmount / fundingChartMax) * 100

                      return (
                        <div
                          key={entry.year}
                          className="municipality-funding-chart__bar-group"
                          onMouseEnter={(event) => {
                            const frame = fundingChartFrameRef.current
                            if (!frame) return
                            const frameRect = frame.getBoundingClientRect()
                            const barRect = event.currentTarget.getBoundingClientRect()
                            if (frameRect.width <= 0) return
                            const leftPct = ((barRect.left + barRect.width / 2 - frameRect.left) / frameRect.width) * 100
                            setFundingChartHover({ year: entry.year, leftPct })
                          }}
                          onMouseMove={(event) => {
                            const frame = fundingChartFrameRef.current
                            if (!frame) return
                            const frameRect = frame.getBoundingClientRect()
                            const barRect = event.currentTarget.getBoundingClientRect()
                            if (frameRect.width <= 0) return
                            const leftPct = ((barRect.left + barRect.width / 2 - frameRect.left) / frameRect.width) * 100
                            setFundingChartHover({ year: entry.year, leftPct })
                          }}
                        >
                          <div className="municipality-funding-chart__track" aria-hidden="true">
                            <div
                              className={`municipality-funding-chart__fill municipality-funding-chart__fill--regular${isCurrentYear ? ' is-current' : ''}`}
                              style={{ height: `${Math.max(entry.regularAmount > 0 ? 4 : 0, regularHeight)}%` }}
                            />
                            <div
                              className={`municipality-funding-chart__fill municipality-funding-chart__fill--emergency${isCurrentYear ? ' is-current' : ''}`}
                              style={{
                                height: `${Math.max(entry.emergencyAmount > 0 ? 4 : 0, emergencyHeight)}%`,
                                bottom: `${Math.max(0, regularHeight)}%`,
                              }}
                            />
                          </div>
                          <span className="municipality-funding-chart__year">{String(entry.year).slice(-2)}</span>
                        </div>
                      )
                    })}
                    {fundingChartTooltip ? (
                      <>
                        <div
                          className="municipality-funding-chart__hover-line"
                          aria-hidden="true"
                          style={{ left: `${fundingChartTooltip.leftPct}%` }}
                        />
                        <div
                          className="municipality-contract-card__tooltip municipality-funding-chart__tooltip"
                          style={{ left: `${Math.min(92, Math.max(8, fundingChartTooltip.leftPct))}%` }}
                        >
                          <strong>{fundingChartTooltip.year}</strong>
                          <span>{ui("Τακτική: ")}{formatEur(fundingChartTooltip.regularAmount, ui)}</span>
                          <span>{ui("Έκτακτη: ")}{formatEur(fundingChartTooltip.emergencyAmount, ui)}</span>
                          <span>{ui("Δήμοι: ")}{formatEur(fundingChartTooltip.municipalityAmount, ui)}</span>
                          <span>{ui("Σύνδεσμοι: ")}{formatEur(fundingChartTooltip.syndesmosAmount, ui)}</span>
                          <span>{ui("Σύνολο: ")}{formatEur(fundingChartTooltip.totalAmount, ui)}</span>
                        </div>
                      </>
                    ) : null}
                  </div>
                </div>

                <div className="municipality-funding-chart__legend" aria-hidden="true">
                  <span>
                    <i className="municipality-funding-chart__legend-swatch municipality-funding-chart__legend-swatch--regular" />
                    {ui("Τακτική")}</span>
                  <span>
                    <i className="municipality-funding-chart__legend-swatch municipality-funding-chart__legend-swatch--emergency" />
                    {ui("Έκτακτη")}</span>
                </div>

                <p className="note-text funding-hero__chart-note">
                  {ui("Από {{v0}} έως {{v1}}.", { v0: fundingData.historyStartYear, v1: renderedYear })}
                </p>
                </>
              ) : (
                <p className="note-text funding-hero__chart-note">{ui("Δεν υπάρχουν διαθέσιμα ιστορικά στοιχεία χρηματοδότησης.")}</p>
              )}
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
