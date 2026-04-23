import { useEffect, useMemo, useRef, useState } from 'react'
import ComponentTag from './ComponentTag'
import DataLoadingCard from './DataLoadingCard'
import { createHomepageRpcCacheKey, loadCachedHomepageRpc, retryHomepageRpc } from '../lib/homepageRpcCache'
import { supabase } from '../lib/supabase'

type FundingHistoryEntry = {
  year: number
  regularAmount: number
  emergencyAmount: number
  municipalityAmount: number
  syndesmosAmount: number
  totalAmount: number
}

type FundingChartHoverState = {
  year: number
  leftPct: number
}

type FundingRpcHistoryRow = {
  year: number | string | null
  regular_amount: number | string | null
  emergency_amount: number | string | null
  municipality_amount: number | string | null
  syndesmos_amount: number | string | null
  total_amount: number | string | null
}

type FundingRpcPayload = {
  year_main: number | string | null
  year_previous: number | string | null
  history_start_year: number | string | null
  current_total: number | string | null
  previous_total: number | string | null
  current_regular_amount: number | string | null
  current_emergency_amount: number | string | null
  current_municipality_amount: number | string | null
  current_syndesmos_amount: number | string | null
  history: FundingRpcHistoryRow[] | null
}

type FundingSpendRpcPayload = {
  latest_funding_year: number | string | null
  total_amount: number | string | null
}

type FundingData = {
  yearMain: number
  yearPrevious: number
  historyStartYear: number
  currentTotal: number
  currentSpendAmount: number
  currentSpendYear: number
  previousTotal: number
  currentRegularAmount: number
  currentEmergencyAmount: number
  currentMunicipalityAmount: number
  currentSyndesmosAmount: number
  history: FundingHistoryEntry[]
}

type FundingProps = {
  currentYear: number
  anchorId?: string
}

function toNumber(value: unknown): number | null {
  if (value == null) return null
  const num = Number(value)
  return Number.isFinite(num) ? num : null
}

function formatNumber(value: number, maximumFractionDigits = 0): string {
  return value.toLocaleString('el-GR', { maximumFractionDigits })
}

function formatPct(value: number | null): string {
  if (value == null || Number.isNaN(value)) return '—'
  return `${value > 0 ? '+' : ''}${formatNumber(value, 0)}%`
}

function formatEur(value: number | null): string {
  if (value == null || Number.isNaN(value)) return '—'
  return value.toLocaleString('el-GR', {
    style: 'currency',
    currency: 'EUR',
    maximumFractionDigits: 0,
  })
}

function formatEurCompact(value: number): string {
  if (Number.isNaN(value)) return '—'
  const abs = Math.abs(value)
  if (abs >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(1)}B €`
  if (abs >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M €`
  if (abs >= 1_000) return `${(value / 1_000).toFixed(0)}k €`
  return formatEur(value)
}

function formatEurCompactMillions(value: number | null): string {
  if (value == null || Number.isNaN(value)) return '—'
  const millionsRounded = Math.round((value / 1_000_000) * 10) / 10
  const millionsText = Number.isInteger(millionsRounded)
    ? String(millionsRounded.toFixed(0))
    : String(millionsRounded.toFixed(1))
  return `${millionsText}Μ €`
}

function pctColor(value: number | null): string {
  if (value == null || Number.isNaN(value) || value === 0) return 'var(--ink)'
  return value > 0 ? '#1f8f55' : '#c63b32'
}

export default function Funding({ currentYear, anchorId = 'funding' }: FundingProps) {
  const [loading, setLoading] = useState(true)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [spendLoadError, setSpendLoadError] = useState<string | null>(null)
  const [fundingData, setFundingData] = useState<FundingData | null>(null)
  const [fundingChartHover, setFundingChartHover] = useState<FundingChartHoverState | null>(null)
  const fundingChartFrameRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setLoadError(null)
    setSpendLoadError(null)

    const loadFunding = async () => {
      try {
        const [fundingResult, spendResult] = await Promise.allSettled([
          loadCachedHomepageRpc(
            createHomepageRpcCacheKey('get_homepage_funding', {
              p_year_main: currentYear,
              p_year_start: 2016,
            }),
            () => retryHomepageRpc(async () => {
              const { data, error } = await supabase.rpc('get_homepage_funding', {
                p_year_main: currentYear,
                p_year_start: 2016,
              })
              if (error) throw error
              if (!data) throw new Error('Homepage funding RPC returned no data')
              return data as FundingRpcPayload
            }),
            { ttlMs: 5 * 60 * 1000 },
          ),
          loadCachedHomepageRpc(
            createHomepageRpcCacheKey('get_latest_funding_year_municipality_spend'),
            () => retryHomepageRpc(async () => {
              const { data, error } = await supabase.rpc('get_latest_funding_year_municipality_spend')
              if (error) throw error
              if (!data) throw new Error('Latest funding-year spend RPC returned no data')
              return data as FundingSpendRpcPayload
            }),
            { ttlMs: 5 * 60 * 1000 },
          ),
        ])

        if (fundingResult.status === 'rejected') throw fundingResult.reason

        const payload = fundingResult.value
        const spendPayload = spendResult.status === 'fulfilled' ? spendResult.value : null
        if (spendResult.status === 'rejected') {
          console.warn('[Funding] latest funding-year spend unavailable', spendResult.reason)
          if (!cancelled) {
            setSpendLoadError('Δεν ήταν δυνατή η φόρτωση της εκτίμησης δαπανών μέχρι σήμερα.')
          }
        }
        const history = (Array.isArray(payload.history) ? payload.history : [])
          .map<FundingHistoryEntry | null>((row) => {
            const year = toNumber(row.year)
            if (year == null) return null

            return {
              year,
              regularAmount: toNumber(row.regular_amount) ?? 0,
              emergencyAmount: toNumber(row.emergency_amount) ?? 0,
              municipalityAmount: toNumber(row.municipality_amount) ?? 0,
              syndesmosAmount: toNumber(row.syndesmos_amount) ?? 0,
              totalAmount: toNumber(row.total_amount) ?? 0,
            }
          })
          .filter((entry): entry is FundingHistoryEntry => entry !== null)

        const nextFundingData: FundingData = {
          yearMain: toNumber(payload.year_main) ?? currentYear,
          yearPrevious: toNumber(payload.year_previous) ?? currentYear - 1,
          historyStartYear: toNumber(payload.history_start_year) ?? (history[0]?.year ?? 2016),
          currentTotal: toNumber(payload.current_total) ?? 0,
          currentSpendAmount: toNumber(spendPayload?.total_amount) ?? 0,
          currentSpendYear: toNumber(spendPayload?.latest_funding_year) ?? (toNumber(payload.year_main) ?? currentYear),
          previousTotal: toNumber(payload.previous_total) ?? 0,
          currentRegularAmount: toNumber(payload.current_regular_amount) ?? 0,
          currentEmergencyAmount: toNumber(payload.current_emergency_amount) ?? 0,
          currentMunicipalityAmount: toNumber(payload.current_municipality_amount) ?? 0,
          currentSyndesmosAmount: toNumber(payload.current_syndesmos_amount) ?? 0,
          history,
        }

        if (!cancelled) setFundingData(nextFundingData)
      } catch (error) {
        console.error('[Funding] failed to load homepage funding data', error)
        if (!cancelled) {
          setFundingData(null)
          setLoadError('Δεν ήταν δυνατή η φόρτωση της κρατικής χρηματοδότησης προς δήμους και συνδέσμους δήμων.')
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    loadFunding()
    return () => {
      cancelled = true
    }
  }, [currentYear])

  const fundingHistory = useMemo(() => fundingData?.history ?? [], [fundingData])
  const fundingChartMax = useMemo(() => {
    const values = fundingHistory.map((entry) => entry.totalAmount).filter((value) => Number.isFinite(value))
    return values.length > 0 ? Math.max(1, ...values) : 1
  }, [fundingHistory])
  const fundingProgressPct = useMemo(() => {
    if (!fundingData) return 0
    if (spendLoadError) return 0
    if (!Number.isFinite(fundingData.currentTotal) || fundingData.currentTotal <= 0) return 0
    return Math.max(0, Math.min(100, (fundingData.currentSpendAmount / fundingData.currentTotal) * 100))
  }, [fundingData, spendLoadError])
  const fundingChartTicks = [1, 0.5, 0]
  const fundingDeltaPct = useMemo(() => {
    if (!fundingData) return null
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

  if (loading) {
    return (
      <section id={anchorId} className="funding-hero hero section-rule dev-tag-anchor">
        <div className="dev-tag-stack dev-tag-stack--right">
          <ComponentTag name="Funding" />
          <ComponentTag name="funding-hero hero section-rule" kind="CLASS" />
        </div>
        <div className="hero-left funding-hero__left dev-tag-anchor">
          <ComponentTag name="funding-hero__left" kind="CLASS" className="component-tag--overlay" />
          <DataLoadingCard message="Ανακτάται η χρηματοδότηση προς δήμους και συνδέσμους δήμων." />
        </div>
        <div className="hero-right funding-hero__right dev-tag-anchor">
          <ComponentTag name="funding-hero__right" kind="CLASS" className="component-tag--overlay" />
        </div>
      </section>
    )
  }

  return (
    <section id={anchorId} className="funding-hero hero section-rule dev-tag-anchor">
      <div className="dev-tag-stack dev-tag-stack--right">
        <ComponentTag name="Funding" />
        <ComponentTag name="funding-hero hero section-rule" kind="CLASS" />
      </div>

      <div className="hero-left funding-hero__left dev-tag-anchor">
        <ComponentTag name="funding-hero__left" kind="CLASS" className="component-tag--overlay" />
        <div className="funding-hero__background-year" aria-hidden="true">
          {fundingData?.yearMain ?? currentYear}
        </div>
        <div className="hero-amount-card funding-hero__amount-card dev-tag-anchor">
          <ComponentTag name="funding-hero__amount-card" kind="CLASS" className="component-tag--overlay" />
          <div className="eyebrow">{`ΧΡΗΜΑΤΟΔΟΤΗΣΗ - ${fundingData?.yearMain ?? currentYear}`}</div>
          <div className="funding-hero__headline">
            <div className="hero-amount">{formatEurCompact(fundingData?.currentTotal ?? 0)}</div>
            <div className="funding-hero__delta" style={{ color: pctColor(fundingDeltaPct) }}>
              {fundingDeltaPct == null ? '—' : formatPct(fundingDeltaPct)}
            </div>
          </div>
          <p className="funding-hero__lede">
            Ετήσια κατανομή για δράσεις πυροπροστασίας προς δήμους και συνδέσμους δήμων από Κεντρικούς Αυτοτελείς Πόρους.
          </p>
          {!loadError && !spendLoadError ? (
            <div
              className="funding-hero__progress"
              aria-label={`Δαπάνες ${formatEur(fundingData?.currentSpendAmount ?? 0)} σε σχέση με χρηματοδότηση ${formatEur(fundingData?.currentTotal ?? 0)} για το ${fundingData?.currentSpendYear ?? fundingData?.yearMain ?? currentYear}`}
            >
              <div className="funding-hero__progress-divider" aria-hidden="true" />
              <p className="funding-hero__progress-value">{`Εκτίμηση δαπανών μέχρι σήμερα: ${formatEurCompactMillions(fundingData?.currentSpendAmount ?? 0)}`}</p>
              <div className="funding-hero__progress-track" aria-hidden="true">
                <div
                  className="funding-hero__progress-fill"
                  style={{ width: `${fundingProgressPct}%` }}
                />
              </div>
              <div className="funding-hero__progress-labels" aria-hidden="true">
                <span>0 €</span>
                <span>{formatEurCompactMillions(fundingData?.currentTotal ?? 0)}</span>
              </div>
            </div>
          ) : null}

          {!loadError && spendLoadError ? (
            <p className="funding-hero__error">{spendLoadError}</p>
          ) : null}

          {loadError ? (
            <p className="funding-hero__error">{loadError}</p>
          ) : (
            <>
              <div className="hero-subgrid funding-hero__subgrid">
                <div>
                  <span className="label">Δήμοι</span>
                  <strong>{formatEur(fundingData?.currentMunicipalityAmount ?? 0)}</strong>
                </div>
                <div>
                  <span className="label">Σύνδεσμοι δήμων</span>
                  <strong>{formatEur(fundingData?.currentSyndesmosAmount ?? 0)}</strong>
                </div>
              </div>
            </>
          )}
        </div>
      </div>

      <div className="hero-right funding-hero__right dev-tag-anchor">
        <ComponentTag name="funding-hero__right" kind="CLASS" className="component-tag--overlay" />
        <div className="funding-hero__chart-card dev-tag-anchor">
          <ComponentTag name="funding-hero__chart-card" kind="CLASS" className="component-tag--overlay" />
          <div className="hero-chart funding-hero__chart">
            <div className="hero-chart__head">
              <span className="eyebrow">Χρηματοδότηση ανά έτος</span>
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
                          {tick === 0 ? '0 €' : formatEurCompact(fundingChartMax * tick)}
                        </span>
                      ))}
                    </div>
                  </div>
                  <div
                    ref={fundingChartFrameRef}
                    className="municipality-funding-chart"
                    aria-label="Ετήσια χρηματοδότηση ΚΑΠ προς δήμους και συνδέσμους δήμων"
                    onMouseLeave={() => setFundingChartHover(null)}
                  >
                    {fundingHistory.map((entry) => {
                      const isCurrentYear = entry.year === (fundingData?.yearMain ?? currentYear)
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
                          <span>Τακτική: {formatEur(fundingChartTooltip.regularAmount)}</span>
                          <span>Έκτακτη: {formatEur(fundingChartTooltip.emergencyAmount)}</span>
                          <span>Δήμοι: {formatEur(fundingChartTooltip.municipalityAmount)}</span>
                          <span>Σύνδεσμοι: {formatEur(fundingChartTooltip.syndesmosAmount)}</span>
                          <span>Σύνολο: {formatEur(fundingChartTooltip.totalAmount)}</span>
                        </div>
                      </>
                    ) : null}
                  </div>
                </div>

                <div className="municipality-funding-chart__legend" aria-hidden="true">
                  <span>
                    <i className="municipality-funding-chart__legend-swatch municipality-funding-chart__legend-swatch--regular" />
                    Τακτική
                  </span>
                  <span>
                    <i className="municipality-funding-chart__legend-swatch municipality-funding-chart__legend-swatch--emergency" />
                    Έκτακτη
                  </span>
                </div>

                <p className="note-text funding-hero__chart-note">
                  {`Από ${fundingData?.historyStartYear ?? fundingHistory[0]?.year ?? 2016} έως ${fundingData?.yearMain ?? currentYear}.`}
                </p>
                </>
              ) : (
                <p className="note-text funding-hero__chart-note">Δεν υπάρχουν διαθέσιμα ιστορικά στοιχεία χρηματοδότησης.</p>
              )}
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
