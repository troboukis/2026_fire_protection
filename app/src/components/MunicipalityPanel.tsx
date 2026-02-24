import { useEffect, useState } from 'react'
import * as d3 from 'd3'
import { supabase } from '../lib/supabase'
import type { Municipality, MuniFireYear } from '../types'

interface Props {
  id: string
  onBack: () => void
}

/* ── Helpers ──────────────────────────────────────────────────── */
// Converts a raw DB value to a clean string, returning null for empty / "nan" / "none"
function cleanStr(v: unknown): string | null {
  if (v == null) return null
  const s = String(v).trim()
  if (s === '' || s.toLowerCase() === 'nan' || s.toLowerCase() === 'none') return null
  return s
}

function fmtNum(n: number | null, decimals = 0): string {
  if (n == null) return '—'
  return n.toLocaleString('el-GR', { maximumFractionDigits: decimals })
}

function fmtEur(n: number | null): string {
  if (n == null || isNaN(n)) return '—'
  return n.toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 })
}

function fmtDate(d: string | null): string {
  if (!d) return '—'
  const dt = new Date(d)
  if (isNaN(dt.getTime())) return '—'
  return dt.toLocaleDateString('el-GR', { day: '2-digit', month: 'short', year: 'numeric' })
}

/* ── Procurement types ────────────────────────────────────────── */
interface ProcurementDecision {
  ada: string
  org_type: string | null
  issue_date: string | null
  subject: string | null
  decision_type: string | null
  amount_eur: number | null
  document_url: string | null
  authority_level: string | null
  org_name_clean: string | null
  contractor_name: string | null
}

interface ProcurementLine {
  line_type: string
  counterparty_name: string | null
  amount_eur: number | null
  kae_ale_number: string | null
}

interface CoverageAuthority {
  org_type: string
  org_name_clean: string
  authority_level: string | null
  coverage_method: string | null
}

function mapProcurementRow(r: Record<string, unknown>): ProcurementDecision {
  return {
    ada:             String(r.ada ?? ''),
    org_type:        cleanStr(r.org_type),
    issue_date:      cleanStr(r.issue_date),
    subject:         cleanStr(r.subject),
    decision_type:   cleanStr(r.decision_type),
    amount_eur:      r.amount_eur != null ? (isNaN(Number(r.amount_eur)) ? null : Number(r.amount_eur)) : null,
    document_url:    cleanStr(r.document_url),
    authority_level: cleanStr(r.authority_level),
    org_name_clean:  cleanStr(r.org_name_clean),
    contractor_name: cleanStr(r.contractor_name),
  }
}

async function fetchProcurement(municipalityId: string): Promise<ProcurementDecision[]> {
  const { data, error } = await supabase
    .from('procurement_decisions')
    .select('ada, org_type, issue_date, subject, decision_type, amount_eur, document_url, authority_level, org_name_clean, contractor_name, subject_has_anatrop_or_anaklis')
    .eq('municipality_id', municipalityId)
    .order('issue_date', { ascending: false })
    .limit(500)
  if (error) throw error
  return (data ?? [])
    .filter(r => (r.subject_has_anatrop_or_anaklis as boolean | null | undefined) !== true)
    .map(r => mapProcurementRow(r as Record<string, unknown>))
}

async function fetchCoverageAuthorities(municipalityId: string): Promise<CoverageAuthority[]> {
  const { data, error } = await supabase
    .from('org_municipality_coverage')
    .select('org_type, org_name_clean, authority_level, coverage_method')
    .eq('municipality_id', municipalityId)
    .in('authority_level', ['region', 'decentralized'])

  // Table may not be deployed yet in some environments; fail soft for Sprint 5 prep.
  if (error) return []

  const out = new Map<string, CoverageAuthority>()
  for (const row of data ?? []) {
    const orgType = String(row.org_type ?? '').trim()
    const orgName = String(row.org_name_clean ?? '').trim()
    if (!orgType || !orgName) continue
    const key = `${orgType}||${orgName}`
    if (!out.has(key)) {
      out.set(key, {
        org_type: orgType,
        org_name_clean: orgName,
        authority_level: row.authority_level != null ? String(row.authority_level) : null,
        coverage_method: row.coverage_method != null ? String(row.coverage_method) : null,
      })
    }
  }
  return [...out.values()]
}

async function fetchCoverageProcurement(authorities: CoverageAuthority[]): Promise<ProcurementDecision[]> {
  if (authorities.length === 0) return []

  const allowed = new Set(authorities.map(a => `${a.org_type}||${a.org_name_clean}`))
  const names = [...new Set(authorities.map(a => a.org_name_clean))]
  const chunkSize = 100
  const all: ProcurementDecision[] = []

  for (let i = 0; i < names.length; i += chunkSize) {
    const chunk = names.slice(i, i + chunkSize)
    const { data, error } = await supabase
      .from('procurement_decisions')
      .select('ada, org_type, issue_date, subject, decision_type, amount_eur, document_url, authority_level, org_name_clean, contractor_name, subject_has_anatrop_or_anaklis')
      .in('org_name_clean', chunk)
      .in('authority_level', ['region', 'decentralized'])
      .order('issue_date', { ascending: false })
      .limit(1000)

    if (error) throw error

    for (const row of (data ?? []).filter(r => (r.subject_has_anatrop_or_anaklis as boolean | null | undefined) !== true)) {
      const mapped = mapProcurementRow(row as Record<string, unknown>)
      const key = `${mapped.org_type ?? ''}||${mapped.org_name_clean ?? ''}`
      if (allowed.has(key)) all.push(mapped)
    }
  }

  return all
}

async function fetchDecisionLines(adas: string[]): Promise<Map<string, ProcurementLine[]>> {
  if (adas.length === 0) return new Map()
  const CHUNK = 200
  const map = new Map<string, ProcurementLine[]>()
  for (let i = 0; i < adas.length; i += CHUNK) {
    const chunk = adas.slice(i, i + CHUNK)
    const { data, error } = await supabase
      .from('procurement_decision_lines')
      .select('ada, line_type, counterparty_name, amount_eur, kae_ale_number')
      .in('ada', chunk)
      .order('line_index', { ascending: true })
    if (error) continue
    for (const r of data ?? []) {
      const ada = String(r.ada ?? '')
      if (!ada) continue
      if (!map.has(ada)) map.set(ada, [])
      map.get(ada)!.push({
        line_type:         String(r.line_type ?? ''),
        counterparty_name: cleanStr(r.counterparty_name),
        amount_eur:        r.amount_eur != null ? (isNaN(Number(r.amount_eur)) ? null : Number(r.amount_eur)) : null,
        kae_ale_number:    cleanStr(r.kae_ale_number),
      })
    }
  }
  return map
}

async function fetchNationalProcurement(): Promise<ProcurementDecision[]> {
  const { data, error } = await supabase
    .from('procurement_decisions')
    .select('ada, org_type, issue_date, subject, decision_type, amount_eur, document_url, authority_level, org_name_clean, contractor_name, subject_has_anatrop_or_anaklis')
    .eq('authority_level', 'national')
    .order('issue_date', { ascending: false })
    .limit(3000)
  if (error) throw error
  return (data ?? [])
    .filter(r => (r.subject_has_anatrop_or_anaklis as boolean | null | undefined) !== true)
    .map(r => mapProcurementRow(r as Record<string, unknown>))
}

function typeLabel(dt: string | null): string {
  if (!dt) return '—'
  const up = dt.toUpperCase()
  if (up.startsWith('ΑΝΑΘΕΣΗ')) return 'Ανάθεση'
  if (up.startsWith('ΕΓΚΡΙΣΗ')) return 'Εγκρ.'
  if (up.startsWith('ΑΝΑΛΗΨΗ')) return 'Ανάληψη'
  if (up.startsWith('ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ') || up.startsWith('ΠΛΗΡΩΜΗ')) return 'Πληρωμή'
  // For unrecognised types: show first two words of the raw value
  const words = dt.trim().split(/\s+/)
  return words.slice(0, 2).join(' ')
}

function typeBadgeClass(dt: string | null): string {
  if (!dt) return 'proc-badge'
  const up = dt.toUpperCase()
  if (up.startsWith('ΑΝΑΘΕΣΗ')) return 'proc-badge proc-badge-a'
  if (up.startsWith('ΕΓΚΡΙΣΗ')) return 'proc-badge proc-badge-e'
  if (up.startsWith('ΑΝΑΛΗΨΗ')) return 'proc-badge proc-badge-n'
  if (up.startsWith('ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ') || up.startsWith('ΠΛΗΡΩΜΗ')) return 'proc-badge proc-badge-p'
  return 'proc-badge'
}

function orgTypeDisplay(orgType: string | null): string {
  return (orgType ?? '').trim() || '—'
}

function decisionYear(decision: ProcurementDecision): number | null {
  if (!decision.issue_date) return null
  const dt = new Date(decision.issue_date)
  if (isNaN(dt.getTime())) return null
  return dt.getFullYear()
}

function countDecisionsForYear(rows: ProcurementDecision[], year: number): number {
  return rows.filter(r => decisionYear(r) === year).length
}

function postedDecisionsPhrase(count: number, loading: boolean): string {
  if (loading) return 'έχει αναρτήσει … αποφάσεις στη Διαύγεια'
  if (count === 0) return 'δεν έχει αναρτήσει αποφάσεις στη Διαύγεια'
  if (count === 1) return 'έχει αναρτήσει 1 απόφαση στη Διαύγεια'
  return `έχει αναρτήσει ${fmtNum(count)} αποφάσεις στη Διαύγεια`
}

function coveragePostedPhrase(count: number, loading: boolean): string {
  if (loading) return 'έχουν αναρτηθεί … αποφάσεις.'
  if (count === 0) return 'δεν έχουν αναρτηθεί αποφάσεις.'
  if (count === 1) return 'έχει αναρτηθεί 1 απόφαση.'
  return `έχουν αναρτηθεί ${fmtNum(count)} αποφάσεις.`
}

type ProcTableFilters = {
  year: string
  orgKey: string
  decisionType: string
}

function sortDecisionsDesc(rows: ProcurementDecision[]): ProcurementDecision[] {
  return [...rows].sort((a, b) => (b.issue_date ?? '').localeCompare(a.issue_date ?? ''))
}

function uniqueYears(rows: ProcurementDecision[]): number[] {
  return [...new Set(rows.map(decisionYear).filter((yr): yr is number => yr != null))].sort((a, b) => b - a)
}

function orgKey(row: ProcurementDecision): string {
  return `${(row.org_type ?? '').trim()}||${(row.org_name_clean ?? '').trim()}`
}

function uniqueOrgs(rows: ProcurementDecision[]): Array<{ key: string; label: string }> {
  const out = new Map<string, { key: string; label: string }>()
  for (const row of rows) {
    const type = (row.org_type ?? '').trim()
    const name = (row.org_name_clean ?? '').trim()
    if (!name) continue
    const key = orgKey(row)
    if (!out.has(key)) out.set(key, { key, label: `${type || '—'} · ${name}` })
  }
  return [...out.values()].sort((a, b) => a.label.localeCompare(b.label, 'el'))
}

function uniqueDecisionTypes(rows: ProcurementDecision[]): string[] {
  return [...new Set(rows.map(r => (r.decision_type ?? '').trim()).filter(Boolean))].sort((a, b) => a.localeCompare(b, 'el'))
}

function applyProcFilters(rows: ProcurementDecision[], filters: ProcTableFilters): ProcurementDecision[] {
  return rows.filter(row => {
    const yr = decisionYear(row)
    if (filters.year !== 'all' && yr !== Number(filters.year)) return false
    if (filters.orgKey !== 'all' && orgKey(row) !== filters.orgKey) return false
    if (filters.decisionType !== 'all' && (row.decision_type ?? '') !== filters.decisionType) return false
    return true
  })
}

function ProcurementDecisionTableSection({
  title,
  decisions,
  loading,
  filters,
  onFiltersChange,
  emptyText,
  decisionLines,
}: {
  title: string
  decisions: ProcurementDecision[]
  loading: boolean
  filters: ProcTableFilters
  onFiltersChange: (next: ProcTableFilters) => void
  emptyText: string
  decisionLines: Map<string, ProcurementLine[]>
}) {
  const [expanded, setExpanded] = useState(false)
  const yearOptions = uniqueYears(decisions)
  const orgOptions = uniqueOrgs(decisions)
  const typeOptions = uniqueDecisionTypes(decisions)
  const rows = applyProcFilters(decisions, filters)
  const visibleRows = expanded ? rows : rows.slice(0, 5)

  useEffect(() => {
    setExpanded(false)
  }, [filters.year, filters.orgKey, filters.decisionType, title])

  return (
    <div className="section">
      <div className="section-header">
        <p className="section-title">{title}</p>
        {!loading && rows.length > 0 && (
          <span className="dec-count">{fmtNum(rows.length)} αποφάσεις</span>
        )}
      </div>

      <div className="proc-filters">
        <label className="proc-filter">
          <span>Έτος</span>
          <select value={filters.year} onChange={e => onFiltersChange({ ...filters, year: e.target.value })}>
            <option value="all">Όλα</option>
            {[2026, ...yearOptions.filter(y => y !== 2026)].map(yr => (
              <option key={yr} value={String(yr)}>{yr}</option>
            ))}
          </select>
        </label>

        <label className="proc-filter">
          <span>Φορέας</span>
          <select value={filters.orgKey} onChange={e => onFiltersChange({ ...filters, orgKey: e.target.value })}>
            <option value="all">Όλοι</option>
            {orgOptions.map(org => (
              <option key={org.key} value={org.key}>{org.label}</option>
            ))}
          </select>
        </label>

        <label className="proc-filter">
          <span>Τύπος</span>
          <select
            value={filters.decisionType}
            onChange={e => onFiltersChange({ ...filters, decisionType: e.target.value })}
          >
            <option value="all">Όλοι</option>
            {typeOptions.map(t => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>
        </label>
      </div>

      {loading ? (
        <p className="panel-loading" style={{ padding: '14px 0' }}>Φόρτωση αποφάσεων…</p>
      ) : rows.length === 0 ? (
        <p className="dec-empty">{emptyText}</p>
      ) : (
        <>
          <div className="dec-feed">
            {visibleRows.map((d, i) => {
              const label = typeLabel(d.decision_type)
              const isMuni = (d.authority_level ?? '').toLowerCase() === 'municipality'
              const lines = decisionLines.get(d.ada) ?? []
              const meaningfulLines = lines.filter(l => l.amount_eur != null || l.counterparty_name || l.kae_ale_number)
              // Only show the financial section if there's actual financial data
              const isFinancial = d.amount_eur != null || meaningfulLines.length > 0 || d.contractor_name != null
              return (
                <div className={`dec-item${isMuni ? ' dec-item--muni' : ''}`} key={`${d.ada}-${i}`}>
                  <div className="dec-item-top">
                    {label !== '—' && (
                      <span className={typeBadgeClass(d.decision_type)}>{label}</span>
                    )}
                    <div className="dec-item-right">
                      {d.document_url ? (
                        <a href={d.document_url} target="_blank" rel="noreferrer" className="dec-item-ada-link">{d.ada}</a>
                      ) : (
                        <span className="dec-item-ada">{d.ada}</span>
                      )}
                    </div>
                  </div>
                  <p className="dec-item-subject">{d.subject ?? '—'}</p>
                  {isFinancial && (
                    <div className="dec-item-amounts">
                      {meaningfulLines.length > 0
                        ? meaningfulLines.map((line, li) => (
                            <div className="dec-item-amount-row" key={li}>
                              {line.amount_eur != null && (
                                <span className="dec-item-amount">{fmtEur(line.amount_eur)}</span>
                              )}
                              {line.kae_ale_number && (
                                <span className="dec-item-kae">ΚΑΕ {line.kae_ale_number}</span>
                              )}
                              {line.counterparty_name && (
                                <span className="dec-item-beneficiary">{line.counterparty_name}</span>
                              )}
                            </div>
                          ))
                        : (
                          <div className="dec-item-amount-row">
                            {d.amount_eur != null && (
                              <span className="dec-item-amount">{fmtEur(d.amount_eur)}</span>
                            )}
                            {d.contractor_name && (
                              <span className="dec-item-beneficiary">{d.contractor_name}</span>
                            )}
                          </div>
                        )
                      }
                    </div>
                  )}
                  <div className="dec-item-footer">
                    <span className="dec-item-org">
                      {orgTypeDisplay(d.org_type)} · {d.org_name_clean ?? '—'}
                    </span>
                    <span className="dec-item-date">{fmtDate(d.issue_date)}</span>
                  </div>
                </div>
              )
            })}
          </div>

          {rows.length > 5 && (
            <button type="button" className="dec-more-btn" onClick={() => setExpanded(prev => !prev)}>
              {expanded ? 'Λιγότερες αποφάσεις' : `+${fmtNum(rows.length - 5)} ακόμα`}
            </button>
          )}
        </>
      )}
    </div>
  )
}

/* ── Fire history heatmap ─────────────────────────────────── */
const HEAT_YEARS       = Array.from({ length: 25 }, (_, i) => 2000 + i)
const HEAT_LABEL_YEARS = [2000, 2005, 2010, 2015, 2020, 2024]

interface HeatTip { x: number; yr: number; count: number; ha: number }

function FireHeatmap({ data }: { data: MuniFireYear[] }) {
  const CELL = 10, GAP = 1, PAD_L = 10, PAD_R = 10, PAD_B = 14
  const [tip, setTip] = useState<HeatTip | null>(null)

  const byYear     = new Map(data.map(d => [d.year, d]))
  const counts     = data.map(d => d.incident_count).filter(v => v > 0)
  const maxCount   = counts.length > 0 ? Math.max(...counts) : 1
  const colorScale = d3.scalePow().exponent(0.4).domain([0, maxCount]).range([0.12, 1]).clamp(true)
  const totalW     = PAD_L + HEAT_YEARS.length * (CELL + GAP) - GAP + PAD_R

  return (
    <div style={{ marginTop: 10 }}>
      <p className="heat-label">Πλήθος πυρκαγιών</p>
      <div style={{ position: 'relative' }}>
        <svg width={totalW} height={CELL + PAD_B} style={{ display: 'block' }}
          onMouseLeave={() => setTip(null)}>
          {HEAT_YEARS.map((yr, i) => {
            const d     = byYear.get(yr)
            const count = d?.incident_count ?? 0
            const fill  = count > 0 ? d3.interpolateReds(colorScale(count)) : '#e8e5df'
            const x     = PAD_L + i * (CELL + GAP)
            return (
              <g key={yr}
                onMouseEnter={e => setTip({ x: e.nativeEvent.offsetX, yr, count, ha: d?.total_burned_ha ?? 0 })}
                onMouseMove={e  => setTip(prev => prev ? { ...prev, x: e.nativeEvent.offsetX } : null)}
                onMouseLeave={() => setTip(null)}
              >
                <rect x={x} y={0} width={CELL} height={CELL} fill={fill} />
                {HEAT_LABEL_YEARS.includes(yr) && (
                  <text x={x + CELL / 2} y={CELL + PAD_B - 2}
                    textAnchor="middle" fontSize={7} fill="#aaa">
                    {yr}
                  </text>
                )}
              </g>
            )
          })}
        </svg>
        {tip && (
          <div className="heat-tooltip" style={{ left: tip.x }}>
            <span className="heat-tooltip-yr">{tip.yr}</span>
            <span className="heat-tooltip-val">
              {tip.count > 0
                ? `${tip.count} πυρκαγιές · ${fmtNum(Math.round(tip.ha))} εκτ.`
                : '—'}
            </span>
          </div>
        )}
      </div>
    </div>
  )
}

/* ── Main panel ───────────────────────────────────────────────── */
export function MunicipalityPanel({ id, onBack }: Props) {
  const [data, setData]               = useState<Municipality | null>(null)
  const [loading, setLoading]         = useState(true)
  const [error, setError]             = useState<string | null>(null)
  const [fireHistory, setFireHistory] = useState<MuniFireYear[]>([])
  const [fireLoading, setFireLoading] = useState(true)
  const [localProcurement, setLocalProcurement] = useState<ProcurementDecision[]>([])
  const [nationalProcurement, setNationalProcurement] = useState<ProcurementDecision[]>([])
  const [procLoading, setProcLoading] = useState(true)
  const [localFilters, setLocalFilters] = useState<ProcTableFilters>({ year: '2026', orgKey: 'all', decisionType: 'all' })
  const [nationalFilters, setNationalFilters] = useState<ProcTableFilters>({ year: '2026', orgKey: 'all', decisionType: 'all' })
  const [municipalDecisionCount2026, setMunicipalDecisionCount2026] = useState<number>(0)
  const [coverageDecisionCount2026, setCoverageDecisionCount2026] = useState<number>(0)
  const [decisionLines, setDecisionLines] = useState<Map<string, ProcurementLine[]>>(new Map())

  // Fetch municipality info
  useEffect(() => {
    setLoading(true); setError(null); setData(null)
    supabase
      .from('municipalities')
      .select('id, name, forest_ha')
      .eq('id', id)
      .single()
      .then(({ data: row, error: err }) => {
        if (err) setError(err.message)
        else setData(row)
        setLoading(false)
      })
  }, [id])

  // Fetch fire history
  useEffect(() => {
    setFireHistory([]); setFireLoading(true)
    supabase
      .from('v_municipality_fire_summary')
      .select('year, incident_count, total_burned_ha, max_single_fire_ha')
      .eq('municipality_id', id)
      .order('year', { ascending: true })
      .then(({ data: rows }) => {
        setFireHistory(
          (rows ?? []).map(r => ({
            year:               Number(r.year),
            incident_count:     Number(r.incident_count),
            total_burned_ha:    Number(r.total_burned_ha ?? 0),
            max_single_fire_ha: r.max_single_fire_ha != null ? Number(r.max_single_fire_ha) : null,
          }))
        )
        setFireLoading(false)
      })
  }, [id])

  // Fetch procurement decisions
  useEffect(() => {
    let cancelled = false
    setLocalProcurement([])
    setNationalProcurement([])
    setDecisionLines(new Map())
    setMunicipalDecisionCount2026(0)
    setCoverageDecisionCount2026(0)
    setProcLoading(true)
    setLocalFilters({ year: '2026', orgKey: 'all', decisionType: 'all' })
    setNationalFilters({ year: '2026', orgKey: 'all', decisionType: 'all' })

    Promise.all([
      fetchProcurement(id),
      fetchCoverageAuthorities(id),
      fetchNationalProcurement(),
    ])
      .then(async ([directDecisions, coverage, nationalDecisions]) => {
        if (cancelled) return

        let coverageDecisions: ProcurementDecision[] = []
        if (coverage.length > 0) {
          try {
            coverageDecisions = await fetchCoverageProcurement(coverage)
          } catch {
            coverageDecisions = []
          }
          if (cancelled) return
        }

        setMunicipalDecisionCount2026(
          countDecisionsForYear(
            directDecisions.filter(d => (d.authority_level ?? '').toLowerCase() === 'municipality'),
            2026,
          ),
        )
        setCoverageDecisionCount2026(countDecisionsForYear(coverageDecisions, 2026))

        const localDecisions = sortDecisionsDesc(
          [...directDecisions, ...coverageDecisions].filter(d => (d.authority_level ?? '').toLowerCase() !== 'national')
        )
        setLocalProcurement(localDecisions)
        setNationalProcurement(sortDecisionsDesc(nationalDecisions))

        // Fetch line-level amounts + counterparties for local decisions
        const lines = await fetchDecisionLines(localDecisions.map(d => d.ada))
        if (!cancelled) setDecisionLines(lines)
      })
      .catch(() => {
        if (cancelled) return
        setLocalProcurement([])
        setNationalProcurement([])
        setMunicipalDecisionCount2026(0)
        setCoverageDecisionCount2026(0)
      })
      .finally(() => {
        if (cancelled) return
        setProcLoading(false)
      })

    return () => { cancelled = true }
  }, [id])

  if (loading) return <div className="panel-scroll"><p className="panel-loading">Φόρτωση…</p></div>
  if (error)   return <div className="panel-scroll"><p className="panel-error">Σφάλμα: {error}</p></div>
  if (!data)   return null

  // Derived fire stats
  const totalFires = fireHistory.reduce((s, y) => s + y.incident_count, 0)
  const totalHa    = fireHistory.reduce((s, y) => s + y.total_burned_ha, 0)
  const worstYear  = fireHistory.length > 0
    ? fireHistory.reduce((best, y) => y.total_burned_ha > best.total_burned_ha ? y : best)
    : null

  return (
    <div className="panel-scroll">

      {/* ── Back ── */}
      <button className="detail-back" onClick={onBack}>
        ← Επιστροφή στον χάρτη
      </button>

      {/* ── Identity ── */}
      <p className="detail-eyebrow">Δημος</p>
      <h1 className="detail-name">{data.name}</h1>
      <p className="detail-meta">
        Ο δήμος {data.name} το 2026 {postedDecisionsPhrase(municipalDecisionCount2026, procLoading)} που σχετίζονται
        με την πυροπροστασία. Σε επίπεδο περιφέρειας ή άλλων φορέων που αφορούν στην περιοχή,{' '}
        {coveragePostedPhrase(coverageDecisionCount2026, procLoading)}
      </p>
      {!fireLoading && <FireHeatmap data={fireHistory} />}

      {/* ── Key stats ── */}
      <div className="stats-grid">
        <div className="stat-cell">
          <span className="stat-label">Πληθος πυρκαγιων 2000–2024</span>
          <span className="stat-value">
            {fireLoading ? <span style={{ fontSize: 13, color: 'var(--ink-3)' }}>…</span>
              : totalFires > 0 ? fmtNum(totalFires) : '—'}
          </span>
        </div>

        <div className="stat-cell">
          <span className="stat-label">Συνολικη καμμενη εκταση</span>
          <span className="stat-value">
            {fireLoading ? <span style={{ fontSize: 13, color: 'var(--ink-3)' }}>…</span>
              : totalHa > 0 ? fmtNum(Math.round(totalHa)) : '—'}
            {!fireLoading && totalHa > 0 && <span className="stat-unit">εκτ.</span>}
          </span>
        </div>

        <div className="stat-cell">
          <span className="stat-label">Χειροτερη χρονια</span>
          <span className="stat-value">
            {fireLoading ? <span style={{ fontSize: 13, color: 'var(--ink-3)' }}>…</span>
              : worstYear ? String(worstYear.year) : '—'}
          </span>
          {!fireLoading && worstYear && (
            <div className="stat-footnote">
              {fmtNum(Math.round(worstYear.total_burned_ha))} εκτ.
            </div>
          )}
        </div>
      </div>

      <ProcurementDecisionTableSection
        title="Αποφάσεις πυροπροστασίας"
        decisions={localProcurement}
        loading={procLoading}
        filters={localFilters}
        onFiltersChange={setLocalFilters}
        emptyText="Δεν βρέθηκαν αποφάσεις για τα επιλεγμένα φίλτρα."
        decisionLines={decisionLines}
      />

      <ProcurementDecisionTableSection
        title="Αποφάσεις σε εθνικό επίπεδο"
        decisions={nationalProcurement}
        loading={procLoading}
        filters={nationalFilters}
        onFiltersChange={setNationalFilters}
        emptyText="Δεν βρέθηκαν εθνικού επιπέδου αποφάσεις για τα επιλεγμένα φίλτρα."
        decisionLines={decisionLines}
      />

    </div>
  )
}
