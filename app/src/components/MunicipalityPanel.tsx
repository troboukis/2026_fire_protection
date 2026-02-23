import { useEffect, useState } from 'react'
import * as d3 from 'd3'
import { supabase } from '../lib/supabase'
import type { Municipality, MuniFireYear } from '../types'

interface Props {
  id: string
  onBack: () => void
}

/* ── Helpers ──────────────────────────────────────────────────── */
function fmtNum(n: number | null, decimals = 0): string {
  if (n == null) return '—'
  return n.toLocaleString('el-GR', { maximumFractionDigits: decimals })
}

function fmtEur(n: number | null): string {
  if (n == null) return '—'
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
  issue_date: string | null
  subject: string | null
  decision_type: string | null
  amount_eur: number | null
  document_url: string | null
}

async function fetchProcurement(municipalityId: string): Promise<ProcurementDecision[]> {
  const { data, error } = await supabase
    .from('procurement_decisions')
    .select('ada, issue_date, subject, decision_type, amount_eur, document_url')
    .eq('municipality_id', municipalityId)
    .order('issue_date', { ascending: false })
    .limit(500)
  if (error) throw error
  return (data ?? []).map(r => ({
    ada:           r.ada,
    issue_date:    r.issue_date   ?? null,
    subject:       r.subject      ?? null,
    decision_type: r.decision_type ?? null,
    amount_eur:    r.amount_eur != null ? Number(r.amount_eur) : null,
    document_url:  r.document_url ?? null,
  }))
}

const PROC_TYPES = [
  {
    label: 'Αναθέσεις & Συμβάσεις',
    match: (up: string) =>
      up.startsWith('ΑΝΑΘΕΣΗ') ||
      up.startsWith('ΚΑΤΑΚΥΡΩΣΗ') ||
      up.startsWith('ΣΥΜΒΑΣΗ') ||
      up.startsWith('ΠΕΡΙΛΗΨΗ ΔΙΑΚΗΡΥΞΗΣ'),
  },
  { label: 'Εγκρίσεις δαπανών',    match: (up: string) => up.startsWith('ΕΓΚΡΙΣΗ') },
  { label: 'Αναλήψεις υποχρέωσης', match: (up: string) => up.startsWith('ΑΝΑΛΗΨΗ') },
  {
    label: 'Πληρωμές',
    match: (up: string) =>
      up.startsWith('ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ') ||
      up.startsWith('ΠΛΗΡΩΜΗ') ||
      up.startsWith('ΕΠΙΤΡΟΠΙΚΟ ΕΝΤΑΛΜΑ'),
  },
]

function typeLabel(dt: string | null): string {
  if (!dt) return 'Άλλο'
  const up = dt.toUpperCase()
  if (up.startsWith('ΑΝΑΘΕΣΗ')) return 'Ανάθεση'
  if (up.startsWith('ΕΓΚΡΙΣΗ')) return 'Εγκρ.'
  if (up.startsWith('ΑΝΑΛΗΨΗ')) return 'Ανάληψη'
  if (up.startsWith('ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ') || up.startsWith('ΠΛΗΡΩΜΗ')) return 'Πληρωμή'
  return 'Άλλο'
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

/* ── Expandable procurement type section ─────────────────────── */
function ProcurementTypeSection({
  label, decisions, isOpen, onToggle,
}: {
  label: string
  decisions: ProcurementDecision[]
  isOpen: boolean
  onToggle: () => void
}) {
  const count = decisions.length
  const total = decisions.reduce((s, d) => s + (d.amount_eur ?? 0), 0)
  return (
    <>
      <div className={`cat-row${isOpen ? ' open' : ''}`} onClick={onToggle}>
        <span className="cat-row-label">{label}</span>
        <span className="cat-row-meta">
          <span className="cat-row-count">
            {count > 0
              ? `${count} αποφ.${total > 0 ? ` · ${fmtEur(total)}` : ''}`
              : '—'}
          </span>
          <span className={`cat-chevron${isOpen ? ' open' : ''}`}>▶</span>
        </span>
      </div>
      {isOpen && (
        <div className="cat-decisions">
          {count === 0 ? (
            <p className="cat-empty">Δεν υπάρχουν αποφάσεις αυτής της κατηγορίας.</p>
          ) : (
            <table className="decisions-table">
              <thead>
                <tr>
                  <th>Ημ/νία</th>
                  <th>Θέμα</th>
                  <th style={{ textAlign: 'right' }}>Ποσό</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {decisions.map(d => (
                  <tr key={d.ada}>
                    <td className="proc-date">{fmtDate(d.issue_date)}</td>
                    <td className="proc-subject">
                      {(d.subject ?? '').slice(0, 100)}{(d.subject?.length ?? 0) > 100 ? '…' : ''}
                    </td>
                    <td className="proc-amount">{d.amount_eur != null ? fmtEur(d.amount_eur) : '—'}</td>
                    <td className="proc-link">
                      {d.document_url
                        ? <a href={d.document_url} target="_blank" rel="noreferrer">↗</a>
                        : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </>
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
  const [procurement, setProcurement] = useState<ProcurementDecision[]>([])
  const [procLoading, setProcLoading] = useState(true)
  const [openType, setOpenType]       = useState<string | null>(null)

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
    setProcurement([]); setProcLoading(true); setOpenType(null)
    fetchProcurement(id)
      .then(setProcurement)
      .catch(() => {})
      .finally(() => setProcLoading(false))
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
      <p className="detail-meta">Κωδικός Καλλικράτη: {data.id}</p>
      {!fireLoading && <FireHeatmap data={fireHistory} />}

      {/* ── Key stats (2×2 grid) ── */}
      <div className="stats-grid">
        <div className="stat-cell">
          <span className="stat-label">Δασικη εκταση</span>
          <span className="stat-value">
            {data.forest_ha != null
              ? data.forest_ha.toLocaleString('el-GR', { maximumFractionDigits: 0 })
              : '—'}
            {data.forest_ha != null && <span className="stat-unit">εκτ.</span>}
          </span>
          <div className="stat-footnote">1 εκτάριο = 10 στρέμματα</div>
        </div>

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

      {/* ── Αποφάσεις πυροπροστασίας ── */}
      <div className="section">
        <div className="section-header">
          <p className="section-title">
            Αποφάσεις πυροπροστασιας
            <span className="proc-year-label" style={{ display: 'inline', marginLeft: 6 }}>
              {new Date().getFullYear()}
            </span>
          </p>
          <span className="section-badge">Διαύγεια</span>
        </div>

        {procLoading ? (
          <p className="panel-loading" style={{ padding: '12px 0', textAlign: 'left' }}>Φόρτωση…</p>
        ) : procurement.length === 0 ? (
          <p className="cat-empty" style={{ padding: '0 0 8px' }}>
            Δεν βρέθηκαν αποφάσεις για αυτόν τον δήμο στη Διαύγεια.
          </p>
        ) : (() => {
          const curYear     = new Date().getFullYear()
          const curDecisions   = procurement.filter(d => d.issue_date?.startsWith(String(curYear)))
          const olderDecisions = procurement.filter(d => !d.issue_date?.startsWith(String(curYear)))
          return (
            <>
              {curDecisions.length === 0 ? (
                <p className="cat-empty" style={{ padding: '0 0 8px' }}>
                  Δεν υπάρχουν αποφάσεις για το {curYear}.
                </p>
              ) : (
                <div className="cat-table">
                  {(() => {
                    const assignedADAs = new Set<string>()
                    const sections = PROC_TYPES.map(t => {
                      const matches = curDecisions.filter(
                        d => t.match((d.decision_type ?? '').toUpperCase())
                      )
                      matches.forEach(d => assignedADAs.add(d.ada))
                      return { label: t.label, matches }
                    })
                    const leftover = curDecisions.filter(d => !assignedADAs.has(d.ada))
                    if (leftover.length > 0) {
                      sections.push({ label: 'Λοιπές αποφάσεις', matches: leftover })
                    }
                    return sections
                      .filter(s => s.matches.length > 0)
                      .map(s => (
                        <ProcurementTypeSection
                          key={s.label}
                          label={s.label}
                          decisions={s.matches}
                          isOpen={openType === s.label}
                          onToggle={() => setOpenType(prev => prev === s.label ? null : s.label)}
                        />
                      ))
                  })()}
                </div>
              )}

              {/* Older decisions — flat table */}
              {olderDecisions.length > 0 && (
                <div style={{ marginTop: 20 }}>
                  <p className="proc-year-label">Παλαιότερες αποφάσεις</p>
                  <table className="decisions-table">
                    <thead>
                      <tr>
                        <th>Ημ/νία</th>
                        <th>Τύπος</th>
                        <th>Θέμα</th>
                        <th style={{ textAlign: 'right' }}>Ποσό</th>
                        <th></th>
                      </tr>
                    </thead>
                    <tbody>
                      {olderDecisions.slice(0, 100).map(d => (
                        <tr key={d.ada}>
                          <td className="proc-date">{fmtDate(d.issue_date)}</td>
                          <td><span className={typeBadgeClass(d.decision_type)}>{typeLabel(d.decision_type)}</span></td>
                          <td className="proc-subject">
                            {(d.subject ?? '').slice(0, 80)}{(d.subject?.length ?? 0) > 80 ? '…' : ''}
                          </td>
                          <td className="proc-amount">{d.amount_eur != null ? fmtEur(d.amount_eur) : '—'}</td>
                          <td className="proc-link">
                            {d.document_url
                              ? <a href={d.document_url} target="_blank" rel="noreferrer">↗</a>
                              : '—'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              <p className="spending-note" style={{ marginTop: 10 }}>
                Αφορά αποφάσεις του δήμου που δημοσιεύθηκαν στη Διαύγεια.
                Ενδέχεται να μην καλύπτονται όλες οι σχετικές δαπάνες.
              </p>
            </>
          )
        })()}
      </div>

      {/* ── Χρηματοδότηση ΚΑΠ ── */}
      <div className="section">
        <div className="section-header">
          <p className="section-title">Χρηματοδοτηση ΚΑΠ για πυροπροστασια</p>
        </div>
        <div className="funding-rows">
          <div className="funding-row">
            <span className="funding-year">2026</span>
            <span className="funding-na">Δεν έχει ανακοινωθεί</span>
          </div>
          {[2025, 2024, 2023].map(yr => (
            <div className="funding-row" key={yr}>
              <span className="funding-year">{yr}</span>
              <span className="funding-stub">— Sprint 4</span>
            </div>
          ))}
        </div>
      </div>

    </div>
  )
}
