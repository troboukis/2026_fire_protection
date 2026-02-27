import { useEffect, useState } from 'react'
import { supabase } from '../lib/supabase'
import type { Municipality, MuniFireYear } from '../types'

interface Props {
  id: string
  onBack: () => void
}

/* ── Helpers ──────────────────────────────────────────────────── */
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

/* ── Types ────────────────────────────────────────────────────── */
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

/* ── Mappers ──────────────────────────────────────────────────── */
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

/* ── Fetchers ─────────────────────────────────────────────────── */
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

/* ── Helpers ──────────────────────────────────────────────────── */
function decisionYear(d: ProcurementDecision): number | null {
  if (!d.issue_date) return null
  const dt = new Date(d.issue_date)
  return isNaN(dt.getTime()) ? null : dt.getFullYear()
}

function countDecisionsForYear(rows: ProcurementDecision[], year: number): number {
  return rows.filter(r => decisionYear(r) === year).length
}

function sortDecisionsDesc(rows: ProcurementDecision[]): ProcurementDecision[] {
  return [...rows].sort((a, b) => (b.issue_date ?? '').localeCompare(a.issue_date ?? ''))
}

/* ── Panel ────────────────────────────────────────────────────── */
export function MunicipalityPanel({ id, onBack }: Props) {
  const [data, setData]               = useState<Municipality | null>(null)
  const [loading, setLoading]         = useState(true)
  const [error, setError]             = useState<string | null>(null)
  const [fireHistory, setFireHistory] = useState<MuniFireYear[]>([])
  const [fireLoading, setFireLoading] = useState(true)
  const [localProcurement, setLocalProcurement]       = useState<ProcurementDecision[]>([])
  const [nationalProcurement, setNationalProcurement] = useState<ProcurementDecision[]>([])
  const [procLoading, setProcLoading] = useState(true)
  const [municipalDecisionCount2026, setMunicipalDecisionCount2026] = useState(0)
  const [coverageDecisionCount2026, setCoverageDecisionCount2026]   = useState(0)
  const [decisionLines, setDecisionLines] = useState<Map<string, ProcurementLine[]>>(new Map())

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

  useEffect(() => {
    let cancelled = false
    setLocalProcurement([])
    setNationalProcurement([])
    setDecisionLines(new Map())
    setMunicipalDecisionCount2026(0)
    setCoverageDecisionCount2026(0)
    setProcLoading(true)

    Promise.all([
      fetchProcurement(id),
      fetchCoverageAuthorities(id),
      fetchNationalProcurement(),
    ])
      .then(async ([directDecisions, coverage, nationalDecisions]) => {
        if (cancelled) return
        let coverageDecisions: ProcurementDecision[] = []
        if (coverage.length > 0) {
          try { coverageDecisions = await fetchCoverageProcurement(coverage) } catch { coverageDecisions = [] }
          if (cancelled) return
        }
        setMunicipalDecisionCount2026(
          countDecisionsForYear(
            directDecisions.filter(d => (d.authority_level ?? '').toLowerCase() === 'municipality'),
            2026,
          )
        )
        setCoverageDecisionCount2026(countDecisionsForYear(coverageDecisions, 2026))
        const localDecisions = sortDecisionsDesc(
          [...directDecisions, ...coverageDecisions].filter(d => (d.authority_level ?? '').toLowerCase() !== 'national')
        )
        setLocalProcurement(localDecisions)
        setNationalProcurement(sortDecisionsDesc(nationalDecisions))
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
      .finally(() => { if (!cancelled) setProcLoading(false) })

    return () => { cancelled = true }
  }, [id])

  if (loading) return <div><p>Φόρτωση…</p></div>
  if (error)   return <div><p>Σφάλμα: {error}</p></div>
  if (!data)   return null

  const totalFires = fireHistory.reduce((s, y) => s + y.incident_count, 0)
  const totalHa    = fireHistory.reduce((s, y) => s + y.total_burned_ha, 0)
  const worstYear  = fireHistory.length > 0
    ? fireHistory.reduce((best, y) => y.total_burned_ha > best.total_burned_ha ? y : best)
    : null

  return (
    <div>
      <button onClick={onBack}>← Επιστροφή</button>

      <h1>{data.name}</h1>

      <p>
        Πυρκαγιές 2000–2024: {fireLoading ? '…' : fmtNum(totalFires)} ·{' '}
        Καμμένη έκταση: {fireLoading ? '…' : fmtNum(Math.round(totalHa))} εκτ.
        {worstYear && ` · Χειρότερη χρονιά: ${worstYear.year}`}
      </p>

      <p>
        Αποφάσεις 2026:{' '}
        {procLoading ? '…' : `${municipalDecisionCount2026} δημοτικές, ${coverageDecisionCount2026} περιφερειακές`}
      </p>

      {!procLoading && localProcurement.length > 0 && (
        <div>
          <h2>Τοπικές αποφάσεις ({localProcurement.length})</h2>
          {localProcurement.slice(0, 10).map((d, i) => {
            const lines = decisionLines.get(d.ada) ?? []
            return (
              <div key={`${d.ada}-${i}`}>
                <p>
                  <strong>{d.subject ?? '—'}</strong>
                  {d.amount_eur != null && <> · {fmtEur(d.amount_eur)}</>}
                  {lines.filter(l => l.amount_eur != null).map((l, li) => (
                    <span key={li}> · {fmtEur(l.amount_eur)}</span>
                  ))}
                </p>
                <p>
                  {d.org_name_clean ?? '—'} · {fmtDate(d.issue_date)}
                  {d.document_url && <> · <a href={d.document_url} target="_blank" rel="noreferrer">{d.ada}</a></>}
                </p>
              </div>
            )
          })}
          {localProcurement.length > 10 && <p>+{localProcurement.length - 10} ακόμα</p>}
        </div>
      )}

      {!procLoading && nationalProcurement.length > 0 && (
        <div>
          <h2>Εθνικές αποφάσεις ({nationalProcurement.length})</h2>
          {nationalProcurement.slice(0, 5).map((d, i) => (
            <div key={`${d.ada}-${i}`}>
              <p>
                <strong>{d.subject ?? '—'}</strong>
                {d.amount_eur != null && <> · {fmtEur(d.amount_eur)}</>}
              </p>
              <p>
                {d.org_name_clean ?? '—'} · {fmtDate(d.issue_date)}
                {d.document_url && <> · <a href={d.document_url} target="_blank" rel="noreferrer">{d.ada}</a></>}
              </p>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
