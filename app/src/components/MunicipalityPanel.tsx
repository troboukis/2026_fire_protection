import { useEffect, useState } from 'react'
import ComponentTag from './ComponentTag'
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

/* ── Mappers ──────────────────────────────────────────────────── */
function mapMunicipalRawRow(r: Record<string, unknown>): ProcurementDecision {
  return {
    ada:             String(r.procurement_id ?? ''),
    org_type:        cleanStr(r.org_type),
    issue_date:      cleanStr(r.issue_date),
    subject:         cleanStr(r.subject),
    decision_type:   cleanStr(r.decision_type),
    amount_eur:      r.amount_eur != null ? (isNaN(Number(r.amount_eur)) ? null : Number(r.amount_eur)) : null,
    document_url:    cleanStr(r.document_url),
    authority_level: cleanStr(r.authority_level),
    org_name_clean:  cleanStr(r.org_name_clean) ?? cleanStr(r.organization_value),
    contractor_name: cleanStr(r.contractor_name),
  }
}

function mapNationalRawRow(r: Record<string, unknown>): ProcurementDecision {
  const diavgeiaAda = cleanStr(r.diavgeia_ada)
  return {
    ada:             diavgeiaAda ?? String(r.reference_number ?? r.id ?? ''),
    org_type:        null,
    issue_date:      cleanStr(r.submission_at),
    subject:         cleanStr(r.title),
    decision_type:   cleanStr(r.procedure_type_value),
    amount_eur:      r.total_cost_without_vat != null
      ? (isNaN(Number(r.total_cost_without_vat)) ? null : Number(r.total_cost_without_vat))
      : r.total_cost_with_vat != null
        ? (isNaN(Number(r.total_cost_with_vat)) ? null : Number(r.total_cost_with_vat))
        : null,
    document_url:    diavgeiaAda ? `https://diavgeia.gov.gr/doc/${diavgeiaAda}` : null,
    authority_level: 'national',
    org_name_clean:  cleanStr(r.organization_value),
    contractor_name: cleanStr(r.first_member_name),
  }
}

/* ── Fetchers ─────────────────────────────────────────────────── */
async function fetchProcurement(municipalityId: string): Promise<ProcurementDecision[]> {
  const { data, error } = await supabase
    .from('v_raw_procurements_municipality')
    .select('procurement_id, org_type, issue_date, subject, decision_type, amount_eur, document_url, authority_level, org_name_clean, organization_value, contractor_name')
    .eq('municipality_id', municipalityId)
    .order('issue_date', { ascending: false })
    .limit(1000)
  if (error) throw error
  return (data ?? []).map(r => mapMunicipalRawRow(r as Record<string, unknown>))
}

async function fetchNationalProcurement(): Promise<ProcurementDecision[]> {
  const { data, error } = await supabase
    .from('raw_procurements')
    .select('id, reference_number, diavgeia_ada, submission_at, title, procedure_type_value, total_cost_with_vat, total_cost_without_vat, organization_value, first_member_name')
    .ilike('organization_value', 'ΥΠΟΥΡΓΕΙΟ%')
    .order('submission_at', { ascending: false })
    .limit(500)
  if (error) throw error
  return (data ?? []).map(r => mapNationalRawRow(r as Record<string, unknown>))
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
    setMunicipalDecisionCount2026(0)
    setCoverageDecisionCount2026(0)
    setProcLoading(true)

    Promise.all([
      fetchProcurement(id),
      fetchNationalProcurement(),
    ])
      .then(([localDecisions, nationalDecisions]) => {
        if (cancelled) return
        setMunicipalDecisionCount2026(
          countDecisionsForYear(
            localDecisions.filter(d => (d.authority_level ?? '').toLowerCase() === 'municipality'),
            2026,
          )
        )
        setCoverageDecisionCount2026(
          countDecisionsForYear(
            localDecisions.filter(d => {
              const level = (d.authority_level ?? '').toLowerCase()
              return level === 'region' || level === 'decentralized'
            }),
            2026,
          )
        )
        setLocalProcurement(sortDecisionsDesc(localDecisions))
        setNationalProcurement(sortDecisionsDesc(nationalDecisions))
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

  if (loading) return <div><ComponentTag name="MunicipalityPanel" /><p>Φόρτωση…</p></div>
  if (error)   return <div><ComponentTag name="MunicipalityPanel" /><p>Σφάλμα: {error}</p></div>
  if (!data)   return null

  const totalFires = fireHistory.reduce((s, y) => s + y.incident_count, 0)
  const totalHa    = fireHistory.reduce((s, y) => s + y.total_burned_ha, 0)
  const worstYear  = fireHistory.length > 0
    ? fireHistory.reduce((best, y) => y.total_burned_ha > best.total_burned_ha ? y : best)
    : null

  return (
    <div>
      <ComponentTag name="MunicipalityPanel" />
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
            return (
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
