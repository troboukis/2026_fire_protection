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
  const chunk = <T,>(arr: T[], size: number): T[][] => {
    const out: T[][] = []
    for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size))
    return out
  }

  const { data, error } = await supabase
    .from('procurement')
    .select('id, submission_at, title, procedure_type_value, contract_budget, budget, diavgeia_ada, organization_key')
    .eq('municipality_key', municipalityId)
    .order('submission_at', { ascending: false })
    .limit(1000)
  if (error) throw error
  const baseRows = (data ?? []) as Array<{
    id: number
    submission_at: string | null
    title: string | null
    procedure_type_value: string | null
    contract_budget: number | null
    budget: number | null
    diavgeia_ada: string | null
    organization_key: string | null
  }>

  const procurementIds = baseRows.map((r) => r.id)
  const orgKeys = Array.from(new Set(baseRows.map((r) => cleanStr(r.organization_key)).filter(Boolean))) as string[]

  const paymentByProcId = new Map<number, { amount_without_vat: number | null; beneficiary_name: string | null }>()
  for (const ids of chunk(procurementIds, 500)) {
    const { data: paymentRows } = await supabase
      .from('payment')
      .select('procurement_id, amount_without_vat, beneficiary_name')
      .in('procurement_id', ids)
    for (const row of (paymentRows ?? []) as Array<{ procurement_id: number; amount_without_vat: number | null; beneficiary_name: string | null }>) {
      if (!paymentByProcId.has(row.procurement_id)) paymentByProcId.set(row.procurement_id, row)
    }
  }

  const orgNameByKey = new Map<string, string>()
  for (const keys of chunk(orgKeys, 500)) {
    const { data: orgRows } = await supabase
      .from('organization')
      .select('organization_key, organization_normalized_value, organization_value')
      .in('organization_key', keys)
    for (const row of (orgRows ?? []) as Array<{ organization_key: string; organization_normalized_value: string | null; organization_value: string | null }>) {
      if (!orgNameByKey.has(row.organization_key)) {
        orgNameByKey.set(row.organization_key, cleanStr(row.organization_normalized_value) ?? cleanStr(row.organization_value) ?? row.organization_key)
      }
    }
  }

  return baseRows.map((r) => {
    const p = paymentByProcId.get(r.id)
    const orgName = cleanStr(r.organization_key) ? (orgNameByKey.get(cleanStr(r.organization_key) as string) ?? null) : null
    const authorityLevel = ((): string | null => {
      const n = (orgName ?? '').toUpperCase()
      if (!n) return null
      if (n.startsWith('ΔΗΜΟΣ')) return 'municipality'
      if (n.startsWith('ΠΕΡΙΦΕΡΕΙΑ')) return 'region'
      if (n.includes('ΑΠΟΚΕΝΤΡΩΜΕΝΗ')) return 'decentralized'
      return 'other'
    })()
    const ada = cleanStr(r.diavgeia_ada)
    return {
      ada: ada ?? String(r.id),
      org_type: null,
      issue_date: cleanStr(r.submission_at),
      subject: cleanStr(r.title),
      decision_type: cleanStr(r.procedure_type_value),
      amount_eur: p?.amount_without_vat ?? (r.contract_budget != null ? Number(r.contract_budget) : r.budget != null ? Number(r.budget) : null),
      document_url: ada ? `https://diavgeia.gov.gr/doc/${ada}` : null,
      authority_level: authorityLevel,
      org_name_clean: orgName,
      contractor_name: cleanStr(p?.beneficiary_name),
    }
  })
}

async function fetchNationalProcurement(): Promise<ProcurementDecision[]> {
  const { data: orgRows, error: orgError } = await supabase
    .from('organization')
    .select('organization_key')
    .ilike('organization_normalized_value', 'ΥΠΟΥΡΓΕΙΟ%')
    .limit(2000)
  if (orgError) throw orgError
  const orgKeys = Array.from(new Set((orgRows ?? []).map((r) => cleanStr((r as { organization_key?: string | null }).organization_key)).filter(Boolean))) as string[]
  if (orgKeys.length === 0) return []

  const { data, error } = await supabase
    .from('procurement')
    .select('id, diavgeia_ada, submission_at, title, procedure_type_value, contract_budget, budget, organization_key')
    .in('organization_key', orgKeys)
    .order('submission_at', { ascending: false })
    .limit(500)
  if (error) throw error

  const orgNameByKey = new Map<string, string>()
  for (const row of (orgRows ?? []) as Array<{ organization_key: string }>) {
    if (!orgNameByKey.has(row.organization_key)) orgNameByKey.set(row.organization_key, row.organization_key)
  }
  return (data ?? []).map((row) => mapNationalRawRow({
    id: (row as { id: number }).id,
    diavgeia_ada: (row as { diavgeia_ada: string | null }).diavgeia_ada,
    submission_at: (row as { submission_at: string | null }).submission_at,
    title: (row as { title: string | null }).title,
    procedure_type_value: (row as { procedure_type_value: string | null }).procedure_type_value,
    total_cost_without_vat: (row as { contract_budget: number | null }).contract_budget ?? (row as { budget: number | null }).budget,
    total_cost_with_vat: null,
    organization_value: orgNameByKey.get(cleanStr((row as { organization_key: string | null }).organization_key) ?? '') ?? cleanStr((row as { organization_key: string | null }).organization_key),
    first_member_name: null,
    reference_number: null,
  } as Record<string, unknown>))
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
      .from('municipality_normalized_name')
      .select('municipality_key, municipality_normalized_value')
      .eq('municipality_key', id)
      .limit(1)
      .then(({ data: row, error: err }) => {
        if (err) setError(err.message)
        else {
          const first = (row?.[0] ?? null) as { municipality_key?: string | null; municipality_normalized_value?: string | null } | null
          const municipalityId = cleanStr(first?.municipality_key)
          const municipalityName = cleanStr(first?.municipality_normalized_value)
          if (!municipalityId || !municipalityName) {
            setError('Δεν βρέθηκε όνομα δήμου στο municipality_normalized_name')
            setData(null)
          } else {
            setData({
              id: municipalityId,
              name: municipalityName,
              forest_ha: null,
            })
          }
        }
        setLoading(false)
      })
  }, [id])

  useEffect(() => {
    setFireHistory([]); setFireLoading(true)
    supabase
      .from('forest_fire')
      .select('year, burned_total_ha')
      .eq('municipality_key', id)
      .then(({ data: rows }) => {
        const agg = new Map<number, { incident_count: number; total_burned_ha: number; max_single_fire_ha: number | null }>()
        for (const r of (rows ?? []) as Array<{ year: number | string | null; burned_total_ha: number | string | null }>) {
          const year = Number(r.year)
          if (!Number.isFinite(year)) continue
          const ha = Number(r.burned_total_ha ?? 0)
          const prev = agg.get(year) ?? { incident_count: 0, total_burned_ha: 0, max_single_fire_ha: null }
          prev.incident_count += 1
          prev.total_burned_ha += Number.isFinite(ha) ? ha : 0
          prev.max_single_fire_ha = prev.max_single_fire_ha == null ? ha : Math.max(prev.max_single_fire_ha, ha)
          agg.set(year, prev)
        }
        const yearly = Array.from(agg.entries())
          .sort((a, b) => a[0] - b[0])
          .map(([year, v]) => ({ year, ...v }))
        setFireHistory(
          yearly
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
