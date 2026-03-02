import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import ComponentTag from '../components/ComponentTag'
import { supabase } from '../lib/supabase'

type ContractRow = {
  id: number
  contract_signed_date: string | null
  organization_value: string | null
  title: string | null
  reference_number: string | null
  cpv_value: string | null
  procedure_type_value: string | null
  beneficiary_name: string | null
  amount_without_vat: number | null
  diavgeia_ada: string | null
  total_count: number
}

function clean(v: unknown): string {
  if (v == null) return ''
  const s = String(v).trim()
  if (!s || s.toLowerCase() === 'nan' || s.toLowerCase() === 'none') return ''
  return s
}

function fmtDate(iso: string | null): string {
  if (!iso) return '—'
  const dt = new Date(iso)
  if (Number.isNaN(dt.getTime())) return '—'
  return new Intl.DateTimeFormat('el-GR', { day: '2-digit', month: 'short', year: 'numeric' }).format(dt)
}

function fmtEur(n: number | null): string {
  if (n == null || Number.isNaN(n)) return '—'
  return n.toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 })
}

function isoDateDaysAgo(days: number): string {
  const d = new Date()
  d.setDate(d.getDate() - days)
  const tzOffsetMs = d.getTimezoneOffset() * 60_000
  return new Date(d.getTime() - tzOffsetMs).toISOString().slice(0, 10)
}

function isoToday(): string {
  const d = new Date()
  const tzOffsetMs = d.getTimezoneOffset() * 60_000
  return new Date(d.getTime() - tzOffsetMs).toISOString().slice(0, 10)
}

function periodLabel(dateFrom: string, dateTo: string): string {
  if (dateFrom && dateTo) return `${fmtDate(dateFrom)} - ${fmtDate(dateTo)}`
  if (dateFrom) return `Από ${fmtDate(dateFrom)}`
  if (dateTo) return `Έως ${fmtDate(dateTo)}`
  return 'Όλο το διάστημα'
}

export default function ContractsPage() {
  const [rows, setRows] = useState<ContractRow[]>([])
  const [totalCount, setTotalCount] = useState(0)
  const [loading, setLoading] = useState(true)
  const [q, setQ] = useState('')
  const [org, setOrg] = useState('')
  const [procedure, setProcedure] = useState('')
  const [procedureOptions, setProcedureOptions] = useState<string[]>([])
  const [dateFrom, setDateFrom] = useState(() => isoDateDaysAgo(30))
  const [dateTo, setDateTo] = useState(() => isoToday())
  const [minAmount, setMinAmount] = useState('')
  const [page, setPage] = useState(1)
  const pageSize = 50

  useEffect(() => {
    let cancelled = false
    const loadProcedures = async () => {
      const { data, error } = await supabase
        .from('procurement')
        .select('procedure_type_value')
        .not('procedure_type_value', 'is', null)
        .limit(5000)
      if (cancelled || error) return
      const vals = Array.from(new Set(((data ?? []) as Array<{ procedure_type_value: string | null }>)
        .map((r) => clean(r.procedure_type_value))
        .filter(Boolean)))
      setProcedureOptions(vals.sort((a, b) => a.localeCompare(b, 'el')))
    }
    loadProcedures()
    return () => { cancelled = true }
  }, [])

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    const loadPage = async () => {
      const min = minAmount ? Number(minAmount) : null
      const { data, error } = await supabase.rpc('get_contracts_page', {
        p_q: q || null,
        p_org: org || null,
        p_procedure: procedure || null,
        p_date_from: dateFrom || null,
        p_date_to: dateTo || null,
        p_min_amount: min != null && Number.isFinite(min) ? min : null,
        p_page: page,
        p_page_size: pageSize,
      })
      if (cancelled) return
      if (error) {
        setRows([])
        setTotalCount(0)
        setLoading(false)
        return
      }
      const next = (data ?? []) as ContractRow[]
      const deduped = Array.from(
        new Map(
          next.map((r) => {
            const k =
              clean(r.diavgeia_ada) ||
              `${clean(r.organization_value)}|${clean(r.title)}|${clean(r.contract_signed_date)}|${String(r.amount_without_vat ?? '')}`
            return [k, r] as const
          }),
        ).values(),
      )
      setRows(deduped)
      setTotalCount(next[0]?.total_count ?? 0)
      setLoading(false)
    }
    loadPage()
    return () => { cancelled = true }
  }, [q, org, procedure, dateFrom, dateTo, minAmount, page])

  const totalPages = useMemo(() => Math.max(1, Math.ceil(totalCount / pageSize)), [totalCount])

  return (
    <div className="contracts-page">
      <ComponentTag name="ContractsPage" />
      <header className="contracts-header section-rule">
        <div>
          <div className="eyebrow">Συμβάσεις</div>
          <h1>Όλες οι Συμβάσεις</h1>
          <p>
            {loading
              ? 'Φόρτωση…'
              : `${totalCount.toLocaleString('el-GR')} αποτελέσματα · Περίοδος: ${periodLabel(dateFrom, dateTo)}`}
          </p>
        </div>
        <Link className="contracts-back" to="/">← Επιστροφή στην αρχική</Link>
      </header>

      <section className="contracts-filters section-rule">
        <input value={q} onChange={(e) => { setQ(e.target.value); setPage(1) }} placeholder="Αναζήτηση (τίτλος/φορέας/δικαιούχος/CPV)" />
        <input value={org} onChange={(e) => { setOrg(e.target.value); setPage(1) }} placeholder="Φορέας" />
        <select value={procedure} onChange={(e) => { setProcedure(e.target.value); setPage(1) }}>
          <option value="">Όλες οι διαδικασίες</option>
          {procedureOptions.map((p) => <option key={p} value={p}>{p}</option>)}
        </select>
        <input value={dateFrom} onChange={(e) => { setDateFrom(e.target.value); setPage(1) }} type="date" />
        <input value={dateTo} onChange={(e) => { setDateTo(e.target.value); setPage(1) }} type="date" />
        <input value={minAmount} onChange={(e) => { setMinAmount(e.target.value); setPage(1) }} type="number" min="0" placeholder="Ελάχιστο ποσό (χωρίς ΦΠΑ)" />
      </section>

      <section className="contracts-table-wrap section-rule">
        <table className="contracts-table">
          <thead>
            <tr>
              <th>Ημερομηνία</th>
              <th>Φορέας</th>
              <th>Τίτλος</th>
              <th>Γιατί (CPV)</th>
              <th>Δικαιούχος</th>
              <th>Διαδικασία</th>
              <th>Ποσό χωρίς ΦΠΑ</th>
              <th>ΑΔΑΜ</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => {
              const refNo = clean(r.reference_number)
              return (
                <tr key={r.id}>
                  <td>{fmtDate(r.contract_signed_date)}</td>
                  <td>{clean(r.organization_value) || '—'}</td>
                  <td>{clean(r.title) || '—'}</td>
                  <td>{clean(r.cpv_value) || '—'}</td>
                  <td>{clean(r.beneficiary_name).toLocaleUpperCase('el-GR') || '—'}</td>
                  <td>{clean(r.procedure_type_value) || '—'}</td>
                  <td className="contracts-amount">{fmtEur(r.amount_without_vat)}</td>
                  <td>{refNo || '—'}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
        <div style={{ display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '0.8rem' }}>
          <button onClick={() => setPage((p) => Math.max(1, p - 1))} disabled={page <= 1 || loading}>Προηγούμενη</button>
          <span>Σελίδα {page} / {totalPages}</span>
          <button onClick={() => setPage((p) => Math.min(totalPages, p + 1))} disabled={page >= totalPages || loading}>Επόμενη</button>
        </div>
      </section>
    </div>
  )
}
