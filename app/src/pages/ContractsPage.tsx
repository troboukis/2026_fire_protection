import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { supabase } from '../lib/supabase'

type ContractRow = {
  id: number
  submission_at: string | null
  organization_value: string | null
  title: string | null
  cpv_values: string | null
  procedure_type_value: string | null
  first_member_name: string | null
  total_cost_without_vat: number | null
  diavgeia_ada: string | null
}

function clean(v: unknown): string {
  if (v == null) return ''
  const s = String(v).trim()
  if (!s || s.toLowerCase() === 'nan' || s.toLowerCase() === 'none') return ''
  return s
}

function firstPipePart(v: string | null): string {
  const s = clean(v)
  if (!s) return ''
  return s.split('|').map(x => x.trim()).filter(Boolean)[0] ?? ''
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

export default function ContractsPage() {
  const [rows, setRows] = useState<ContractRow[]>([])
  const [loading, setLoading] = useState(true)
  const [q, setQ] = useState('')
  const [org, setOrg] = useState('')
  const [procedure, setProcedure] = useState('')
  const [dateFrom, setDateFrom] = useState('')
  const [dateTo, setDateTo] = useState('')
  const [minAmount, setMinAmount] = useState('')

  useEffect(() => {
    let cancelled = false
    setLoading(true)

    const loadAll = async () => {
      const pageSize = 1000
      let from = 0
      let all: ContractRow[] = []

      while (true) {
        const { data, error } = await supabase
          .from('raw_procurements')
          .select(
            'id, submission_at, organization_value, title, cpv_values, procedure_type_value, first_member_name, total_cost_without_vat, diavgeia_ada',
          )
          .not('submission_at', 'is', null)
          .order('submission_at', { ascending: false })
          .range(from, from + pageSize - 1)

        if (error) break
        const batch = (data ?? []) as ContractRow[]
        all = all.concat(batch)
        if (batch.length < pageSize) break
        from += pageSize
      }

      if (!cancelled) {
        setRows(all)
        setLoading(false)
      }
    }

    loadAll()
    return () => { cancelled = true }
  }, [])

  const procedures = useMemo(() => {
    const vals = new Set<string>()
    for (const r of rows) {
      const p = clean(r.procedure_type_value)
      if (p) vals.add(p)
    }
    return [...vals].sort((a, b) => a.localeCompare(b, 'el'))
  }, [rows])

  const filtered = useMemo(() => {
    const query = q.trim().toLocaleLowerCase('el-GR')
    const orgQuery = org.trim().toLocaleLowerCase('el-GR')
    const min = minAmount ? Number(minAmount) : null
    const fromTs = dateFrom ? new Date(dateFrom).getTime() : null
    const toTs = dateTo ? new Date(dateTo).getTime() : null

    return rows.filter((r) => {
      const title = clean(r.title).toLocaleLowerCase('el-GR')
      const organization = clean(r.organization_value).toLocaleLowerCase('el-GR')
      const beneficiary = clean(r.first_member_name).toLocaleLowerCase('el-GR')
      const cpv = firstPipePart(r.cpv_values).toLocaleLowerCase('el-GR')
      const proc = clean(r.procedure_type_value)
      const amount = r.total_cost_without_vat != null ? Number(r.total_cost_without_vat) : null
      const ts = r.submission_at ? new Date(r.submission_at).getTime() : null

      if (query && !`${title} ${organization} ${beneficiary} ${cpv}`.includes(query)) return false
      if (orgQuery && !organization.includes(orgQuery)) return false
      if (procedure && proc !== procedure) return false
      if (min != null && !Number.isNaN(min) && (amount == null || amount < min)) return false
      if (fromTs != null && (ts == null || ts < fromTs)) return false
      if (toTs != null && (ts == null || ts > toTs + 86_399_999)) return false
      return true
    })
  }, [rows, q, org, procedure, minAmount, dateFrom, dateTo])

  return (
    <div className="contracts-page">
      <header className="contracts-header section-rule">
        <div>
          <div className="eyebrow">Συμβάσεις</div>
          <h1>Όλες οι Συμβάσεις</h1>
          <p>{loading ? 'Φόρτωση…' : `${filtered.length.toLocaleString('el-GR')} αποτελέσματα`}</p>
        </div>
        <Link className="contracts-back" to="/">← Επιστροφή στην αρχική</Link>
      </header>

      <section className="contracts-filters section-rule">
        <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Αναζήτηση (τίτλος/φορέας/δικαιούχος/CPV)" />
        <input value={org} onChange={(e) => setOrg(e.target.value)} placeholder="Φορέας" />
        <select value={procedure} onChange={(e) => setProcedure(e.target.value)}>
          <option value="">Όλες οι διαδικασίες</option>
          {procedures.map((p) => <option key={p} value={p}>{p}</option>)}
        </select>
        <input value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} type="date" />
        <input value={dateTo} onChange={(e) => setDateTo(e.target.value)} type="date" />
        <input value={minAmount} onChange={(e) => setMinAmount(e.target.value)} type="number" min="0" placeholder="Ελάχιστο ποσό (χωρίς ΦΠΑ)" />
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
              <th>Έγγραφο</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((r) => {
              const ada = clean(r.diavgeia_ada)
              const docUrl = ada ? `https://diavgeia.gov.gr/doc/${ada}` : null
              return (
                <tr key={r.id}>
                  <td>{fmtDate(r.submission_at)}</td>
                  <td>{clean(r.organization_value) || '—'}</td>
                  <td>{clean(r.title) || '—'}</td>
                  <td>{firstPipePart(r.cpv_values) || '—'}</td>
                  <td>{clean(r.first_member_name).toLocaleUpperCase('el-GR') || '—'}</td>
                  <td>{clean(r.procedure_type_value) || '—'}</td>
                  <td className="contracts-amount">{fmtEur(r.total_cost_without_vat)}</td>
                  <td>{docUrl ? <a href={docUrl} target="_blank" rel="noreferrer">Άνοιγμα</a> : '—'}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </section>
    </div>
  )
}

