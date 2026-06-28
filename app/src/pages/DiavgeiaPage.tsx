import { useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import ComponentTag from '../components/ComponentTag'
import DataLoadingCard from '../components/DataLoadingCard'
import DevViewToggle from '../components/DevViewToggle'
import { supabase } from '../lib/supabase'

type DiavgeiaPageRow = {
  id: number
  org_type: string | null
  org_name_clean: string | null
  subject: string | null
  decision_date: string | null
  ada: string | null
  diavgeia_document_type_decision_uid: string | null
  document_url: string | null
  spending_contractors_value: string | null
  min_decision_date: string | null
  max_decision_date: string | null
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

function parseGreekAmount(value: string | null | undefined): number | null {
  const text = clean(value)
  if (!text) return null
  const match = text.match(/\d[\d.,]*/)
  if (!match) return null
  const normalized = match[0].includes(',')
    ? match[0].replace(/\./g, '').replace(',', '.')
    : match[0]
  const amount = Number(normalized)
  return Number.isFinite(amount) && amount > 0 ? amount : null
}

function fmtEurFromText(value: string | null | undefined): string {
  const amount = parseGreekAmount(value)
  if (amount == null) return '—'
  return amount.toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 })
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

function isoDateInputValue(value: string | null): string {
  if (!value) return ''
  const dt = new Date(value)
  if (Number.isNaN(dt.getTime())) return ''
  const tzOffsetMs = dt.getTimezoneOffset() * 60_000
  return new Date(dt.getTime() - tzOffsetMs).toISOString().slice(0, 10)
}

function periodLabel(dateFrom: string, dateTo: string): string {
  if (dateFrom && dateTo) return `${fmtDate(dateFrom)} - ${fmtDate(dateTo)}`
  if (dateFrom) return `Από ${fmtDate(dateFrom)}`
  if (dateTo) return `Έως ${fmtDate(dateTo)}`
  return ''
}

function truncateWords(value: string, maxWords: number): string {
  const text = clean(value)
  if (!text) return '—'
  const words = text.split(/\s+/).filter(Boolean)
  if (words.length <= maxWords) return text
  return `${words.slice(0, maxWords).join(' ')} ...`
}

export default function DiavgeiaPage() {
  const [searchParams] = useSearchParams()
  const defaultDateFrom = useMemo(() => isoDateDaysAgo(30), [])
  const defaultDateTo = useMemo(() => isoToday(), [])
  const initialMunicipalityKey = useMemo(() => clean(searchParams.get('municipalityKey')), [searchParams])
  const initialAllDates = useMemo(() => clean(searchParams.get('allDates')) === '1', [searchParams])
  const initialDateFrom = useMemo(() => initialAllDates ? '' : defaultDateFrom, [defaultDateFrom, initialAllDates])
  const initialDateTo = useMemo(() => initialAllDates ? '' : defaultDateTo, [defaultDateTo, initialAllDates])
  const [rows, setRows] = useState<DiavgeiaPageRow[]>([])
  const [totalCount, setTotalCount] = useState(0)
  const [minDecisionDate, setMinDecisionDate] = useState<string | null>(null)
  const [maxDecisionDate, setMaxDecisionDate] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [q, setQ] = useState('')
  const [dateFrom, setDateFrom] = useState(initialDateFrom)
  const [dateTo, setDateTo] = useState(initialDateTo)
  const [municipalityKey, setMunicipalityKey] = useState(initialMunicipalityKey)
  const [municipalityLabel, setMunicipalityLabel] = useState('')
  const [page, setPage] = useState(1)
  const pageSize = 50

  useEffect(() => {
    setMunicipalityKey(initialMunicipalityKey)
    setDateFrom(initialDateFrom)
    setDateTo(initialDateTo)
    setPage(1)
  }, [initialDateFrom, initialDateTo, initialMunicipalityKey])

  useEffect(() => {
    let cancelled = false
    const key = clean(municipalityKey)
    setMunicipalityLabel('')
    if (!key) return

    const loadMunicipalityLabel = async () => {
      const { data, error } = await supabase
        .from('municipality_normalized_name')
        .select('municipality_normalized_value, municipality_value')
        .eq('municipality_key', key)
        .limit(1)

      if (cancelled || error) return
      const row = ((data ?? []) as Array<{
        municipality_normalized_value: string | null
        municipality_value: string | null
      }>)[0] ?? null
      const label = clean(row?.municipality_normalized_value) || clean(row?.municipality_value)
      if (label) setMunicipalityLabel(label.startsWith('ΔΗΜΟΣ ') ? label : `ΔΗΜΟΣ ${label}`)
    }

    loadMunicipalityLabel()
    return () => { cancelled = true }
  }, [municipalityKey])

  useEffect(() => {
    let cancelled = false
    setLoading(true)

    const loadPage = async () => {
      try {
        const { data, error } = await supabase.rpc('get_diavgeia_page', {
          p_q: clean(q) || null,
          p_date_from: clean(dateFrom) || null,
          p_date_to: clean(dateTo) || null,
          p_municipality_key: clean(municipalityKey) || null,
          p_page: page,
          p_page_size: pageSize,
        })
        if (error) throw error

        if (cancelled) return
        const pageRows = (data ?? []) as DiavgeiaPageRow[]
        setRows(pageRows)
        setTotalCount(pageRows[0]?.total_count ?? 0)
        setMinDecisionDate(pageRows[0]?.min_decision_date ?? null)
        setMaxDecisionDate(pageRows[0]?.max_decision_date ?? null)
        setLoading(false)
      } catch {
        if (cancelled) return
        setRows([])
        setTotalCount(0)
        setMinDecisionDate(null)
        setMaxDecisionDate(null)
        setLoading(false)
      }
    }

    loadPage()
    return () => { cancelled = true }
  }, [dateFrom, dateTo, municipalityKey, page, q])

  const totalPages = useMemo(() => Math.max(1, Math.ceil(totalCount / pageSize)), [totalCount])
  const visiblePeriodLabel = useMemo(() => {
    const explicitPeriod = periodLabel(dateFrom, dateTo)
    if (explicitPeriod) return explicitPeriod
    if (minDecisionDate && maxDecisionDate) return `${fmtDate(minDecisionDate)} - ${fmtDate(maxDecisionDate)}`
    return '—'
  }, [dateFrom, dateTo, maxDecisionDate, minDecisionDate])
  const visibleDateFrom = dateFrom || isoDateInputValue(minDecisionDate)
  const visibleDateTo = dateTo || isoDateInputValue(maxDecisionDate)

  const resetToLast30Days = () => {
    setDateFrom(defaultDateFrom)
    setDateTo(defaultDateTo)
    setPage(1)
  }

  const showAllDates = () => {
    setDateFrom('')
    setDateTo('')
    setPage(1)
  }

  return (
    <div className="contracts-page diavgeia-page">
      <DevViewToggle />
      <ComponentTag name="DiavgeiaPage" />
      <header className="contracts-header section-rule">
        <div>
          <div className="eyebrow">ΑΝΑΖΗΤΗΣΗ</div>
          <h1>Διαύγεια</h1>
          <p>
            {loading
              ? 'Φόρτωση…'
              : `${totalCount.toLocaleString('el-GR')} αποφάσεις · Περίοδος: ${visiblePeriodLabel}${municipalityLabel ? ` · ${municipalityLabel}` : ''}`}
          </p>
        </div>
      </header>

      <section className="contracts-filters contracts-filters--diavgeia section-rule">
        <input
          className="contracts-filter contracts-filter--search"
          value={q}
          onChange={(e) => { setQ(e.target.value); setPage(1) }}
          placeholder="Αναζήτηση (τίτλος/φορέας/ΑΔΑ/τύπος)"
        />
        <input
          className="contracts-filter contracts-filter--date contracts-filter--date-from"
          value={visibleDateFrom}
          onChange={(e) => { setDateFrom(e.target.value); setPage(1) }}
          type="date"
          aria-label="Ημερομηνία από"
        />
        <input
          className="contracts-filter contracts-filter--date contracts-filter--date-to"
          value={visibleDateTo}
          onChange={(e) => { setDateTo(e.target.value); setPage(1) }}
          type="date"
          aria-label="Ημερομηνία έως"
        />
        <button type="button" className="contracts-filter-button contracts-filter-button--recent" onClick={resetToLast30Days}>
          Τελευταίες 30 ημέρες
        </button>
        <button type="button" className="contracts-filter-button contracts-filter-button--all" onClick={showAllDates}>
          Όλο το διάστημα
        </button>
      </section>

      <section className="contracts-table-wrap section-rule">
        {loading ? (
          <DataLoadingCard message="Ανακτώνται οι αποφάσεις Διαύγειας για τη συγκεκριμένη σελίδα αποτελεσμάτων." />
        ) : (
          <>
            <table className="contracts-table diavgeia-table">
              <colgroup>
                <col className="contracts-col contracts-col--date" />
                <col className="contracts-col contracts-col--org" />
                <col className="contracts-col contracts-col--title" />
                <col className="contracts-col contracts-col--procedure" />
                <col className="contracts-col contracts-col--amount" />
                <col className="contracts-col contracts-col--ref" />
              </colgroup>
              <thead>
                <tr>
                  <th>Ημερομηνία</th>
                  <th>Φορέας</th>
                  <th>Θέμα</th>
                  <th>Τύπος απόφασης</th>
                  <th>Αξία</th>
                  <th>ΑΔΑ</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => {
                  const ada = clean(row.ada)
                  const href = clean(row.document_url) || (ada ? `https://diavgeia.gov.gr/doc/${ada}` : '')
                  const orgLabel = [clean(row.org_type), clean(row.org_name_clean)].filter(Boolean).join(' · ') || '—'
                  return (
                    <tr key={row.id}>
                      <td data-label="Ημερομηνία">{fmtDate(row.decision_date)}</td>
                      <td data-label="Φορέας">{orgLabel}</td>
                      <td data-label="Θέμα">{truncateWords(clean(row.subject), 18)}</td>
                      <td data-label="Τύπος απόφασης">{clean(row.diavgeia_document_type_decision_uid) || '—'}</td>
                      <td data-label="Αξία">{fmtEurFromText(row.spending_contractors_value)}</td>
                      <td data-label="ΑΔΑ">
                        {href ? (
                          <a href={href} target="_blank" rel="noreferrer">{ada || 'Άνοιγμα'}</a>
                        ) : (
                          ada || '—'
                        )}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
            {rows.length === 0 && (
              <article className="wire-item">
                <h2>Δεν βρέθηκαν αποφάσεις Διαύγειας για τα επιλεγμένα φίλτρα.</h2>
              </article>
            )}
            <div className="contracts-pagination">
              <button onClick={() => setPage((p) => Math.max(1, p - 1))} disabled={page <= 1 || loading}>Προηγούμενη</button>
              <span>Σελίδα {page} / {totalPages}</span>
              <button onClick={() => setPage((p) => Math.min(totalPages, p + 1))} disabled={page >= totalPages || loading}>Επόμενη</button>
            </div>
          </>
        )}
      </section>
    </div>
  )
}
