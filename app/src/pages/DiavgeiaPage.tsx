import { translatedOrganizationName } from '../i18n/organizationNames'
import { useProcurementI18n } from '../i18n/useProcurementI18n'
import { useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import AmountSummary from '../components/AmountSummary'
import ComponentTag from '../components/ComponentTag'
import DataLoadingCard from '../components/DataLoadingCard'
import DevViewToggle from '../components/DevViewToggle'
import DiavgeiaModal from '../components/DiavgeiaModal'
import OrganizationFilter from '../components/OrganizationFilter'
import type { DiavgeiaDecisionCardView } from '../components/DiavgeiaDecisionCard'
import { parseDiavgeiaAmountCents, summarizeDiavgeiaAmounts } from '../lib/diavgeiaAmount'
import { buildDiavgeiaDecisionCardView } from '../lib/diavgeiaDecision'
import { sortDiavgeiaRows, type DiavgeiaSort } from '../lib/diavgeiaSort'
import { MAX_SEARCH_QUERY_LENGTH, matchesSearchQuery } from '../lib/searchQuery'
import { supabase } from '../lib/supabase'

const DEFAULT_DECISION_TYPE = 'ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ'

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
  municipality_key: string | null
  protocol_number: string | null
  thematic_categories: string | null
  spending_signers: string | null
  spending_contractors_name: string | null
  spending_contractors_afm: string | null
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

function organizationLabel(row: Pick<DiavgeiaPageRow, 'org_type' | 'org_name_clean'>): string {
  return [clean(row.org_type), clean(row.org_name_clean)].filter(Boolean).join(' · ')
}

async function loadOrganizationOptions(signal: AbortSignal): Promise<string[]> {
  const organizations = new Set<string>()
  const batchSize = 1000
  // Options cover all dates, independently of the filtered document snapshot.
  for (let offset = 0; ; offset += batchSize) {
    const { data, error } = await supabase
      .from('diavgeia')
      .select('org_type, org_name_clean')
      .order('id')
      .range(offset, offset + batchSize - 1)
      .abortSignal(signal)
    if (error) throw error
    for (const row of data ?? []) {
      const label = organizationLabel(row)
      if (label) organizations.add(label)
    }
    if ((data ?? []).length < batchSize) return [...organizations].sort((a, b) => a.localeCompare(b, 'el'))
  }
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

function truncateWords(value: string, maxWords: number): string {
  const text = clean(value)
  if (!text) return '—'
  const words = text.split(/\s+/).filter(Boolean)
  if (words.length <= maxWords) return text
  return `${words.slice(0, maxWords).join(' ')} ...`
}

async function loadContractLinkedDecisionIds(): Promise<Set<number>> {
  const ids = new Set<number>()
  const batchSize = 1000
  // Read every link: the API limits each response to 1,000 rows.
  for (let offset = 0; ; offset += batchSize) {
    const { data, error } = await supabase
      .from('diavgeia_procurement')
      .select('diavgeia_id')
      .order('diavgeia_id')
      .order('procurement_id')
      .range(offset, offset + batchSize - 1)
    if (error) throw error
    for (const row of data ?? []) ids.add(row.diavgeia_id)
    if ((data ?? []).length < batchSize) return ids
  }
}

export default function DiavgeiaPage() {
  const { p, t, format, category, organization: organizationText, periodLabel } = useProcurementI18n()
  const [searchParams] = useSearchParams()
  const defaultDateFrom = useMemo(() => isoDateDaysAgo(30), [])
  const defaultDateTo = useMemo(() => isoToday(), [])
  const initialMunicipalityKey = useMemo(() => clean(searchParams.get('municipalityKey')), [searchParams])
  const initialQ = useMemo(() => clean(searchParams.get('q')), [searchParams])
  const initialAllDates = useMemo(() => clean(searchParams.get('allDates')) === '1', [searchParams])
  const initialDateFromParam = useMemo(() => clean(searchParams.get('dateFrom')), [searchParams])
  const initialDateToParam = useMemo(() => clean(searchParams.get('dateTo')), [searchParams])
  const initialDateFrom = useMemo(() => initialAllDates ? '' : (initialDateFromParam || defaultDateFrom), [defaultDateFrom, initialAllDates, initialDateFromParam])
  const initialDateTo = useMemo(() => initialAllDates ? '' : (initialDateToParam || defaultDateTo), [defaultDateTo, initialAllDates, initialDateToParam])
  const [sourceRows, setSourceRows] = useState<DiavgeiaPageRow[]>([])
  const [contractLinkedDecisionIds, setContractLinkedDecisionIds] = useState<Set<number>>(new Set())
  const [loading, setLoading] = useState(true)
  const [loadFailed, setLoadFailed] = useState(false)
  const [selectedDecision, setSelectedDecision] = useState<DiavgeiaDecisionCardView | null>(null)
  const [q, setQ] = useState(initialQ)
  const [decisionType, setDecisionType] = useState(DEFAULT_DECISION_TYPE)
  const [organization, setOrganization] = useState('')
  const [allOrganizations, setAllOrganizations] = useState<string[]>([])
  const [organizationsLoading, setOrganizationsLoading] = useState(true)
  const [organizationsFailed, setOrganizationsFailed] = useState(false)
  const [excludeContractAmounts, setExcludeContractAmounts] = useState(false)
  const [dateFrom, setDateFrom] = useState(initialDateFrom)
  const [dateTo, setDateTo] = useState(initialDateTo)
  const [municipalityKey, setMunicipalityKey] = useState(initialMunicipalityKey)
  const [municipalityLabel, setMunicipalityLabel] = useState('')
  const [page, setPage] = useState(1)
  const [sort, setSort] = useState<DiavgeiaSort>('date_desc')
  const pageSize = 50

  useEffect(() => {
    let cancelled = false
    const controller = new AbortController()
    const load = async () => {
      try {
        const options = await loadOrganizationOptions(controller.signal)
        if (!cancelled) setAllOrganizations(options)
      } catch (error) {
        if (!cancelled) {
          console.error('Failed to load organization options', error)
          setOrganizationsFailed(true)
        }
      } finally {
        if (!cancelled) setOrganizationsLoading(false)
      }
    }
    void load()
    return () => { cancelled = true; controller.abort() }
  }, [])

  useEffect(() => {
    setQ(initialQ)
    setDecisionType(DEFAULT_DECISION_TYPE)
    setOrganization('')
    setMunicipalityKey(initialMunicipalityKey)
    setDateFrom(initialDateFrom)
    setDateTo(initialDateTo)
    setPage(1)
    setSelectedDecision(null)
  }, [initialDateFrom, initialDateTo, initialMunicipalityKey, initialQ])

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
    setLoadFailed(false)

    const loadPage = async () => {
      try {
        const [{ data, error }, linkedDecisionIds] = await Promise.all([
          supabase.rpc('get_diavgeia_page_snapshot', {
            p_date_from: clean(dateFrom) || null,
            p_date_to: clean(dateTo) || null,
            p_municipality_key: clean(municipalityKey) || null,
          }),
          loadContractLinkedDecisionIds(),
        ])
        if (error) throw error

        if (cancelled) return
        const payload = (data ?? {}) as { rows?: DiavgeiaPageRow[] }
        const pageRows = Array.isArray(payload.rows) ? payload.rows : []
        setSourceRows(pageRows)
        setContractLinkedDecisionIds(linkedDecisionIds)
        setLoading(false)
      } catch (error) {
        if (cancelled) return
        console.error('Failed to load Diavgeia decisions', error)
        setSourceRows([])
        setContractLinkedDecisionIds(new Set())
        setLoadFailed(true)
        setLoading(false)
      }
    }

    loadPage()
    return () => { cancelled = true }
  }, [dateFrom, dateTo, municipalityKey])

  const decisionTypeOptions = useMemo(() => {
    const types = new Set(sourceRows.map((row) => clean(row.diavgeia_document_type_decision_uid)).filter(Boolean))
    // Keep the active choice visible even when the new date range has no matches.
    if (decisionType) types.add(decisionType)
    return [...types].sort((a, b) => a.localeCompare(b, 'el'))
  }, [sourceRows, decisionType])

  const organizationOptions = useMemo(() => {
    const organizations = new Set([...allOrganizations, ...sourceRows.map(organizationLabel).filter(Boolean)])
    if (organization) organizations.add(organization)
    return [...organizations].sort((a, b) => a.localeCompare(b, 'el'))
  }, [allOrganizations, sourceRows, organization])

  const filteredRows = useMemo(() => sourceRows.filter((row) => (
    (!decisionType || clean(row.diavgeia_document_type_decision_uid) === decisionType)
    && (!organization || organizationLabel(row) === organization)
    && matchesSearchQuery([
      row.subject,
      row.org_type,
      row.org_name_clean,
      translatedOrganizationName(row.org_name_clean ?? ''),
      row.ada,
      row.diavgeia_document_type_decision_uid,
    ].join(' '), q)
  )), [sourceRows, q, decisionType, organization])
  const paymentSummary = useMemo(() => {
    if (decisionType !== 'ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ') return null
    const summary = summarizeDiavgeiaAmounts(filteredRows)
    if (excludeContractAmounts) {
      const linkedSummary = summarizeDiavgeiaAmounts(
        filteredRows.filter((row) => contractLinkedDecisionIds.has(row.id)),
      )
      summary.totalCents -= linkedSummary.totalCents
    }
    return summary
  }, [decisionType, filteredRows, excludeContractAmounts, contractLinkedDecisionIds])
  const totalCount = filteredRows.length
  const sortedRows = useMemo(() => sortDiavgeiaRows(filteredRows, sort), [filteredRows, sort])
  const rows = useMemo(() => {
    const pageStart = Math.max((page - 1) * pageSize, 0)
    return sortedRows.slice(pageStart, pageStart + pageSize)
  }, [sortedRows, page])
  const minDecisionDate = sourceRows[0]?.min_decision_date ?? null
  const maxDecisionDate = sourceRows[0]?.max_decision_date ?? null

  const totalPages = useMemo(() => Math.max(1, Math.ceil(totalCount / pageSize)), [totalCount])
  const visiblePeriodLabel = dateFrom || dateTo
    ? periodLabel(dateFrom, dateTo)
    : minDecisionDate && maxDecisionDate
      ? `${format.date(minDecisionDate)} - ${format.date(maxDecisionDate)}`
      : '—'
  const visibleDateFrom = dateFrom || isoDateInputValue(minDecisionDate)
  const visibleDateTo = dateTo || isoDateInputValue(maxDecisionDate)

  return (
    <div className={`contracts-page diavgeia-page${selectedDecision ? ' contracts-page--modal-open' : ''}`}>
      <DevViewToggle />
      <ComponentTag name="DiavgeiaPage" />
      <header className="contracts-header contracts-header--amount-summary section-rule">
        <div>
          <div className="eyebrow">{p('searchHeading')}</div>
          <h1>{t('navigation:diavgeia')}</h1>
          <p>
            {loading
              ? t('common:loading')
              : loadFailed
                ? p('decisionsLoadFailed')
                : `${p('decisionsPeriod', { count: totalCount, period: visiblePeriodLabel })}${municipalityLabel ? ` · ${municipalityLabel}` : ''}`}
          </p>
        </div>
        {paymentSummary && (
          <AmountSummary
            {...paymentSummary}
            loading={loading}
            loadFailed={loadFailed}
            ariaLabel={p('paymentsTotal')}
          >
            <label className="diavgeia-payment-summary__exclude">
              <input
                type="checkbox"
                checked={excludeContractAmounts}
                onChange={(event) => setExcludeContractAmounts(event.target.checked)}
              />
              <span>{p('excludeContractAmounts')}</span>
            </label>
          </AmountSummary>
        )}
      </header>

      <section className="contracts-filters contracts-filters--diavgeia section-rule">
        <input
          className="contracts-filter contracts-filter--search"
          value={q}
          maxLength={MAX_SEARCH_QUERY_LENGTH}
          onChange={(e) => { setQ(e.target.value); setPage(1) }}
          placeholder={p('searchExample', { example: 'πυροπροστασία | πυρόσβεση' })}
          title={p('searchHelp')}
        />
        <OrganizationFilter
          options={organizationOptions}
          loading={organizationsLoading}
          failed={organizationsFailed}
          value={organization}
          onChange={(value) => { setOrganization(value); setPage(1) }}
        />
        <select
          className="contracts-filter contracts-filter--decision-type"
          value={decisionType}
          onChange={(e) => { setDecisionType(e.target.value); setPage(1) }}
          aria-label={p('decisionType')}
          title={category(decisionType) || p('allDecisionTypes')}
        >
          <option value="">{p('allDecisionTypes')}</option>
          {decisionTypeOptions.map((type) => <option key={type} value={type}>{category(type)}</option>)}
        </select>
        <input
          className="contracts-filter contracts-filter--date contracts-filter--date-from"
          value={visibleDateFrom}
          onChange={(e) => { setDateFrom(e.target.value); setPage(1) }}
          type="date"
          aria-label={p('dateFrom')}
        />
        <input
          className="contracts-filter contracts-filter--date contracts-filter--date-to"
          value={visibleDateTo}
          onChange={(e) => { setDateTo(e.target.value); setPage(1) }}
          type="date"
          aria-label={p('dateTo')}
        />
      </section>

      <section className="contracts-table-wrap section-rule">
        <div className="contracts-status-legend diavgeia-status-legend" aria-label={p('decisionLinkLegend')}>
          <span><i className="contracts-status-legend__swatch diavgeia-status-legend__swatch--linked" /><span>{p('linkedToContract')}</span></span>
        </div>
        {loading ? (
          <DataLoadingCard message={p('loadingDecisions')} />
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
                  <th aria-sort={sort.startsWith('date_') ? (sort === 'date_asc' ? 'ascending' : 'descending') : 'none'}>
                    <button
                      type="button"
                      className={`contracts-sort-header${sort.startsWith('date_') ? ' contracts-sort-header--active' : ''}`}
                      onClick={() => {
                        setSort((current) => current === 'date_desc' ? 'date_asc' : 'date_desc')
                        setPage(1)
                      }}
                      aria-label={p('sortDate')}
                    >
                      <span>{p('date')}</span>
                      <span className="contracts-sort-header__icon">
                        {sort.startsWith('date_') ? (sort === 'date_asc' ? '↑' : '↓') : '↕'}
                      </span>
                    </button>
                  </th>
                  <th>{p('organization')}</th>
                  <th>{p('subject')}</th>
                  <th>{p('decisionType')}</th>
                  <th aria-sort={sort.startsWith('amount_') ? (sort === 'amount_asc' ? 'ascending' : 'descending') : 'none'}>
                    <button
                      type="button"
                      className={`contracts-sort-header${sort.startsWith('amount_') ? ' contracts-sort-header--active' : ''}`}
                      onClick={() => {
                        setSort((current) => current === 'amount_desc' ? 'amount_asc' : 'amount_desc')
                        setPage(1)
                      }}
                      aria-label={p('sortValue')}
                    >
                      <span>{p('value')}</span>
                      <span className="contracts-sort-header__icon">
                        {sort.startsWith('amount_') ? (sort === 'amount_asc' ? '↑' : '↓') : '↕'}
                      </span>
                    </button>
                  </th>
                  <th>{p('ada')}</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => {
                  const ada = clean(row.ada)
                  const href = clean(row.document_url) || (ada ? `https://diavgeia.gov.gr/doc/${ada}` : '')
                  const orgLabel = organizationLabel(row) || '—'
                  return (
                    <tr key={row.id} className={contractLinkedDecisionIds.has(row.id) ? 'diavgeia-row--linked' : undefined}>
                      <td data-label={p('date')}>{format.date(row.decision_date)}</td>
                      <td data-label={p('organization')}>{organizationText(orgLabel)}</td>
                      <td data-label={p('subject')}>
                        <button
                          type="button"
                          className="contracts-title-button"
                          lang="el"
                          onClick={() => setSelectedDecision(buildDiavgeiaDecisionCardView({
                            diavgeia_id: row.id,
                            org_type: row.org_type,
                            org_name_clean: row.org_name_clean,
                            subject: row.subject,
                            decision_date: row.decision_date,
                            ada: row.ada,
                            diavgeia_document_type_decision_uid: row.diavgeia_document_type_decision_uid,
                            spending_contractors_value: row.spending_contractors_value,
                            document_url: row.document_url,
                            municipality_key: row.municipality_key,
                            protocol_number: row.protocol_number,
                            thematic_categories: row.thematic_categories,
                            spending_signers: row.spending_signers,
                            spending_contractors_name: row.spending_contractors_name,
                            spending_contractors_afm: row.spending_contractors_afm,
                          }))}
                        >
                          {truncateWords(clean(row.subject), 18)}
                        </button>
                      </td>
                      <td data-label={p('decisionType')}>{category(clean(row.diavgeia_document_type_decision_uid)) || '—'}</td>
                      <td data-label={p('value')}>{format.currency((parseDiavgeiaAmountCents(row.spending_contractors_value) ?? NaN) / 100, { maximumFractionDigits: 0 })}</td>
                      <td data-label={p('ada')}>
                        {href ? (
                          <a href={href} target="_blank" rel="noreferrer">{ada || p('open')}</a>
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
                <h2>{loadFailed
                  ? p('decisionsRetry')
                  : p('decisionsEmpty')}</h2>
              </article>
            )}
            <div className="contracts-pagination">
              <button onClick={() => setPage((p) => Math.max(1, p - 1))} disabled={page <= 1 || loading}>{p('previous')}</button>
              <span>{p('page', { page, total: totalPages })}</span>
              <button onClick={() => setPage((p) => Math.min(totalPages, p + 1))} disabled={page >= totalPages || loading}>{p('next')}</button>
            </div>
          </>
        )}
      </section>

      {selectedDecision && (
        <DiavgeiaModal
          decision={selectedDecision}
          onClose={() => setSelectedDecision(null)}
        />
      )}
    </div>
  )
}
