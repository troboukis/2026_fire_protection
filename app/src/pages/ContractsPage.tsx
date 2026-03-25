import { useEffect, useMemo, useState } from 'react'
import ContractModal, { type ContractModalContract } from '../components/ContractModal'
import ComponentTag from '../components/ComponentTag'
import DataLoadingCard from '../components/DataLoadingCard'
import DevViewToggle from '../components/DevViewToggle'
import { downloadContractDocument } from '../lib/contractDocument'
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

function normalizeMunicipalityToken(v: string): string {
  return v
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/ς/g, 'σ')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase()
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

function truncateWords(value: string, maxWords: number): string {
  const text = clean(value)
  if (!text) return '—'
  const words = text.split(/\s+/).filter(Boolean)
  if (words.length <= maxWords) return text
  return `${words.slice(0, maxWords).join(' ')} ...`
}

function fmtDateLabel(iso: string | null): string {
  if (!iso) return '—'
  const dt = new Date(iso)
  if (Number.isNaN(dt.getTime())) return '—'
  return new Intl.DateTimeFormat('el-GR', { day: '2-digit', month: 'short', year: 'numeric' }).format(dt)
}

function cleanText(v: unknown): string | null {
  const s = clean(v)
  return s || null
}

function firstPipePart(v: unknown): string | null {
  const s = cleanText(v)
  if (!s) return null
  return s.split('|').map((x) => x.trim()).filter(Boolean)[0] ?? null
}

export default function ContractsPage() {
  const [rows, setRows] = useState<ContractRow[]>([])
  const [totalCount, setTotalCount] = useState(0)
  const [loading, setLoading] = useState(true)
  const [selectedContract, setSelectedContract] = useState<ContractModalContract | null>(null)
  const [openingContractId, setOpeningContractId] = useState<number | null>(null)
  const [q, setQ] = useState('')
  const [procedure, setProcedure] = useState('')
  const [procedureOptions, setProcedureOptions] = useState<string[]>([])
  const [municipalityNameTokens, setMunicipalityNameTokens] = useState<Set<string>>(new Set())
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
    const loadMunicipalityNames = async () => {
      const { data, error } = await supabase
        .from('municipality_normalized_name')
        .select('municipality_normalized_value')
        .not('municipality_normalized_value', 'is', null)
        .limit(5000)
      if (cancelled || error) return
      const tokens = new Set<string>()
      for (const row of (data ?? []) as Array<{ municipality_normalized_value: string | null }>) {
        const v = clean(row.municipality_normalized_value)
        if (!v) continue
        tokens.add(normalizeMunicipalityToken(v))
      }
      setMunicipalityNameTokens(tokens)
    }
    loadMunicipalityNames()
    return () => { cancelled = true }
  }, [])

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    const loadPage = async () => {
      const min = minAmount ? Number(minAmount) : null
      const { data, error } = await supabase.rpc('get_contracts_page', {
        p_q: q || null,
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
  }, [q, procedure, dateFrom, dateTo, minAmount, page])

  const totalPages = useMemo(() => Math.max(1, Math.ceil(totalCount / pageSize)), [totalCount])

  const downloadContractPdf = async (contract: ContractModalContract) => {
    await downloadContractDocument(contract)
  }

  const openContractModal = async (row: ContractRow) => {
    setOpeningContractId(row.id)
    try {
      const { data: procurement, error: procurementError } = await supabase
        .from('procurement')
        .select(`
          id,
          title,
          submission_at,
          short_descriptions,
          procedure_type_value,
          reference_number,
          contract_number,
          contract_signed_date,
          start_date,
          end_date,
          organization_vat_number,
          organization_key,
          assign_criteria,
          contract_type,
          units_operator,
          funding_details_cofund,
          funding_details_self_fund,
          funding_details_espa,
          funding_details_regular_budget,
          auction_ref_no,
          budget,
          contract_budget,
          diavgeia_ada
        `)
        .eq('id', row.id)
        .single()

      if (procurementError || !procurement) return

      const [
        paymentsResult,
        cpvResult,
        organizationResult,
      ] = await Promise.all([
        supabase
          .from('payment')
          .select('beneficiary_name, beneficiary_vat_number, signers, payment_ref_no, amount_without_vat, amount_with_vat')
          .eq('procurement_id', row.id),
        supabase
          .from('cpv')
          .select('cpv_key, cpv_value')
          .eq('procurement_id', row.id),
        supabase
          .from('organization')
          .select('organization_normalized_value, organization_value')
          .eq('organization_key', procurement.organization_key)
          .limit(1),
      ])

      const paymentRows = (paymentsResult.data ?? []) as Array<{
        beneficiary_name: string | null
        beneficiary_vat_number: string | null
        signers: string | null
        payment_ref_no: string | null
        amount_without_vat: number | null
        amount_with_vat: number | null
      }>
      const paymentPrimary = paymentRows[0] ?? null
      const amountWithoutVat = paymentRows.reduce<number | null>((sum, current) => {
        if (current.amount_without_vat == null || Number.isNaN(Number(current.amount_without_vat))) return sum
        return (sum ?? 0) + Number(current.amount_without_vat)
      }, null) ?? procurement.contract_budget ?? procurement.budget ?? null
      const amountWithVat = paymentRows.reduce<number | null>((sum, current) => {
        if (current.amount_with_vat == null || Number.isNaN(Number(current.amount_with_vat))) return sum
        return (sum ?? 0) + Number(current.amount_with_vat)
      }, null)

      const cpvItems = ((cpvResult.data ?? []) as Array<{ cpv_key: string | null; cpv_value: string | null }>)
        .map((item) => ({
          code: clean(item.cpv_key) || '—',
          label: clean(item.cpv_value) || '—',
        }))
        .filter((item) => item.code !== '—' || item.label !== '—')
        .reduce<Array<{ code: string; label: string }>>((acc, current) => {
          if (!acc.find((item) => item.code === current.code && item.label === current.label)) acc.push(current)
          return acc
        }, [])

      const primaryCpv = cpvItems[0] ?? null
      const organization = ((organizationResult.data ?? []) as Array<{
        organization_normalized_value: string | null
        organization_value: string | null
      }>)[0] ?? null
      const beneficiary = paymentPrimary?.beneficiary_name ?? row.beneficiary_name
      const whyText = firstPipePart(procurement.short_descriptions) ?? primaryCpv?.label ?? clean(row.cpv_value) ?? '—'
      const cpvLabel = primaryCpv?.label ?? clean(row.cpv_value) ?? '—'
      const diavgeiaAda = clean(procurement.diavgeia_ada)
      const modal: ContractModalContract = {
        id: String(procurement.id),
        who: clean(organization?.organization_normalized_value) || clean(organization?.organization_value) || clean(row.organization_value) || '—',
        what: clean(procurement.title) || clean(row.title) || '—',
        when: fmtDateLabel(cleanText(procurement.submission_at)),
        why: whyText,
        beneficiary: clean(beneficiary) || '—',
        contractType: clean(procurement.procedure_type_value) || clean(row.procedure_type_value) || '—',
        howMuch: fmtEur(amountWithoutVat),
        withoutVatAmount: fmtEur(amountWithoutVat),
        withVatAmount: fmtEur(amountWithVat),
        referenceNumber: clean(procurement.reference_number) || clean(row.reference_number) || '—',
        contractNumber: clean(procurement.contract_number) || '—',
        cpv: cpvLabel,
        cpvCode: primaryCpv?.code ?? '—',
        cpvItems,
        signedAt: fmtDateLabel(cleanText(procurement.contract_signed_date)),
        startDate: fmtDateLabel(cleanText(procurement.start_date)),
        endDate: fmtDateLabel(cleanText(procurement.end_date)),
        organizationVat: clean(procurement.organization_vat_number) || '—',
        beneficiaryVat: clean(paymentPrimary?.beneficiary_vat_number) || '—',
        signers: clean(paymentPrimary?.signers) || '—',
        assignCriteria: clean(procurement.assign_criteria) || '—',
        contractKind: clean(procurement.contract_type) || '—',
        unitsOperator: clean(procurement.units_operator) || '—',
        fundingCofund: clean(procurement.funding_details_cofund) || '—',
        fundingSelf: clean(procurement.funding_details_self_fund) || '—',
        fundingEspa: clean(procurement.funding_details_espa) || '—',
        fundingRegular: clean(procurement.funding_details_regular_budget) || '—',
        auctionRefNo: clean(procurement.auction_ref_no) || '—',
        paymentRefNo: clean(paymentPrimary?.payment_ref_no) || '—',
        shortDescription: firstPipePart(procurement.short_descriptions) ?? '—',
        rawBudget: fmtEur(procurement.budget),
        contractBudget: fmtEur(procurement.contract_budget),
        documentUrl: diavgeiaAda ? `https://diavgeia.gov.gr/doc/${diavgeiaAda}` : null,
      }

      setSelectedContract(modal)
    } finally {
      setOpeningContractId(null)
    }
  }

  return (
    <div className={`contracts-page${selectedContract ? ' contracts-page--modal-open' : ''}`}>
      <DevViewToggle />
      <ComponentTag name="ContractsPage" />
      <header className="contracts-header section-rule">
        <div>
          <div className="eyebrow">ΑΝΑΖΗΤΗΣΗ</div>
          <h1>Όλες οι Συμβάσεις</h1>
          <p>
            {loading
              ? 'Φόρτωση…'
              : `${totalCount.toLocaleString('el-GR')} αποτελέσματα · Περίοδος: ${periodLabel(dateFrom, dateTo)}`}
          </p>
        </div>
      </header>

      <section className="contracts-filters section-rule">
        <input
          className="contracts-filter contracts-filter--search"
          value={q}
          onChange={(e) => { setQ(e.target.value); setPage(1) }}
          placeholder="Αναζήτηση (τίτλος/φορέας/δικαιούχος/CPV)"
        />
        <select
          className="contracts-filter contracts-filter--procedure"
          value={procedure}
          onChange={(e) => { setProcedure(e.target.value); setPage(1) }}
        >
          <option value="">Όλες οι διαδικασίες</option>
          {procedureOptions.map((p) => <option key={p} value={p}>{p}</option>)}
        </select>
        <input
          className="contracts-filter contracts-filter--date"
          value={dateFrom}
          onChange={(e) => { setDateFrom(e.target.value); setPage(1) }}
          type="date"
        />
        <input
          className="contracts-filter contracts-filter--date"
          value={dateTo}
          onChange={(e) => { setDateTo(e.target.value); setPage(1) }}
          type="date"
        />
        <input
          className="contracts-filter contracts-filter--amount"
          value={minAmount}
          onChange={(e) => { setMinAmount(e.target.value); setPage(1) }}
          type="number"
          min="0"
          placeholder="Ελάχιστο ποσό (χωρίς ΦΠΑ)"
        />
      </section>

      <section className="contracts-table-wrap section-rule">
        {loading ? (
          <DataLoadingCard message="Εκτελείται αναζήτηση συμβάσεων και προετοιμάζεται ο πίνακας αποτελεσμάτων." />
        ) : (
          <>
            <table className="contracts-table">
              <colgroup>
                <col className="contracts-col contracts-col--date" />
                <col className="contracts-col contracts-col--org" />
                <col className="contracts-col contracts-col--title" />
                <col className="contracts-col contracts-col--cpv" />
                <col className="contracts-col contracts-col--beneficiary" />
                <col className="contracts-col contracts-col--procedure" />
                <col className="contracts-col contracts-col--amount" />
                <col className="contracts-col contracts-col--ref" />
              </colgroup>
              <thead>
                <tr>
                  <th>Ημερομηνία</th>
                  <th>Φορέας</th>
                  <th>Τίτλος</th>
                  <th>Περιγραφή Εργασίας</th>
                  <th>Δικαιούχος</th>
                  <th>Διαδικασία</th>
                  <th>Ποσό χωρίς ΦΠΑ</th>
                  <th>ΑΔΑΜ</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => {
                  const refNo = clean(r.reference_number)
                  const orgRaw = clean(r.organization_value)
                  const orgToken = normalizeMunicipalityToken(orgRaw)
                  const orgDisplay = municipalityNameTokens.has(orgToken) && !orgToken.startsWith('ΔΗΜΟΣ ')
                    ? `ΔΗΜΟΣ ${orgRaw}`
                    : (orgRaw || '—')
                  return (
                    <tr key={r.id}>
                      <td data-label="Ημερομηνία">{fmtDate(r.contract_signed_date)}</td>
                      <td data-label="Φορέας">{orgDisplay}</td>
                      <td data-label="Τίτλος">
                        <button
                          type="button"
                          className="contracts-title-button"
                          onClick={() => { void openContractModal(r) }}
                          disabled={openingContractId === r.id}
                        >
                          {clean(r.title) || '—'}
                        </button>
                      </td>
                      <td data-label="Περιγραφή Εργασίας">{truncateWords(clean(r.cpv_value), 10)}</td>
                      <td data-label="Δικαιούχος">{clean(r.beneficiary_name).toLocaleUpperCase('el-GR') || '—'}</td>
                      <td data-label="Διαδικασία">{clean(r.procedure_type_value) || '—'}</td>
                      <td data-label="Ποσό χωρίς ΦΠΑ" className="contracts-amount">{fmtEur(r.amount_without_vat)}</td>
                      <td data-label="ΑΔΑΜ">{refNo || '—'}</td>
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
          </>
        )}
      </section>

      {selectedContract && (
        <ContractModal
          contract={selectedContract}
          onClose={() => setSelectedContract(null)}
          onDownloadPdf={() => downloadContractPdf(selectedContract)}
        />
      )}
    </div>
  )
}
