import { useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import ContractModal, { type ContractModalContract } from '../components/ContractModal'
import ComponentTag from '../components/ComponentTag'
import DataLoadingCard from '../components/DataLoadingCard'
import DevViewToggle from '../components/DevViewToggle'
import { buildContractAuthorityLabel, type ContractAuthorityScope } from '../lib/contractAuthority'
import { buildDiavgeiaDocumentUrl, downloadContractDocument } from '../lib/contractDocument'
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

type ContractsPageInitialFilters = {
  q: string
  procedure: string
  dateFrom: string
  dateTo: string
  minAmount: string
  organizationKeys: string[]
  regionKey: string
  municipalityKey: string
}

function parseIsoDateParam(value: string | null, fallback: string): string {
  const next = clean(value)
  return /^\d{4}-\d{2}-\d{2}$/.test(next) ? next : fallback
}

function parseContractsPageFilters(searchParams: URLSearchParams): ContractsPageInitialFilters {
  const organizationKeys = Array.from(
    new Set(
      searchParams
        .getAll('organizationKey')
        .flatMap((value) => value.split(','))
        .map((value) => clean(value))
        .filter(Boolean),
    ),
  )

  return {
    q: clean(searchParams.get('q')),
    procedure: clean(searchParams.get('procedure')),
    dateFrom: parseIsoDateParam(searchParams.get('dateFrom'), isoDateDaysAgo(30)),
    dateTo: parseIsoDateParam(searchParams.get('dateTo'), isoToday()),
    minAmount: clean(searchParams.get('minAmount')),
    organizationKeys,
    regionKey: clean(searchParams.get('regionKey')),
    municipalityKey: clean(searchParams.get('municipalityKey')),
  }
}

function chunk<T>(items: T[], size: number): T[][] {
  const out: T[][] = []
  for (let index = 0; index < items.length; index += size) out.push(items.slice(index, index + size))
  return out
}

function normalizeSearchText(value: unknown): string {
  return clean(value)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/ς/g, 'σ')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase()
}

function matchesScopedQuery(row: ContractRow, q: string): boolean {
  const query = normalizeSearchText(q)
  if (!query) return true

  const haystack = normalizeSearchText([
    row.title,
    row.organization_value,
    row.beneficiary_name,
    row.cpv_value,
    row.reference_number,
  ].join(' '))

  return haystack.includes(query)
}

function dedupeContractRows(rows: ContractRow[]): ContractRow[] {
  return Array.from(
    new Map(
      rows.map((row) => {
        const key =
          clean(row.diavgeia_ada) ||
          `${clean(row.organization_value)}|${clean(row.title)}|${clean(row.contract_signed_date)}|${String(row.amount_without_vat ?? '')}`
        return [key, row] as const
      }),
    ).values(),
  )
}

async function loadOrganizationScopedRows(organizationKeys: string[], dateFrom: string, dateTo: string): Promise<ContractRow[]> {
  if (!organizationKeys.length) return []

  const pageSize = 1000
  const procurements: Array<{
    id: number
    organization_key: string | null
    contract_signed_date: string | null
    title: string | null
    reference_number: string | null
    procedure_type_value: string | null
    contract_budget: number | null
    budget: number | null
    diavgeia_ada: string | null
  }> = []

  let from = 0
  while (true) {
    const to = from + pageSize - 1
    const { data, error } = await supabase
      .from('procurement')
      .select('id, organization_key, contract_signed_date, title, reference_number, procedure_type_value, contract_budget, budget, diavgeia_ada')
      .in('organization_key', organizationKeys)
      .not('contract_signed_date', 'is', null)
      .gte('contract_signed_date', dateFrom)
      .lte('contract_signed_date', dateTo)
      .order('contract_signed_date', { ascending: false })
      .order('id', { ascending: false })
      .range(from, to)

    if (error) throw error

    const rows = (data ?? []) as typeof procurements
    procurements.push(...rows)
    if (rows.length < pageSize) break
    from += pageSize
  }

  const organizationByKey = new Map<string, string>()
  const { data: orgRows } = await supabase
    .from('organization')
    .select('organization_key, organization_normalized_value, organization_value')
    .in('organization_key', organizationKeys)

  for (const row of ((orgRows ?? []) as Array<{
    organization_key: string
    organization_normalized_value: string | null
    organization_value: string | null
  }>)) {
    organizationByKey.set(row.organization_key, clean(row.organization_normalized_value) || clean(row.organization_value) || row.organization_key)
  }

  const procurementIds = procurements.map((row) => row.id)
  const paymentsByProcurementId = new Map<number, { amount_without_vat: number | null; beneficiary_name: string | null }>()
  const cpvByProcurementId = new Map<number, string>()

  for (const ids of chunk(procurementIds, 200)) {
    const [{ data: paymentRows }, { data: cpvRows }] = await Promise.all([
      supabase
        .from('payment')
        .select('procurement_id, beneficiary_name, amount_without_vat')
        .in('procurement_id', ids),
      supabase
        .from('cpv')
        .select('procurement_id, cpv_value')
        .in('procurement_id', ids),
    ])

    const paymentMap = new Map<number, Array<{ beneficiary_name: string | null; amount_without_vat: number | null }>>()
    for (const row of ((paymentRows ?? []) as Array<{
      procurement_id: number
      beneficiary_name: string | null
      amount_without_vat: number | null
    }>)) {
      if (!paymentMap.has(row.procurement_id)) paymentMap.set(row.procurement_id, [])
      paymentMap.get(row.procurement_id)!.push(row)
    }

    for (const [procurementId, rows] of paymentMap.entries()) {
      const amountWithoutVat = rows.reduce<number | null>((sum, row) => {
        if (row.amount_without_vat == null || Number.isNaN(Number(row.amount_without_vat))) return sum
        return (sum ?? 0) + Number(row.amount_without_vat)
      }, null)
      const beneficiaryNames = Array.from(new Set(rows.map((row) => clean(row.beneficiary_name)).filter(Boolean)))
      paymentsByProcurementId.set(procurementId, {
        amount_without_vat: amountWithoutVat,
        beneficiary_name: beneficiaryNames.join(' | ') || null,
      })
    }

    const cpvMap = new Map<number, string[]>()
    for (const row of ((cpvRows ?? []) as Array<{ procurement_id: number; cpv_value: string | null }>)) {
      const cpvValue = clean(row.cpv_value)
      if (!cpvValue) continue
      if (!cpvMap.has(row.procurement_id)) cpvMap.set(row.procurement_id, [])
      const values = cpvMap.get(row.procurement_id)!
      if (!values.includes(cpvValue)) values.push(cpvValue)
    }
    for (const [procurementId, values] of cpvMap.entries()) {
      cpvByProcurementId.set(procurementId, values.join(' | '))
    }
  }

  return dedupeContractRows(procurements.map((row) => {
    const payment = paymentsByProcurementId.get(row.id)
    return {
      id: row.id,
      contract_signed_date: row.contract_signed_date,
      organization_value: clean(row.organization_key) ? organizationByKey.get(clean(row.organization_key)!) ?? clean(row.organization_key) : null,
      title: row.title,
      reference_number: row.reference_number,
      cpv_value: cpvByProcurementId.get(row.id) ?? null,
      procedure_type_value: row.procedure_type_value,
      beneficiary_name: payment?.beneficiary_name ?? null,
      amount_without_vat: payment?.amount_without_vat ?? row.contract_budget ?? row.budget ?? null,
      diavgeia_ada: row.diavgeia_ada,
      total_count: 0,
    }
  }))
}

async function loadRegionScopedRows(regionKey: string, dateFrom: string, dateTo: string): Promise<ContractRow[]> {
  if (!regionKey) return []

  const currentYear = new Date().getFullYear()
  const startYear = Number(dateFrom.slice(0, 4))
  const endYear = Math.min(currentYear, Number(dateTo.slice(0, 4)))
  const years = Array.from({ length: Math.max(endYear - startYear + 1, 0) }, (_, index) => startYear + index)

  const results = await Promise.all(years.map(async (year) => {
    const { data, error } = await supabase.rpc('get_region_contracts', {
      p_region_key: regionKey,
      p_year: year,
      p_limit: null,
      p_offset: 0,
    })
    if (error) throw error
    return (data ?? []) as Array<{
      procurement_id: number
      contract_signed_date: string | null
      organization_value: string | null
      title: string | null
      procedure_type_value: string | null
      beneficiary_name: string | null
      amount_without_vat: number | null
      diavgeia_ada: string | null
      reference_number: string | null
    }>
  }))

  const allRows = results.flat()
    .filter((row) => {
      const signedAt = clean(row.contract_signed_date)
      return signedAt >= dateFrom && signedAt <= dateTo
    })

  const procurementIds = allRows.map((row) => row.procurement_id)
  const cpvByProcurementId = new Map<number, string>()

  for (const ids of chunk(procurementIds, 200)) {
    const { data: cpvRows } = await supabase
      .from('cpv')
      .select('procurement_id, cpv_value')
      .in('procurement_id', ids)

    const cpvMap = new Map<number, string[]>()
    for (const row of ((cpvRows ?? []) as Array<{ procurement_id: number; cpv_value: string | null }>)) {
      const cpvValue = clean(row.cpv_value)
      if (!cpvValue) continue
      if (!cpvMap.has(row.procurement_id)) cpvMap.set(row.procurement_id, [])
      const values = cpvMap.get(row.procurement_id)!
      if (!values.includes(cpvValue)) values.push(cpvValue)
    }
    for (const [procurementId, values] of cpvMap.entries()) {
      cpvByProcurementId.set(procurementId, values.join(' | '))
    }
  }

  return dedupeContractRows(allRows.map((row) => ({
    id: row.procurement_id,
    contract_signed_date: row.contract_signed_date,
    organization_value: row.organization_value,
    title: row.title,
    reference_number: row.reference_number,
    cpv_value: cpvByProcurementId.get(row.procurement_id) ?? null,
    procedure_type_value: row.procedure_type_value,
    beneficiary_name: row.beneficiary_name,
    amount_without_vat: row.amount_without_vat,
    diavgeia_ada: row.diavgeia_ada,
    total_count: 0,
  })))
}

async function loadMunicipalityScopedRows(municipalityKey: string, dateFrom: string, dateTo: string): Promise<ContractRow[]> {
  if (!municipalityKey) return []

  const pageSize = 1000
  const procurements: Array<{
    id: number
    organization_key: string | null
    contract_signed_date: string | null
    title: string | null
    reference_number: string | null
    procedure_type_value: string | null
    contract_budget: number | null
    budget: number | null
    diavgeia_ada: string | null
  }> = []

  let from = 0
  while (true) {
    const to = from + pageSize - 1
    const { data, error } = await supabase
      .from('procurement')
      .select('id, organization_key, contract_signed_date, title, reference_number, procedure_type_value, contract_budget, budget, diavgeia_ada')
      .eq('municipality_key', municipalityKey)
      .not('contract_signed_date', 'is', null)
      .gte('contract_signed_date', dateFrom)
      .lte('contract_signed_date', dateTo)
      .order('contract_signed_date', { ascending: false })
      .order('id', { ascending: false })
      .range(from, to)

    if (error) throw error

    const rows = (data ?? []) as typeof procurements
    procurements.push(...rows)
    if (rows.length < pageSize) break
    from += pageSize
  }

  const { data: municipalityRows } = await supabase
    .from('municipality')
    .select('municipality_normalized_value, municipality_value')
    .eq('municipality_key', municipalityKey)
    .limit(1)

  const municipalityLabel = ((municipalityRows ?? []) as Array<{
    municipality_normalized_value: string | null
    municipality_value: string | null
  }>).map((row) => clean(row.municipality_normalized_value) || clean(row.municipality_value))
    .find(Boolean) ?? null

  const organizationKeys = Array.from(new Set(
    procurements
      .map((row) => cleanText(row.organization_key))
      .filter(Boolean),
  )) as string[]

  const organizationByKey = new Map<string, string>()
  for (const keys of chunk(organizationKeys, 200)) {
    const { data: orgRows } = await supabase
      .from('organization')
      .select('organization_key, organization_normalized_value, organization_value')
      .in('organization_key', keys)

    for (const row of ((orgRows ?? []) as Array<{
      organization_key: string
      organization_normalized_value: string | null
      organization_value: string | null
    }>)) {
      organizationByKey.set(row.organization_key, clean(row.organization_normalized_value) || clean(row.organization_value) || row.organization_key)
    }
  }

  const procurementIds = procurements.map((row) => row.id)
  const paymentsByProcurementId = new Map<number, { amount_without_vat: number | null; beneficiary_name: string | null }>()
  const cpvByProcurementId = new Map<number, string>()

  for (const ids of chunk(procurementIds, 200)) {
    const [{ data: paymentRows }, { data: cpvRows }] = await Promise.all([
      supabase
        .from('payment')
        .select('procurement_id, beneficiary_name, amount_without_vat')
        .in('procurement_id', ids),
      supabase
        .from('cpv')
        .select('procurement_id, cpv_value')
        .in('procurement_id', ids),
    ])

    const paymentMap = new Map<number, Array<{ beneficiary_name: string | null; amount_without_vat: number | null }>>()
    for (const row of ((paymentRows ?? []) as Array<{
      procurement_id: number
      beneficiary_name: string | null
      amount_without_vat: number | null
    }>)) {
      if (!paymentMap.has(row.procurement_id)) paymentMap.set(row.procurement_id, [])
      paymentMap.get(row.procurement_id)!.push(row)
    }

    for (const [procurementId, rows] of paymentMap.entries()) {
      const amountWithoutVat = rows.reduce<number | null>((sum, row) => {
        if (row.amount_without_vat == null || Number.isNaN(Number(row.amount_without_vat))) return sum
        return (sum ?? 0) + Number(row.amount_without_vat)
      }, null)
      const beneficiaryNames = Array.from(new Set(rows.map((row) => clean(row.beneficiary_name)).filter(Boolean)))
      paymentsByProcurementId.set(procurementId, {
        amount_without_vat: amountWithoutVat,
        beneficiary_name: beneficiaryNames.join(' | ') || null,
      })
    }

    const cpvMap = new Map<number, string[]>()
    for (const row of ((cpvRows ?? []) as Array<{ procurement_id: number; cpv_value: string | null }>)) {
      const cpvValue = clean(row.cpv_value)
      if (!cpvValue) continue
      if (!cpvMap.has(row.procurement_id)) cpvMap.set(row.procurement_id, [])
      const values = cpvMap.get(row.procurement_id)!
      if (!values.includes(cpvValue)) values.push(cpvValue)
    }
    for (const [procurementId, values] of cpvMap.entries()) {
      cpvByProcurementId.set(procurementId, values.join(' | '))
    }
  }

  return dedupeContractRows(procurements.map((row) => {
    const payment = paymentsByProcurementId.get(row.id)
    return {
      id: row.id,
      contract_signed_date: row.contract_signed_date,
      organization_value: clean(row.organization_key)
        ? organizationByKey.get(clean(row.organization_key)!) ?? clean(row.organization_key)
        : municipalityLabel,
      title: row.title,
      reference_number: row.reference_number,
      cpv_value: cpvByProcurementId.get(row.id) ?? null,
      procedure_type_value: row.procedure_type_value,
      beneficiary_name: payment?.beneficiary_name ?? null,
      amount_without_vat: payment?.amount_without_vat ?? row.contract_budget ?? row.budget ?? null,
      diavgeia_ada: row.diavgeia_ada,
      total_count: 0,
    }
  }))
}

export default function ContractsPage() {
  const [searchParams] = useSearchParams()
  const searchParamsKey = searchParams.toString()
  const initialFilters = useMemo(() => parseContractsPageFilters(searchParams), [searchParamsKey])
  const hasScopedSource =
    initialFilters.organizationKeys.length > 0 ||
    Boolean(initialFilters.regionKey) ||
    Boolean(initialFilters.municipalityKey)
  const [rows, setRows] = useState<ContractRow[]>([])
  const [totalCount, setTotalCount] = useState(0)
  const [loading, setLoading] = useState(true)
  const [selectedContract, setSelectedContract] = useState<ContractModalContract | null>(null)
  const [openingContractId, setOpeningContractId] = useState<number | null>(null)
  const [q, setQ] = useState(initialFilters.q)
  const [procedure, setProcedure] = useState(initialFilters.procedure)
  const [procedureOptions, setProcedureOptions] = useState<string[]>([])
  const [municipalityNameTokens, setMunicipalityNameTokens] = useState<Set<string>>(new Set())
  const [dateFrom, setDateFrom] = useState(initialFilters.dateFrom)
  const [dateTo, setDateTo] = useState(initialFilters.dateTo)
  const [minAmount, setMinAmount] = useState(initialFilters.minAmount)
  const [organizationKeys, setOrganizationKeys] = useState<string[]>(initialFilters.organizationKeys)
  const [regionKey, setRegionKey] = useState(initialFilters.regionKey)
  const [municipalityKey, setMunicipalityKey] = useState(initialFilters.municipalityKey)
  const [page, setPage] = useState(1)
  const pageSize = 50

  useEffect(() => {
    setQ(initialFilters.q)
    setProcedure(initialFilters.procedure)
    setDateFrom(initialFilters.dateFrom)
    setDateTo(initialFilters.dateTo)
    setMinAmount(initialFilters.minAmount)
    setOrganizationKeys(initialFilters.organizationKeys)
    setRegionKey(initialFilters.regionKey)
    setMunicipalityKey(initialFilters.municipalityKey)
    setPage(1)
    setSelectedContract(null)
    setOpeningContractId(null)
  }, [initialFilters])

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
      try {
        if (hasScopedSource) {
          const scopedBaseRows = organizationKeys.length
            ? await loadOrganizationScopedRows(organizationKeys, dateFrom, dateTo)
            : regionKey
              ? await loadRegionScopedRows(regionKey, dateFrom, dateTo)
              : await loadMunicipalityScopedRows(municipalityKey, dateFrom, dateTo)

          if (cancelled) return

          const min = minAmount ? Number(minAmount) : null
          const filteredRows = scopedBaseRows
            .filter((row) => matchesScopedQuery(row, q))
            .filter((row) => !procedure || clean(row.procedure_type_value) === procedure)
            .filter((row) => {
              if (min == null || !Number.isFinite(min)) return true
              return Number(row.amount_without_vat ?? 0) >= min
            })
            .sort((a, b) => {
              const byDate = clean(b.contract_signed_date).localeCompare(clean(a.contract_signed_date))
              if (byDate !== 0) return byDate
              return b.id - a.id
            })

          const total = filteredRows.length
          const pageStart = Math.max((page - 1) * pageSize, 0)
          const pageRows = filteredRows.slice(pageStart, pageStart + pageSize)
          setRows(pageRows)
          setTotalCount(total)
          setLoading(false)
          return
        }

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
        const deduped = dedupeContractRows(next)
        setRows(deduped)
        setTotalCount(next[0]?.total_count ?? 0)
        setLoading(false)
      } catch {
        if (cancelled) return
        setRows([])
        setTotalCount(0)
        setLoading(false)
      }
    }
    loadPage()
    return () => { cancelled = true }
  }, [q, procedure, dateFrom, dateTo, minAmount, page, organizationKeys, regionKey, municipalityKey, hasScopedSource])

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
          municipality_key,
          region_key,
          canonical_owner_scope,
          organization_vat_number,
          organization_key,
          assign_criteria,
          contract_type,
          award_procedure,
          units_operator,
          funding_details_cofund,
          funding_details_self_fund,
          funding_details_espa,
          funding_details_regular_budget,
          auction_ref_no,
          contract_related_ada,
          prev_reference_no,
          next_ref_no,
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
        municipalityResult,
        regionResult,
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
          .select('organization_normalized_value, organization_value, authority_scope')
          .eq('organization_key', procurement.organization_key)
          .limit(1),
        procurement.municipality_key
          ? supabase
            .from('municipality')
            .select('municipality_normalized_value, municipality_value')
            .eq('municipality_key', procurement.municipality_key)
            .limit(1)
          : Promise.resolve({ data: [] }),
        procurement.region_key
          ? supabase
            .from('region')
            .select('region_normalized_value, region_value')
            .eq('region_key', procurement.region_key)
            .limit(1)
          : Promise.resolve({ data: [] }),
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
        authority_scope: ContractAuthorityScope | null
      }>)[0] ?? null
      const municipality = ((municipalityResult.data ?? []) as Array<{
        municipality_normalized_value: string | null
        municipality_value: string | null
      }>)[0] ?? null
      const region = ((regionResult.data ?? []) as Array<{
        region_normalized_value: string | null
        region_value: string | null
      }>)[0] ?? null
      const beneficiary = paymentPrimary?.beneficiary_name ?? row.beneficiary_name
      const whyText = firstPipePart(procurement.short_descriptions) ?? primaryCpv?.label ?? clean(row.cpv_value) ?? '—'
      const cpvLabel = primaryCpv?.label ?? clean(row.cpv_value) ?? '—'
      const contractRelatedAda = clean(procurement.contract_related_ada)
      const diavgeiaAda = clean(procurement.diavgeia_ada)
      const organizationName = clean(organization?.organization_normalized_value) || clean(organization?.organization_value) || clean(row.organization_value) || '—'
      const who = buildContractAuthorityLabel({
        canonicalOwnerScope: clean(procurement.canonical_owner_scope),
        organizationScope: clean(organization?.authority_scope),
        organizationName,
        municipalityLabel: clean(municipality?.municipality_normalized_value) || clean(municipality?.municipality_value),
        regionLabel: clean(region?.region_normalized_value) || clean(region?.region_value),
      })
      const modal: ContractModalContract = {
        id: String(procurement.id),
        who,
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
        awardProcedure: clean(procurement.award_procedure) || '—',
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
        contractRelatedAda: contractRelatedAda || '—',
        previousReferenceNumber: clean(procurement.prev_reference_no) || '—',
        nextReferenceNumber: clean(procurement.next_ref_no) || '—',
        documentUrl: buildDiavgeiaDocumentUrl(contractRelatedAda, diavgeiaAda),
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
