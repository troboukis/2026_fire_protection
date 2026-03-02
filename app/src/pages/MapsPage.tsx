import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import ComponentTag from '../components/ComponentTag'
import ContractModal, { type ContractModalContract } from '../components/ContractModal'
import { GreeceMap } from '../components/GreeceMap'
import MapSelectionPanel, { type SelectionKind, type SelectionSource } from '../components/MapSelectionPanel'
import type { LatestContractCardView } from '../components/LatestContractCard'
import { supabase } from '../lib/supabase'
import type { GeoData, GeoFeature } from '../types'

type ProcurementMapRow = {
  id: number
  municipality_key: string | null
  contract_signed_date?: string | null
  organization_key?: string | null
}

type MunicipalityRow = {
  municipality_key: string | null
  municipality_normalized_value: string | null
  municipality_value: string | null
}

type SearchKind = 'municipality' | 'region'
type SearchOption = {
  kind: SearchKind
  value: string
  label: string
}

type MunicipalityRegionRow = {
  municipality_id: string
  municipality_name?: string
  region_id: string
}

type MunicipalityLatestContract = LatestContractCardView

const PAGE_SIZE = 1000

function isLocalOrRegionalAuthority(orgName: string): boolean {
  const n = orgName.toUpperCase()
  if (!n) return false
  if (n.startsWith('ΔΗΜΟΣ')) return true
  if (n.startsWith('ΠΕΡΙΦΕΡΕΙΑ')) return true
  if (n.includes('ΑΠΟΚΕΝΤΡΩΜΕΝΗ')) return true
  if (n.includes('ΔΗΜΟΤΙΚΗ')) return true
  if (n.includes('ΠΕΡΙΦΕΡΕΙΑΚ')) return true
  return false
}

function extractYear(value: string): number | null {
  const v = value.trim()
  if (!v) return null
  const isoMatch = v.match(/^(\d{4})-/)
  if (isoMatch) return Number(isoMatch[1])
  const parsed = new Date(v).getFullYear()
  return Number.isFinite(parsed) ? parsed : null
}

function stripAccents(input: string): string {
  return input
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
}

function greeklishToGreek(input: string): string {
  let s = input.toLowerCase()
  const digraphs: Array<[string, string]> = [
    ['th', 'θ'],
    ['ch', 'χ'],
    ['ps', 'ψ'],
    ['ks', 'ξ'],
    ['ou', 'ου'],
    ['ai', 'αι'],
    ['ei', 'ει'],
    ['oi', 'οι'],
    ['mp', 'μπ'],
    ['nt', 'ντ'],
    ['gk', 'γκ'],
    ['gg', 'γγ'],
    ['tz', 'τζ'],
    ['ts', 'τσ'],
  ]
  for (const [from, to] of digraphs) s = s.replace(new RegExp(from, 'g'), to)

  const chars: Record<string, string> = {
    a: 'α', b: 'β', c: 'κ', d: 'δ', e: 'ε', f: 'φ', g: 'γ', h: 'η', i: 'ι', j: 'τζ',
    k: 'κ', l: 'λ', m: 'μ', n: 'ν', o: 'ο', p: 'π', q: 'κ', r: 'ρ', s: 'σ', t: 'τ',
    u: 'υ', v: 'β', w: 'ω', x: 'ξ', y: 'υ', z: 'ζ',
  }
  return s.split('').map((ch) => chars[ch] ?? ch).join('')
}

function normalizeSearch(input: string): string {
  return stripAccents(input)
    .toLowerCase()
    .replace(/ς/g, 'σ')
    .replace(/[^a-zα-ω0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
}

function normalizeMunicipalityId(input: unknown): string {
  const s = String(input ?? '').trim()
  if (!s) return ''
  return s.replace(/\.0+$/, '')
}

const REGIONS = [
  'ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ ΚΑΙ ΘΡΑΚΗΣ',
  'ΑΤΤΙΚΗΣ',
  'ΒΟΡΕΙΟΥ ΑΙΓΑΙΟΥ',
  'ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ',
  'ΔΥΤΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ',
  'ΗΠΕΙΡΟΥ',
  'ΘΕΣΣΑΛΙΑΣ',
  'ΙΟΝΙΩΝ ΝΗΣΩΝ',
  'ΚΕΝΤΡΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ',
  'ΚΡΗΤΗΣ',
  'ΝΟΤΙΟΥ ΑΙΓΑΙΟΥ',
  'ΠΕΛΟΠΟΝΝΗΣΟΥ',
  'ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ',
] as const

export default function MapsPage() {
  const mapYear = 2026
  const [geojson, setGeojson] = useState<GeoData | null>(null)
  const [choroplethData, setChoroplethData] = useState<Record<string, number>>({})
  const [procMunicipalities, setProcMunicipalities] = useState<Set<string>>(new Set())
  const [municipalityRegionById, setMunicipalityRegionById] = useState<Map<string, string>>(new Map())
  const [municipalityCurrentYearCountById, setMunicipalityCurrentYearCountById] = useState<Map<string, number>>(new Map())
  const [municipalityOptions, setMunicipalityOptions] = useState<Array<{ id: string; label: string }>>([])
  const [loading, setLoading] = useState(true)
  const [selectedRegion, setSelectedRegion] = useState('')
  const [selectedMunicipalityDropdown, setSelectedMunicipalityDropdown] = useState('')
  const [selectedMunicipalityIdsForMap, setSelectedMunicipalityIdsForMap] = useState<Set<string>>(new Set())
  const [selectedMunicipalityIdForPanel, setSelectedMunicipalityIdForPanel] = useState<string | null>(null)
  const [searchText, setSearchText] = useState('')
  const [panelSource, setPanelSource] = useState<SelectionSource | null>(null)
  const [panelKind, setPanelKind] = useState<SelectionKind | null>(null)
  const [panelLabel, setPanelLabel] = useState('')
  const [selectedContract, setSelectedContract] = useState<ContractModalContract | null>(null)
  const [municipalityLatestContracts, setMunicipalityLatestContracts] = useState<MunicipalityLatestContract[]>([])
  const [municipalityLatestLoading, setMunicipalityLatestLoading] = useState(false)
  const [regionLatestContracts, setRegionLatestContracts] = useState<MunicipalityLatestContract[]>([])
  const [regionLatestLoading, setRegionLatestLoading] = useState(false)

  useEffect(() => {
    let cancelled = false
    setLoading(true)

    const fetchAllProcurementRows = async (): Promise<ProcurementMapRow[]> => {
      const out: ProcurementMapRow[] = []
      let from = 0

      while (true) {
        const to = from + PAGE_SIZE - 1
        const { data, error } = await supabase
          .from('procurement')
          .select('id, municipality_key, contract_signed_date, organization_key')
          .not('municipality_key', 'is', null)
          .order('id', { ascending: true })
          .range(from, to)

        if (error) throw error

        const page = (data ?? []) as ProcurementMapRow[]
        out.push(...page)

        if (page.length < PAGE_SIZE) break
        from += PAGE_SIZE
      }

      return out
    }

    const load = async () => {
      try {
        const [geoRes, rows] = await Promise.all([
          fetch('/municipalities.geojson'),
          fetchAllProcurementRows(),
        ])

        if (cancelled) return

        const geoData = (await geoRes.json()) as GeoData
        if (!cancelled) setGeojson(geoData)
        const geoNameById = new Map<string, string>()
        for (const f of geoData.features) {
          const id = normalizeMunicipalityId((f.properties as { municipality_code?: string | null }).municipality_code)
          const name = String((f.properties as { name?: string | null }).name ?? '').trim()
          if (id && name && !geoNameById.has(id)) geoNameById.set(id, name)
        }

        const countByMunicipality = new Map<string, number>()
        let procurementRowsForYear = 0
        const orgKeys = Array.from(new Set(
          rows.map((r) => String(r.organization_key ?? '').trim()).filter(Boolean),
        ))
        const orgNameByKey = new Map<string, string>()
        for (let i = 0; i < orgKeys.length; i += PAGE_SIZE) {
          const chunk = orgKeys.slice(i, i + PAGE_SIZE)
          const { data: orgRows, error: orgError } = await supabase
            .from('organization')
            .select('organization_key, organization_normalized_value, organization_value')
            .in('organization_key', chunk)
          if (orgError) throw orgError
          for (const row of (orgRows ?? []) as Array<{ organization_key: string; organization_normalized_value: string | null; organization_value: string | null }>) {
            orgNameByKey.set(
              row.organization_key,
              String(row.organization_normalized_value ?? row.organization_value ?? row.organization_key).trim(),
            )
          }
        }

        for (const row of rows) {
          const municipalityId = normalizeMunicipalityId(row.municipality_key)
          if (!municipalityId) continue

          const issueDate = String(row.contract_signed_date ?? '').trim()
          const issueYear = extractYear(issueDate)
          if (issueYear !== mapYear) continue
          const orgKey = String(row.organization_key ?? '').trim()
          const orgName = orgNameByKey.get(orgKey) ?? orgKey
          if (!isLocalOrRegionalAuthority(orgName)) continue
          procurementRowsForYear += 1
          countByMunicipality.set(municipalityId, (countByMunicipality.get(municipalityId) ?? 0) + 1)
        }

        const nationalTotalCount = Array.from(countByMunicipality.values()).reduce((s, n) => s + n, 0)
        const nextChoropleth: Record<string, number> = {}
        for (const [municipalityId, count] of countByMunicipality.entries()) {
          nextChoropleth[municipalityId] = nationalTotalCount > 0 ? (count / nationalTotalCount) * 100 : 0
        }

        if (!cancelled) {
          setChoroplethData(nextChoropleth)
          setProcMunicipalities(new Set(countByMunicipality.keys()))
          console.log('[MapsPage] choropleth summary', {
            procurementRows: rows.length,
            procurementRowsForYear,
            mapYear,
            municipalitiesWithProcurements: countByMunicipality.size,
            nationalTotalProcurements: nationalTotalCount,
          })

          setMunicipalityCurrentYearCountById(new Map(countByMunicipality.entries()))
        }

        const regionRes = await fetch('/municipality_regions.json')
        const fullRegionRows = (await regionRes.json()) as MunicipalityRegionRow[]
        const fullRegionByMunicipality = new Map<string, string>()
        for (const row of fullRegionRows) {
          const municipalityId = normalizeMunicipalityId(row.municipality_id)
          const regionId = String(row.region_id ?? '').trim()
          if (!municipalityId || !regionId) continue
          fullRegionByMunicipality.set(municipalityId, regionId)
        }

        if (!cancelled) {
          setMunicipalityRegionById(fullRegionByMunicipality)
        }

        const { data: municipalitiesData } = await supabase
          .from('municipality')
          .select('municipality_key, municipality_normalized_value, municipality_value')
          .order('municipality_normalized_value', { ascending: true })

        if (!cancelled) {
          const fromDb = ((municipalitiesData ?? []) as MunicipalityRow[])
            .map((r) => {
              const id = normalizeMunicipalityId(r.municipality_key)
              const dbLabel = String(r.municipality_normalized_value ?? r.municipality_value ?? '').trim()
              const label = geoNameById.get(id) ?? dbLabel
              return { id, label }
            })
            .filter((m) => m.id && m.label)
            .reduce<Array<{ id: string; label: string }>>((acc, m) => {
              if (!acc.find((x) => x.id === m.id)) acc.push(m)
              return acc
            }, [])
            .sort((a, b) => a.label.localeCompare(b.label, 'el'))

          if (fromDb.length > 0) {
            setMunicipalityOptions(fromDb)
          }
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    load()
    return () => { cancelled = true }
  }, [mapYear])

  const activeMunicipalityCount = useMemo(
    () => procMunicipalities.size,
    [procMunicipalities],
  )

  const municipalities = useMemo(() => {
    if (municipalityOptions.length > 0) return municipalityOptions
    if (!geojson) return [] as Array<{ id: string; label: string }>
    const seen = new Set<string>()
    const out: Array<{ id: string; label: string }> = []
    for (const f of geojson.features) {
      const id = String((f.properties as { municipality_code?: string | null }).municipality_code ?? '').trim()
      const label = String((f.properties as { name?: string | null }).name ?? '').trim()
      if (!id || !label || seen.has(id)) continue
      seen.add(id)
      out.push({ id, label })
    }
    return out.sort((a, b) => a.label.localeCompare(b.label, 'el'))
  }, [geojson])

  const municipalityLabelById = useMemo(() => {
    return new Map(municipalities.map((m) => [m.id, m.label]))
  }, [municipalities])

  const municipalityFeatureById = useMemo(() => {
    const out = new Map<string, GeoFeature>()
    if (!geojson) return out
    for (const f of geojson.features) {
      const id = String((f.properties as { municipality_code?: string | null }).municipality_code ?? '').trim()
      if (!id) continue
      out.set(id, f)
    }
    return out
  }, [geojson])

  const regionToMunicipalityIds = useMemo(() => {
    const out = new Map<string, string[]>()
    for (const [municipalityId, regionId] of municipalityRegionById.entries()) {
      if (!regionId) continue
      const prev = out.get(regionId) ?? []
      prev.push(municipalityId)
      out.set(regionId, prev)
    }
    return out
  }, [municipalityRegionById])

  const selectedRegionCurrentYearCount = useMemo(() => {
    if (!selectedRegion) return 0
    const ids = regionToMunicipalityIds.get(selectedRegion) ?? []
    let total = 0
    for (const municipalityId of ids) {
      total += municipalityCurrentYearCountById.get(municipalityId) ?? 0
    }
    return total
  }, [selectedRegion, regionToMunicipalityIds, municipalityCurrentYearCountById])

  const searchOptions = useMemo(() => {
    const municipalityOpts: SearchOption[] = municipalities.map((m) => ({
      kind: 'municipality',
      value: m.id,
      label: m.label,
    }))
    const regionOpts: SearchOption[] = REGIONS.map((r) => ({
      kind: 'region',
      value: r,
      label: r,
    }))
    return [...municipalityOpts, ...regionOpts]
  }, [municipalities])

  const hiddenProcDots = useMemo(() => new Set<string>(), [])

  const searchResults = useMemo(() => {
    const raw = searchText.trim()
    if (!raw) return [] as SearchOption[]
    const queryNorm = normalizeSearch(raw)
    const greeklishNorm = normalizeSearch(greeklishToGreek(raw))
    const q = new Set([queryNorm, greeklishNorm].filter(Boolean))

    const scored = searchOptions
      .map((o) => {
        const labelNorm = normalizeSearch(o.label)
        let best = 0
        for (const token of q) {
          if (!token) continue
          if (labelNorm === token) best = Math.max(best, 3)
          else if (labelNorm.startsWith(token)) best = Math.max(best, 2)
          else if (labelNorm.includes(token)) best = Math.max(best, 1)
        }
        return { option: o, score: best }
      })
      .filter((x) => x.score > 0)
      .sort((a, b) => b.score - a.score || a.option.label.localeCompare(b.option.label, 'el'))
      .slice(0, 8)

    return scored.map((x) => x.option)
  }, [searchOptions, searchText])

  const openPanel = (source: SelectionSource, kind: SelectionKind, label: string) => {
    setPanelSource(source)
    setPanelKind(kind)
    setPanelLabel(label)
  }

  const formatDateEl = (iso: string | null): string => {
    if (!iso) return '—'
    const dt = new Date(iso)
    if (Number.isNaN(dt.getTime())) return '—'
    return new Intl.DateTimeFormat('el-GR', { day: '2-digit', month: 'short', year: 'numeric' }).format(dt)
  }

  const cleanText = (v: unknown): string | null => {
    if (v == null) return null
    const s = String(v).trim()
    if (!s || s.toLowerCase() === 'nan' || s.toLowerCase() === 'none') return null
    return s
  }

  const firstPipePart = (v: unknown): string | null => {
    const s = cleanText(v)
    if (!s) return null
    return s.split('|').map(x => x.trim()).filter(Boolean)[0] ?? null
  }

  const openContractModal = async (id: string) => {
    const contractId = Number(id)
    if (!Number.isFinite(contractId)) return

    const { data: proc } = await supabase
      .from('procurement')
      .select(`
        id,
        organization_key,
        title,
        submission_at,
        contract_signed_date,
        short_descriptions,
        procedure_type_value,
        reference_number,
        contract_number,
        contract_budget,
        budget,
        assign_criteria,
        contract_type,
        units_operator,
        funding_details_cofund,
        funding_details_self_fund,
        funding_details_espa,
        funding_details_regular_budget,
        auction_ref_no,
        organization_vat_number,
        start_date,
        end_date,
        diavgeia_ada
      `)
      .eq('id', contractId)
      .limit(1)
      .maybeSingle()
    if (!proc) return

    const [{ data: pRows }, { data: cpvRows }, { data: oRows }] = await Promise.all([
      supabase
        .from('payment')
        .select('beneficiary_name, beneficiary_vat_number, signers, payment_ref_no, amount_without_vat, amount_with_vat')
        .eq('procurement_id', contractId)
        .limit(1),
      supabase
        .from('cpv')
        .select('cpv_key, cpv_value')
        .eq('procurement_id', contractId),
      supabase
        .from('organization')
        .select('organization_key, organization_normalized_value, organization_value')
        .eq('organization_key', String(proc.organization_key ?? ''))
        .limit(1),
    ])

    const p = (pRows?.[0] ?? null) as {
      beneficiary_name: string | null
      beneficiary_vat_number: string | null
      signers: string | null
      payment_ref_no: string | null
      amount_without_vat: number | null
      amount_with_vat: number | null
    } | null
    const c = (cpvRows?.[0] ?? null) as { cpv_key: string | null; cpv_value: string | null } | null
    const org = (oRows?.[0] ?? null) as {
      organization_key: string
      organization_normalized_value: string | null
      organization_value: string | null
    } | null

    const amountWithoutVat = p?.amount_without_vat ?? null
    const diavgeiaAda = cleanText(proc.diavgeia_ada)
    const who = cleanText(org?.organization_normalized_value) ?? cleanText(org?.organization_value) ?? cleanText(proc.organization_key) ?? '—'
    const modal: ContractModalContract = {
      id: String(proc.id),
      who,
      what: cleanText(proc.title) ?? '—',
      when: formatDateEl(cleanText(proc.submission_at)),
      why: firstPipePart(proc.short_descriptions) ?? cleanText(c?.cpv_value) ?? '—',
      beneficiary: cleanText(p?.beneficiary_name) ?? '—',
      contractType: cleanText(proc.procedure_type_value) ?? '—',
      howMuch: (amountWithoutVat == null ? '—' : amountWithoutVat.toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 })),
      withoutVatAmount: (amountWithoutVat == null ? '—' : amountWithoutVat.toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 })),
      withVatAmount: (p?.amount_with_vat == null ? '—' : Number(p.amount_with_vat).toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 })),
      referenceNumber: cleanText(proc.reference_number) ?? '—',
      contractNumber: cleanText(proc.contract_number) ?? '—',
      cpv: cleanText(c?.cpv_value) ?? '—',
      cpvCode: cleanText(c?.cpv_key) ?? '—',
      signedAt: formatDateEl(cleanText(proc.contract_signed_date)),
      startDate: formatDateEl(cleanText(proc.start_date)),
      endDate: formatDateEl(cleanText(proc.end_date)),
      organizationVat: cleanText(proc.organization_vat_number) ?? '—',
      beneficiaryVat: cleanText(p?.beneficiary_vat_number) ?? '—',
      signers: cleanText(p?.signers) ?? '—',
      assignCriteria: cleanText(proc.assign_criteria) ?? '—',
      contractKind: cleanText(proc.contract_type) ?? '—',
      unitsOperator: cleanText(proc.units_operator) ?? '—',
      fundingCofund: cleanText(proc.funding_details_cofund) ?? '—',
      fundingSelf: cleanText(proc.funding_details_self_fund) ?? '—',
      fundingEspa: cleanText(proc.funding_details_espa) ?? '—',
      fundingRegular: cleanText(proc.funding_details_regular_budget) ?? '—',
      auctionRefNo: cleanText(proc.auction_ref_no) ?? '—',
      paymentRefNo: cleanText(p?.payment_ref_no) ?? '—',
      shortDescription: firstPipePart(proc.short_descriptions) ?? '—',
      rawBudget: (proc.budget == null ? '—' : Number(proc.budget).toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 })),
      contractBudget: (proc.contract_budget == null ? '—' : Number(proc.contract_budget).toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 })),
      documentUrl: diavgeiaAda ? `https://diavgeia.gov.gr/doc/${diavgeiaAda}` : null,
    }

    setSelectedContract(modal)
  }

  useEffect(() => {
    let cancelled = false
    const selectedMunicipalityId = selectedMunicipalityIdForPanel
    if (!selectedMunicipalityId || panelKind !== 'municipality') {
      setMunicipalityLatestContracts([])
      setMunicipalityLatestLoading(false)
      return
    }

    const fmtDate = (value: string | null | undefined): string => {
      const s = String(value ?? '').trim()
      if (!s) return '—'
      const dt = new Date(s)
      if (Number.isNaN(dt.getTime())) return '—'
      return dt.toLocaleDateString('el-GR', { day: '2-digit', month: 'short', year: 'numeric' })
    }

    const fmtEur = (value: number | string | null | undefined): string => {
      const n = Number(value ?? NaN)
      if (!Number.isFinite(n)) return '—'
      return n.toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 })
    }

    const loadMunicipalityContracts = async () => {
      setMunicipalityLatestLoading(true)
      try {
        const { data, error } = await supabase
          .from('procurement')
          .select('id, contract_signed_date, diavgeia_ada, title, procedure_type_value, contract_budget, budget, organization_key')
          .or(`municipality_key.eq.${selectedMunicipalityId},municipality_key.eq.${selectedMunicipalityId}.0`)
          .gte('contract_signed_date', `${mapYear}-01-01`)
          .lte('contract_signed_date', `${mapYear}-12-31`)
          .order('contract_signed_date', { ascending: false, nullsFirst: false })
          .limit(200)

        if (error) throw error
        const rows = (data ?? []) as Array<{
          id: number
          contract_signed_date: string | null
          diavgeia_ada: string | null
          title: string | null
          procedure_type_value: string | null
          contract_budget: number | string | null
          budget: number | string | null
          organization_key: string | null
        }>

        const paymentByProcId = new Map<number, { beneficiary: string | null; amountWithoutVat: number | null }>()
        const procurementIds = rows.map((r) => r.id)
        if (procurementIds.length > 0) {
          const { data: paymentRows } = await supabase
            .from('payment')
            .select('procurement_id, beneficiary_name, amount_without_vat')
            .in('procurement_id', procurementIds)
          for (const row of (paymentRows ?? []) as Array<{ procurement_id: number; beneficiary_name: string | null; amount_without_vat: number | null }>) {
            if (!paymentByProcId.has(row.procurement_id)) {
              paymentByProcId.set(row.procurement_id, {
                beneficiary: String(row.beneficiary_name ?? '').trim() || null,
                amountWithoutVat: row.amount_without_vat != null ? Number(row.amount_without_vat) : null,
              })
            }
          }
        }

        const orgKeys = Array.from(new Set(rows.map((r) => String(r.organization_key ?? '').trim()).filter(Boolean)))
        const orgNameByKey = new Map<string, string>()
        if (orgKeys.length > 0) {
          const { data: orgRows } = await supabase
            .from('organization')
            .select('organization_key, organization_normalized_value, organization_value')
            .in('organization_key', orgKeys)
          for (const row of (orgRows ?? []) as Array<{ organization_key: string; organization_normalized_value: string | null; organization_value: string | null }>) {
            orgNameByKey.set(
              row.organization_key,
              String(row.organization_normalized_value ?? row.organization_value ?? row.organization_key).trim(),
            )
          }
        }

        const mapped: MunicipalityLatestContract[] = rows
          .map((r) => ({
            id: String(r.id),
            who: orgNameByKey.get(String(r.organization_key ?? '').trim()) || String(r.organization_key ?? '—').trim() || '—',
            when: fmtDate(r.contract_signed_date),
            what: String(r.title ?? '').trim() || '—',
            why: `Διαδικασία: ${String(r.procedure_type_value ?? '—').trim() || '—'}`,
            howMuch: fmtEur(paymentByProcId.get(r.id)?.amountWithoutVat ?? null),
            beneficiary: paymentByProcId.get(r.id)?.beneficiary ?? '—',
            contractType: String(r.procedure_type_value ?? '—').trim() || '—',
            documentUrl: String(r.diavgeia_ada ?? '').trim() ? `https://diavgeia.gov.gr/doc/${String(r.diavgeia_ada).trim()}` : null,
          }))
          .filter((r) => isLocalOrRegionalAuthority(r.who))
          .slice(0, 8)

        if (!cancelled) setMunicipalityLatestContracts(mapped)
      } catch (e) {
        if (!cancelled) {
          console.error('[MapsPage] municipality latest contracts failed', e)
          setMunicipalityLatestContracts([])
        }
      } finally {
        if (!cancelled) setMunicipalityLatestLoading(false)
      }
    }

    loadMunicipalityContracts()
    return () => {
      cancelled = true
    }
  }, [selectedMunicipalityIdForPanel, panelKind, mapYear])

  useEffect(() => {
    let cancelled = false
    if (panelKind !== 'region' || !selectedRegion) {
      setRegionLatestContracts([])
      setRegionLatestLoading(false)
      return
    }

    const fmtDate = (value: string | null | undefined): string => {
      const s = String(value ?? '').trim()
      if (!s) return '—'
      const dt = new Date(s)
      if (Number.isNaN(dt.getTime())) return '—'
      return dt.toLocaleDateString('el-GR', { day: '2-digit', month: 'short', year: 'numeric' })
    }

    const fmtEur = (value: number | string | null | undefined): string => {
      const n = Number(value ?? NaN)
      if (!Number.isFinite(n)) return '—'
      return n.toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 })
    }

    const loadRegionContracts = async () => {
      setRegionLatestLoading(true)
      try {
        const regionMunicipalityIds = regionToMunicipalityIds.get(selectedRegion) ?? []
        if (regionMunicipalityIds.length === 0) {
          if (!cancelled) setRegionLatestContracts([])
          return
        }

        const municipalityKeys = Array.from(new Set(
          regionMunicipalityIds.flatMap((id) => [id, `${id}.0`]),
        ))

        const { data, error } = await supabase
          .from('procurement')
          .select('id, contract_signed_date, diavgeia_ada, title, procedure_type_value, contract_budget, budget, organization_key')
          .in('municipality_key', municipalityKeys)
          .gte('contract_signed_date', `${mapYear}-01-01`)
          .lte('contract_signed_date', `${mapYear}-12-31`)
          .order('contract_signed_date', { ascending: false, nullsFirst: false })
          .limit(500)

        if (error) throw error
        const rows = (data ?? []) as Array<{
          id: number
          contract_signed_date: string | null
          diavgeia_ada: string | null
          title: string | null
          procedure_type_value: string | null
          contract_budget: number | string | null
          budget: number | string | null
          organization_key: string | null
        }>

        const paymentByProcId = new Map<number, { beneficiary: string | null; amountWithoutVat: number | null }>()
        const procurementIds = rows.map((r) => r.id)
        if (procurementIds.length > 0) {
          const { data: paymentRows } = await supabase
            .from('payment')
            .select('procurement_id, beneficiary_name, amount_without_vat')
            .in('procurement_id', procurementIds)
          for (const row of (paymentRows ?? []) as Array<{ procurement_id: number; beneficiary_name: string | null; amount_without_vat: number | null }>) {
            if (!paymentByProcId.has(row.procurement_id)) {
              paymentByProcId.set(row.procurement_id, {
                beneficiary: String(row.beneficiary_name ?? '').trim() || null,
                amountWithoutVat: row.amount_without_vat != null ? Number(row.amount_without_vat) : null,
              })
            }
          }
        }

        const orgKeys = Array.from(new Set(rows.map((r) => String(r.organization_key ?? '').trim()).filter(Boolean)))
        const orgNameByKey = new Map<string, string>()
        if (orgKeys.length > 0) {
          const { data: orgRows } = await supabase
            .from('organization')
            .select('organization_key, organization_normalized_value, organization_value')
            .in('organization_key', orgKeys)
          for (const row of (orgRows ?? []) as Array<{ organization_key: string; organization_normalized_value: string | null; organization_value: string | null }>) {
            orgNameByKey.set(
              row.organization_key,
              String(row.organization_normalized_value ?? row.organization_value ?? row.organization_key).trim(),
            )
          }
        }

        const mapped: MunicipalityLatestContract[] = rows
          .map((r) => ({
            id: String(r.id),
            who: orgNameByKey.get(String(r.organization_key ?? '').trim()) || String(r.organization_key ?? '—').trim() || '—',
            when: fmtDate(r.contract_signed_date),
            what: String(r.title ?? '').trim() || '—',
            why: `Διαδικασία: ${String(r.procedure_type_value ?? '—').trim() || '—'}`,
            howMuch: fmtEur(paymentByProcId.get(r.id)?.amountWithoutVat ?? null),
            beneficiary: paymentByProcId.get(r.id)?.beneficiary ?? '—',
            contractType: String(r.procedure_type_value ?? '—').trim() || '—',
            documentUrl: String(r.diavgeia_ada ?? '').trim() ? `https://diavgeia.gov.gr/doc/${String(r.diavgeia_ada).trim()}` : null,
          }))
          .filter((r) => isLocalOrRegionalAuthority(r.who))
          .slice(0, 8)

        if (!cancelled) setRegionLatestContracts(mapped)
      } catch (e) {
        if (!cancelled) {
          console.error('[MapsPage] region latest contracts failed', e)
          setRegionLatestContracts([])
        }
      } finally {
        if (!cancelled) setRegionLatestLoading(false)
      }
    }

    loadRegionContracts()
    return () => {
      cancelled = true
    }
  }, [panelKind, selectedRegion, regionToMunicipalityIds, mapYear])

  const handleMapDeselect = () => {
    setSelectedMunicipalityIdsForMap(new Set())
    setSelectedMunicipalityDropdown('')
    setSelectedMunicipalityIdForPanel(null)
    setSelectedRegion('')
    setPanelSource(null)
    setPanelKind(null)
    setPanelLabel('')
  }

  const handleMunicipalityDropdownChange = (value: string) => {
    setSelectedMunicipalityDropdown(value)
    if (!value) {
      setSelectedMunicipalityIdsForMap(new Set())
      setSelectedMunicipalityIdForPanel(null)
      setPanelSource(null)
      setPanelKind(null)
      setPanelLabel('')
      return
    }
    const region = municipalityRegionById.get(value)
    if (region) setSelectedRegion(region)
    setSelectedMunicipalityIdsForMap(new Set([value]))
    setSelectedMunicipalityIdForPanel(value)
    const label = municipalityLabelById.get(value) ?? value
    openPanel('dropdown', 'municipality', label)
  }

  const handleRegionDropdownChange = (value: string) => {
    setSelectedRegion(value)
    if (!value) {
      setSelectedMunicipalityIdsForMap(new Set())
      setSelectedMunicipalityIdForPanel(null)
      setPanelSource(null)
      setPanelKind(null)
      setPanelLabel('')
      return
    }
    setSelectedMunicipalityDropdown('')
    const ids = regionToMunicipalityIds.get(value) ?? []
    setSelectedMunicipalityIdsForMap(new Set(ids))
    setSelectedMunicipalityIdForPanel(null)
    openPanel('dropdown', 'region', value)
  }

  const handleMapMunicipalityClick = (municipalityId: string) => {
    const feature = municipalityFeatureById.get(municipalityId)
    const label = municipalityLabelById.get(municipalityId) ?? municipalityId
    const region = municipalityRegionById.get(municipalityId) ?? null
    const pct = choroplethData[municipalityId] ?? 0
    const countCurrentYear = municipalityCurrentYearCountById.get(municipalityId) ?? 0
    // Debug payload for municipality mapping/coverage validation.
    console.log('[MapsPage] municipality click', {
      municipalityId,
      label,
      region,
      pctOfNational: pct,
      currentYearContracts: countCurrentYear,
      geojsonProperties: feature?.properties ?? null,
    })

    setSelectedMunicipalityIdsForMap(new Set([municipalityId]))
    setSelectedMunicipalityDropdown(municipalityId)
    setSelectedMunicipalityIdForPanel(municipalityId)
    if (region) setSelectedRegion(region)
    openPanel('map', 'municipality', label)
  }

  const applySearchSelection = (opt: SearchOption) => {
    if (opt.kind === 'municipality') {
      setSelectedMunicipalityDropdown(opt.value)
      setSelectedMunicipalityIdsForMap(new Set([opt.value]))
      setSelectedMunicipalityIdForPanel(opt.value)
      const region = municipalityRegionById.get(opt.value)
      if (region) setSelectedRegion(region)
      openPanel('search', 'municipality', opt.label)
      return
    }
    setSelectedRegion(opt.value)
    setSelectedMunicipalityDropdown('')
    const ids = regionToMunicipalityIds.get(opt.value) ?? []
    setSelectedMunicipalityIdsForMap(new Set(ids))
    setSelectedMunicipalityIdForPanel(null)
    openPanel('search', 'region', opt.label)
  }

  return (
    <div className="maps-page">
      <ComponentTag name="MapsPage" />
      <div className="maps-page__texture" aria-hidden="true" />
      <div className="maps-page__sun" aria-hidden="true" />

      <header className="maps-header section-rule">
        <div>
          <div className="eyebrow">Χαρτογραφία</div>
          <h1>Χάρτες</h1>
          <p>
            {loading
              ? 'Φόρτωση χάρτη…'
              : `Ελλάδα / ${activeMunicipalityCount.toLocaleString('el-GR')} δήμοι με καταγεγραμμένες συμβάσεις το ${mapYear}`}
          </p>
        </div>
        <div className="maps-header__links">
          <Link className="contracts-back" to="/">← Αρχική</Link>
          <Link className="contracts-back" to="/contracts">Όλες οι συμβάσεις</Link>
        </div>
      </header>

      <section className="maps-controls section-rule">
        <ComponentTag name="MapsFilters" />
        <div className="maps-controls__row">
          <label className="maps-controls__search">
            <span>ΑΝΑΖΗΤΗΣΗ (ΔΗΜΟΣ / ΠΕΡΙΦΕΡΕΙΑ)</span>
            <input
              type="text"
              value={searchText}
              onChange={(e) => setSearchText(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && searchResults.length > 0) {
                  applySearchSelection(searchResults[0])
                }
              }}
              placeholder="Π.χ. Νεα Σμυρνη, attikis, peiraia"
            />
            {searchText.trim() && (
              <div className="maps-search-results">
                {searchResults.length === 0 ? (
                  <button type="button" className="maps-search-empty" disabled>
                    Δεν βρέθηκε αποτέλεσμα
                  </button>
                ) : (
                  searchResults.map((opt) => (
                    <button
                      key={`${opt.kind}-${opt.value}`}
                      type="button"
                      onClick={() => applySearchSelection(opt)}
                    >
                      <small>{opt.kind === 'municipality' ? 'ΔΗΜΟΣ' : 'ΠΕΡΙΦΕΡΕΙΑ'}</small>
                      <span>{opt.label}</span>
                    </button>
                  ))
                )}
              </div>
            )}
          </label>
          <label>
            <span>ΕΠΕΛΕΞΕ ΔΗΜΟ</span>
            <select
              value={selectedMunicipalityDropdown}
              onChange={(e) => handleMunicipalityDropdownChange(e.target.value)}
            >
              <option value="">— Δήμοι —</option>
              {loading && (
                <option value="" disabled>Φόρτωση δήμων…</option>
              )}
              {!loading && municipalities.length === 0 && (
                <option value="" disabled>Δεν βρέθηκαν δήμοι</option>
              )}
              {municipalities.map((m) => (
                <option key={m.id} value={m.id}>{m.label}</option>
              ))}
            </select>
          </label>

          <label>
            <span>ΕΠΕΛΕΞΕ ΠΕΡΙΦΕΡΕΙΑ</span>
            <select
              value={selectedRegion}
              onChange={(e) => handleRegionDropdownChange(e.target.value)}
            >
              <option value="">— Περιφέρειες —</option>
              {REGIONS.map((region) => (
                <option key={region} value={region}>{region}</option>
              ))}
            </select>
          </label>
        </div>
      </section>

      <section className="maps-stage section-rule">
        <div className="maps-stage__contours" aria-hidden="true" />
        <div className="maps-stage__frame">
          <GreeceMap
            geojson={geojson}
            choroplethData={choroplethData}
            procMunicipalities={hiddenProcDots}
            onDeselect={handleMapDeselect}
            onMunicipalityClick={handleMapMunicipalityClick}
            selectedMunicipalityIds={selectedMunicipalityIdsForMap}
          />
        </div>
      </section>

      <MapSelectionPanel
        source={panelSource}
        kind={panelKind}
        label={panelLabel}
        onContractOpen={openContractModal}
        municipalityLatestContracts={municipalityLatestContracts}
        municipalityLatestLoading={municipalityLatestLoading}
        regionLatestContracts={regionLatestContracts}
        regionLatestLoading={regionLatestLoading}
        municipalityFeature={
          selectedMunicipalityIdForPanel
            ? (municipalityFeatureById.get(selectedMunicipalityIdForPanel) ?? null)
            : null
        }
        currentYear={mapYear}
        regionCurrentYearCount={selectedRegionCurrentYearCount}
        municipalityCurrentYearCount={
          selectedMunicipalityIdForPanel
            ? (municipalityCurrentYearCountById.get(selectedMunicipalityIdForPanel) ?? 0)
            : null
        }
      />

      {selectedContract && (
        <ContractModal
          contract={selectedContract}
          onClose={() => setSelectedContract(null)}
        />
      )}
    </div>
  )
}
