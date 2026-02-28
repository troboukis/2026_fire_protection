import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import ComponentTag from '../components/ComponentTag'
import { GreeceMap } from '../components/GreeceMap'
import MapSelectionPanel, { type SelectionKind, type SelectionSource } from '../components/MapSelectionPanel'
import { supabase } from '../lib/supabase'
import type { GeoData, GeoFeature } from '../types'

type ProcurementMapRow = {
  municipality_id: string | null
  issue_date?: string | null
  amount_eur: number | string | null
}

type MunicipalityRow = {
  id: string | null
  name: string | null
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

function toFiniteNumber(v: unknown): number | null {
  if (v == null) return null
  const n = Number(v)
  if (!Number.isFinite(n)) return null
  return n
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
  const calendarYear = new Date().getFullYear()
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

  useEffect(() => {
    let cancelled = false
    setLoading(true)

    const load = async () => {
      try {
        const [geoRes, procRes] = await Promise.all([
          fetch('/municipalities.geojson'),
          supabase
            .from('v_raw_procurements_municipality')
            .select('municipality_id, issue_date, amount_eur'),
        ])

        if (cancelled) return

        const geoData = (await geoRes.json()) as GeoData
        if (!cancelled) setGeojson(geoData)

        const rows = ((procRes.data ?? []) as ProcurementMapRow[])
        const amountByMunicipality = new Map<string, number>()
        const yearCountsByMunicipality = new Map<string, Map<number, number>>()
        let nationalTotal = 0

        for (const row of rows) {
          const municipalityId = String(row.municipality_id ?? '').trim()
          if (!municipalityId) continue

          const issueDate = String(row.issue_date ?? '').trim()
          const issueYear = extractYear(issueDate)
          if (issueYear != null) {
            const yc = yearCountsByMunicipality.get(municipalityId) ?? new Map<number, number>()
            yc.set(issueYear, (yc.get(issueYear) ?? 0) + 1)
            yearCountsByMunicipality.set(municipalityId, yc)
          }

          const amount = toFiniteNumber(row.amount_eur)
          if (amount == null || amount <= 0) continue
          amountByMunicipality.set(municipalityId, (amountByMunicipality.get(municipalityId) ?? 0) + amount)
          nationalTotal += amount
        }

        const nextChoropleth: Record<string, number> = {}
        for (const [municipalityId, amount] of amountByMunicipality.entries()) {
          nextChoropleth[municipalityId] = nationalTotal > 0 ? (amount / nationalTotal) * 100 : 0
        }

        if (!cancelled) {
          setChoroplethData(nextChoropleth)
          setProcMunicipalities(new Set(amountByMunicipality.keys()))

          const currentYearCounts = new Map<string, number>()
          for (const [municipalityId, yCounts] of yearCountsByMunicipality.entries()) {
            currentYearCounts.set(municipalityId, yCounts.get(calendarYear) ?? 0)
          }
          setMunicipalityCurrentYearCountById(currentYearCounts)
        }

        const regionRes = await fetch('/municipality_regions.json')
        const fullRegionRows = (await regionRes.json()) as MunicipalityRegionRow[]
        const fullRegionByMunicipality = new Map<string, string>()
        for (const row of fullRegionRows) {
          const municipalityId = String(row.municipality_id ?? '').trim()
          const regionId = String(row.region_id ?? '').trim()
          if (!municipalityId || !regionId) continue
          fullRegionByMunicipality.set(municipalityId, regionId)
        }

        if (!cancelled) {
          setMunicipalityRegionById(fullRegionByMunicipality)
        }

        const { data: municipalitiesData } = await supabase
          .from('municipalities')
          .select('id, name')
          .order('name', { ascending: true })

        if (!cancelled) {
          const fromDb = ((municipalitiesData ?? []) as MunicipalityRow[])
            .map((r) => ({
              id: String(r.id ?? '').trim(),
              label: String(r.name ?? '').trim(),
            }))
            .filter((m) => m.id && m.label)

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
  }, [calendarYear])

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
    setSelectedMunicipalityIdsForMap(new Set([municipalityId]))
    setSelectedMunicipalityDropdown(municipalityId)
    setSelectedMunicipalityIdForPanel(municipalityId)
    const region = municipalityRegionById.get(municipalityId)
    if (region) setSelectedRegion(region)
    const label = municipalityLabelById.get(municipalityId) ?? municipalityId
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
              : `Ελλάδα / ${activeMunicipalityCount.toLocaleString('el-GR')} δήμοι με καταγεγραμμένες συμβάσεις`}
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
        municipalityFeature={
          selectedMunicipalityIdForPanel
            ? (municipalityFeatureById.get(selectedMunicipalityIdForPanel) ?? null)
            : null
        }
        currentYear={calendarYear}
        municipalityCurrentYearCount={
          selectedMunicipalityIdForPanel
            ? (municipalityCurrentYearCountById.get(selectedMunicipalityIdForPanel) ?? 0)
            : null
        }
      />
    </div>
  )
}
