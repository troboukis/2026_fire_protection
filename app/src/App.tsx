import { useEffect, useRef, useState } from 'react'
import { BrowserRouter, Routes, Route, useParams, useNavigate } from 'react-router-dom'
import * as d3 from 'd3'
import { GreeceMap } from './components/GreeceMap'
import { MunicipalityPanel } from './components/MunicipalityPanel'
import { supabase } from './lib/supabase'
import type { GlobalFireYear, GeoData } from './types'

/* ── Helpers ─────────────────────────────────────────────────── */
function fmtInt(n: number): string {
  return n.toLocaleString('el-GR')
}

/* ── Global fire summary from v_global_fire_summary view ─────── */
async function fetchGlobalFireYears(): Promise<GlobalFireYear[]> {
  const { data, error } = await supabase
    .from('v_global_fire_summary')
    .select('year, incident_count, total_burned_stremata, total_burned_ha')
    .order('year', { ascending: true })

  if (error) throw error

  return (data ?? []).map(r => ({
    year:                  Number(r.year),
    incident_count:        Number(r.incident_count),
    total_burned_stremata: Number(r.total_burned_stremata ?? 0),
    total_burned_ha:       Number(r.total_burned_ha ?? 0),
  }))
}

/* ── Choropleth fetchers ─────────────────────────────────────── */
// All-time: each municipality's share of total national burned area 2000-2024.
async function fetchChoroplethAllTime(): Promise<Record<string, number>> {
  const { data, error } = await supabase
    .from('v_municipality_fire_totals')
    .select('municipality_id, pct_of_national')
  if (error) throw error
  const out: Record<string, number> = {}
  for (const r of data ?? []) {
    if (r.municipality_id && r.pct_of_national != null)
      out[r.municipality_id] = Number(r.pct_of_national)
  }
  return out
}

/* ── Municipalities with procurement data (for map dots) ─────── */
async function fetchProcMunicipalities(): Promise<Set<string>> {
  const { data, error } = await supabase
    .from('v_municipality_procurement_summary')
    .select('municipality_id')
    .eq('year', new Date().getFullYear())
  if (error) throw error
  return new Set((data ?? []).map(r => String(r.municipality_id)))
}

// Single year: each municipality's share of that year's total burned area.
async function fetchChoroplethYear(year: number): Promise<Record<string, number>> {
  const { data, error } = await supabase
    .from('v_municipality_fire_summary')
    .select('municipality_id, total_burned_ha')
    .eq('year', year)
  if (error) throw error
  const rows = data ?? []
  const total = rows.reduce((s, r) => s + Number(r.total_burned_ha ?? 0), 0)
  const out: Record<string, number> = {}
  for (const r of rows) {
    if (r.municipality_id && Number(r.total_burned_ha) > 0)
      out[r.municipality_id] = Number(r.total_burned_ha) / total * 100
  }
  return out
}


/* ── Floating fire-year dropdown ─────────────────────────────── */
function FireYearControl({
  selectedYear,
  years,
  onChange,
}: {
  selectedYear: number | null
  years: number[]
  onChange: (yr: number | null) => void
}) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!open) return
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [open])

  const select = (yr: number | null) => {
    onChange(yr)
    setOpen(false)
  }

  return (
    <div className="fire-ctrl" ref={ref}>
      <button
        type="button"
        className={`fire-ctrl-btn${selectedYear ? ' has-year' : ''}`}
        onClick={() => years.length > 0 && setOpen(o => !o)}
        aria-haspopup="menu"
        aria-expanded={open}
        disabled={years.length === 0}
      >
        <span className="fire-ctrl-btn-main">Επίλεξε έτος</span>
        <span className={`fire-ctrl-btn-year${selectedYear ? ' active' : ''}`}>
          {selectedYear ? String(selectedYear) : '—'}
        </span>
        <span className="fire-ctrl-chevron">{open ? '▲' : '▼'}</span>
      </button>

      {open && (
        <div className="fire-ctrl-menu" role="menu" aria-label="Επιλογή έτους πυρκαγιών">
          {selectedYear && (
            <button
              type="button"
              className="fire-ctrl-item clear"
              onClick={() => select(null)}
              role="menuitem"
            >
              Όλα τα έτη
            </button>
          )}
          {years.map(yr => (
            <button
              type="button"
              key={yr}
              className={`fire-ctrl-item${selectedYear === yr ? ' active' : ''}`}
              onClick={() => select(selectedYear === yr ? null : yr)}
              role="menuitem"
            >
              <span>{yr}</span>
              {selectedYear === yr && <span className="fire-ctrl-item-check">✓</span>}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}

/* ── Welcome panel ───────────────────────────────────────────── */
function WelcomePanel() {
  return (
    <div className="panel-scroll">
      <p className="welcome-eyebrow">Ανοιχτά δεδομένα · Δημόσια λογοδοσία</p>
      <h1 className="welcome-title">
        Πυρκαγιές &amp; Δαπάνες Δασοπυρόσβεσης
      </h1>
      <p className="welcome-body">
        Ο χάρτης εμφανίζει τους 326 δήμους της Ελλάδας. Για κάθε δήμο
        μπορείτε να δείτε το 25ετές ιστορικό πυρκαγιών και τις δημόσιες
        συμβάσεις για δασοπυρόσβεση μέσω Διαύγειας.
      </p>
      <p className="welcome-cta">
        <span className="welcome-cta-arrow">→</span>
        Επιλέξτε δήμο στον χάρτη
      </p>

      <div className="legend">
        <p className="legend-title">Υπόμνημα</p>
        <div className="legend-item">
          <div className="legend-swatch" style={{ background: '#ddd7ca', border: '1px solid #b0aba4' }} />
          <span>Δήμος χωρίς καταγεγραμμένες πυρκαγιές</span>
        </div>
        <div className="legend-item">
          <div className="legend-swatch" style={{ background: 'linear-gradient(to right, #fcbba1, #67000d)', width: 48, borderRadius: 2 }} />
          <span>Χαμηλό → Υψηλό μερίδιο καμμένης έκτασης</span>
        </div>
        <div className="legend-item">
          <div className="legend-swatch" style={{ background: '#1a3a5c' }} />
          <span>Επιλεγμένος δήμος</span>
        </div>
      </div>
    </div>
  )
}

/* ── Main layout ─────────────────────────────────────────────── */
function Layout({
  geojson,
  globalFireYears,
  fireLoading,
  fireError,
  procMunicipalities,
}: {
  geojson: GeoData | null
  globalFireYears: GlobalFireYear[]
  fireLoading: boolean
  fireError: string | null
  procMunicipalities: Set<string>
}) {
  const { id }   = useParams<{ id?: string }>()
  const navigate = useNavigate()
  const [fireYear, setFireYear]           = useState<number | null>(null)
  const [choroplethData, setChoroplethData] = useState<Record<string, number>>({})

  // Reload choropleth whenever the selected year changes
  useEffect(() => {
    const fetch = fireYear === null
      ? fetchChoroplethAllTime()
      : fetchChoroplethYear(fireYear)
    fetch.then(setChoroplethData).catch(() => {})
  }, [fireYear])

  // Editorial copy: sum 2000-onward from the aggregate view
  const periodYears = globalFireYears.filter(y => y.year >= 2000)
  const firstYear = 2000
  const lastYear    = periodYears.length > 0 ? Math.max(...periodYears.map(y => y.year)) : null
  const totalCount  = periodYears.reduce((s, y) => s + y.incident_count, 0)
  const totalBurned = periodYears.reduce((s, y) => s + y.total_burned_stremata, 0)
  const yearOptions = [...periodYears].map(y => y.year).sort((a, b) => b - a)
  const fireTitle = fireYear != null
    ? `Δασικές πυρκαγιές: ${fireYear}`
    : `Δασικές πυρκαγιές: ${firstYear} - ${lastYear ?? '—'}`

  let fireSubtitle = 'Φόρτωση δεδομένων πυρκαγιών…'
  if (fireError) {
    fireSubtitle = 'Δεν ήταν δυνατή η φόρτωση των δεδομένων πυρκαγιών από τη βάση.'
  } else if (!fireLoading && globalFireYears.length > 0) {
    if (fireYear != null) {
      const yr    = globalFireYears.find(y => y.year === fireYear)
      const count = yr?.incident_count ?? 0
      const burned = Math.round(yr?.total_burned_stremata ?? 0)
      fireSubtitle = `Kαταγράφηκαν ${fmtInt(count)} πυρκαγιές, οι οποίες έκαψαν συνολικά ${fmtInt(burned)} στρέμματα.`
    } else {
      fireSubtitle = `Kαταγράφηκαν ${fmtInt(totalCount)} πυρκαγιές στην Ελλάδα, οι οποίες έκαψαν συνολικά ${fmtInt(Math.round(totalBurned))} στρέμματα.`
    }
  }

  return (
    <div className="app">

      <header className="header">
        <span className="header-logo">Project Πυρ</span>
        <span className="header-sep" />
        <span className="header-sub">Διαφάνεια πυροπροστασίας στην Ελλάδα</span>
      </header>

      <div className="main">

        <aside className="panel">
          {id
            ? <MunicipalityPanel id={id} onBack={() => navigate('/')} />
            : <WelcomePanel />
          }
        </aside>

        <div className="map-container">
            <div className="map-top-ui">
            <div className="map-editorial-copy">
              <h2>{fireTitle}</h2>
              <p>{fireSubtitle}</p>
              <div className="map-editorial-chipline">
                <FireYearControl selectedYear={fireYear} years={yearOptions} onChange={setFireYear} />
              </div>
            </div>
          </div>

          <div className="map-stage">
            <GreeceMap
              geojson={geojson}
              choroplethData={choroplethData}
              procMunicipalities={procMunicipalities}
              onDeselect={() => navigate('/')}
            />
          </div>
        </div>

      </div>

      <footer className="footer">
        <div className="footer-sources">
          <span className="footer-label">Πηγές:</span>
          <span className="footer-item">Δημόσια δεδομένα: Διαύγεια</span>
          <span className="footer-sep">·</span>
          <span className="footer-item">Ιστορικό πυρκαγιών: Πυροσβεστική</span>
          <span className="footer-sep">·</span>
          <span className="footer-item">Δασικές εκτάσεις: Δασικοί χάρτες 2022</span>
        </div>
        <span className="footer-credit">
          Έρευνα &amp; Ανάπτυξη:{' '}
          <a href="https://troboukis.github.io" target="_blank" rel="noreferrer">
            Θανάσης Τρομπούκης
          </a>
        </span>
      </footer>

    </div>
  )
}

/* ── Root ─────────────────────────────────────────────────────── */
export default function App() {
  const [geojson, setGeojson]                 = useState<GeoData | null>(null)
  const [globalFireYears, setGlobalFireYears] = useState<GlobalFireYear[]>([])
  const [fireLoading, setFireLoading]         = useState(true)
  const [fireError, setFireError]             = useState<string | null>(null)
  useEffect(() => {
    d3.json<GeoData>('/municipalities.geojson').then(data => {
      if (data) setGeojson(data)
    })
  }, [])

  // Global fire years from aggregate view (25 rows — avoids the broken 84k fetch)
  useEffect(() => {
    let cancelled = false
    setFireLoading(true)
    setFireError(null)
    fetchGlobalFireYears()
      .then(years => { if (!cancelled) setGlobalFireYears(years) })
      .catch((err: unknown) => {
        if (!cancelled) setFireError(err instanceof Error ? err.message : 'Unknown error')
      })
      .finally(() => { if (!cancelled) setFireLoading(false) })
    return () => { cancelled = true }
  }, [])

  const [procMunicipalities, setProcMunicipalities] = useState<Set<string>>(new Set())
  useEffect(() => {
    fetchProcMunicipalities().then(setProcMunicipalities).catch(() => {})
  }, [])

  const layoutProps = { geojson, globalFireYears, fireLoading, fireError, procMunicipalities }

  return (
    <BrowserRouter>
      <Routes>
        <Route path="/"                 element={<Layout {...layoutProps} />} />
        <Route path="/municipality/:id" element={<Layout {...layoutProps} />} />
      </Routes>
    </BrowserRouter>
  )
}
