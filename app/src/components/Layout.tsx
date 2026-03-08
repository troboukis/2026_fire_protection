import { useEffect, useState } from 'react'
import { NavLink, Outlet, useLocation, useNavigate } from 'react-router-dom'
import { supabase } from '../lib/supabase'
import ComponentTag from './ComponentTag'
import DevViewToggle from './DevViewToggle'

declare const __LAST_COMMIT_ISO__: string

function formatDateTimeEl(iso: string): string {
  const dt = new Date(iso)
  if (Number.isNaN(dt.getTime())) return '—'
  return new Intl.DateTimeFormat('el-GR', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(dt)
}

export default function Layout() {
  const buildTimeLabel = formatDateTimeEl(__LAST_COMMIT_ISO__)
  const [lastDbUpdateLabel, setLastDbUpdateLabel] = useState(buildTimeLabel)
  const location = useLocation()
  const navigate = useNavigate()

  useEffect(() => {
    let cancelled = false

    const loadLastDbUpdate = async () => {
      // Exclude forest_fire here: this endpoint currently returns 500 for updated_at reads,
      // and the header should degrade gracefully instead of polluting the console on every page load.
      const tables = ['procurement', 'payment', 'diavgeia', 'fund']
      const results = await Promise.all(
        tables.map(async (table) => {
          const { data, error } = await supabase
            .from(table)
            .select('updated_at')
            .order('updated_at', { ascending: false })
            .limit(1)
          if (error) return null
          const row = (data?.[0] ?? null) as { updated_at?: string | null } | null
          const v = row?.updated_at ?? null
          if (!v) return null
          const s = String(v).trim()
          return s && s.toLowerCase() !== 'nan' && s.toLowerCase() !== 'none' ? s : null
        }),
      )
      const latestIso = results
        .filter((v): v is string => Boolean(v))
        .sort((a, b) => new Date(b).getTime() - new Date(a).getTime())[0]
      if (!cancelled && latestIso) setLastDbUpdateLabel(formatDateTimeEl(latestIso))
    }

    loadLastDbUpdate()
    return () => { cancelled = true }
  }, [])

  const handleAbout = () => {
    if (location.pathname === '/') {
      document.getElementById('about')?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    } else {
      navigate('/', { state: { scrollTo: 'about' } })
    }
  }

  return (
    <div className="pyro-app">
      <DevViewToggle />
      <ComponentTag name="Layout" />
      <div className="page-grid" aria-hidden="true" />

      <header className="site-header">
        <div className="brand-block">
          <div className="eyebrow">παρατηρητηριο για την πυροπροστασία</div>
          <div className="brand-line">
            <NavLink to="/" className="brand-home-link">
              <h1>FireWatch <span className="beta-badge">BETA</span></h1>
            </NavLink>
            <span className="brand-mark">Τελευταία ενημέρωση / {lastDbUpdateLabel}</span>
          </div>
        </div>
        <nav className="top-nav" aria-label="Κύρια πλοήγηση">
          <NavLink to="/">Αρχική</NavLink>
          <NavLink to="/analysis">Ανάλυση</NavLink>
          <NavLink to="/contracts">Συμβάσεις</NavLink>
          <NavLink to="/maps">Χάρτης</NavLink>
          <button type="button" onClick={handleAbout}>Σχετικά</button>
        </nav>
      </header>

      <Outlet />
    </div>
  )
}
