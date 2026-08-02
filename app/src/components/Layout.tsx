import { useEffect, useState, type MouseEvent } from 'react'
import { NavLink, Outlet, useLocation, useNavigate } from 'react-router-dom'
import ComponentTag from './ComponentTag'
import DevViewToggle from './DevViewToggle'
import { showCookiePreferences } from '../cookieConsent'
import { supabase } from '../lib/supabase'

declare const __LAST_COMMIT_ISO__: string

function latestIso(...values: Array<string | null | undefined>): string | null {
  let latestValue: string | null = null
  let latestTime = Number.NEGATIVE_INFINITY

  for (const value of values) {
    if (!value) continue
    const time = new Date(value).getTime()
    if (Number.isNaN(time) || time <= latestTime) continue
    latestValue = value
    latestTime = time
  }

  return latestValue
}

function formatDateTimeEl(iso: string): string {
  const dt = new Date(iso)
  if (Number.isNaN(dt.getTime())) return '—'
  return new Intl.DateTimeFormat('el-GR', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    timeZone: 'Europe/Athens',
  }).format(dt)
}

export default function Layout() {
  const [lastUpdateIso, setLastUpdateIso] = useState(__LAST_COMMIT_ISO__)
  const lastUpdateLabel = formatDateTimeEl(lastUpdateIso)
  const latestYear = new Date(lastUpdateIso).getFullYear() || new Date().getFullYear()
  const location = useLocation()
  const navigate = useNavigate()
  const homeDocumentHref = import.meta.env.BASE_URL

  useEffect(() => {
    let isCancelled = false

    const loadLastUpdateTime = async () => {
      let currentFiresIso: string | null = null

      try {
        const { data } = await supabase
          .from('current_fires')
          .select('last_seen_at')
          .order('last_seen_at', { ascending: false, nullsFirst: false })
          .limit(1)
          .maybeSingle()

        currentFiresIso = typeof data?.last_seen_at === 'string' ? data.last_seen_at : null
      } catch {
        // Keep the fallback timestamp when current fire freshness is unavailable.
      }

      if (isCancelled) return
      setLastUpdateIso(latestIso(currentFiresIso, __LAST_COMMIT_ISO__) ?? __LAST_COMMIT_ISO__)
    }

    void loadLastUpdateTime()

    return () => {
      isCancelled = true
    }
  }, [])

  const handleAbout = () => {
    if (location.pathname === '/') {
      document.getElementById('about')?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    } else {
      navigate('/', { state: { scrollTo: 'about' } })
    }
  }

  const handleHomeReload = (event: MouseEvent<HTMLAnchorElement>) => {
    event.preventDefault()
    if (location.pathname === '/') {
      window.location.reload()
      return
    }
    window.location.assign(homeDocumentHref)
  }

  const reloadWhenAlreadyOn = (path: string) => (event: MouseEvent<HTMLAnchorElement>) => {
    if (location.pathname !== path) return
    event.preventDefault()
    window.location.reload()
  }

  return (
    <div className="pyro-app">
      <DevViewToggle />
      <ComponentTag name="Layout" />
      <div className="page-grid" aria-hidden="true" />

      <header className="site-header">
        <div className="brand-block">
          <div className="eyebrow">παρατηρητήριο για τις δασικές πυρκαγιές</div>
          <div className="brand-line">
            <NavLink to="/" className="brand-home-link" onClick={handleHomeReload}>
              <h1>FireWatch</h1>
            </NavLink>
            <span className="brand-mark">Τελευταία ενημέρωση / {lastUpdateLabel}</span>
          </div>
        </div>
        <nav className="top-nav" aria-label="Κύρια πλοήγηση">
          <NavLink to="/" onClick={handleHomeReload} data-nav-item="home">Αρχική</NavLink>
          <NavLink to="/maps" onClick={reloadWhenAlreadyOn('/maps')} data-nav-item="maps">Χάρτης</NavLink>
          <NavLink to="/municipalities" onClick={reloadWhenAlreadyOn('/municipalities')} data-nav-item="municipalities">Δήμοι</NavLink>
          <NavLink to="/environment-ministry" onClick={reloadWhenAlreadyOn('/environment-ministry')} data-nav-item="environment">Υπ. Περιβάλλοντος</NavLink>
          <NavLink to="/contracts" onClick={reloadWhenAlreadyOn('/contracts')} data-nav-item="contracts">Συμβάσεις</NavLink>
          <NavLink to="/diavgeia" onClick={reloadWhenAlreadyOn('/diavgeia')} data-nav-item="diavgeia">
            <span>Διαύγεια</span>
            <span className="nav-new-badge">New</span>
          </NavLink>
          <NavLink to="/analysis" onClick={reloadWhenAlreadyOn('/analysis')} data-nav-item="analysis">Ανάλυση</NavLink>
          <button type="button" onClick={handleAbout} data-nav-item="about">Σχετικά</button>
        </nav>
      </header>

      <Outlet />

      <footer className="site-footer">
        © {latestYear} FireWatch · <a href="https://troboukis.gr/" target="_blank" rel="noreferrer">Thanasis Troboukis</a> · <NavLink to="/terms">Όροι χρήσης</NavLink> · <NavLink to="/privacy">Απόρρητο &amp; cookies</NavLink> · <button type="button" onClick={showCookiePreferences}>Ρυθμίσεις cookies</button>
      </footer>
    </div>
  )
}
