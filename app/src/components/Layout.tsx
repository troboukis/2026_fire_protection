import type { MouseEvent } from 'react'
import { NavLink, Outlet, useLocation, useNavigate } from 'react-router-dom'
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
  const lastUpdateLabel = formatDateTimeEl(__LAST_COMMIT_ISO__)
  const location = useLocation()
  const navigate = useNavigate()
  const homeDocumentHref = import.meta.env.BASE_URL

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
              <h1>FireWatch <span className="beta-badge">BETA</span></h1>
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
          <NavLink to="/analysis" onClick={reloadWhenAlreadyOn('/analysis')} data-nav-item="analysis">Ανάλυση</NavLink>
          <button type="button" onClick={handleAbout} data-nav-item="about">Σχετικά</button>
        </nav>
      </header>

      <Outlet />
    </div>
  )
}
