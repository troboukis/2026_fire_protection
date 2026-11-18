import { useEffect, useState, type MouseEvent } from 'react'
import { Outlet, useLocation } from 'react-router-dom'
import { LocalizedNavLink as NavLink } from '../routing/LocalizedLink'
import { useLocalizedNavigate as useNavigate } from '../routing/useLocalizedNavigate'
import LanguageSwitcher from '../routing/LanguageSwitcher'
import { documentHref, localizeHref, stripLanguagePath } from '../routing/localizedHref'
import ComponentTag from './ComponentTag'
import DevViewToggle from './DevViewToggle'
import { showCookiePreferences } from '../cookieConsent'
import { supabase } from '../lib/supabase'
import { useAppI18n } from '../i18n/useAppI18n'

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

export default function Layout() {
  const { t, format, language } = useAppI18n()
  const [lastUpdateIso, setLastUpdateIso] = useState(__LAST_COMMIT_ISO__)
  const lastUpdateLabel = format.dateTime(lastUpdateIso)
  const latestYear = new Date(lastUpdateIso).getFullYear() || new Date().getFullYear()
  const location = useLocation()
  const navigate = useNavigate()
  const homeDocumentHref = documentHref(localizeHref('/', language), import.meta.env.BASE_URL)
  const pagePathname = stripLanguagePath(location.pathname)

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
    if (pagePathname === '/') {
      document.getElementById('about')?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    } else {
      navigate('/', { state: { scrollTo: 'about' } })
    }
  }

  const handleHomeReload = (event: MouseEvent<HTMLAnchorElement>) => {
    event.preventDefault()
    if (pagePathname === '/') {
      window.location.reload()
      return
    }
    window.location.assign(homeDocumentHref)
  }

  const reloadWhenAlreadyOn = (path: string) => (event: MouseEvent<HTMLAnchorElement>) => {
    if (pagePathname !== path) return
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
          <div className="brand-line">
            <NavLink to="/" className="brand-home-link" onClick={handleHomeReload}>
              <h1>
                <img
                  className="brand-logo"
                  src={`${import.meta.env.BASE_URL}FireWatch_logo.svg`}
                  alt={t('layout:brandAlt')}
                  width={2172}
                  height={724}
                />
              </h1>
            </NavLink>
            <span className="brand-mark">{t('layout:lastUpdated')} / {lastUpdateLabel} <LanguageSwitcher /></span>
          </div>
        </div>
        <nav className="top-nav" aria-label={t('layout:mainNavigation')}>
          <NavLink to="/" onClick={handleHomeReload} data-nav-item="home">{t('navigation:home')}</NavLink>
          <NavLink to="/maps" onClick={reloadWhenAlreadyOn('/maps')} data-nav-item="maps">{t('navigation:maps')}</NavLink>
          <NavLink to="/municipalities" onClick={reloadWhenAlreadyOn('/municipalities')} data-nav-item="municipalities">{t('navigation:municipalities')}</NavLink>
          <NavLink to="/environment-ministry" onClick={reloadWhenAlreadyOn('/environment-ministry')} data-nav-item="environment">{t('navigation:environmentMinistry')}</NavLink>
          <NavLink to="/contracts" onClick={reloadWhenAlreadyOn('/contracts')} data-nav-item="contracts">{t('navigation:contracts')}</NavLink>
          <NavLink to="/diavgeia" onClick={reloadWhenAlreadyOn('/diavgeia')} data-nav-item="diavgeia">{t('navigation:diavgeia')}</NavLink>
          <NavLink to="/analysis" onClick={reloadWhenAlreadyOn('/analysis')} data-nav-item="analysis">
            {t('navigation:analysis')} <span className="top-nav__new">{t('navigation:new')}</span>
          </NavLink>
          <button type="button" onClick={handleAbout} data-nav-item="about">{t('navigation:about')}</button>
        </nav>
      </header>

      <Outlet />

      <footer className="site-footer">
        © {latestYear} FireWatch · <a href="https://troboukis.gr/" target="_blank" rel="noreferrer">Thanasis Troboukis</a> · <NavLink to="/terms">{t('navigation:terms')}</NavLink> · <NavLink to="/privacy">{t('navigation:privacy')}</NavLink> · <button type="button" onClick={showCookiePreferences}>{t('navigation:cookiePreferences')}</button>
      </footer>
    </div>
  )
}
