import React, { useEffect } from 'react'
import ReactDOM from 'react-dom/client'
import { Analytics } from '@vercel/analytics/react'
import { BrowserRouter, Route, Routes, useLocation } from 'react-router-dom'
import Layout from './components/Layout'
import RouteLanguageProvider from './routing/RouteLanguageProvider'
import { pageRoutes } from './routing/pageRoutes'
import { trackPageView } from './analytics'
import { initCookieConsent } from './cookieConsent'
import './index.css'

const analyticsEnabled = import.meta.env.PROD

void initCookieConsent()

function RouteTracker() {
  const location = useLocation()
  useEffect(() => {
    trackPageView(location.pathname)
  }, [location.pathname])
  return null
}

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <BrowserRouter basename={import.meta.env.BASE_URL}>
      <RouteLanguageProvider>
        <RouteTracker />
        <Routes>
          <Route element={<Layout />}>
            {pageRoutes.map(page => <Route key={page.path} path={page.path} element={page.element} />)}
          </Route>
        </Routes>
      </RouteLanguageProvider>
    </BrowserRouter>
    {analyticsEnabled ? <Analytics /> : null}
  </React.StrictMode>,
)
