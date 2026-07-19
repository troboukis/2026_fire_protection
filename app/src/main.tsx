import React, { Suspense, lazy, useEffect } from 'react'
import ReactDOM from 'react-dom/client'
import { Analytics } from '@vercel/analytics/react'
import { SpeedInsights } from '@vercel/speed-insights/react'
import { BrowserRouter, Route, Routes, useLocation } from 'react-router-dom'
import App from './App'
import Layout from './components/Layout'
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

const AnalysisPage = lazy(() => import('./pages/AnalysisPage'))
const ContractsPage = lazy(() => import('./pages/ContractsPage'))
const DiavgeiaPage = lazy(() => import('./pages/DiavgeiaPage'))
const EnvironmentMinistryPage = lazy(() => import('./pages/EnvironmentMinistryPage'))
const MapsPage = lazy(() => import('./pages/MapsPage'))
const MediaPage = lazy(() => import('./pages/MediaPage'))
const MunicipalitiesPage = lazy(() => import('./pages/MunicipalitiesPage'))
const PrivacyPage = lazy(() => import('./pages/PrivacyPage'))
const TermsPage = lazy(() => import('./pages/TermsPage'))

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <BrowserRouter basename={import.meta.env.BASE_URL}>
      <RouteTracker />
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<App />} />
          <Route path="/analysis" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><AnalysisPage /></Suspense>} />
          <Route path="/contracts" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><ContractsPage /></Suspense>} />
          <Route path="/diavgeia" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><DiavgeiaPage /></Suspense>} />
          <Route path="/environment-ministry" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><EnvironmentMinistryPage /></Suspense>} />
          <Route path="/municipalities" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><MunicipalitiesPage /></Suspense>} />
          <Route path="/maps" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><MapsPage /></Suspense>} />
          <Route path="/media" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><MediaPage /></Suspense>} />
          <Route path="/privacy" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><PrivacyPage /></Suspense>} />
          <Route path="/terms" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><TermsPage /></Suspense>} />
        </Route>
      </Routes>
    </BrowserRouter>
    {analyticsEnabled ? <Analytics /> : null}
    {analyticsEnabled ? <SpeedInsights /> : null}
  </React.StrictMode>,
)
