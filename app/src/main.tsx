import React, { Suspense, lazy, useEffect } from 'react'
import ReactDOM from 'react-dom/client'
import { HashRouter, Route, Routes, useLocation } from 'react-router-dom'
import App from './App'
import Layout from './components/Layout'
import { initGA, trackPageView } from './analytics'
import './index.css'

initGA()

function RouteTracker() {
  const location = useLocation()
  useEffect(() => {
    trackPageView(location.pathname)
  }, [location.pathname])
  return null
}

const AnalysisPage = lazy(() => import('./pages/AnalysisPage'))
const ContractsPage = lazy(() => import('./pages/ContractsPage'))
const EnvironmentMinistryPage = lazy(() => import('./pages/EnvironmentMinistryPage'))
const MapsPage = lazy(() => import('./pages/MapsPage'))
const MunicipalitiesPage = lazy(() => import('./pages/MunicipalitiesPage'))

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <HashRouter>
      <RouteTracker />
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<App />} />
          <Route path="/analysis" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><AnalysisPage /></Suspense>} />
          <Route path="/contracts" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><ContractsPage /></Suspense>} />
          <Route path="/environment-ministry" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><EnvironmentMinistryPage /></Suspense>} />
          <Route path="/municipalities" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><MunicipalitiesPage /></Suspense>} />
          <Route path="/maps" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><MapsPage /></Suspense>} />
        </Route>
      </Routes>
    </HashRouter>
  </React.StrictMode>,
)
