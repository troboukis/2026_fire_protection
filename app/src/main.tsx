import React, { Suspense, lazy } from 'react'
import ReactDOM from 'react-dom/client'
import { HashRouter, Route, Routes } from 'react-router-dom'
import App from './App'
import Layout from './components/Layout'
import './index.css'

const AnalysisPage = lazy(() => import('./pages/AnalysisPage'))
const ContractsPage = lazy(() => import('./pages/ContractsPage'))
const MapsPage = lazy(() => import('./pages/MapsPage'))
const MunicipalitiesPage = lazy(() => import('./pages/MunicipalitiesPage'))

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <HashRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<App />} />
          <Route path="/analysis" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><AnalysisPage /></Suspense>} />
          <Route path="/contracts" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><ContractsPage /></Suspense>} />
          <Route path="/municipalities" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><MunicipalitiesPage /></Suspense>} />
          <Route path="/maps" element={<Suspense fallback={<main className="page-loading">Φόρτωση σελίδας…</main>}><MapsPage /></Suspense>} />
        </Route>
      </Routes>
    </HashRouter>
  </React.StrictMode>,
)
