import React from 'react'
import ReactDOM from 'react-dom/client'
import { HashRouter, Route, Routes } from 'react-router-dom'
import App from './App'
import Layout from './components/Layout'
import AnalysisPage from './pages/AnalysisPage'
import ContractsPage from './pages/ContractsPage'
import MapsPage from './pages/MapsPage'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <HashRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route path="/" element={<App />} />
          <Route path="/analysis" element={<AnalysisPage />} />
          <Route path="/contracts" element={<ContractsPage />} />
          <Route path="/maps" element={<MapsPage />} />
        </Route>
      </Routes>
    </HashRouter>
  </React.StrictMode>,
)
