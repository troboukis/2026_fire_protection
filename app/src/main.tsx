import React from 'react'
import ReactDOM from 'react-dom/client'
import { HashRouter, Route, Routes } from 'react-router-dom'
import App from './App'
import ContractsPage from './pages/ContractsPage'
import MapsPage from './pages/MapsPage'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <HashRouter>
      <Routes>
        <Route path="/" element={<App />} />
        <Route path="/contracts" element={<ContractsPage />} />
        <Route path="/maps" element={<MapsPage />} />
      </Routes>
    </HashRouter>
  </React.StrictMode>,
)
