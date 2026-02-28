import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter, Route, Routes } from 'react-router-dom'
import App from './App'
import ContractsPage from './pages/ContractsPage'
import MapsPage from './pages/MapsPage'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<App />} />
        <Route path="/contracts" element={<ContractsPage />} />
        <Route path="/maps" element={<MapsPage />} />
      </Routes>
    </BrowserRouter>
  </React.StrictMode>,
)
