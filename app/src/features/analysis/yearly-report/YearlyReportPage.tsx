import { Link } from 'react-router-dom'
import ErrorBoundary from '../../../components/ErrorBoundary'
import YearlyReport from './YearlyReport'
import '../AnalysisLayout.css'
import './YearlyReport.css'

export default function YearlyReportPage() {
  return (
    <main className="analysis-detail-page">
      <nav aria-label="Πλοήγηση ανάλυσης"><Link className="analysis-back" to="/analysis">← Όλες οι αναλύσεις</Link></nav>
      <ErrorBoundary fallback={<div className="yr-empty-note">Η αναφορά δεν είναι διαθέσιμη προσωρινά.</div>}>
        <YearlyReport />
      </ErrorBoundary>
    </main>
  )
}
