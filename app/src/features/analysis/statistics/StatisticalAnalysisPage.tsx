import { Link } from 'react-router-dom'
import ErrorBoundary from '../../../components/ErrorBoundary'
import StatisticalAnalysis from './StatisticalAnalysis'
import '../AnalysisLayout.css'
import './StatisticalAnalysis.css'

export default function StatisticalAnalysisPage() {
  return (
    <main className="analysis-detail-page">
      <nav aria-label="Πλοήγηση ανάλυσης"><Link className="analysis-back" to="/analysis">← Όλες οι αναλύσεις</Link></nav>
      <ErrorBoundary fallback={<div className="ca-empty-note">Η ενότητα ανάλυσης δεν είναι διαθέσιμη προσωρινά.</div>}>
        <StatisticalAnalysis />
      </ErrorBoundary>
    </main>
  )
}
