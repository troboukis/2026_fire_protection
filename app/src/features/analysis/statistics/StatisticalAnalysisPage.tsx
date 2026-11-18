import { useOverviewUi } from '../../../i18n/overview'
import { LocalizedLink as Link } from '../../../routing/LocalizedLink'
import ErrorBoundary from '../../../components/ErrorBoundary'
import StatisticalAnalysis from './StatisticalAnalysis'
import '../AnalysisLayout.css'
import './StatisticalAnalysis.css'

export default function StatisticalAnalysisPage() {
  const ui = useOverviewUi()
  return (
    <main className="analysis-detail-page">
      <nav aria-label={ui("Πλοήγηση ανάλυσης")}><Link className="analysis-back" to="/analysis">{ui("← Όλες οι αναλύσεις")}</Link></nav>
      <ErrorBoundary fallback={<div className="ca-empty-note">{ui("Η ενότητα ανάλυσης δεν είναι διαθέσιμη προσωρινά.")}</div>}>
        <StatisticalAnalysis />
      </ErrorBoundary>
    </main>
  )
}
