import { useOverviewUi } from '../../../i18n/overview'
import { LocalizedLink as Link } from '../../../routing/LocalizedLink'
import ErrorBoundary from '../../../components/ErrorBoundary'
import YearlyReport from './YearlyReport'
import '../AnalysisLayout.css'
import './YearlyReport.css'

export default function YearlyReportPage() {
  const ui = useOverviewUi()
  return (
    <main className="analysis-detail-page">
      <nav aria-label={ui("Πλοήγηση ανάλυσης")}><Link className="analysis-back" to="/analysis">{ui("← Όλες οι αναλύσεις")}</Link></nav>
      <ErrorBoundary fallback={<div className="yr-empty-note">{ui("Η αναφορά δεν είναι διαθέσιμη προσωρινά.")}</div>}>
        <YearlyReport />
      </ErrorBoundary>
    </main>
  )
}
