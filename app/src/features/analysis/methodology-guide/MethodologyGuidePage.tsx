import { useOverviewUi } from '../../../i18n/overview'
import { LocalizedLink as Link } from '../../../routing/LocalizedLink'
import ErrorBoundary from '../../../components/ErrorBoundary'
import MethodologyGuide from './MethodologyGuide'
import '../AnalysisLayout.css'
import './MethodologyGuide.css'

export default function MethodologyGuidePage() {
  const ui = useOverviewUi()
  return (
    <main className="analysis-detail-page">
      <nav aria-label={ui("Πλοήγηση ανάλυσης")}><Link className="analysis-back" to="/analysis">{ui("← Όλες οι αναλύσεις")}</Link></nav>
      <ErrorBoundary fallback={<div className="mg-empty-note" lang="el">Ο οδηγός δεν είναι διαθέσιμος προσωρινά.</div>}>
        <MethodologyGuide />
      </ErrorBoundary>
    </main>
  )
}
