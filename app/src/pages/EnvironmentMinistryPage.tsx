import { useOverviewUi } from '../i18n/overview'
import EnvironmentMinistryDashboard from '../components/EnvironmentMinistryDashboard'
import ErrorBoundary from '../components/ErrorBoundary'

export default function EnvironmentMinistryPage() {
  const ui = useOverviewUi()
  return (
    <main>
      <ErrorBoundary fallback={<div className="environment-dashboard__error">{ui("Η ενότητα του Υπουργείου Περιβάλλοντος δεν είναι διαθέσιμη προσωρινά.")}</div>}>
        <EnvironmentMinistryDashboard />
      </ErrorBoundary>
    </main>
  )
}
