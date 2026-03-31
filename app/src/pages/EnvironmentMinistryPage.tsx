import EnvironmentMinistryDashboard from '../components/EnvironmentMinistryDashboard'
import ErrorBoundary from '../components/ErrorBoundary'

export default function EnvironmentMinistryPage() {
  return (
    <main>
      <ErrorBoundary fallback={<div className="environment-dashboard__error">Η ενότητα του Υπουργείου Περιβάλλοντος δεν είναι διαθέσιμη προσωρινά.</div>}>
        <EnvironmentMinistryDashboard />
      </ErrorBoundary>
    </main>
  )
}
