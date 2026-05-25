import ContractAnalysis from '../components/ContractAnalysis'
import ErrorBoundary from '../components/ErrorBoundary'

export default function AnalysisPage() {
  return (
    <main>
      <ErrorBoundary fallback={<div className="ca-empty-note">Η ενότητα ανάλυσης δεν είναι διαθέσιμη προσωρινά.</div>}>
        <ContractAnalysis />
      </ErrorBoundary>
    </main>
  )
}
