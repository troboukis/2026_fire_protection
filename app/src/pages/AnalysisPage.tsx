import ContractAnalysis from '../components/ContractAnalysis'
import DirectAwardHistogram from '../components/DirectAwardHistogram'
import ErrorBoundary from '../components/ErrorBoundary'

export default function AnalysisPage() {
  return (
    <main>
      <ErrorBoundary fallback={<div className="ca-empty-note">Η ενότητα ανάλυσης δεν είναι διαθέσιμη προσωρινά.</div>}>
        <ContractAnalysis />
      </ErrorBoundary>
      <ErrorBoundary fallback={<div className="ca-empty-note">Η ενότητα κατανομής δεν είναι διαθέσιμη προσωρινά.</div>}>
        <DirectAwardHistogram />
      </ErrorBoundary>
    </main>
  )
}
