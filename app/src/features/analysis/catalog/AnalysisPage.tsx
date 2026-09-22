import AnalysisCard from './AnalysisCard'
import './AnalysisCatalog.css'

export default function AnalysisPage() {
  return (
    <main className="analysis-catalog">
      <AnalysisCard
        to="/analysis/west-attica-fire-2026/"
        title="Πώς επηρέασαν τα AntiNERO έργα τη μεγάλη πυρκαγιά της Δυτικής Αττικής το 2026;"
        description="WWF, Flame και FireWatch παρουσιάζουν μία ανάλυση της μεγαλύτερης πυρκαγιάς του 2026."
        imageSrc={`${import.meta.env.BASE_URL}social/west-attica-fire-2026.png`}
        imageAlt="Χάρτης της μεγάλης πυρκαγιάς στη Δυτική Αττική και τη Βοιωτία το 2026"
        reloadDocument
      />
      <AnalysisCard
        to="/analysis/statistics"
        title="Στατιστική Ανάλυση"
        description="Δείτε τη στατιστική εικόνα των δημόσιων συμβάσεων πυροπροστασίας από το 2024 έως σήμερα, με στοιχεία για τις δαπάνες, τις απευθείας αναθέσεις και τους αναδόχους."
        imageSrc={`${import.meta.env.BASE_URL}social/statistical-analysis.png`}
        imageAlt="Διάγραμμα της στατιστικής ανάλυσης συμβάσεων πυροπροστασίας"
      />
      <AnalysisCard
        to="/analysis/antinero-west-attica"
        title="Συμβάσεις AntiNERO στη Δυτική Αττική"
        description="Περιηγηθείτε στο «δίκτυο» των συμβάσεων και εγγράφων από τη Διαύγεια για τα έργα AntiNERO στη Δυτική Αττική. Δείτε τους αναδόχους, τα ποσά και το ιστορικό κάθε σύμβασης από την ανάθεση έως την παραλαβή του έργου."
        imageSrc={`${import.meta.env.BASE_URL}social/antinero-west-attica.png`}
        imageAlt="Δίκτυο συμβάσεων και αποφάσεων AntiNERO στη Δυτική Αττική"
      />
    </main>
  )
}
