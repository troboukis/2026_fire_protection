import ComponentTag from './ComponentTag'
import DataLoadingCard from './DataLoadingCard'

type TopAuthorityItem = {
  name: string
  contracts: number
  total_m: number
}

type Props = {
  rows: TopAuthorityItem[]
  totalSpendM: number
  loading?: boolean
}

export default function TopAuthoritiesSection({ rows, totalSpendM, loading = false }: Props) {
  const sortedRows = [...rows]
    .map((row) => ({
      ...row,
      pct: totalSpendM > 0 ? (row.total_m / totalSpendM) * 100 : 0,
    }))
    .sort((a, b) => b.pct - a.pct || b.total_m - a.total_m || a.name.localeCompare(b.name, 'el'))
  const midpoint = Math.ceil(sortedRows.length / 2)
  const columns = [sortedRows.slice(0, midpoint), sortedRows.slice(midpoint)]

  return (
    <div className="ca-table-block">
      <ComponentTag name="TopAuthoritiesSection" />
      <div className="eyebrow">Κορυφαίοι Φορείς κατά Συνολική Δαπάνη (χ.ΦΠΑ)</div>
      {loading ? (
        <DataLoadingCard message="Ανακτάται η κατάταξη φορέων με βάση τη συνολική δαπάνη." />
      ) : (
        <>
          <div className="ca-double-grid">
            {columns.map((column, columnIdx) => (
              <div className="ca-breakdown-block" key={`top-org-col-${columnIdx}`}>
                <div className="ca-bars">
                  {column.map((org, idx) => (
                    <div className="ca-bar-row" key={`${org.name}-${columnIdx}-${idx}`}>
                      <div className="ca-bar-label">
                        <span className="ca-bar-title ca-top-org-label">
                          <span>{org.name}</span>
                          <span className="ca-bar-title__dot" aria-hidden="true" />
                          <span className="ca-bar-title__meta">
                            <strong>€ {org.total_m.toFixed(1)}M</strong>
                            <span> ({org.contracts.toLocaleString('el-GR')} συμβ.)</span>
                          </span>
                        </span>
                        <span className="ca-bar-pct">{org.pct.toFixed(1)}%</span>
                      </div>
                      <div className="ca-bar-track">
                        <div className="ca-bar-fill" style={{ width: `${org.pct}%`, background: 'rgba(211,72,45,0.65)' }} />
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
          <p className="ca-sub-note">
            Δυναμική κατάταξη φορέων με βάση τη συνολική δαπάνη χωρίς ΦΠΑ, όπως προκύπτει από τα δημοσιευμένα στοιχεία. Τα στοιχεία που βλέπετε επηρεάζονται από το φίλτρο έτους που έχετε επιλέξει.
          </p>
        </>
      )}
    </div>
  )
}
