import ComponentTag from './ComponentTag'

type TopAuthorityItem = {
  name: string
  contracts: number
  total_m: number
}

type Props = {
  rows: TopAuthorityItem[]
  maxValue: number
}

export default function TopAuthoritiesSection({ rows, maxValue }: Props) {
  const midpoint = Math.ceil(rows.length / 2)
  const columns = [rows.slice(0, midpoint), rows.slice(midpoint)]

  return (
    <div className="ca-table-block">
      <ComponentTag name="TopAuthoritiesSection" />
      <div className="eyebrow">Κορυφαίοι Φορείς κατά Συνολική Δαπάνη (χ.ΦΠΑ)</div>
      <div className="ca-double-grid">
        {columns.map((column, columnIdx) => (
          <div className="ca-breakdown-block" key={`top-org-col-${columnIdx}`}>
            <div className="ca-bars">
              {column.map((org, idx) => (
                <div className="ca-bar-row" key={`${org.name}-${columnIdx}-${idx}`}>
                  <div className="ca-bar-label">
                    <span className="ca-top-org-label">{org.name}</span>
                    <span className="ca-top-org-value ca-accent ca-mono">{org.total_m.toFixed(1)}M €</span>
                  </div>
                  <div className="ca-bar-track">
                    <div className="ca-bar-fill" style={{ width: `${maxValue > 0 ? (org.total_m / maxValue) * 100 : 0}%`, background: 'rgba(211,72,45,0.65)' }} />
                  </div>
                  <div className="ca-bar-meta">
                    <span className="ca-mono">{org.contracts.toLocaleString('el-GR')} συμβάσεις</span>
                    <strong>{org.total_m.toFixed(1)}M €</strong>
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
    </div>
  )
}
