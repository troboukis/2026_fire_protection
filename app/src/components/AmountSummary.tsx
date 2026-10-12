import type { ReactNode } from 'react'

type AmountSummaryProps = {
  totalCents: number
  missingCount: number
  loading: boolean
  loadFailed: boolean
  ariaLabel: string
  children?: ReactNode
}

export default function AmountSummary({ totalCents, missingCount, loading, loadFailed, ariaLabel, children }: AmountSummaryProps) {
  return (
    <div className="diavgeia-payment-summary" aria-label={ariaLabel} aria-live="polite" aria-busy={loading}>
      <div className="diavgeia-payment-summary__amount">
        {loading || loadFailed ? '—' : (totalCents / 100).toLocaleString('el-GR', {
          style: 'currency', currency: 'EUR', minimumFractionDigits: 2, maximumFractionDigits: 2,
        })}
      </div>
      <div className="diavgeia-payment-summary__missing">
        {loading
          ? 'Φόρτωση…'
          : loadFailed
            ? 'Το σύνολο δεν είναι διαθέσιμο.'
            : missingCount === 1
              ? '1 έγγραφο δεν έχει ποσό'
              : `${missingCount.toLocaleString('el-GR')} έγγραφα δεν έχουν ποσό`}
      </div>
      {children}
    </div>
  )
}
