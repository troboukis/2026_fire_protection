import type { ReactNode } from 'react'
import { useAppI18n } from '../i18n/useAppI18n'

type AmountSummaryProps = {
  totalCents: number
  missingCount: number
  loading: boolean
  loadFailed: boolean
  ariaLabel: string
  children?: ReactNode
}

export default function AmountSummary({ totalCents, missingCount, loading, loadFailed, ariaLabel, children }: AmountSummaryProps) {
  const { t, format } = useAppI18n()
  return (
    <div className="diavgeia-payment-summary" aria-label={ariaLabel} aria-live="polite" aria-busy={loading}>
      <div className="diavgeia-payment-summary__amount">
        {loading || loadFailed ? '—' : format.currency(totalCents / 100)}
      </div>
      <div className="diavgeia-payment-summary__missing">
        {loading
          ? t('common:loading')
          : loadFailed
            ? t('common:totalUnavailable')
            : t('common:documentsWithoutAmount', { count: missingCount })}
      </div>
      {children}
    </div>
  )
}
