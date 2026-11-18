import { useAppI18n } from '../i18n/useAppI18n'

type Props = {
  title?: string
  message?: string
  className?: string
  compact?: boolean
}

export default function DataLoadingCard({
  title,
  message,
  className,
  compact = false,
}: Props) {
  const { t } = useAppI18n()
  return (
    <div
      className={`data-loading-card${compact ? ' data-loading-card--compact' : ''}${className ? ` ${className}` : ''}`}
      role="status"
      aria-live="polite"
    >
      <div className="data-loading-card__graphic" aria-hidden="true">
        <span />
        <span />
        <span />
      </div>
      <div className="data-loading-card__copy">
        <strong>{title ?? t('common:loadingData')}</strong>
        <p>{message ?? t('common:retrievingLatest')}</p>
      </div>
    </div>
  )
}
