type Props = {
  title?: string
  message?: string
  className?: string
  compact?: boolean
}

export default function DataLoadingCard({
  title = 'Φόρτωση δεδομένων',
  message = 'Ανακτώνται τα πιο πρόσφατα στοιχεία.',
  className,
  compact = false,
}: Props) {
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
        <strong>{title}</strong>
        <p>{message}</p>
      </div>
    </div>
  )
}
