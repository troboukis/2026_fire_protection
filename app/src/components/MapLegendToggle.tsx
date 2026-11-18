import { useOverviewUi } from '../i18n/overview'
import type { ReactNode } from 'react'

type MapLegendToggleProps = {
  visible: boolean
  onToggle: () => void
  children: ReactNode
  className?: string
  label: string
}

export default function MapLegendToggle({
  visible,
  onToggle,
  children,
  className = '',
  label,
}: MapLegendToggleProps) {
  const ui = useOverviewUi()
  return (
    <button
      type="button"
      className={`map-legend-toggle${visible ? '' : ' is-hidden'}${className ? ` ${className}` : ''}`}
      aria-pressed={visible}
      aria-label={`${visible ? ui("Απόκρυψη") : ui("Εμφάνιση")}: ${label}`}
      onClick={onToggle}
    >
      {children}
    </button>
  )
}
