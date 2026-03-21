import { useMemo } from 'react'

export type ProfileSignalBarItem = {
  label: string
  value: number
  meta?: string
  tone?: 'accent' | 'ink' | 'soft'
}

type Props = {
  items: ProfileSignalBarItem[]
  emptyLabel?: string
}

export default function ProfileSignalBars({ items, emptyLabel = 'Δεν υπάρχουν αρκετά δεδομένα.' }: Props) {
  const maxValue = useMemo(
    () => Math.max(1, ...items.map((item) => item.value).filter((value) => Number.isFinite(value))),
    [items],
  )

  if (items.length === 0) {
    return <div className="profile-signal-bars profile-signal-bars--empty">{emptyLabel}</div>
  }

  return (
    <div className="profile-signal-bars">
      {items.map((item) => {
        const width = `${Math.max(0, Math.min(100, (item.value / maxValue) * 100))}%`
        return (
          <div className="profile-signal-bars__row" key={`${item.label}-${item.meta ?? ''}`}>
            <div className="profile-signal-bars__copy">
              <strong>{item.label}</strong>
              {item.meta && <span>{item.meta}</span>}
            </div>
            <div className="profile-signal-bars__track" aria-hidden="true">
              <div
                className={`profile-signal-bars__fill profile-signal-bars__fill--${item.tone ?? 'accent'}`}
                style={{ width }}
              />
            </div>
          </div>
        )
      })}
    </div>
  )
}
