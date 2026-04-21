import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { supabase } from '../lib/supabase'

type CurrentFireRow = {
  incident_key: string
  municipality_raw: string | null
  fuel_type: string | null
  start_date: string | null
  status_updated_at: string | null
  last_seen_at: string | null
  status: string | null
}

type FireTickerItem = {
  id: string
  municipalityLabel: string
  fuelType: string
  startDate: string
  status: string
}

function cleanText(value: unknown): string | null {
  if (value == null) return null
  const text = String(value).trim()
  if (!text || text.toLowerCase() === 'nan' || text.toLowerCase() === 'none') return null
  return text
}

function formatDateEl(value: string | null): string {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  return new Intl.DateTimeFormat('el-GR', {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
  }).format(date)
}

const STATUS_LABELS: Record<string, string> = {
  'ΜΕΡΙΚΟΣ ΕΛΕΓΧΟΣ': 'ΥΠΟ ΜΕΡΙΚΟ ΕΛΕΓΧΟ',
}

function buildTickerItem(row: CurrentFireRow): FireTickerItem {
  return {
    id: row.incident_key,
    municipalityLabel: `ΔΗΜΟΣ ${cleanText(row.municipality_raw) ?? '—'}`,
    fuelType: cleanText(row.fuel_type) ?? '—',
    startDate: formatDateEl(cleanText(row.start_date)),
    status: STATUS_LABELS[cleanText(row.status) ?? ''] ?? cleanText(row.status) ?? '—',
  }
}

function renderTickerEntries(items: FireTickerItem[], keyPrefix = '') {
  return items.flatMap((item) => [
    <span key={`${keyPrefix}${item.id}-separator`} className="fire-ticker__separator" aria-hidden="true">
      <span className="fire-ticker__dot" />
    </span>,
    <article key={`${keyPrefix}${item.id}`} className="fire-ticker__entry">
      <div className="fire-ticker__entry-copy">
        <span className="fire-ticker__entry-eyebrow">{item.municipalityLabel}</span>
        <strong className="fire-ticker__entry-title">{item.fuelType}</strong>
        <span className="fire-ticker__entry-meta">Ξέσπασε: {item.startDate}</span>
        <span className="fire-ticker__entry-meta">{item.status}</span>
      </div>
    </article>,
  ])
}

async function fetchCurrentFires() {
  return supabase
    .from('current_fires')
    .select('incident_key, municipality_raw, fuel_type, start_date, status_updated_at, last_seen_at, status')
    .or('status.is.null,status.neq.ΛΗΞΗ')
    .order('status_updated_at', { ascending: false, nullsFirst: false })
    .order('last_seen_at', { ascending: false, nullsFirst: false })
    .limit(12)
}

export default function FireNowTicker() {
  const [items, setItems] = useState<FireTickerItem[]>([])
  const [loadFailed, setLoadFailed] = useState(false)
  const [loading, setLoading] = useState(true)
  const [groupCount, setGroupCount] = useState(2)

  const viewportRef = useRef<HTMLDivElement>(null)
  const groupRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    let cancelled = false

    const load = async () => {
      const { data, error } = await fetchCurrentFires()

      if (cancelled) return

      if (error) {
        console.error('Failed to load current fires for ticker', error)
        setItems([])
        setLoadFailed(true)
        setLoading(false)
        return
      }

      setLoadFailed(false)
      setItems(((data ?? []) as CurrentFireRow[]).map(buildTickerItem))
      setLoading(false)
    }

    load()

    const channel = supabase
      .channel('current_fires_ticker')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'current_fires' }, () => {
        if (!cancelled) load()
      })
      .subscribe()

    return () => {
      cancelled = true
      supabase.removeChannel(channel)
    }
  }, [])

  // After items render, measure widths and calculate how many groups are needed
  // so the track always fills the viewport with no gap at the loop point.
  useLayoutEffect(() => {
    const viewport = viewportRef.current
    const group = groupRef.current
    if (!viewport || !group) return

    const groupWidth = group.offsetWidth
    if (groupWidth === 0) return

    const viewportWidth = viewport.offsetWidth
    // Need at least 2× viewport width worth of groups so no gap is ever visible.
    const needed = Math.ceil((viewportWidth * 2) / groupWidth) + 1
    setGroupCount(Math.max(2, needed))
  }, [items])

  if (loading) return null

  const renderedItems = items.length
    ? items
    : [loadFailed
      ? {
          id: 'error',
          municipalityLabel: 'Δήμος —',
          fuelType: 'Δεν ήταν δυνατή η φόρτωση δεδομένων',
          startDate: '—',
          status: '—',
        }
      : {
        id: 'fallback',
        municipalityLabel: 'Δήμος —',
        fuelType: 'Δεν υπάρχουν ενεργές πυρκαγιές',
        startDate: '—',
        status: '—',
      }]

  return (
    <div className="fire-ticker" aria-label="Πυρκαγιές Τώρα">
      <div className="fire-ticker__title">
        <span className="eyebrow">live</span>
        <strong>Ενεργές πυρκαγιές</strong>
        <div className="fire-ticker__source record-beneficiary-row__meta">
          <span>ΠΗΓΗ: Πυροσβεστικό Σώμα Ελλάδας</span>
        </div>
      </div>
      <div className="fire-ticker__viewport" ref={viewportRef}>
        <div className="fire-ticker__marquee">
          <div
            className="fire-ticker__track"
            style={{ '--fire-ticker-group-count': groupCount } as React.CSSProperties}
          >
            {Array.from({ length: groupCount }, (_, i) => (
              <div
                key={i}
                className="fire-ticker__group"
                ref={i === 0 ? groupRef : undefined}
                aria-hidden={i > 0 ? 'true' : undefined}
              >
                {renderTickerEntries(renderedItems, i === 0 ? '' : `g${i}-`)}
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}
