import type { CSSProperties } from 'react'
import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { supabase } from '../lib/supabase'

type CurrentFireRow = {
  incident_key: string
  is_current: boolean
  municipality_key: string | null
  municipality_raw: string | null
  fuel_type: string | null
  start_date: string | null
  status_updated_at: string | null
  last_seen_at: string | null
  status: string | null
}

type FireTickerItem = {
  id: string
  municipalityKey: string | null
  municipalityLabel: string
  fuelType: string
  startDate: string
  status: string
  statusColor?: string
}

type FireStatusCount = {
  status: string
  count: number
  color?: string
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
  'ΠΛΗΡΗΣ ΕΛΕΓΧΟΣ': 'ΥΠΟ ΠΛΗΡΗ ΕΛΕΓΧΟ',
}

const STATUS_COLORS: Record<string, string> = {
  'ΣΕ ΕΞΕΛΙΞΗ': '#b91c1c',
  'ΥΠΟ ΜΕΡΙΚΟ ΕΛΕΓΧΟ': '#c2680a',
  'ΥΠΟ ΠΛΗΡΗ ΕΛΕΓΧΟ': '#166534',
}

const STATUS_ORDER: Record<string, number> = {
  'ΣΕ ΕΞΕΛΙΞΗ': 0,
  'ΥΠΟ ΜΕΡΙΚΟ ΕΛΕΓΧΟ': 1,
  'ΥΠΟ ΠΛΗΡΗ ΕΛΕΓΧΟ': 2,
}

function normalizeStatus(value: string | null): string | null {
  const cleaned = cleanText(value)
  if (!cleaned || cleaned === 'ΛΗΞΗ') return null
  return STATUS_LABELS[cleaned] ?? cleaned
}

function buildStatusCounts(rows: CurrentFireRow[]): FireStatusCount[] {
  const counts = new Map<string, number>()

  for (const row of rows) {
    const status = normalizeStatus(row.status)
    if (!status) continue
    counts.set(status, (counts.get(status) ?? 0) + 1)
  }

  return Array.from(counts.entries())
    .map(([status, count]) => ({
      status,
      count,
      color: STATUS_COLORS[status],
    }))
    .sort((a, b) => {
      const orderA = STATUS_ORDER[a.status] ?? Number.MAX_SAFE_INTEGER
      const orderB = STATUS_ORDER[b.status] ?? Number.MAX_SAFE_INTEGER
      if (orderA !== orderB) return orderA - orderB
      return a.status.localeCompare(b.status, 'el')
    })
}

function buildTickerItem(row: CurrentFireRow): FireTickerItem {
  const status = normalizeStatus(row.status) ?? '—'
  return {
    id: row.incident_key,
    municipalityKey: cleanText(row.municipality_key),
    municipalityLabel: `ΔΗΜΟΣ ${cleanText(row.municipality_raw) ?? '—'}`,
    fuelType: cleanText(row.fuel_type) ?? '—',
    startDate: formatDateEl(cleanText(row.start_date)),
    status,
    statusColor: STATUS_COLORS[status],
  }
}

function renderTickerEntries(items: FireTickerItem[], keyPrefix = '', onClickMunicipality?: (key: string) => void) {
  return items.flatMap((item) => [
    <span key={`${keyPrefix}${item.id}-separator`} className="fire-ticker__separator" aria-hidden="true">
      <span className="fire-ticker__dot" />
    </span>,
    <article
      key={`${keyPrefix}${item.id}`}
      className={`fire-ticker__entry${item.municipalityKey ? ' fire-ticker__entry--clickable' : ''}`}
      onClick={item.municipalityKey && onClickMunicipality ? () => onClickMunicipality(item.municipalityKey!) : undefined}
    >
      <div className="fire-ticker__entry-copy">
        <span className="fire-ticker__entry-eyebrow">{item.municipalityLabel}</span>
        <strong className="fire-ticker__entry-title">{item.fuelType}</strong>
        <span className="fire-ticker__entry-meta">Ξέσπασε: {item.startDate}</span>
        <span className="fire-ticker__entry-meta" style={item.statusColor ? { color: item.statusColor, fontWeight: 700 } : undefined}>{item.status}</span>
      </div>
    </article>,
  ])
}

async function fetchCurrentFires() {
  return supabase
    .from('current_fires')
    .select('incident_key, is_current, municipality_key, municipality_raw, fuel_type, start_date, status_updated_at, last_seen_at, status')
    .eq('is_current', true)
    .or('status.is.null,status.neq.ΛΗΞΗ')
    .order('status_updated_at', { ascending: false, nullsFirst: false })
    .order('last_seen_at', { ascending: false, nullsFirst: false })
}

export default function FireNowTicker() {
  const navigate = useNavigate()
  const [items, setItems] = useState<FireTickerItem[]>([])
  const [activeCount, setActiveCount] = useState<number | null>(null)
  const [statusCounts, setStatusCounts] = useState<FireStatusCount[]>([])
  const [loadFailed, setLoadFailed] = useState(false)
  const [loading, setLoading] = useState(true)
  const [groupCount, setGroupCount] = useState(2)
  const [animDuration, setAnimDuration] = useState(42)

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
        setActiveCount(null)
        setStatusCounts([])
        setLoadFailed(true)
        setLoading(false)
        return
      }

      const rows = (data ?? []) as CurrentFireRow[]
      setLoadFailed(false)
      setActiveCount(rows.length)
      setStatusCounts(buildStatusCounts(rows))
      setItems(rows.slice(0, 12).map(buildTickerItem))
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
    setAnimDuration(Math.round(groupWidth / 35))
  }, [items])

  if (loading) return null

  const titleCount = activeCount == null ? '—' : String(activeCount)

  const handleMunicipalityClick = (key: string) => {
    navigate(`/municipalities?municipality=${encodeURIComponent(key)}`)
  }

  const renderedItems = items.length
    ? items
    : [loadFailed
      ? {
          id: 'error',
          municipalityKey: null,
          municipalityLabel: 'Δήμος —',
          fuelType: 'Δεν ήταν δυνατή η φόρτωση δεδομένων',
          startDate: '—',
          status: '—',
        }
      : {
        id: 'fallback',
        municipalityKey: null,
        municipalityLabel: 'Δήμος —',
        fuelType: 'Δεν υπάρχουν ενεργές πυρκαγιές',
        startDate: '—',
        status: '—',
      }]

  return (
    <div className="fire-ticker" aria-label="Πυρκαγιές Τώρα">
      <div className="fire-ticker__title">
        <span className="eyebrow">live</span>
        <strong>Ενεργές πυρκαγιές: {titleCount}</strong>
        <div className="fire-ticker__status-list" aria-label="Κατανομή ενεργών πυρκαγιών ανά κατάσταση">
          {statusCounts.flatMap((entry, index) => {
            const nodes = []
            if (index > 0) {
              nodes.push(<span key={`${entry.status}-separator`} className="fire-ticker__status-separator" aria-hidden="true" />)
            }
            nodes.push(
              <span
                key={entry.status}
                className="fire-ticker__status-pill"
                style={entry.color ? { color: entry.color } : undefined}
              >
                {entry.count} {entry.status}
              </span>,
            )
            return nodes
          })}
        </div>
      </div>
      <div className="fire-ticker__viewport" ref={viewportRef}>
        <div className="fire-ticker__marquee">
          <div
            className="fire-ticker__track"
            style={{ '--fire-ticker-group-count': groupCount, '--fire-ticker-duration': `${animDuration}s` } as CSSProperties}
          >
            {Array.from({ length: groupCount }, (_, i) => (
              <div
                key={i}
                className="fire-ticker__group"
                ref={i === 0 ? groupRef : undefined}
                aria-hidden={i > 0 ? 'true' : undefined}
              >
                {renderTickerEntries(renderedItems, i === 0 ? '' : `g${i}-`, handleMunicipalityClick)}
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}
