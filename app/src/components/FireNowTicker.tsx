import type { CSSProperties } from 'react'
import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import ComponentTag from './ComponentTag'
import { dispatchCurrentFireHover } from '../lib/currentFireHover'
import {
  CURRENT_FIRE_STATUS_COLORS,
  CURRENT_FIRE_STATUS_ORDER,
  normalizeCurrentFireStatus,
} from '../lib/currentFireStatus'
import { logError } from '../lib/logger'
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
  lat: number | string | null
  lon: number | string | null
}

type FireTickerItem = {
  id: string
  municipalityKey: string | null
  municipalityLabel: string
  fuelType: string
  startDate: string
  status: string
  statusColor?: string
  hasLocation: boolean
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

function normalizeStatus(value: string | null): string | null {
  return normalizeCurrentFireStatus(cleanText(value))
}

function hasValidCoordinatePair(lat: unknown, lon: unknown): boolean {
  const parsedLat = Number(lat)
  const parsedLon = Number(lon)
  return Number.isFinite(parsedLat) && Number.isFinite(parsedLon)
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
      color: CURRENT_FIRE_STATUS_COLORS[status],
    }))
    .sort((a, b) => {
      const orderA = CURRENT_FIRE_STATUS_ORDER[a.status] ?? Number.MAX_SAFE_INTEGER
      const orderB = CURRENT_FIRE_STATUS_ORDER[b.status] ?? Number.MAX_SAFE_INTEGER
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
    statusColor: CURRENT_FIRE_STATUS_COLORS[status],
    hasLocation: hasValidCoordinatePair(row.lat, row.lon),
  }
}

function LocationIcon() {
  return (
    <svg
      className="fire-ticker__location-icon"
      viewBox="0 0 24 24"
      aria-label="Έχει γεωγραφική θέση"
      role="img"
    >
      <path
        d="M12 21s6-5.32 6-11a6 6 0 1 0-12 0c0 5.68 6 11 6 11Z"
        fill="none"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="2"
      />
      <circle cx="12" cy="10" r="2" fill="currentColor" />
    </svg>
  )
}

function renderTickerEntries(
  items: FireTickerItem[],
  keyPrefix = '',
  onClickMunicipality?: (key: string) => void,
  onHoverFire?: (incidentKey: string | null) => void,
) {
  return items.flatMap((item) => [
    <span key={`${keyPrefix}${item.id}-separator`} className="fire-ticker__separator" aria-hidden="true">
      <span className="fire-ticker__dot" />
    </span>,
    <article
      key={`${keyPrefix}${item.id}`}
      className={`fire-ticker__entry${item.municipalityKey ? ' fire-ticker__entry--clickable' : ''}`}
      onClick={item.municipalityKey && onClickMunicipality ? () => onClickMunicipality(item.municipalityKey!) : undefined}
      onMouseEnter={item.hasLocation && onHoverFire ? () => onHoverFire(item.id) : undefined}
      onMouseLeave={item.hasLocation && onHoverFire ? () => onHoverFire(null) : undefined}
      onFocus={item.hasLocation && onHoverFire ? () => onHoverFire(item.id) : undefined}
      onBlur={item.hasLocation && onHoverFire ? () => onHoverFire(null) : undefined}
    >
      <div className="fire-ticker__entry-copy">
        <span className="fire-ticker__entry-eyebrow">
          {item.hasLocation ? <LocationIcon /> : null}
          <span>{item.municipalityLabel}</span>
        </span>
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
    .select('incident_key, is_current, municipality_key, municipality_raw, fuel_type, start_date, status_updated_at, last_seen_at, status, lat, lon')
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
  const [isMobileTicker, setIsMobileTicker] = useState(() => window.matchMedia('(max-width: 680px)').matches)
  const [scrollForFit, setScrollForFit] = useState(false)

  const viewportRef = useRef<HTMLDivElement>(null)
  const groupRef = useRef<HTMLDivElement>(null)
  const exceedsCountThreshold = (activeCount ?? 0) > (isMobileTicker ? 1 : 4)
  const shouldScroll = !loadFailed && (exceedsCountThreshold || scrollForFit)

  useEffect(() => {
    const media = window.matchMedia('(max-width: 680px)')
    const handleChange = () => setIsMobileTicker(media.matches)
    handleChange()
    media.addEventListener('change', handleChange)
    return () => media.removeEventListener('change', handleChange)
  }, [])

  useEffect(() => {
    let cancelled = false

    const load = async () => {
      const { data, error } = await fetchCurrentFires()

      if (cancelled) return

      if (error) {
        if (import.meta.env.DEV) logError('Failed to load current fires for ticker', error)
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
      setItems(rows.map(buildTickerItem))
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

  useLayoutEffect(() => {
    if (loadFailed || !items.length || exceedsCountThreshold) {
      setScrollForFit(false)
      return
    }

    let frameId: number | null = null

    const measureFit = () => {
      const viewport = viewportRef.current
      const group = groupRef.current
      if (!viewport || !group) return

      const groupStyle = window.getComputedStyle(group)
      const inlinePadding =
        Number.parseFloat(groupStyle.paddingLeft || '0') +
        Number.parseFloat(groupStyle.paddingRight || '0')
      const contentWidth = Array.from(group.children).reduce((total, child) => {
        return total + child.getBoundingClientRect().width
      }, inlinePadding)
      const firstChildTop = group.children[0]?.getBoundingClientRect().top
      const wrapsToMultipleLines = Array.from(group.children).some((child) => {
        if (firstChildTop == null) return false
        return Math.abs(child.getBoundingClientRect().top - firstChildTop) > 1
      })
      const viewportWidth = viewport.getBoundingClientRect().width

      setScrollForFit((current) => {
        const next = wrapsToMultipleLines || contentWidth > viewportWidth + 1
        return current === next ? current : next
      })
    }

    const scheduleMeasure = () => {
      if (frameId != null) window.cancelAnimationFrame(frameId)
      frameId = window.requestAnimationFrame(measureFit)
    }

    scheduleMeasure()

    const viewport = viewportRef.current
    const group = groupRef.current
    if (!viewport || !group) return

    const observer = new ResizeObserver(scheduleMeasure)
    observer.observe(viewport)
    observer.observe(group)
    window.addEventListener('orientationchange', scheduleMeasure)

    return () => {
      observer.disconnect()
      window.removeEventListener('orientationchange', scheduleMeasure)
      if (frameId != null) window.cancelAnimationFrame(frameId)
    }
  }, [exceedsCountThreshold, items, loadFailed])

  useLayoutEffect(() => {
    if (!shouldScroll) {
      setGroupCount(1)
      setAnimDuration(42)
      return
    }

    let frameId: number | null = null

    const measure = () => {
      const viewport = viewportRef.current
      const group = groupRef.current
      if (!viewport || !group) return

      const groupWidth = group.offsetWidth
      if (groupWidth === 0) return

      const viewportWidth = viewport.offsetWidth
      const needed = Math.ceil((viewportWidth * 2) / groupWidth) + 1
      setGroupCount(Math.max(2, needed))
      setAnimDuration(Math.round(groupWidth / 35))
    }

    const scheduleMeasure = () => {
      if (frameId != null) window.cancelAnimationFrame(frameId)
      frameId = window.requestAnimationFrame(measure)
    }

    scheduleMeasure()

    const viewport = viewportRef.current
    const group = groupRef.current
    if (!viewport || !group) return

    const observer = new ResizeObserver(scheduleMeasure)
    observer.observe(viewport)
    observer.observe(group)
    window.addEventListener('orientationchange', scheduleMeasure)

    return () => {
      observer.disconnect()
      window.removeEventListener('orientationchange', scheduleMeasure)
      if (frameId != null) window.cancelAnimationFrame(frameId)
    }
  }, [items, shouldScroll])

  const titleCount = loading || activeCount == null ? '—' : String(activeCount)

  const handleMunicipalityClick = (key: string) => {
    navigate(`/municipalities?municipality=${encodeURIComponent(key)}`)
  }

  const handleFireHover = (incidentKey: string | null) => {
    dispatchCurrentFireHover(incidentKey)
  }

  const visibleStatusCounts = loading
    ? [{ status: 'Ανάκτηση δεδομένων', count: 0 }]
    : statusCounts

  const renderedItems = loading
    ? [{
        id: 'loading',
        municipalityKey: null,
        municipalityLabel: 'Δήμος —',
        fuelType: 'Ανάκτηση ενεργών πυρκαγιών',
        startDate: '—',
        status: '—',
        hasLocation: false,
      }]
    : items.length
    ? items
    : [loadFailed
      ? {
          id: 'error',
          municipalityKey: null,
          municipalityLabel: 'Δήμος —',
          fuelType: 'Δεν ήταν δυνατή η φόρτωση δεδομένων',
          startDate: '—',
          status: '—',
          hasLocation: false,
        }
      : {
        id: 'fallback',
        municipalityKey: null,
        municipalityLabel: 'Δήμος —',
        fuelType: 'Δεν υπάρχουν ενεργές πυρκαγιές',
        startDate: '—',
        status: '—',
        hasLocation: false,
      }]
  const renderedGroupCount = shouldScroll ? groupCount : 1

  return (
    <section className="fire-ticker-section section-rule dev-tag-anchor" aria-label="Πυρκαγιές Τώρα">
      <div className="dev-tag-stack dev-tag-stack--right">
        <ComponentTag name="FireNowTicker" />
        <ComponentTag name="fire-ticker-section section-rule" kind="CLASS" />
      </div>
      <div className={`fire-ticker${shouldScroll ? '' : ' fire-ticker--static'}`}>
        <div className="fire-ticker__title">
          <span className="eyebrow">live</span>
          <strong>Ενεργές πυρκαγιές: {titleCount}</strong>
          <div className="fire-ticker__status-list" aria-label="Κατανομή ενεργών πυρκαγιών ανά κατάσταση">
            {visibleStatusCounts.map((entry, index) => {
              const nodes = []
              if (!loading && index > 0) {
                nodes.push(<span key={`${entry.status}-separator`} className="fire-ticker__status-separator" aria-hidden="true" />)
              }
              nodes.push(
                <span
                  key={entry.status}
                  className="fire-ticker__status-pill"
                  style={entry.color ? { color: entry.color } : undefined}
                >
                  {loading ? entry.status : `${entry.count} ${entry.status}`}
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
              style={{ '--fire-ticker-group-count': renderedGroupCount, '--fire-ticker-duration': `${animDuration}s` } as CSSProperties}
            >
              {Array.from({ length: renderedGroupCount }, (_, i) => (
                <div
                  key={i}
                  className="fire-ticker__group"
                  ref={i === 0 ? groupRef : undefined}
                  aria-hidden={i > 0 ? 'true' : undefined}
                >
                  {renderTickerEntries(renderedItems, i === 0 ? '' : `g${i}-`, handleMunicipalityClick, handleFireHover)}
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
