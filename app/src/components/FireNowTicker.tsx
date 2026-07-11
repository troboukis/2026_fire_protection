import type { CSSProperties } from 'react'
import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import ComponentTag from './ComponentTag'
import DataLoadingCard from './DataLoadingCard'
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

type NoticePlace = {
  name?: unknown
  municipality_name?: unknown
  municipality_key?: unknown
}

type NoticeInstruction = {
  instruction_text?: unknown
  from_places?: NoticePlace[]
  to_places?: NoticePlace[]
}

type NoticeInstructions = {
  affected_places?: NoticePlace[]
  instructions?: NoticeInstruction[]
}

type NoticeRow = {
  notice_id: string
  current_fire_incident_key: string | null
  posted_at: string | null
  notice_type: string | null
  instructions_geocoded: NoticeInstructions | null
  municipality_keys: string[] | null
  matched_municipality_key: string | null
}

type FireTickerItem = {
  kind: 'fire' | 'notice' | 'state'
  id: string
  municipalityKey: string | null
  municipalityLabel: string
  title: string
  primaryMeta: string
  secondaryMeta: string
  statusColor?: string
  hasLocation: boolean
  hoverIncidentKey: string | null
  sortTime: number
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

function formatNoticeTime(value: string | null): string {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'

  const diffMs = Date.now() - date.getTime()
  const minuteMs = 60 * 1000
  const hourMs = 60 * minuteMs

  if (diffMs >= 0 && diffMs < hourMs) {
    const minutes = Math.max(1, Math.round(diffMs / minuteMs))
    return `πριν ${minutes} λεπ.`
  }

  if (diffMs >= 0 && diffMs < 24 * hourMs) {
    const hours = Math.max(1, Math.round(diffMs / hourMs))
    return `πριν ${hours} ώρ.`
  }

  return new Intl.DateTimeFormat('el-GR', {
    hour: '2-digit',
    minute: '2-digit',
  }).format(date)
}

function parseTime(value: string | null): number {
  if (!value) return 0
  const time = new Date(value).getTime()
  return Number.isFinite(time) ? time : 0
}

function truncateText(value: string, maxLength = 110): string {
  if (value.length <= maxLength) return value
  return `${value.slice(0, maxLength - 1).trim()}…`
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
    kind: 'fire',
    id: row.incident_key,
    municipalityKey: cleanText(row.municipality_key),
    municipalityLabel: `ΔΗΜΟΣ ${cleanText(row.municipality_raw) ?? '—'}`,
    title: cleanText(row.fuel_type) ?? '—',
    primaryMeta: `Ξέσπασε: ${formatDateEl(cleanText(row.start_date))}`,
    secondaryMeta: status,
    statusColor: CURRENT_FIRE_STATUS_COLORS[status],
    hasLocation: hasValidCoordinatePair(row.lat, row.lon),
    hoverIncidentKey: row.incident_key,
    sortTime: parseTime(cleanText(row.status_updated_at)) || parseTime(cleanText(row.last_seen_at)) || parseTime(cleanText(row.start_date)),
  }
}

function isNoticeInstructions(value: unknown): value is NoticeInstructions {
  return typeof value === 'object' && value !== null
}

function placeLabel(place: NoticePlace): string | null {
  return cleanText(place.name) ?? cleanText(place.municipality_name)
}

function joinPlaceLabels(places: NoticePlace[] | undefined, limit = 3): string | null {
  const labels = (places ?? [])
    .map(placeLabel)
    .filter((label): label is string => Boolean(label))
  const uniqueLabels = Array.from(new Set(labels))
  if (!uniqueLabels.length) return null
  const visible = uniqueLabels.slice(0, limit).join(', ')
  const extraCount = uniqueLabels.length - limit
  return extraCount > 0 ? `${visible} +${extraCount}` : visible
}

function noticePrimaryMunicipalityKey(instructions: NoticeInstructions | null, row: NoticeRow): string | null {
  const keys = [
    ...(instructions?.affected_places ?? []),
    ...((instructions?.instructions ?? []).flatMap((instruction) => instruction.from_places ?? [])),
  ]
    .map((place) => cleanText(place.municipality_key))
    .filter((key): key is string => Boolean(key))

  return keys[0] ?? cleanText(row.matched_municipality_key) ?? cleanText(row.municipality_keys?.[0])
}

function noticeLocationLabel(instructions: NoticeInstructions | null, row: NoticeRow): string {
  const affected = joinPlaceLabels(instructions?.affected_places)
  if (affected) return `ΠΕΡΙΟΧΗ ${affected}`

  const firstInstruction = instructions?.instructions?.[0]
  const from = joinPlaceLabels(firstInstruction?.from_places)
  if (from) return `ΠΕΡΙΟΧΗ ${from}`

  return row.matched_municipality_key || row.municipality_keys?.[0]
    ? 'ΠΕΡΙΟΧΗ 112'
    : 'ΕΙΔΟΠΟΙΗΣΗ 112'
}

function noticeTitle(row: NoticeRow, instructions: NoticeInstructions | null): string {
  const firstInstruction = instructions?.instructions?.find((instruction) => {
    return cleanText(instruction.instruction_text) || instruction.from_places?.length || instruction.to_places?.length
  })

  const from = joinPlaceLabels(firstInstruction?.from_places)
  const to = joinPlaceLabels(firstInstruction?.to_places)
  if (from && to) return `Απομάκρυνση από ${from} προς ${to}`
  if (from) return `Οδηγία απομάκρυνσης από ${from}`

  const instructionText = cleanText(firstInstruction?.instruction_text)
  if (instructionText) return truncateText(instructionText.replace(/^Ενεργοποίηση\s+112\s*[-–—:]?\s*/i, ''))

  const noticeType = cleanText(row.notice_type)
  if (noticeType) return noticeType

  const affected = joinPlaceLabels(instructions?.affected_places)
  return affected ? 'Προειδοποίηση για δασική πυρκαγιά' : 'Ειδοποίηση 112'
}

function buildNoticeItem(row: NoticeRow): FireTickerItem {
  const instructions = isNoticeInstructions(row.instructions_geocoded) ? row.instructions_geocoded : null
  const municipalityKey = noticePrimaryMunicipalityKey(instructions, row)
  return {
    kind: 'notice',
    id: row.notice_id,
    municipalityKey,
    municipalityLabel: noticeLocationLabel(instructions, row),
    title: noticeTitle(row, instructions),
    primaryMeta: formatNoticeTime(cleanText(row.posted_at)),
    secondaryMeta: '',
    statusColor: '#b45309',
    hasLocation: Boolean(row.current_fire_incident_key),
    hoverIncidentKey: cleanText(row.current_fire_incident_key),
    sortTime: parseTime(cleanText(row.posted_at)),
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
      className={`fire-ticker__entry fire-ticker__entry--${item.kind}${item.id === 'loading' ? ' fire-ticker__entry--loading' : ''}${item.municipalityKey ? ' fire-ticker__entry--clickable' : ''}`}
      onClick={item.municipalityKey && onClickMunicipality ? () => onClickMunicipality(item.municipalityKey!) : undefined}
      onMouseEnter={item.hasLocation && item.hoverIncidentKey && onHoverFire ? () => onHoverFire(item.hoverIncidentKey) : undefined}
      onMouseLeave={item.hasLocation && onHoverFire ? () => onHoverFire(null) : undefined}
      onFocus={item.hasLocation && item.hoverIncidentKey && onHoverFire ? () => onHoverFire(item.hoverIncidentKey) : undefined}
      onBlur={item.hasLocation && onHoverFire ? () => onHoverFire(null) : undefined}
    >
      {item.id === 'loading' ? (
        <DataLoadingCard
          compact
          className="fire-ticker__loading-card"
          message="Ανακτώνται οι ενεργές πυρκαγιές."
        />
      ) : (
        <div className="fire-ticker__entry-copy">
          <span className="fire-ticker__entry-eyebrow">
            {item.kind === 'notice' ? <span className="fire-ticker__notice-badge">112</span> : item.hasLocation ? <LocationIcon /> : null}
            <span>{item.municipalityLabel}</span>
          </span>
          <strong className="fire-ticker__entry-title">{item.title}</strong>
          <span className="fire-ticker__entry-meta">{item.primaryMeta}</span>
          <span className="fire-ticker__entry-meta" style={item.statusColor ? { color: item.statusColor, fontWeight: 700 } : undefined}>{item.secondaryMeta}</span>
        </div>
      )}
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

async function fetchRecent112Notices() {
  return supabase
    .from('112_notice')
    .select('notice_id, current_fire_incident_key, posted_at, notice_type, instructions_geocoded, municipality_keys, matched_municipality_key')
    .order('posted_at', { ascending: false, nullsFirst: false })
    .limit(20)
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
      const [firesResult, noticesResult] = await Promise.all([
        fetchCurrentFires(),
        fetchRecent112Notices(),
      ])

      if (cancelled) return

      if (firesResult.error) {
        if (import.meta.env.DEV) {
          if (firesResult.error) logError('Failed to load current fires for ticker', firesResult.error)
        }
        setItems([])
        setActiveCount(null)
        setStatusCounts([])
        setLoadFailed(true)
        setLoading(false)
        return
      }

      if (noticesResult.error && import.meta.env.DEV) {
        logError('Failed to load 112 notices for ticker', noticesResult.error)
      }

      const rows = (firesResult.data ?? []) as CurrentFireRow[]
      const activeIncidentKeys = new Set(rows.map((row) => row.incident_key))
      const recentCutoff = Date.now() - 24 * 60 * 60 * 1000
      const noticeRows = (noticesResult.error ? [] : (noticesResult.data ?? []) as NoticeRow[]).filter((row) => {
        const postedTime = parseTime(cleanText(row.posted_at))
        return postedTime >= recentCutoff || Boolean(row.current_fire_incident_key && activeIncidentKeys.has(row.current_fire_incident_key))
      })
      const fireItems = rows.map(buildTickerItem)
      const noticeItems = noticeRows.map(buildNoticeItem)
      setLoadFailed(false)
      setActiveCount(rows.length)
      setStatusCounts(buildStatusCounts(rows))
      setItems([...fireItems, ...noticeItems].sort((a, b) => b.sortTime - a.sortTime))
      setLoading(false)
    }

    load()

    const channel = supabase
      .channel('current_fires_ticker')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'current_fires' }, () => {
        if (!cancelled) load()
      })
      .on('postgres_changes', { event: '*', schema: 'public', table: '112_notice' }, () => {
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
        kind: 'state' as const,
        id: 'loading',
        municipalityKey: null,
        municipalityLabel: 'Δήμος —',
        title: 'Ανάκτηση ενεργών πυρκαγιών',
        primaryMeta: '—',
        secondaryMeta: '—',
        hasLocation: false,
        hoverIncidentKey: null,
        sortTime: 0,
      }]
    : items.length
    ? items
    : [loadFailed
      ? {
          kind: 'state' as const,
          id: 'error',
          municipalityKey: null,
          municipalityLabel: 'Δήμος —',
          title: 'Δεν ήταν δυνατή η φόρτωση δεδομένων',
          primaryMeta: '—',
          secondaryMeta: '—',
          hasLocation: false,
          hoverIncidentKey: null,
          sortTime: 0,
        }
      : {
        kind: 'state' as const,
        id: 'fallback',
        municipalityKey: null,
        municipalityLabel: 'Δήμος —',
        title: 'Δεν υπάρχουν ενεργές πυρκαγιές',
        primaryMeta: '—',
        secondaryMeta: '—',
        hasLocation: false,
        hoverIncidentKey: null,
        sortTime: 0,
      }]
  const renderedGroupCount = shouldScroll ? groupCount : 1

  return (
    <section className="fire-ticker-section section-rule dev-tag-anchor" aria-label="Πυρκαγιές Τώρα">
      <div className="dev-tag-stack dev-tag-stack--right">
        <ComponentTag name="FireNowTicker" />
        <ComponentTag name="fire-ticker-section section-rule" kind="CLASS" />
      </div>
      <div className={`fire-ticker${shouldScroll ? '' : ' fire-ticker--static'}${loading ? ' fire-ticker--loading' : ''}`} aria-busy={loading}>
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
