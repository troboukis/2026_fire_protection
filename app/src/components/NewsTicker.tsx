import { useEffect, useRef, useState } from 'react'
import ComponentTag from './ComponentTag'
import DataLoadingCard from './DataLoadingCard'
import NewsArticleCard, { type NewsArticleActiveFire, type NewsArticleCardView } from './NewsArticleCard'
import { createHomepageRpcCacheKey, loadCachedHomepageRpc, retryHomepageRpc } from '../lib/homepageRpcCache'
import { isAbortError } from '../lib/isAbortError'
import { logError } from '../lib/logger'
import { supabase } from '../lib/supabase'

type NewsTickerRpcRow = {
  id: number | string
  article_title: string | null
  source: string | null
  article_url: string | null
  image_url: string | null
  published_at: string | null
  municipality_name: string | null
  area: string | null
  active_fire_incident_key: string | null
}

function cleanText(value: unknown): string | null {
  if (value == null) return null
  const text = String(value).trim()
  if (!text || text.toLowerCase() === 'nan' || text.toLowerCase() === 'none') return null
  return text
}

function formatDateEl(value: string | null, includeTime = false): string {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  const options: Intl.DateTimeFormatOptions = {
    day: '2-digit',
    month: 'short',
    year: 'numeric',
  }
  if (includeTime) {
    options.hour = '2-digit'
    options.minute = '2-digit'
  }
  return new Intl.DateTimeFormat('el-GR', options).format(date)
}

function formatTimeEl(value: string | null): string {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  return new Intl.DateTimeFormat('el-GR', {
    hour: '2-digit',
    minute: '2-digit',
  }).format(date)
}

function buildActiveFire(incidentKey: string | null): NewsArticleActiveFire | null {
  if (!incidentKey) return null
  return {
    incidentKey,
  }
}

function buildNewsArticle(row: NewsTickerRpcRow): NewsArticleCardView | null {
  const articleUrl = cleanText(row.article_url)
  if (!articleUrl) return null

  return {
    id: String(row.id),
    title: cleanText(row.article_title) ?? '—',
    source: cleanText(row.source) ?? '—',
    articleUrl,
    imageUrl: cleanText(row.image_url),
    publishedAt: formatDateEl(cleanText(row.published_at)),
    publishedTime: formatTimeEl(cleanText(row.published_at)),
    municipalityName: cleanText(row.municipality_name),
    area: cleanText(row.area),
    activeFire: buildActiveFire(cleanText(row.active_fire_incident_key)),
  }
}

export default function NewsTicker() {
  const itemsRef = useRef<HTMLDivElement | null>(null)
  const [items, setItems] = useState<NewsArticleCardView[]>([])
  const [loading, setLoading] = useState(true)
  const [loadFailed, setLoadFailed] = useState(false)
  const [canScrollPrev, setCanScrollPrev] = useState(false)
  const [canScrollNext, setCanScrollNext] = useState(false)

  useEffect(() => {
    let cancelled = false
    const controller = new AbortController()
    let hasLoadedOnce = false

    const fetchRpcRows = async () => retryHomepageRpc(async () => {
      const { data, error } = await supabase
        .rpc('get_news_ticker_articles')
        .abortSignal(controller.signal)
      if (error) throw error
      return (data ?? []) as NewsTickerRpcRow[]
    })

    const load = async (useCache: boolean) => {
      try {
        const rows = useCache
          ? await loadCachedHomepageRpc(
            createHomepageRpcCacheKey('get_news_ticker_articles', { schema_version: 2 }),
            fetchRpcRows,
            {
              ttlMs: 5 * 60 * 1000,
              useStaleOnError: true,
              validateData: (data) => Array.isArray(data) && data.length > 0,
            },
          )
          : await fetchRpcRows()

        if (cancelled) return
        setItems(rows.map(buildNewsArticle).filter((item): item is NewsArticleCardView => item != null))
        setLoadFailed(false)
        hasLoadedOnce = true
      } catch (error) {
        if (isAbortError(error)) return
        if (!cancelled) {
          if (import.meta.env.DEV) logError('Failed to load news ticker articles', error)
          if (!hasLoadedOnce) {
            setItems([])
            setLoadFailed(true)
          }
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    load(true)

    const channel = supabase
      .channel('news_fires_ticker')
      .on('postgres_changes', { event: '*', schema: 'public', table: 'news_fires' }, () => {
        if (!cancelled) load(false)
      })
      .on('postgres_changes', { event: '*', schema: 'public', table: 'current_fires' }, () => {
        if (!cancelled) load(false)
      })
      .subscribe()

    return () => {
      cancelled = true
      controller.abort()
      supabase.removeChannel(channel)
    }
  }, [])

  useEffect(() => {
    const container = itemsRef.current
    if (!container) return

    let frameId: number | null = null

    const updatePager = () => {
      const scrollMax = container.scrollWidth - container.clientWidth
      setCanScrollPrev(container.scrollLeft > 1)
      setCanScrollNext(container.scrollLeft < scrollMax - 1)
    }

    const scheduleUpdatePager = () => {
      if (frameId != null) window.cancelAnimationFrame(frameId)
      frameId = window.requestAnimationFrame(updatePager)
    }

    scheduleUpdatePager()
    const observer = new ResizeObserver(scheduleUpdatePager)
    observer.observe(container)
    container.addEventListener('scroll', updatePager, { passive: true })
    window.addEventListener('resize', scheduleUpdatePager)
    window.addEventListener('orientationchange', scheduleUpdatePager)

    return () => {
      observer.disconnect()
      container.removeEventListener('scroll', updatePager)
      window.removeEventListener('resize', scheduleUpdatePager)
      window.removeEventListener('orientationchange', scheduleUpdatePager)
      if (frameId != null) window.cancelAnimationFrame(frameId)
    }
  }, [items, loading])

  const scrollNews = (direction: -1 | 1) => {
    const container = itemsRef.current
    if (!container) return

    const firstCard = container.querySelector<HTMLElement>('.news-article-card, .news-wire__loading-card')
    const step = firstCard?.getBoundingClientRect().width ?? container.clientWidth
    container.scrollBy({ left: direction * (step + 1), behavior: 'smooth' })
  }

  return (
    <section className="news-ticker news-wire section-rule dev-tag-anchor" aria-label="Ροή ειδήσεων πυρκαγιών">
      <div className="dev-tag-stack dev-tag-stack--right">
        <ComponentTag name="NewsTicker" />
        <ComponentTag name="news-ticker news-wire section-rule" kind="CLASS" />
      </div>
      <div className="news-wire__label dev-tag-anchor">
        <span className="eyebrow">ειδήσεις</span>
        <strong>Τα τελευταία ρεπορτάζ για δασικές πυρκαγιές</strong><p className="note-text">Περάστε το ποντίκι πάνω από ένα άρθρο για να αποκαλυφθεί στον χάρτη η γεωγραφική θέση της πυρκαγιάς εφόσον αυτή παραμένει ενεργή.</p>
        <div className="news-wire__pager" aria-label="Πλοήγηση άρθρων ειδήσεων">
          <button
            type="button"
            className="news-wire__pager-button"
            aria-label="Προηγούμενο άρθρο"
            onClick={() => scrollNews(-1)}
            disabled={loading || !canScrollPrev}
          >
            ‹
          </button>
          <button
            type="button"
            className="news-wire__pager-button"
            aria-label="Επόμενο άρθρο"
            onClick={() => scrollNews(1)}
            disabled={loading || !canScrollNext}
          >
            ›
          </button>
        </div>
      </div>
      <div className="news-wire__items dev-tag-anchor" ref={itemsRef}>
        {loading && (
          <DataLoadingCard
            className="news-wire__loading-card"
            message="Ανακτώνται τα τελευταία ρεπορτάζ πυρκαγιών."
          />
        )}
        {!loading && items.map((item, index) => (
          <NewsArticleCard
            key={item.id}
            item={item}
            priority={index === 0}
          />
        ))}
        {!loading && loadFailed && (
          <article className="wire-item news-article-card">
            <h2>Δεν ήταν δυνατή η φόρτωση των τελευταίων ρεπορτάζ.</h2>
          </article>
        )}
        {!loading && !loadFailed && items.length === 0 && (
          <article className="wire-item news-article-card">
            <h2>Δεν βρέθηκαν πρόσφατα ρεπορτάζ.</h2>
          </article>
        )}
      </div>
    </section>
  )
}
