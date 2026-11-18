import type { To } from 'react-router-dom'
import type { AppLanguage } from '../i18n/languages'

export const STANDALONE_PATH = '/analysis/west-attica-fire-2026'
export const APP_PAGE_PATHS = [
  '/', '/maps', '/municipalities', '/environment-ministry', '/contracts', '/diavgeia',
  '/analysis', '/analysis/statistics', '/analysis/antinero-west-attica', '/analysis/yearly_report', '/analysis/methodology_guide',
  '/media', '/privacy', '/terms',
] as const

export function languageFromPath(pathname: string): AppLanguage {
  return /^\/en(?:\/|$)/.test(pathname) ? 'en' : 'el'
}

export function stripLanguagePath(pathname: string): string {
  return pathname.replace(/^\/en(?=\/|$)/, '') || '/'
}

function splitHref(href: string) {
  const absolute = href.match(/^(https?:\/\/(?:www\.)?fire-watch-app\.gr)(?=\/|[?#]|$)/i)
  const origin = absolute?.[1] ?? ''
  const relative = origin ? href.slice(origin.length) || '/' : href
  if (!relative.startsWith('/') || relative.startsWith('//')) return null
  const [, pathname, suffix] = relative.match(/^([^?#]*)([\s\S]*)$/)!
  return { origin, pathname, suffix }
}

export function isAppPagePath(pathname: string): boolean {
  const path = stripLanguagePath(pathname).replace(/\/+$/, '') || '/'
  return (APP_PAGE_PATHS as readonly string[]).includes(path)
}

export function isStandaloneHref(href: string): boolean {
  const parts = splitHref(href)
  return !!parts && stripLanguagePath(parts.pathname).replace(/\/+$/, '') === STANDALONE_PATH
}

export function localizeHref(href: string, language: AppLanguage): string {
  const parts = splitHref(href)
  if (!parts) return href
  const path = stripLanguagePath(parts.pathname)
  const legacyReport = /^\/reports(?:\/|$)/.test(path) && !/\.[^/]+$/.test(path)
  if (!isAppPagePath(path) && !isStandaloneHref(path) && !legacyReport) return href
  if (language === 'el' && languageFromPath(parts.pathname) === 'el') return href
  const localized = language === 'en' ? `/en${path === '/' ? '/' : path}` : path
  // Leave the query and hash byte-for-byte intact, including duplicate keys,
  // Greek encodings, '+' versus '%20', and third-party tracking parameters.
  return `${parts.origin}${localized}${parts.suffix}`
}

export function localizeTo(to: To, language: AppLanguage): To {
  if (typeof to === 'string') return localizeHref(to, language)
  return to.pathname ? { ...to, pathname: localizeHref(to.pathname, language) } : to
}

export function documentHref(href: string, base = '/'): string {
  if (!href.startsWith('/') || href.startsWith('//')) return href
  return `${base.replace(/\/$/, '')}${href}`
}
