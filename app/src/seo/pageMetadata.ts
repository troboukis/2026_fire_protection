import pages from './pages.json'
import greek from '../i18n/catalogs/metadata.el.json'
import english from '../i18n/catalogs/metadata.en.json'
import type { AppLanguage } from '../i18n/languages'
import { documentHref, languageFromPath, localizeHref, stripLanguagePath } from '../routing/localizedHref'

export const SITE_ORIGIN = 'https://www.fire-watch-app.gr'
export const metadataPages = pages
export type MetadataPage = (typeof pages)[number]
export type PageMetadata = {
  page: MetadataPage
  language: AppLanguage
  title: string
  description: string
  socialTitle: string
  canonical: string
  alternates: { language: AppLanguage | 'x-default'; href: string }[]
  image: string
  imageWidth: number
  imageHeight: number
  imageAlt: string
  locale: 'el_GR' | 'en_GB'
  alternateLocale: 'el_GR' | 'en_GB'
  // Page copy that is temporarily shown in Greek on the English route as well.
  greekOnly: boolean
}

const targets = new Map(Object.entries(greek).map(([key, source]) => [source, english[key as keyof typeof english]]))
export function metadataText(source: string, language: AppLanguage): string {
  return language === 'en' ? targets.get(source) ?? source : source
}

// Canonicalization changes metadata only. It never rewrites the visited URL,
// search parameters, selected records or the language switcher's destination.
export function getPageMetadata(pathname: string): PageMetadata | null {
  const path = pathname.split(/[?#]/, 1)[0]
  const language = languageFromPath(path)
  const stripped = stripLanguagePath(path).replace(/\/+$/, '') || '/'
  const current = stripped.replace(/^\/reports(?=\/|$)/, '/analysis')
  const page = pages.find(item => (item.path.replace(/\/$/, '') || '/') === current)
  if (!page) return null
  const url = (target: AppLanguage) => SITE_ORIGIN + localizeHref(page.path, target)
  return {
    page, language,
    title: metadataText(page.title, language),
    description: metadataText(page.description, language),
    socialTitle: metadataText(page.socialTitle, language),
    canonical: url(language),
    alternates: [{ language: 'el', href: url('el') }, { language: 'en', href: url('en') }, { language: 'x-default', href: url('el') }],
    image: SITE_ORIGIN + (language === 'en' ? page.imageEn ?? page.image : page.image),
    imageWidth: language === 'en' ? page.imageWidthEn ?? page.imageWidth : page.imageWidth,
    imageHeight: language === 'en' ? page.imageHeightEn ?? page.imageHeight : page.imageHeight,
    imageAlt: metadataText(language === 'en' ? page.imageAltEn ?? page.imageAlt : page.imageAlt, language),
    locale: language === 'en' ? 'en_GB' : 'el_GR',
    alternateLocale: language === 'en' ? 'el_GR' : 'en_GB',
    greekOnly: language === 'en' && !!page.englishPending,
  }
}

export function metadataDocument(pathname: string): string | null {
  const metadata = getPageMetadata(pathname)
  if (!metadata) return null
  const path = localizeHref(metadata.page.path, metadata.language).replace(/^\//, '').replace(/\/$/, '')
  return path ? `${path}/index.html` : 'index.html'
}

export type HeadElement = { tag: 'meta' | 'link'; attributes: Record<string, string> }
export function metadataElements(metadata: PageMetadata): HeadElement[] {
  const meta = (key: string, content: string, property = false): HeadElement => ({ tag: 'meta', attributes: { [property ? 'property' : 'name']: key, content } })
  return [
    meta('description', metadata.description),
    { tag: 'link', attributes: { rel: 'canonical', href: metadata.canonical } },
    ...metadata.alternates.map(({ language, href }): HeadElement => ({ tag: 'link', attributes: { rel: 'alternate', hreflang: language, href } })),
    meta('og:type', metadata.page.type, true), meta('og:locale', metadata.locale, true),
    meta('og:locale:alternate', metadata.alternateLocale, true), meta('og:site_name', 'FireWatch', true),
    meta('og:title', metadata.socialTitle, true), meta('og:description', metadata.description, true),
    meta('og:url', metadata.canonical, true), meta('og:image', metadata.image, true),
    meta('og:image:width', String(metadata.imageWidth), true), meta('og:image:height', String(metadata.imageHeight), true),
    meta('og:image:alt', metadata.imageAlt, true),
    meta('twitter:card', 'summary_large_image'), meta('twitter:title', metadata.socialTitle),
    meta('twitter:description', metadata.description), meta('twitter:image', metadata.image), meta('twitter:image:alt', metadata.imageAlt),
  ]
}

export function fallbackNavigation(metadata: PageMetadata, base = '/') {
  return pages.filter(page => page.navigation).map(page => ({
    href: documentHref(localizeHref(page.path, metadata.language), base),
    title: metadataText(page.title, metadata.language).replace(/\s*\|\s*FireWatch$/, ''),
    lang: metadata.language === 'en' && page.englishPending ? 'el' : undefined,
  }))
}
