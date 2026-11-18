import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import { getPageMetadata, metadataDocument, metadataElements, metadataPages, SITE_ORIGIN } from './pageMetadata'
import { applyPageMetadata, fillStaticFallback, metadataSitemap, staticFallback } from './metadataHtml'
import { publishedPages, publishedDeepLinks, legacyReportLinks } from '../routing/publishedUrls.fixture'
import { localizeHref } from '../routing/localizedHref'
import { standaloneHtml } from '../../server/localizedPages'

const attr = (tag: string, name: string) => tag.match(new RegExp(`${name}="([^"]*)"`))?.[1]
const file = (name: string) => readFileSync(new URL(`../../${name}`, import.meta.url), 'utf8')

describe('per-page bilingual search and sharing metadata', () => {
  it('covers exactly the existing published pages, keeping the hidden report route', () => {
    expect(metadataPages.map(page => page.path).sort()).toEqual(publishedPages.map(page => page.path).sort())
    expect(metadataPages.find(page => page.path === '/analysis/yearly_report')?.navigation).toBe(false)
  })

  it.each(publishedPages)('provides reciprocal metadata and translated English copy for $path', ({ path }) => {
    for (const language of ['el', 'en'] as const) {
      const url = localizeHref(path, language)
      const metadata = getPageMetadata(url)!
      expect(metadata.language).toBe(language)
      expect(metadata.canonical).toBe(SITE_ORIGIN + url)
      expect(metadata.alternates).toEqual([
        { language: 'el', href: SITE_ORIGIN + path },
        { language: 'en', href: SITE_ORIGIN + localizeHref(path, 'en') },
        { language: 'x-default', href: SITE_ORIGIN + path },
      ])
      expect(new URL(metadata.image).origin).toBe(SITE_ORIGIN)
      expect(metadata.imageWidth).toBeGreaterThan(0)
      expect(metadata.imageHeight).toBeGreaterThan(0)
      // The methodology guide is temporarily published in Greek on both routes (user decision, 2026-11-18).
      if (language === 'en' && !metadata.page.englishPending) expect(metadata.title + metadata.description + metadata.imageAlt).not.toMatch(/[Α-Ωα-ω]/)
      expect(metadata.greekOnly).toBe(language === 'en' && !!metadata.page.englishPending)
      expect(metadataElements(metadata).filter(e => e.attributes.rel === 'canonical')).toHaveLength(1)
    }
  })

  it.each(['/analysis/statistics', '/analysis/antinero-west-attica', '/analysis/west-attica-fire-2026/'])('uses the English preview for %s and retains the Greek image', path => {
    expect(getPageMetadata(path)!.image).not.toContain('-en.png')
    expect(getPageMetadata(localizeHref(path, 'en'))!.image).toContain('-en.png')
  })

  it.each([...publishedDeepLinks.map(item => item.url), ...legacyReportLinks.map(item => item.from)])('resolves metadata without altering the shared URL %s', url => {
    const original = url
    const metadata = getPageMetadata(url)!
    expect(metadata).not.toBeNull()
    expect(metadata.canonical).not.toMatch(/[?#]/)
    expect(metadata.canonical).not.toContain('/reports')
    expect(url).toBe(original)
    expect(metadataDocument(url)).toMatch(/index\.html$/)
  })

  it.each(['/api/gemi/company-url', '/en/api/test', '/assets/app.js', '/en/assets/app.js', '/social/image.png', '/analysis/west-attica-fire-2026/assets/app.js', '/unknown'])('does not assign page metadata to %s', path => {
    expect(getPageMetadata(path)).toBeNull()
    expect(metadataDocument(path)).toBeNull()
  })
})

describe('server-delivered metadata', () => {
  it('replaces duplicates in the head while preserving body, scripts, styles and unrelated tags', () => {
    const body = '<body><div id="root"><p lang="el">Πρωτότυπος τίτλος εγγράφου &amp; ποσό</p></div><script src="/assets/app.js"></script></body></html>'
    const original = `<html lang="el"><head><title>Old</title><meta name="description" content="Old"><meta property="og:title" content="Old"><meta property="og:title" content="Duplicate"><link rel="canonical" href="http://localhost"><link rel="alternate" hreflang="el" href="old"><meta name="theme-color" content="#fff"><link rel="stylesheet" href="/assets/style.css"></head>${body}`
    const metadata = getPageMetadata('/en/contracts')!
    const result = applyPageMetadata(original, metadata)
    expect(result.endsWith(body)).toBe(true)
    expect(result).toContain('<html lang="en">')
    expect(result.match(/<title>/g)).toHaveLength(1)
    expect(result.match(/property="og:title"/g)).toHaveLength(1)
    expect(result.match(/rel="canonical"/g)).toHaveLength(1)
    expect(result.match(/rel="alternate"/g)).toHaveLength(3)
    expect(result).toContain('name="theme-color" content="#fff"')
    expect(result).toContain('href="/assets/style.css"')
    expect(result).not.toContain('localhost')
    expect(applyPageMetadata(result, metadata).match(/property="og:title"/g)).toHaveLength(1)
  })

  it('escapes title and attribute content', () => {
    const metadata = { ...getPageMetadata('/en/maps')!, title: 'A & B <title>', imageAlt: 'A "quoted" image & text' }
    const result = applyPageMetadata('<html lang="el"><head></head><body></body></html>', metadata)
    expect(result).toContain('<title>A &amp; B &lt;title&gt;</title>')
    expect(result).toContain('A &quot;quoted&quot; image &amp; text')
  })

  it('renders English fallback navigation without publishing the hidden report as a navigation link', () => {
    const metadata = getPageMetadata('/en/analysis')!
    const shell = '<html lang="en"><head></head><body><div id="root"><main class="page-loading">Loading page…</main></div><script src="/src/main.tsx"></script></body></html>'
    const result = fillStaticFallback(shell, metadata)
    expect(result).toContain('<h1>Analysis</h1>')
    expect(result).toContain('href="/en/contracts"')
    expect(result).toContain('<a href="/en/analysis/methodology_guide" lang="el">')
    expect(result.replace(/<a [^>]*lang="el"[^>]*>[^<]*<\/a>/g, '')).not.toMatch(/[Α-Ωα-ω]/)
    expect(result).not.toContain('href="/en/analysis/yearly_report"')
    expect(fillStaticFallback(result, metadata)).toBe(result)
    expect(staticFallback(metadata, '/fire_protection/')).toContain('href="/fire_protection/en/contracts"')
  })

  it('delivers translated native metadata and keeps the same map bootstrap assets', () => {
    const original = file('public/analysis/west-attica-fire-2026/index.html')
    const result = standaloneHtml(original, '/assets/language.js', '/en/analysis/west-attica-fire-2026/')
    expect(result).toContain('<html lang="en">')
    expect(result).toContain('Interactive analysis of the major fire in West Attica')
    const assets = (html: string) => [...html.matchAll(/(?:src|href)="([^"]*(?:\.js|\.css)(?:\?[^"]*)?)"/g)].map(m => m[1]).filter(url => url !== '/assets/language.js')
    expect(assets(result)).toEqual(assets(original))
    expect(result).toContain('src="/assets/language.js"')
    expect(result.match(/rel="canonical"/g)).toHaveLength(1)
  })
})

describe('bilingual sitemap', () => {
  it('retains all Greek URLs and includes exactly one English peer with reciprocal alternate links', () => {
    const sitemap = metadataSitemap()
    const entries = [...sitemap.matchAll(/<url>([\s\S]*?)<\/url>/g)].map(m => m[1])
    const urls = entries.map(entry => entry.match(/<loc>(.*?)<\/loc>/)![1])
    expect(entries).toHaveLength(30)
    expect(new Set(urls).size).toBe(30)
    for (const page of publishedPages) {
      expect(urls).toContain(SITE_ORIGIN + page.path)
      expect(urls).toContain(SITE_ORIGIN + localizeHref(page.path, 'en'))
    }
    for (const entry of entries) {
      const links = [...entry.matchAll(/<xhtml:link[^>]*>/g)].map(m => m[0])
      expect(links).toHaveLength(3)
      expect(links.map(link => attr(link, 'hreflang')).sort()).toEqual(['el', 'en', 'x-default'])
      for (const link of links) expect(urls).toContain(attr(link, 'href'))
    }
    expect(file('public/sitemap.xml')).toBe(sitemap)
  })
})
