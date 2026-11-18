import { describe, expect, it } from 'vitest'
import { APP_PAGE_PATHS, documentHref, isStandaloneHref, languageFromPath, localizeHref, localizeTo, stripLanguagePath } from './localizedHref'

describe('explicit URL languages', () => {
  it.each(['/en', '/en/', '/en/contracts', '/en/reports/statistics'])('recognizes the English prefix in %s', path => {
    expect(languageFromPath(path)).toBe('en')
  })
  it.each(['/', '/contracts', '/reports/statistics', '/english', '/enough', '/EN/contracts', '/foo/en/contracts'])('keeps %s Greek without detecting browser preferences', path => {
    expect(languageFromPath(path)).toBe('el')
  })
  it.each(APP_PAGE_PATHS)('provides an English counterpart for %s without changing the Greek URL', path => {
    const english = path === '/' ? '/en/' : `/en${path}`
    expect(localizeHref(path, 'el')).toBe(path)
    expect(localizeHref(path, 'en')).toBe(english)
    expect(localizeHref(english, 'en')).toBe(english)
    expect(localizeHref(english, 'el')).toBe(path)
  })
  it('handles the English root with or without its final slash', () => {
    expect(stripLanguagePath('/en')).toBe('/')
    expect(stripLanguagePath('/en/')).toBe('/')
    expect(localizeHref('/en?tag=one#about', 'el')).toBe('/?tag=one#about')
  })
})

describe('shared queries and hashes', () => {
  const query = '?q=%CE%B1%CE%BA%CF%84%CE%AE+AND+%CF%80%CF%85%CF%81&organizationKey=100&organizationKey=200&tag=a%20b&tag=a+b#%CF%80%CE%B7%CE%B3%CE%AD%CF%82'
  it('preserves every byte of queries, duplicate parameters and encoded anchors', () => {
    expect(localizeHref(`/contracts${query}`, 'en')).toBe(`/en/contracts${query}`)
    expect(localizeHref(`/en/contracts${query}`, 'el')).toBe(`/contracts${query}`)
  })
  it('preserves final slashes and report aliases', () => {
    expect(localizeHref(`/reports/statistics/${query}`, 'en')).toBe(`/en/reports/statistics/${query}`)
    expect(localizeHref(`/en/analysis/statistics/${query}`, 'el')).toBe(`/analysis/statistics/${query}`)
  })
  it('preserves location objects and history state outside the target path', () => {
    const to = Object.freeze({ pathname: '/contracts', search: query.slice(0, query.indexOf('#')), hash: '#symvaseis' })
    expect(localizeTo(to, 'en')).toEqual({ ...to, pathname: '/en/contracts' })
    expect(to.pathname).toBe('/contracts')
    expect(localizeTo({ search: '?q=AntiNERO', hash: '#latest' }, 'en')).toEqual({ search: '?q=AntiNERO', hash: '#latest' })
  })
  it('localizes absolute FireWatch page links while keeping the original origin', () => {
    expect(localizeHref(`https://www.fire-watch-app.gr/municipalities${query}`, 'en')).toBe(`https://www.fire-watch-app.gr/en/municipalities${query}`)
    expect(localizeHref('https://fire-watch-app.gr', 'el')).toBe('https://fire-watch-app.gr')
    expect(localizeHref('https://fire-watch-app.gr', 'en')).toBe('https://fire-watch-app.gr/en/')
  })
  it.each([
    '#methodologia', '?q=AntiNERO', '../contracts', '/api/gemi/company-url?afm=123', '/assets/main.js',
    '/social/yearly-report-2026.svg', '/reports/original.pdf', '/downloads/report.pdf#page=6',
    'https://diavgeia.gov.gr/doc/ΑΔΑ', 'https://www.fire-watch-app.gr.evil.test/contracts',
    'https://example.com/contracts', '//example.com/contracts', 'mailto:hello@example.com', '/unknown',
  ])('leaves sources, assets, API calls and non-page links unchanged: %s', href => {
    expect(localizeHref(href, 'en')).toBe(href)
    expect(localizeHref(href, 'el')).toBe(href)
  })
  it('recognizes both standalone aliases without matching their assets', () => {
    expect(isStandaloneHref('/analysis/west-attica-fire-2026/')).toBe(true)
    expect(isStandaloneHref('/en/analysis/west-attica-fire-2026?tag=one#sources')).toBe(true)
    expect(isStandaloneHref('/analysis/west-attica-fire-2026/assets/map.js')).toBe(false)
    expect(isStandaloneHref('https://example.com/analysis/west-attica-fire-2026')).toBe(false)
  })
  it('supports a router basename for full document navigation', () => {
    expect(documentHref('/en/contracts?municipalityKey=9230', '/fire_protection/')).toBe('/fire_protection/en/contracts?municipalityKey=9230')
    expect(documentHref('/en/')).toBe('/en/')
    expect(documentHref('https://diavgeia.gov.gr/doc/ΑΔΑ', '/preview/')).toBe('https://diavgeia.gov.gr/doc/ΑΔΑ')
  })
})
