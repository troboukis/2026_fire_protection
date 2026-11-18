import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import { previewDocumentUrl, standaloneHtml } from '../../server/localizedPages'
import { APP_PAGE_PATHS } from './localizedHref'

const appFile = (path: string) => readFileSync(new URL(`../../${path}`, import.meta.url), 'utf8')
const { rewrites } = JSON.parse(appFile('vercel.json')) as { rewrites: Array<{ source: string; destination: string }> }
const destination = (path: string) => rewrites.find(rule => new RegExp(`^${rule.source}$`).test(path))?.destination

describe('English document serving', () => {
  it.each(APP_PAGE_PATHS)('serves the English entry for %s before the Greek SPA fallback', path => {
    for (const url of new Set([path === '/' ? '/en' : `/en${path}`, `/en${path.replace(/\/$/, '')}/`])) {
      expect(destination(url)).toBe(path === '/' ? '/en/index.html' : `/en${path}/index.html`)
    }
  })
  it('serves both English standalone forms from a dedicated document', () => {
    expect(destination('/en/analysis/west-attica-fire-2026')).toBe('/en/analysis/west-attica-fire-2026/index.html')
    expect(destination('/en/analysis/west-attica-fire-2026/')).toBe('/en/analysis/west-attica-fire-2026/index.html')
  })
  it('preserves English legacy report requests through the English entry', () => {
    expect(destination('/en/reports/statistics')).toBe('/en/analysis/statistics/index.html')
    expect(destination('/reports/statistics')).toBe('/analysis/statistics/index.html')
  })
  it.each(['/api/gemi/company-url', '/en/api/gemi/company-url', '/assets/main.js', '/en/assets/main.js', '/analysis/west-attica-fire-2026/assets/map.js', '/en/analysis/west-attica-fire-2026/assets/map.js', '/sitemap.xml', '/robots.txt'])('does not send an API or asset request to an app HTML entry: %s', path => {
    expect(destination(path)).toBeUndefined()
  })
  it('provides an English HTML root with the same application bootstrap', () => {
    const html = appFile('en/index.html')
    expect(html).toContain('<html lang="en">')
    expect(html).toContain('src="/src/main.tsx"')
    expect(html).toContain('Loading page…')
  })
})

describe('standalone language adapter integration', () => {
  it.each([
    '/analysis/west-attica-fire-2026', '/analysis/west-attica-fire-2026/',
    '/en/analysis/west-attica-fire-2026', '/en/analysis/west-attica-fire-2026/',
  ])('serves the dedicated built document in preview for %s', pathname => {
    expect(previewDocumentUrl(`${pathname}?tag=a%20b&tag=a+b`)).toBe(`${pathname.replace(/\/$/, '')}/index.html?tag=a%20b&tag=a+b`)
  })
  it.each([['/en', '/en/index.html'], ['/en/', '/en/index.html'], ['/en/contracts', '/en/contracts/index.html'], ['/en/reports/statistics', '/en/analysis/statistics/index.html']])('serves the English built entry in preview for %s', (pathname, document) => {
    expect(previewDocumentUrl(`${pathname}?q=AntiNERO`)).toBe(`${document}?q=AntiNERO`)
  })
  it('keeps preview assets and APIs intact and supports a basename', () => {
    expect(previewDocumentUrl('/en/assets/main.js')).toBe('/en/assets/main.js')
    expect(previewDocumentUrl('/api/gemi/company-url')).toBe('/api/gemi/company-url')
    expect(previewDocumentUrl('/fire_protection/en/contracts?q=AntiNERO', '/fire_protection/')).toBe('/fire_protection/en/contracts/index.html?q=AntiNERO')
  })
  it('only injects the navigation module, preserving all map markup and asset references', () => {
    const original = appFile('public/analysis/west-attica-fire-2026/index.html')
    const script = '/assets/standaloneNavigation-test.js'
    const injected = `  <script type="module" src="${script}"></script>\n  `
    const result = standaloneHtml(original, script)
    expect(result.replace(injected, '')).toBe(original)
    expect(result).toContain('<html lang="el">')
    expect(result).toContain('/analysis/west-attica-fire-2026/assets/')
  })
})
