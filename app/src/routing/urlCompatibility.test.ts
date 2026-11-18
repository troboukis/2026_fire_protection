import { readFileSync } from 'node:fs'
import { Children, createElement, isValidElement, type ReactNode } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { createPath, createRoutesFromElements, matchRoutes, MemoryRouter, Navigate, Routes, type RouteObject } from 'react-router-dom'
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest'
import LegacyReportsRedirect from '../features/analysis/LegacyReportsRedirect'
import { legacyReportLinks, publishedDeepLinks, publishedPages } from './publishedUrls.fixture'
import { languageFromPath, localizeHref } from './localizedHref'

const bootstrap = vi.hoisted(() => ({ tree: null as ReactNode }))

// Capture the real application's route tree without mounting maps, starting
// network requests, or requesting cookie consent in a Node test environment.
vi.mock('react-dom/client', () => ({
  default: { createRoot: () => ({ render: (tree: ReactNode) => { bootstrap.tree = tree } }) },
}))
vi.mock('../App', () => ({ default: () => null }))
vi.mock('../components/Layout', () => ({ default: () => null }))
vi.mock('../analytics', () => ({ trackPageView: vi.fn() }))
vi.mock('../cookieConsent', () => ({ initCookieConsent: vi.fn() }))
vi.mock('@vercel/analytics/react', () => ({ Analytics: () => null }))
vi.mock('react-router-dom', async (importOriginal) => ({
  ...await importOriginal<typeof import('react-router-dom')>(),
  // Inspect the target produced by the real redirect component. Navigation
  // effects do not execute during server rendering.
  Navigate: vi.fn(() => null),
}))

function findRoutes(node: ReactNode): RouteObject[] | null {
  for (const child of Children.toArray(node)) {
    if (!isValidElement<{ children?: ReactNode }>(child)) continue
    if (child.type === Routes) return createRoutesFromElements(child.props.children)
    const nested = findRoutes(child.props.children)
    if (nested) return nested
  }
  return null
}

const appFile = (path: string) => readFileSync(new URL(`../../${path}`, import.meta.url), 'utf8')
let routes: RouteObject[]

beforeAll(async () => {
  vi.stubGlobal('document', { getElementById: () => ({}) })
  await import('../main')
  const captured = findRoutes(bootstrap.tree)
  if (!captured) throw new Error('The application route tree was not captured')
  routes = captured
})

afterAll(() => vi.unstubAllGlobals())

describe('published Greek URLs', () => {
  it.each(publishedPages.filter(page => page.route !== null))('keeps $path reachable through its existing route', ({ path, route }) => {
    for (const pathname of new Set([path, `${path.replace(/\/$/, '')}/`])) {
      const matches = matchRoutes(routes, pathname)
      expect(matches?.[matches.length - 1].route.path).toBe(route)
    }
  })

  it.each(publishedDeepLinks)('continues to accept $url', ({ url, route }) => {
    const matches = matchRoutes(routes, url)
    expect(matches?.[matches.length - 1].route.path).toBe(route)
  })

  it('continues publishing every existing Greek sitemap URL', () => {
    const sitemap = appFile('public/sitemap.xml')
    const actual = Array.from(sitemap.matchAll(/<loc>([^<]+)<\/loc>/g), match => new URL(match[1]).pathname)
    expect(actual).toEqual(expect.arrayContaining(publishedPages.map(page => page.path)))
  })

  it.each(publishedPages)('keeps a Greek HTML entry for $path', ({ html }) => {
    expect(appFile(html)).toMatch(/<html\s+lang="el"/)
  })

  it('keeps dedicated HTML rewrites for published analyses', () => {
    const { rewrites } = JSON.parse(appFile('vercel.json')) as { rewrites: Array<{ source: string; destination: string }> }
    for (const page of publishedPages.filter(page => page.path.startsWith('/analysis'))) {
      const source = page.path.replace(/\/$/, '')
      const destination = `/${page.html.replace(/^public\//, '')}`
      expect(rewrites).toContainEqual({ source, destination })
    }
  })

  it('retains the SPA fallback for shared app URLs and excludes API and assets', () => {
    const { rewrites } = JSON.parse(appFile('vercel.json')) as { rewrites: Array<{ source: string; destination: string }> }
    const fallback = rewrites.find(rule => rule.destination === '/index.html')
    expect(fallback).toBeDefined()
    const matcher = new RegExp(`^${fallback!.source}$`)
    for (const page of publishedPages.filter(page => page.route !== null)) expect(matcher.test(page.path)).toBe(true)
    for (const path of ['/reports/statistics', '/reports/yearly_report/']) expect(matcher.test(path)).toBe(true)
    for (const path of ['/api/gemi/company-url', '/assets/main.js', '/sitemap.xml', '/robots.txt', '/analysis/west-attica-fire-2026/assets/main.js']) {
      expect(matcher.test(path)).toBe(false)
    }
  })
})

describe('shared legacy report URLs', () => {
  const reportLinks = [...legacyReportLinks, ...legacyReportLinks.map(({ from, to }) => ({ from: localizeHref(from, 'en'), to: localizeHref(to, 'en') }))]
  it.each(reportLinks)('preserves the destination, raw query string and hash of $from', ({ from, to }) => {
    const matches = matchRoutes(routes, from)
    expect(matches?.[matches.length - 1].route.path).toBe(localizeHref('/reports/*', languageFromPath(from)))

    const navigate = vi.mocked(Navigate)
    navigate.mockClear()
    renderToStaticMarkup(createElement(MemoryRouter, { initialEntries: [from] }, createElement(LegacyReportsRedirect)))
    const props = navigate.mock.calls[navigate.mock.calls.length - 1]?.[0]
    expect(props?.replace).toBe(true)
    expect(createPath(props!.to as { pathname: string; search: string; hash: string })).toBe(to)

    const destination = matchRoutes(routes, to)
    expect(destination?.[destination.length - 1].route.path).toBe(new URL(to, 'https://www.fire-watch-app.gr').pathname.replace(/\/$/, ''))
  })
})

describe('English counterparts of published URLs', () => {
  it.each(publishedPages.filter(page => page.route !== null))('matches the same page implementation for $path', ({ path, route }) => {
    const greek = matchRoutes(routes, path)!
    const english = matchRoutes(routes, localizeHref(path, 'en'))!
    expect(english[english.length - 1].route.path).toBe(localizeHref(route!, 'en'))
    expect(english[english.length - 1].route.element).toBe(greek[greek.length - 1].route.element)
  })
  it.each(publishedDeepLinks)('opens the English version of $url with all original selections', ({ url, route }) => {
    const matches = matchRoutes(routes, localizeHref(url, 'en'))
    expect(matches?.[matches.length - 1].route.path).toBe(localizeHref(route, 'en'))
  })
})
