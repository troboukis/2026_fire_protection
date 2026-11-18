import { createElement, type ReactNode } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { MemoryRouter } from 'react-router-dom'
import { describe, expect, it, vi } from 'vitest'
import I18nProvider from './I18nProvider'
import type { AppLanguage } from './languages'
import AnalysisPage from '../features/analysis/catalog/AnalysisPage'
import { fireSeasonReview } from '../features/analysis/catalog/fireSeasonReview'

vi.mock('../lib/devView', () => ({ DEV_VIEW_AVAILABLE: false, useDevViewEnabled: () => [false], useDevView: () => ({ enabled: false }) }))
function page(language: AppLanguage, children: ReactNode) {
  return createElement(I18nProvider, { language, children: createElement(MemoryRouter, {
    initialEntries: ['/en/analysis?tag=a%20b&tag=a+b#sources'],
  }, children) })
}

describe('bilingual analysis catalogue', () => {
  it('retains four cards, order, original destinations/images/dates and hidden report visibility', () => {
    const english = renderToStaticMarkup(page('en', createElement(AnalysisPage)))
    const greek = renderToStaticMarkup(page('el', createElement(AnalysisPage)))
    expect((english.match(/class="analysis-card section-rule"/g) ?? [])).toHaveLength(4)
    const destinations = ['https://wwfeu.awsassets.panda.org/downloads/anaskopisi2026f2.pdf', '/en/analysis/west-attica-fire-2026/', '/en/analysis/antinero-west-attica', '/en/analysis/statistics']
    const positions = destinations.map(href => english.indexOf(`href="${href}"`))
    expect(positions.every((position, index) => position >= 0 && (index === 0 || position > positions[index - 1]))).toBe(true)
    expect(english).not.toContain('href="/en/analysis/yearly_report"')
    expect(english).toContain('2026 Fire Season Review')
    expect(english).toContain('354,000 stremmata')
    expect(english).toContain('3 November 2026')
    expect(english).toContain('dateTime="2026-11-03"')
    expect(greek).toContain('Ανασκόπηση Αντιπυρικής Περιόδου 2026')
    expect(greek).toContain('354.000 στρέμματα')
    expect(greek).toContain('3 Νοεμβρίου 2026')
    expect(greek).toContain('href="/analysis/statistics"')
    expect(english.match(/<img[^>]*src="([^"]+)"/g)).toEqual(greek.match(/<img[^>]*src="([^"]+)"/g))
    expect(fireSeasonReview.title).toBe('Ανασκόπηση Αντιπυρικής Περιόδου 2026')
    expect(fireSeasonReview.publishedAt).toBe('2026-11-03')
    expect(english).not.toMatch(/[Α-Ωα-ω]/)
  })
})

