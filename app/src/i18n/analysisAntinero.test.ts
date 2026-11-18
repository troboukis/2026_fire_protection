import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { MemoryRouter } from 'react-router-dom'
import { describe, expect, it } from 'vitest'
import I18nProvider from './I18nProvider'
import type { AppLanguage } from './languages'
import sources from './catalogs/analysisAntinero.el.json'
import AntineroPage from '../features/analysis/antinero/AntineroPage'
import { report } from '../features/analysis/antinero/data/antineroReport'

function render(language: AppLanguage, query = '') {
  return renderToStaticMarkup(createElement(I18nProvider, { language, children: createElement(MemoryRouter, { initialEntries: [`/analysis/antinero-west-attica${query}`] }, createElement(AntineroPage)) }))
}
const escape = (value: string) => renderToStaticMarkup(createElement('span', null, value)).slice(6, -7)
const geometry = (html: string) => [...html.matchAll(/<(?:circle|path|g)\b[^>]*(?:\bd=|\br=|transform=)[^>]*>/g)].map(m => m[0].replace(/aria-label="[^"]*"/g, ''))

describe('AntiNERO language presentation and original evidence', () => {
  it('translates the same complete network and keeps its geometry and identifiers', () => {
    const greek = render('el'), english = render('en')
    expect(english).toContain('Each circle is a contract')
    expect(english).toContain('Shared Diavgeia decisions')
    expect(english).toContain('110,000 stremmata')
    expect(english).toContain('Analysis of 17 contracts (KIMDIS)')
    expect(geometry(english)).toEqual(geometry(greek))
    for (const item of report.contracts) {
      expect(english).toContain(`data-node-id="${item.id}"`)
      expect(english).toContain(escape(item.label))
    }
  })

  it('retains every selected contract title, original description, amount and source link', () => {
    for (const item of report.contracts) {
      const english = render('en', `?node=${item.id}&contract=${item.id}`)
      expect(english).toContain(`<h3 lang="el">${escape(item.label)}</h3>`)
      expect(english).toContain(`<p lang="el">${escape(item.description || item.label)}</p>`)
      expect(english).toContain(`href="${escape(item.url)}"`)
      expect(english).toContain('Ministry of Environment and Energy')
      if (item.withoutVat !== null) expect(english).toContain(new Intl.NumberFormat('en-GB', { style: 'currency', currency: 'EUR' }).format(Number(item.withoutVat)))
      if (item.kind === 'supplement') expect(english).toContain('it does not in itself constitute evidence of the contract signing')
    }
  })

  it('keeps decision titles Greek and excludes evidence titles from the translation corpus', () => {
    const corpus = new Set(Object.values(sources))
    for (const item of report.contracts) {
      expect(corpus.has(item.label)).toBe(false)
      expect(corpus.has(item.description)).toBe(false)
    }
    const item = report.decisions.find(d => d.contractIds.length > 1)!
    const english = render('en', `?node=${encodeURIComponent(item.id)}&contract=${item.contractIds[0]}`)
    expect(english).toContain(`<h3 lang="el">${escape(item.label)}</h3>`)
    expect(english).toContain(`href="${escape(item.url)}"`)
    expect(english).toContain('Date of issue:')
    for (const d of report.decisions) {
      expect(corpus.has(d.label)).toBe(false)
      expect(corpus.has(d.subject)).toBe(false)
    }
  })
})
