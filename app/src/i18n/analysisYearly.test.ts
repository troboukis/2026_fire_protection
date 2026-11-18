import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import I18nProvider from './I18nProvider'
import { getOverviewUi } from './overview'
import { createReportFormatters } from '../features/analysis/yearly-report/reportFormatting'
import * as data from '../features/analysis/yearly-report/reportData'
import YearlyReport from '../features/analysis/yearly-report/YearlyReport'

const en = createReportFormatters(getOverviewUi('en'))
const el = createReportFormatters(getOverviewUi('el'))
const render = (language: 'en' | 'el') => renderToStaticMarkup(createElement(I18nProvider, { language, children: createElement(YearlyReport) }))
const text = (html: string) => html.replace(/<\/(?:p|h[1-6]|div|li|dt|dd|td|th)>/g, '$& ').replace(/<[^>]*>/g, '').replace(/\s+/g, ' ')

describe('annual-report language and financial meaning', () => {
  it('retains the original Greek formatters and their precision', () => {
    for (const key of Object.keys(el) as (keyof typeof el)[]) expect(el[key]).toBe(data[key])
    expect(en.mil(173140768.42)).toBe('173.1 million €')
    expect(en.eur(12000)).toBe('12,000.0 €')
    expect(en.minimumMillions(184099999)).toBe('184.0')
    expect(Number(en.minimumMillions(184099999)) * 1_000_000).toBeLessThanOrEqual(184099999)
    expect(en.pct(-0.001)).toBe('0.0%')
    expect(en.pct(-41.47)).toBe('−41.5%')
    expect(en.pct(null)).toBe('—')
    expect(en.signedPct(12)).toBe('+12.0%')
    expect(en.amount(738400)).toBe('738.4 thousand €')
    expect(en.count(3, true)).toBe('three')
  })

  it('renders the same snapshot totals, source identifiers and links in English', () => {
    const original = JSON.stringify(data.report)
    const greek = render('el'), english = render('en'), copy = text(english)
    expect(copy).toContain(`At least ${en.minimumMillions(data.report.total.amount)} million euros`)
    expect(copy).toContain(`${en.num(data.contracts.contracts)} contracts for wildfire suppression or prevention`)
    expect(copy).toContain(`${en.num(data.report.payments.counted)} payments are added to the expenditure estimate`)
    expect(copy).toContain(`${en.num(data.report.payments.linked)} valid payments linked to a contract are not added again`)
    expect(copy).toContain('We do not deduct VAT from payroll')
    expect(copy).toContain(`includes VAT at ${en.pct(data.report.meta.vat_rate * 100)}`)
    expect(copy).not.toMatch(/undefined|NaN|\[object Object\]|\bto to\b|2026FireWatch/)
    const links = (html: string) => [...html.matchAll(/href="([^"]*)"/g)].map(m => m[1])
    expect(links(english)).toEqual(links(greek))
    expect(JSON.stringify(data.report)).toBe(original)
  })

  it('localizes months and official authorities while retaining original search terms and private names', () => {
    const english = render('en'), copy = text(english)
    expect(copy).toContain('Ministry of Environment and Energy')
    expect(copy).not.toMatch(/Ιούνιος|Μάιος|Ιούλιος/)
    expect(english).toContain('<span lang="el">«πυροπροστασία», «αποψίλωση», «δασοπροστασία» και «αντιπυρική»</span>')
    for (const item of data.report.contractors.top10) {
      const escaped = renderToStaticMarkup(createElement('span', null, item.name)).slice(6, -7)
      expect(english).toContain(escaped)
    }
  })
})
