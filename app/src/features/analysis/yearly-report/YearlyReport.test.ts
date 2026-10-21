import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import YearlyReport from './YearlyReport'
import { REPORT_YEAR, report, shortDate, wholeMillions } from './reportData'

// The snapshot is rebuilt every day; the page must render whatever the current values are.
describe('yearly report page', () => {
  const html = renderToStaticMarkup(createElement(YearlyReport))
  const text = html.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ')

  // Only the figures are checked, so that editing the wording never stops the daily refresh.
  it('renders the headline figures from the snapshot', () => {
    expect(text).toContain(`${wholeMillions(report.total.amount)} εκατομμύρια`)
    expect(text).toContain(`${REPORT_YEAR}`)
    expect(text).toContain(shortDate(report.meta.period_end))
  })

  it('shows three key figures', () => {
    expect(html.match(/class="yr-finding"/g)?.length).toBe(3)
  })

  it('never prints empty or broken values', () => {
    expect(text).not.toMatch(/undefined|NaN|null|\[object Object\]/)
  })
})
