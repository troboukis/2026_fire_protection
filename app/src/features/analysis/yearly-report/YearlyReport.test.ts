import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import YearlyReport from './YearlyReport'
import Funding from '../../../components/Funding'
import { fundingSummary } from '../../../data/fundingSummary'
import { municipalSpending } from '../../../data/municipalSpending'
import { REPORT_YEAR, milText, minimumMillions, report, shortDate } from './reportData'

// The snapshot is rebuilt every day; the page must render whatever the current values are.
describe('yearly report page', () => {
  const html = renderToStaticMarkup(createElement(YearlyReport))
  const text = html.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ')

  // Only the figures are checked, so that editing the wording never stops the daily refresh.
  it('renders the headline figures from the snapshot', () => {
    expect(text).toContain(`${minimumMillions(report.total.amount)} εκατομμύρια`)
    expect(text).toContain(`${REPORT_YEAR}`)
    expect(text).toContain(shortDate(report.meta.period_end))
  })

  it('never prints empty or broken values', () => {
    expect(text).not.toMatch(/undefined|NaN|null|\[object Object\]/)
  })

  it('uses the same municipal estimate on the homepage and in the report', () => {
    const contractsAmount = report.contracts.by_group.find(row => row.group === 'Δήμοι')?.amount ?? 0
    const paymentsAmount = report.payments.by_org_type.find(row => row.org_type === 'ΔΗΜΟΣ')?.amount ?? 0
    const expectedTotal = Math.round((contractsAmount + paymentsAmount) * 100) / 100
    expect(fundingSummary.currentSpendAmount).toBe(expectedTotal)
    expect(fundingSummary.currentSpendYear).toBe(REPORT_YEAR)
    expect(municipalSpending.unlinkedPaymentsCount).toBe(report.payments.municipal.payments)
    expect(text).toContain(`${milText(expectedTotal)} χωρίς ΦΠΑ`)

    const fundingHtml = renderToStaticMarkup(createElement(Funding, { currentYear: REPORT_YEAR }))
    const compactTotal = `${Number((expectedTotal / 1_000_000).toFixed(1))}Μ €`
    expect(fundingHtml).toContain(`Εκτίμηση δαπανών Δήμων μέχρι σήμερα: ${compactTotal}`)
    const progress = Math.max(0, Math.min(100, expectedTotal / fundingSummary.currentTotal * 100))
    expect(fundingHtml).toContain(`width:${progress}%`)
  })
})
