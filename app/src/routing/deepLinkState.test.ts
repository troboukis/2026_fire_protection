import { readFileSync } from 'node:fs'
import { createElement, type ComponentType } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { MemoryRouter } from 'react-router-dom'
import { describe, expect, it, vi } from 'vitest'
import ContractsPage from '../pages/ContractsPage'
import DiavgeiaPage from '../pages/DiavgeiaPage'
import YearlyReport from '../features/analysis/yearly-report/YearlyReport'

// Server rendering exercises the real initial filter state without executing
// effects or reading live data. No database calls are needed for this baseline.
vi.mock('../lib/supabase', () => ({ supabase: {} }))

function renderPage(Page: ComponentType, url: string) {
  return renderToStaticMarkup(createElement(MemoryRouter, { initialEntries: [url] }, createElement(Page)))
}

function inputValue(html: string, className: string): string | undefined {
  const input = html.match(/<input\b[^>]*>/g)?.find(tag => tag.includes(className))
  return input?.match(/\bvalue="([^"]*)"/)?.[1]
}

describe('initial state from shared contract URLs', () => {
  it('restores the Greek search text, dates and minimum amount', () => {
    const html = renderPage(ContractsPage, '/contracts?q=%CE%B1%CF%80%CE%BF%CF%88%CE%AF%CE%BB%CF%89%CF%83%CE%B7+OR+%CF%80%CF%85%CF%81%CE%BF%CF%80%CF%81%CE%BF%CF%83%CF%84%CE%B1%CF%83%CE%AF%CE%B1&dateFrom=2024-01-01&dateTo=2026-11-17&minAmount=1234.50')
    expect(inputValue(html, 'contracts-filter--search')).toBe('αποψίλωση OR πυροπροστασία')
    expect(inputValue(html, 'contracts-filter--date-from')).toBe('2024-01-01')
    expect(inputValue(html, 'contracts-filter--date-to')).toBe('2026-11-17')
    expect(inputValue(html, 'contracts-filter--amount')).toBe('1234.50')
  })

  it('keeps dates and search text when a shared link scopes multiple organizations', () => {
    const html = renderPage(ContractsPage, '/contracts?organizationKey=100&organizationKey=200&municipalityKey=9230&q=AntiNERO&dateFrom=2024-01-01&dateTo=2026-01-01')
    expect(inputValue(html, 'contracts-filter--search')).toBe('AntiNERO')
    expect(inputValue(html, 'contracts-filter--date-from')).toBe('2024-01-01')
    expect(inputValue(html, 'contracts-filter--date-to')).toBe('2026-01-01')
  })
})

describe('initial state from shared Diavgeia URLs', () => {
  it('restores the search text and explicit date range', () => {
    const html = renderPage(DiavgeiaPage, '/diavgeia?municipalityKey=9230&q=AntiNERO&dateFrom=2024-01-01&dateTo=2026-11-17')
    expect(inputValue(html, 'contracts-filter--search')).toBe('AntiNERO')
    expect(inputValue(html, 'contracts-filter--date-from')).toBe('2024-01-01')
    expect(inputValue(html, 'contracts-filter--date-to')).toBe('2026-11-17')
  })

  it('preserves allDates=1 precedence over explicit dates', () => {
    const html = renderPage(DiavgeiaPage, '/diavgeia?allDates=1&dateFrom=2024-01-01&dateTo=2026-11-17&q=AntiNERO')
    expect(inputValue(html, 'contracts-filter--search')).toBe('AntiNERO')
    expect(inputValue(html, 'contracts-filter--date-from')).toBe('')
    expect(inputValue(html, 'contracts-filter--date-to')).toBe('')
  })
})

describe('published section anchors', () => {
  const reportHtml = renderToStaticMarkup(createElement(YearlyReport))

  it.each(['symvaseis', 'xronos', 'plirwmes', 'methodologia'])('keeps the report #%s anchor', (id) => {
    expect(reportHtml).toContain(`id="${id}"`)
    expect(reportHtml).toContain(`href="#${id}"`)
  })

  it.each(['about', 'latest', 'historicalmap'])('keeps the homepage #%s anchor', (id) => {
    const source = readFileSync(new URL('../App.tsx', import.meta.url), 'utf8')
    expect(source).toContain(`id="${id}"`)
  })
})
