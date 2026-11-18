import { createElement, useEffect, useState, type ReactNode } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { act, create, type ReactTestRenderer } from 'react-test-renderer'
import { MemoryRouter } from 'react-router-dom'
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest'
import AmountSummary from '../components/AmountSummary'
import DataLoadingCard from '../components/DataLoadingCard'
import Layout from '../components/Layout'
import PageLoading from '../components/PageLoading'
import I18nProvider from './I18nProvider'
import { type AppLanguage } from './languages'
import { useAppI18n } from './useAppI18n'

vi.mock('../lib/supabase', () => ({ supabase: {} }))
vi.mock('../cookieConsent', () => ({ showCookiePreferences: vi.fn() }))

function render(language: AppLanguage, children: ReactNode) {
  return renderToStaticMarkup(createElement(I18nProvider, { language, children }))
}

describe('language provider and page state', () => {
  let tree: ReactTestRenderer | undefined
  afterEach(() => {
    if (tree) act(() => tree!.unmount())
    tree = undefined
  })

  it('changes translations immediately while retaining local state and the mounted child', () => {
    let mounts = 0
    function FilterProbe() {
      const { t, language } = useAppI18n()
      const [query, setQuery] = useState('')
      useEffect(() => { mounts++ }, [])
      return createElement('input', { 'aria-label': t('navigation:contracts'), lang: language, value: query, onChange: () => setQuery('AntiNERO') })
    }
    const page = (language: AppLanguage) => createElement(I18nProvider, { language, children: createElement(FilterProbe) })
    act(() => { tree = create(page('el')) })
    act(() => tree!.root.findByType('input').props.onChange())
    expect(tree!.root.findByType('input').props.value).toBe('AntiNERO')

    act(() => tree!.update(page('en')))
    expect(tree!.root.findByType('input').props['aria-label']).toBe('Contracts')
    expect(tree!.root.findByType('input').props.lang).toBe('en')
    expect(tree!.root.findByType('input').props.value).toBe('AntiNERO')

    act(() => tree!.update(page('el')))
    expect(tree!.root.findByType('input').props['aria-label']).toBe('Συμβάσεις')
    expect(tree!.root.findByType('input').props.value).toBe('AntiNERO')
    expect(mounts).toBe(1)
  })

  it('isolates nested languages and keeps providerless shared components Greek', () => {
    function Label() {
      return createElement('span', null, useAppI18n().t('navigation:home'))
    }
    expect(render('el', createElement('div', null,
      createElement(Label),
      createElement(I18nProvider, { language: 'en', children: createElement(Label) }),
      createElement(Label),
    ))).toBe('<div><span>Αρχική</span><span>Home</span><span>Αρχική</span></div>')
    expect(renderToStaticMarkup(createElement(Label))).toBe('<span>Αρχική</span>')
  })
})

describe('shared component translations', () => {
  it('preserves the Greek page fallback and provides the corresponding English text', () => {
    expect(render('el', createElement(PageLoading))).toBe('<main class="page-loading">Φόρτωση σελίδας…</main>')
    expect(render('en', createElement(PageLoading))).toBe('<main class="page-loading">Loading page…</main>')
  })

  it('preserves the existing Greek loading messages and markup', () => {
    const html = render('el', createElement(DataLoadingCard))
    expect(html).toContain('role="status"')
    expect(html).toContain('<strong>Φόρτωση δεδομένων</strong>')
    expect(html).toContain('<p>Ανακτώνται τα πιο πρόσφατα στοιχεία.</p>')
  })

  it('translates default loading messages while retaining explicitly supplied source text', () => {
    const html = render('en', createElement(DataLoadingCard))
    expect(html).toContain('<strong>Loading data</strong>')
    expect(html).toContain('<p>Retrieving the latest data.</p>')
    const supplied = render('en', createElement(DataLoadingCard, { title: 'Πρωτότυπος τίτλος', message: 'Πρωτότυπη περιγραφή' }))
    expect(supplied).toContain('<strong>Πρωτότυπος τίτλος</strong>')
    expect(supplied).toContain('<p>Πρωτότυπη περιγραφή</p>')
  })

  it.each([
    ['el', 1, '1.234,56\u00a0€', '1 έγγραφο δεν έχει ποσό'],
    ['el', 1234, '1.234,56\u00a0€', '1.234 έγγραφα δεν έχουν ποσό'],
    ['en', 1, '€1,234.56', '1 document has no amount'],
    ['en', 1234, '€1,234.56', '1,234 documents have no amount'],
  ] as const)('keeps the cents denominator and localized missing count in %s for %s documents', (language, missingCount, amount, missing) => {
    const html = render(language, createElement(AmountSummary, {
      totalCents: 123456, missingCount, loading: false, loadFailed: false, ariaLabel: 'Summary',
    }))
    expect(html).toContain(amount)
    expect(html).toContain(missing)
  })

  it.each(['el', 'en'] as const)('hides the amount while loading or failed in %s', (language) => {
    for (const status of [{ loading: true, loadFailed: false }, { loading: false, loadFailed: true }]) {
      const html = render(language, createElement(AmountSummary, {
        totalCents: 123456, missingCount: 2, ariaLabel: 'Summary', ...status,
      }))
      expect(html).not.toContain('1,234.56')
      expect(html).not.toContain('1.234,56')
      expect(html).toContain('—')
      expect(html).not.toMatch(/common:|navigation:|undefined|NaN/)
    }
  })
})

describe('layout integration', () => {
  beforeAll(() => vi.stubGlobal('__LAST_COMMIT_ISO__', '2026-11-17T12:30:00Z'))
  afterAll(() => vi.unstubAllGlobals())

  it('keeps the existing Greek navigation, links and footer', () => {
    const html = render('el', createElement(MemoryRouter, null, createElement(Layout)))
    for (const label of ['Αρχική', 'Χάρτης', 'Δήμοι', 'Υπ. Περιβάλλοντος', 'Συμβάσεις', 'Διαύγεια', 'Ανάλυση', 'Σχετικά', 'Όροι χρήσης', 'Απόρρητο &amp; cookies', 'Ρυθμίσεις cookies']) {
      expect(html).toContain(label)
    }
    for (const href of ['/', '/maps', '/municipalities', '/environment-ministry', '/contracts', '/diavgeia', '/analysis', '/terms', '/privacy']) {
      expect(html).toContain(`href="${href}"`)
    }
    expect(html).toContain('aria-label="Κύρια πλοήγηση"')
    expect(html).toContain('Τελευταία ενημέρωση / ')
    expect(html).toContain('alt="FireWatch — παρατηρητήριο για τις δασικές πυρκαγιές"')
  })

  it('can render the shared English layout without changing route definitions', () => {
    const html = render('en', createElement(MemoryRouter, null, createElement(Layout)))
    for (const label of ['Home', 'Map', 'Municipalities', 'Environment ministry', 'Contracts', 'Diavgeia', 'Analysis', 'About', 'Terms of use', 'Privacy &amp; cookies', 'Cookie settings']) {
      expect(html).toContain(label)
    }
    expect(html).toContain('aria-label="Main navigation"')
    expect(html).toContain('Last updated / 17 Nov 2026, 14:30')
    expect(html).not.toMatch(/common:|navigation:|layout:|undefined|NaN/)
  })
})
