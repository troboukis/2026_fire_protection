import { createElement, useState, type ReactNode } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { act, create, type ReactTestRenderer } from 'react-test-renderer'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { getOverviewUi, useOverviewUi } from './overview'
import I18nProvider from './I18nProvider'
import type { AppLanguage } from './languages'
import overviewEl from './catalogs/overview.el.json'
import overviewEn from './catalogs/overview.en.json'
import placesEl from './catalogs/places.el.json'
import placesEn from './catalogs/places.en.json'
import glossary from './glossary.el-en.json'
import MapSelectionPanel from '../components/MapSelectionPanel'
import MapLegendToggle from '../components/MapLegendToggle'
import Funding from '../components/Funding'
import MunicipalitiesPage from '../pages/MunicipalitiesPage'
import { fundingSummary } from '../data/fundingSummary'

vi.mock('../lib/supabase', () => ({ supabase: {} }))
vi.mock('../lib/devView', () => ({ DEV_VIEW_AVAILABLE: false, useDevViewEnabled: () => [false] }))

function render(language: AppLanguage, children: ReactNode) {
  return renderToStaticMarkup(createElement(I18nProvider, { language, children: createElement(MemoryRouter, null, children) }))
}

describe('reviewed overview and municipality copy', () => {
  it.each([[overviewEl, overviewEn], [placesEl, placesEn]])('preserves all keys, placeholders and numeric references', (source, target) => {
    expect(Object.keys(target).sort()).toEqual(Object.keys(source).sort())
    for (const key of Object.keys(source)) {
      const before = (source as Record<string, string>)[key]
      const after = (target as Record<string, string>)[key]
      expect(after.trim(), key).not.toBe('')
      expect(after.match(/\{\{[^{}]+\}\}/g)?.sort() ?? [], before).toEqual(before.match(/\{\{[^{}]+\}\}/g)?.sort() ?? [])
      // Locale separators may change; the underlying numerical references do not.
      const numbers = (text: string) => text.replace(/(\d+) χιλιάδες/g, (_, value) => String(Number(value) * 1000)).replace(/\{\{[^{}]+\}\}/g, '').match(/\d+(?:[.,]\d+)*/g)?.map(x => x.replace(/[.,]/g, '')).sort() ?? []
      expect(numbers(after), before).toEqual(numbers(before))
    }
  })

  it('uses the user-specified Association of Municipalities terminology in copy and glossary', () => {
    const ui = getOverviewUi('en')
    expect(ui('Σύνδεσμοι δήμων')).toBe('Associations of Municipalities')
    expect(ui('Σύνδεσμοι:')).toBe('Associations of Municipalities:')
    expect(glossary.entries).toEqual(expect.arrayContaining([
      expect.objectContaining({ source: 'σύνδεσμος δήμων', target: 'Association of Municipalities' }),
      expect.objectContaining({ source: 'σύνδεσμοι δήμων', target: 'Associations of Municipalities' }),
    ]))
  })

  it('keeps the Greek text and surrounding whitespace exact', () => {
    const source = '  Συνολική Δαπάνη '
    expect(getOverviewUi('el')(source)).toBe(source)
    expect(getOverviewUi('en')(source)).toBe('  Total spending ')
    expect(getOverviewUi('el')('Το 2024 υποβλήθηκε 1 υπεύθυνη δήλωση καθαρισμού οικοπέδων.')).toBe('Το 2024 υποβλήθηκε 1 υπεύθυνη δήλωση καθαρισμού οικοπέδων.')
  })

  it('retains an unknown original title and never translates an interpolated source title', () => {
    const original = 'Πρωτότυπος τίτλος σύμβασης 26SYMV012345678'
    expect(getOverviewUi('en')(original)).toBe(original)
    expect(getOverviewUi('en')('Σύμβαση: {{v0}}', { v0: original })).toBe('Contract: ' + original)
  })

  it('distinguishes statutory declaration counts from the reference year', () => {
    const ui = getOverviewUi('en')
    expect(ui('Το 2024 υποβλήθηκε 1 υπεύθυνη δήλωση καθαρισμού οικοπέδων.')).toBe('In 2024, 1 statutory declaration of plot clearance was submitted.')
    expect(ui('Το 2024 υποβλήθηκαν {{v0}} υπεύθυνες δηλώσεις καθαρισμού οικοπέδων.', { v0: 12 })).toBe('In 2024, 12 statutory declarations of plot clearance were submitted.')
  })

  it('retains the funding/payment distinction, EUR, VAT basis and population denominator', () => {
    const ui = getOverviewUi('en')
    expect(ui('Συμβάσεις ΚΗΜΔΗΣ: {{v0}}. Οριστικοποιήσεις πληρωμής δήμων στη Διαύγεια χωρίς σύνδεση με σύμβαση: {{v1}}. Ποσά χωρίς ΦΠΑ, έως {{v2}}.', { v0: '€123', v1: '€45', v2: '17/11/2026' })).toContain('Amounts excluding VAT')
    expect(ui('ανά 100.000 κατοίκους')).toBe('per 100,000 residents')
    expect(ui('{{v0}} στρέμματα', { v0: 100 })).toBe('100 stremmata')
    expect(ui.format.currency(1234, { maximumFractionDigits: 0 })).toBe('€1,234')
  })

  it('keeps partial and full fire control distinct from review/checking', () => {
    expect(getOverviewUi('en')('ΥΠΟ ΜΕΡΙΚΟ ΕΛΕΓΧΟ')).toBe('PARTIALLY UNDER CONTROL')
    expect(getOverviewUi('en')('ΥΠΟ ΠΛΗΡΗ ΕΛΕΓΧΟ')).toBe('FULLY UNDER CONTROL')
  })

  it.each([0, 1, 1234])('renders the original map selection counts without changing the selected key (%i)', count => {
    const html = render('en', createElement(MapSelectionPanel, {
      source: 'dropdown', kind: 'municipality', label: 'ΑΘΗΝΑΙΩΝ', municipalityKey: '9186',
      currentYear: 2026, municipalitySignedCurrentCount: count, municipalityActivePreviousCount: count,
    }))
    expect(html).toContain('Municipality ΑΘΗΝΑΙΩΝ')
    expect(html).toContain('/en/municipalities?municipality=9186')
    expect(html).toContain(count === 0 ? 'no contracts' : count === 1 ? '1 contract' : '1,234 contracts')
    expect(html).not.toMatch(/undefined|NaN|places:|overview:/)
  })

  it('shows the English municipality search without changing its page structure', () => {
    const html = render('en', createElement(MunicipalitiesPage))
    expect(html).toContain('Search by municipality')
    expect(html.toLowerCase()).toContain('municipality profile')
    expect(html).toContain('Clear')
    expect(html).not.toMatch(/places:|overview:|undefined|NaN/)
  })

  it('renders funding values with the same underlying data and reviewed association label', () => {
    const original = structuredClone(fundingSummary)
    const html = render('en', createElement(Funding, { currentYear: 2026 }))
    expect(html).toContain('Associations of Municipalities')
    expect(html).toContain('Regular')
    expect(html).toContain('Emergency')
    expect(html).toContain('KAP')
    expect(fundingSummary).toEqual(original)
  })
})

describe('map display state while switching language', () => {
  let tree: ReactTestRenderer | undefined
  afterEach(() => { if (tree) act(() => tree!.unmount()); tree = undefined })

  it('preserves a hidden legend and isolates fixed Greek/English contexts', () => {
    function Probe() {
      const ui = useOverviewUi()
      const [visible, setVisible] = useState(true)
      return createElement(MapLegendToggle, { visible, onToggle: () => setVisible(v => !v), label: ui('Χάρτης'), children: 'Legend' })
    }
    const element = (language: AppLanguage) => createElement(I18nProvider, { language, children: createElement(Probe) })
    act(() => { tree = create(element('el')) })
    act(() => tree!.root.findByType('button').props.onClick())
    act(() => tree!.update(element('en')))
    expect(tree!.root.findByType('button').props['aria-pressed']).toBe(false)
    expect(tree!.root.findByType('button').props['aria-label']).toBe('Show: Map')
    expect(getOverviewUi('el')('Χάρτης')).toBe('Χάρτης')
    expect(getOverviewUi('en')('Χάρτης')).toBe('Map')
  })
})
