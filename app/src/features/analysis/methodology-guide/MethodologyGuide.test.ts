import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { MemoryRouter } from 'react-router-dom'
import { describe, expect, it } from 'vitest'
import I18nProvider from '../../../i18n/I18nProvider'
import type { AppLanguage } from '../../../i18n/languages'
import MethodologyGuidePage from './MethodologyGuidePage'
import { evaluateTitle, matchPrefix, negativeTokens, NEGATIVE_GROUPS, NEGATIVE_KEYWORDS, TESTER_PRESETS } from './guideData'

function render(language: AppLanguage) {
  const path = language === 'en' ? '/en/analysis/methodology_guide' : '/analysis/methodology_guide'
  return renderToStaticMarkup(createElement(I18nProvider, { language, children: createElement(MemoryRouter, { initialEntries: [path] }, createElement(MethodologyGuidePage)) }))
}

describe('methodology guide page', () => {
  // User decision (2026-11-18): the English route shows the Greek guide until it is translated.
  it('renders the same Greek guide on both routes, marked as Greek', () => {
    const greek = render('el')
    const english = render('en')
    for (const html of [greek, english]) {
      expect(html).toContain('<article class="mg-root" lang="el">')
      expect(html).toContain('<section id="dokimi"')
      expect(html).toContain('Κρατάμε = ( ')
      expect(html.replace(/<[^>]+>/g, ' ')).not.toMatch(/undefined|NaN|\[object Object\]/)
    }
    expect(english).toContain('href="/en/analysis"')
    expect(english).toContain('All analyses')
    expect(greek).toContain('href="/analysis"')
  })

  it('links every table-of-contents entry to a section', () => {
    const html = render('el')
    const targets = [...html.matchAll(/<a href="#([^"]+)"/g)].map(match => match[1])
    expect(targets).toHaveLength(10)
    for (const id of targets) expect(html).toContain(`<section id="${id}"`)
  })
})

describe('title tester rule', () => {
  const run = (label: string) => {
    const preset = TESTER_PRESETS.find(item => item.label === label)!
    return evaluateTitle({ title: preset.title, cpv: preset.cpv, organization: '' })
  }

  it('reproduces the worked examples of the guide, showing the words as written in the title', () => {
    expect(run('Αποψίλωση οικοπέδων')).toMatchObject({ verdict: 'keep', droppedAt: null, requiresPositive: false })
    expect(run('Κατάσβεση')).toMatchObject({ verdict: 'keep', positiveHits: ['κατάσβεση'] })
    expect(run('Επισκευή δρόμου')).toMatchObject({ verdict: 'drop', droppedAt: 3 })
    expect(run('Γήπεδο')).toMatchObject({ verdict: 'drop', droppedAt: 2, negativeHits: [{ words: 'γήπεδο', field: 'title' }] })
    expect(run('Σχολικά κτίρια')).toMatchObject({ verdict: 'drop', droppedAt: 2, positiveHits: ['πυροπροστασίας'] })
  })

  it('needs a listed CPV and checks the organization for negative keywords', () => {
    expect(evaluateTitle({ title: 'Αντιπυρική ζώνη', organization: '', cpv: 'none' })).toMatchObject({ verdict: 'drop', droppedAt: 1 })
    const hospital = evaluateTitle({ title: 'Αποψίλωση', organization: 'Γενικό Νοσοκομείο', cpv: 'primary' })
    expect(hospital).toMatchObject({ verdict: 'drop', droppedAt: 2, negativeHits: [{ words: 'Νοσοκομείο', field: 'organization' }] })
  })

  it('shows multi-word matches as written', () => {
    expect(evaluateTitle({ title: 'Μελέτη πρόληψης δασικών πυρκαγιών', organization: '', cpv: 'secondary' }).positiveHits).toEqual(['δασικών πυρκαγιών'])
    expect(evaluateTitle({ title: 'Πυροπροστασία των κτιριακών εγκαταστάσεων', organization: '', cpv: 'primary' }).negativeHits.map(hit => hit.words)).toContain('Πυροπροστασία … κτιριακών … εγκαταστάσεων')
  })

  it('allows up to the configured number of words between phrase roots', () => {
    const phrase = negativeTokens('πυροπροστασ κτιριακ εγκαταστασ')
    expect(matchPrefix(negativeTokens('Πυροπροστασία των κτιριακών εγκαταστάσεων'), phrase, 2)).not.toBeNull()
    expect(matchPrefix(negativeTokens('Πυροπροστασία α β γ κτιριακών εγκαταστάσεων'), phrase, 2)).toBeNull()
  })

  it('lists every negative keyword in exactly one group', () => {
    const grouped = NEGATIVE_GROUPS.flatMap(([, items]) => items)
    expect(grouped.sort()).toEqual(Object.keys(NEGATIVE_KEYWORDS).sort())
  })
})
