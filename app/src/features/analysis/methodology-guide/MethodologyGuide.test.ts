import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { MemoryRouter } from 'react-router-dom'
import { describe, expect, it } from 'vitest'
import I18nProvider from '../../../i18n/I18nProvider'
import type { AppLanguage } from '../../../i18n/languages'
import MethodologyGuidePage from './MethodologyGuidePage'
import { getOverviewUi } from '../../../i18n/overview'
import messages1 from '../../../i18n/catalogs/analysisMethodology1.el.json'
import messages2 from '../../../i18n/catalogs/analysisMethodology2.el.json'
import { guideCode } from './guideCode'
import { CODE_EFFIS, CODE_FIRMS, CODE_JOIN, COPY_PAYLOADS, evaluateTitle, matchPrefix, negativeTokens, NEGATIVE_GROUPS, NEGATIVE_KEYWORDS, TESTER_PRESETS } from './guideData'

function render(language: AppLanguage) {
  const path = language === 'en' ? '/en/analysis/methodology_guide' : '/analysis/methodology_guide'
  return renderToStaticMarkup(createElement(I18nProvider, { language, children: createElement(MemoryRouter, { initialEntries: [path] }, createElement(MethodologyGuidePage)) }))
}

describe('methodology guide page', () => {
  it('translates the guide while keeping the Greek source examples and the same section anchors', () => {
    const greek = render('el')
    const english = render('en')
    for (const html of [greek, english]) {
      for (const { title } of TESTER_PRESETS) expect(html).toContain(`<td lang="el">${title}</td>`)
      expect(html.replace(/<[^>]+>/g, ' ')).not.toMatch(/undefined|NaN|\[object Object\]/)
    }
    expect(greek).toContain('<article class="mg-root" lang="el">')
    expect(greek).toContain('Κρατάμε = ( ')
    expect(english).toContain('<article class="mg-root" lang="en">')
    expect(english).toContain('How we collect public data on forest fires')
    expect(english).not.toContain('Enter a Greek title')
    expect(english).toContain('Keep = ( ')
    const withoutSources = english.replace(/<(td|span|code|mark|textarea)\b[^>]*\blang="el"[^>]*>[\s\S]*?<\/\1>/g, '')
    expect(withoutSources.match(/\p{Script=Greek}+/gu) ?? []).toEqual([])
    expect(english).toContain('href="/en/analysis"')
    expect(english).toContain('All analyses')
    expect(greek).toContain('href="/analysis"')
    const geometry = (html: string) => html.match(/<(?:circle|rect|line|pattern|clipPath)\b[^>]*>/g)
    expect(geometry(english)).toEqual(geometry(greek))
  })

  it('links every table-of-contents entry to a section', () => {
    for (const language of ['el', 'en'] as const) {
      const html = render(language)
      const targets = [...html.matchAll(/<a href="#([^"]+)"/g)].map(match => match[1])
      expect(targets).toHaveLength(language === 'el' ? 10 : 9)
      for (const id of targets) expect(html).toContain(`<section id="${id}"`)
    }
  })

  it('never submits the source contract titles or raw keyword lists for translation', () => {
    const corpus = Object.values({ ...messages1, ...messages2 }).join('\n')
    for (const { title } of TESTER_PRESETS) expect(corpus).not.toContain(title)
    for (const root of Object.keys(NEGATIVE_KEYWORDS)) expect(Object.values({ ...messages1, ...messages2 })).not.toContain(root)
  })

  it('localizes only example code comments and copy text, preserving the executable code and raw payloads', () => {
    const clean = (code: string) => code.replace(/#[^\n]*/g, '').replace(/"(?:ΤΟ_ΚΛΕΙΔΙ_ΣΑΣ|YOUR_MAP_KEY)"/g, '"KEY"').replace(/"(?:πυρκαγιές|fires)"/g, '"FIRES"')
    for (const code of [CODE_EFFIS, CODE_FIRMS, CODE_JOIN]) {
      expect(guideCode(code, getOverviewUi('el'))).toBe(code)
      const english = guideCode(code, getOverviewUi('en'))
      expect(english).not.toMatch(/\p{Script=Greek}/u)
      expect(clean(english)).toBe(clean(code))
    }
    expect(COPY_PAYLOADS.positive).toContain('πυροπροστασ')
    expect(COPY_PAYLOADS.primary['75251120-7']).toBe('Υπηρεσίες καταπολέμησης δασοπυρκαγιών')
  })

  it('omits the tester and both keyword panels only in English, keeping both CPV panels', () => {
    const greek = render('el')
    const english = render('en')
    expect(greek).toContain('<section id="dokimi"')
    expect(greek).toContain('href="#dokimi"')
    expect(greek).toContain('<textarea')
    expect(english).not.toContain('<section id="dokimi"')
    expect(english).not.toContain('href="#dokimi"')
    expect(english).not.toContain('<textarea')
    for (const [html, count] of [[greek, 4], [english, 2]] as const) {
      const lists = html.slice(html.indexOf('<section id="listes"'), html.indexOf('<section id="piges"'))
      expect(lists.match(/class="mg-kit__box"/g)).toHaveLength(count)
      expect(lists).toContain('>Primary CPVs</h4>')
      expect(lists).toContain('>Secondary CPVs</h4>')
      if (count === 2) {
        expect(lists).not.toContain('>Positive keywords</h4>')
        expect(lists).not.toContain('>Negative keywords</h4>')
        expect(lists).toContain('5 · Lists')
      }
    }
    expect(english).toContain('9 · Pitfalls')
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
