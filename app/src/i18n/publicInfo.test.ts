import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import I18nProvider from './I18nProvider'
import MediaPage from '../pages/MediaPage'
import TermsPage from '../pages/TermsPage'
import PrivacyPage from '../pages/PrivacyPage'
import { cookieConsentTranslation } from './cookieConsentTranslation'
import greek from './catalogs/publicInfo.el.json'
import english from './catalogs/publicInfo.en.json'
import mediaGreek from './catalogs/mediaTitles.el.json'
import mediaEnglish from './catalogs/mediaTitles.en.json'
import type { AppLanguage } from './languages'

const render = (language: AppLanguage, page: typeof MediaPage) => renderToStaticMarkup(createElement(I18nProvider, { language, children: createElement(page) }))
const attributes = (html: string, attribute: string) => [...html.matchAll(new RegExp(` ${attribute}="([^"]*)"`, 'g'))].map(match => match[1])

describe('public information pages', () => {
  it.each([[greek, english], [mediaGreek, mediaEnglish]])('retains every message, placeholder, number and contact address', (greek, english) => {
    expect(Object.keys(english).sort()).toEqual(Object.keys(greek).sort())
    const targets: Record<string, string> = english
    for (const [key, before] of Object.entries(greek)) {
      const after = targets[key]
      expect(after.trim(), key).not.toBe('')
      expect(after.match(/\{\{[^{}]+\}\}/g)?.sort() ?? [], key).toEqual(before.match(/\{\{[^{}]+\}\}/g)?.sort() ?? [])
      expect(after.match(/\d+/g)?.sort() ?? [], key).toEqual(before.match(/\d+/g)?.sort() ?? [])
      if (before.includes('troboukis[at]gmail[dot]com')) expect(after).toContain('troboukis[at]gmail[dot]com')
      expect(after).not.toMatch(/[Α-Ωα-ω]/)
    }
  })

  it.each([MediaPage, TermsPage, PrivacyPage])('keeps source links, images, calendar dates and section structure in both languages', Page => {
    const el = render('el', Page), en = render('en', Page)
    for (const attribute of ['href', 'src', 'dateTime']) expect(attributes(en, attribute)).toEqual(attributes(el, attribute))
    expect(en.match(/<section/g)?.length).toBe(el.match(/<section/g)?.length)
    expect(en).not.toMatch(/publicInfo:|undefined|NaN/)
  })

  it('translates Greek media headlines, retains English originals and keeps sources and dates', () => {
    const el = render('el', MediaPage), en = render('en', MediaPage)
    const titles = (html: string) => [...html.matchAll(/<h3[^>]*>(.*?)<\/h3>/g)].map(match => match[1])
    expect(titles(en)[0]).toBe('FireWatch: The platform that reveals what happened before every forest fire')
    expect(titles(en).join(' ')).not.toMatch(/[Α-Ωα-ω]/)
    expect(en).toContain('On air with Niki Lymperaki')
    expect(en).toContain('#NaMaste @Deftero with Ioanna Niaoti')
    expect(en).toContain('48% less was spent on fire protection than last year')
    for (const title of ['UPDATE', 'Mega News', 'Online', "Europe&#x27;s infernal summer: Record heat, wildfires, and migration"]) expect(titles(en)).toContain(title)
    expect(titles(en)).toHaveLength(19)
    expect(el).toContain('<h3 lang="el">FireWatch: Η πλατφόρμα')
    expect(el).toContain('Στον αέρα με τη Νίκυ Λυμπεράκη')
    expect(en.match(/<h3 lang="en">/g)).toHaveLength(19)
    expect(en).toContain('ThessPost')
    const dates = [...en.matchAll(/<time[^>]*>(.*?)<\/time>/g)].map(match => match[1])
    expect(dates.join('\n')).toMatch(/Monday,? 3 August 2026/)
    expect(en).toContain('<time>Sunday, 09:20</time>')
    expect(en).toContain('01 / Press &amp; online media')
    expect(el).toContain('01 / Τύπος &amp; διαδίκτυο')
    expect(el).not.toContain('&amp;amp;')
  })

  it('retains the policies date, cookie identifiers and distinction between consent and legitimate interest', () => {
    const privacy = render('en', PrivacyPage), terms = render('en', TermsPage)
    expect(terms).toContain('dateTime="2026-07-13">13 July 2026')
    expect(terms).toContain('Thanasis Troboukis')
    expect(terms).toContain('must call 112')
    expect(privacy).toContain('legitimate interest')
    expect(privacy).toContain('legal basis for this processing is your consent')
    expect(privacy).toContain('<code>firewatch_consent</code> stores only your choices for up to 182 days')
    expect(privacy).toContain('<code>_ga</code> and <code>_ga_*</code>')
    expect(privacy).not.toMatch(/[Α-Ωα-ω]/)
  })

  it('builds complete cookie dialogs with matching action labels and language-specific links', () => {
    const el = cookieConsentTranslation('el'), en = cookieConsentTranslation('en')
    expect(el.consentModal?.title).toBe('Επιλογές απορρήτου')
    expect(en.consentModal?.acceptAllBtn).toBe('Accept all')
    expect(en.consentModal?.acceptNecessaryBtn).toBe('Reject all')
    expect(en.preferencesModal.savePreferencesBtn).toBe('Save preferences')
    expect(en.preferencesModal.serviceCounterLabel).toBe('Service|Services')
    expect(el.consentModal?.footer).toContain('href="/privacy"')
    expect(en.consentModal?.footer).toContain('href="/en/privacy"')
    expect(en.preferencesModal.sections?.map(section => section.linkedCategory).filter(Boolean)).toEqual(['necessary', 'analytics'])
    expect(JSON.stringify(en)).toContain('182 days')
    expect(JSON.stringify(en)).toContain('Up to 2 years')
    expect(JSON.stringify(en)).toContain('firewatch_consent')
  })
})
