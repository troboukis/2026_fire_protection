import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { act, create } from 'react-test-renderer'
import { describe, expect, it, vi } from 'vitest'
import I18nProvider from './I18nProvider'
import { taxonomyUi } from './taxonomy'
import { translatedOrganizationName } from './organizationNames'
import aliases from './organizationAliases.json'
import OrganizationFilter from '../components/OrganizationFilter'
import { rankMunicipalityMatches } from '../lib/municipalitySearch'

const en = taxonomyUi('en')
const el = taxonomyUi('el')

describe('reviewed authority names', () => {
  it('covers the complete captured name inventory and retains every Greek spelling', () => {
    expect(Object.keys(aliases)).toHaveLength(1804)
    for (const source of Object.keys(aliases)) {
      expect(en.organization(source), source).not.toMatch(/[Α-Ωα-ω]/)
      expect(en.organization(source), source).not.toContain('undefined')
      expect(el.organization(source)).toBe(source)
    }
  })

  it('uses consistent explicit aliases, with distinct historical/combined names', () => {
    expect(en.organization('ΔΗΜΟΣ ΔΟΞΑΤΟΥ')).toBe('Municipality of Doxato')
    expect(en.organization('Δ. ΔΟΞΑΤΟΥ')).toBe('Municipality of Doxato')
    expect(en.organization('Δοξάτου')).toBe('Doxato')
    expect(en.organization('ΑΘΗΝΑΙΩΝ')).toBe('Athens')
    expect(en.organization('ΒΟΡΕΙΑΣ ΚΕΡΚΥΡΑΣ')).toBe('Northern Corfu')
    expect(en.organization('ΚΕΡΚΥΡΑΣ')).toBe('Corfu')
    expect(en.organization('Βόρειας Κέρκυρας / Κεντρικής Κέρκυρας και Διαποντίων Νήσων / Νότιας Κέρκυρας')).toContain(' / ')
    expect(en.organization('ΥΠΟΥΡΓΕΙΟ ΟΙΚΟΝΟΜΙΚΩΝ')).toBe('Ministry of Finance')
    expect(en.organization('ΥΠΟΥΡΓΕΙΟ ΠΕΡΙΒΑΛΛΟΝΤΟΣ ΚΑΙ ΕΝΕΡΓΕΙΑΣ')).toBe('Ministry of Environment and Energy')
  })

  it('corrects place and institutional names without translating unknown source content', () => {
    expect(en.organization('Αγράφων')).toBe('Agrafa')
    expect(en.organization('Φυλής')).toBe('Fyli')
    expect(en.organization('Πύλης')).toBe('Pyli')
    expect(en.organization('ΣΕΡΒΙΩΝ')).toBe('Servia')
    expect(en.organization('Λειψών')).toBe('Lipsi')
    expect(en.organization('ΕΘΝΙΚΟ ΜΕΤΣΟΒΙΟ ΠΟΛΥΤΕΧΝΕΙΟ')).toBe('National Technical University of Athens')
    expect(en.organization('ΣΤΑΔΙΟ ΕΙΡΗΝΗΣ ΚΑΙ ΦΙΛΙΑΣ')).toBe('Peace and Friendship Stadium')
    expect(en.organization('ΑΛΛΟΣ ΦΟΡΕΑΣ · ΠΡΑΣΙΝΟ ΤΑΜΕΙΟ')).toBe('Other body · Green Fund')
    const original = 'Πρωτότυπος τίτλος σύμβασης – πυροπροστασία 2026'
    expect(en.organization(original)).toBe(original)
    expect(translatedOrganizationName('org_unknown_identifier')).toBeUndefined()
  })

  it('keeps Greek select values and emits the original selected organization', () => {
    const options = ['ΔΗΜΟΣ ΔΟΞΑΤΟΥ', 'ΑΛΛΟΣ ΦΟΡΕΑΣ · ΠΡΑΣΙΝΟ ΤΑΜΕΙΟ']
    const onChange = vi.fn()
    const filter = createElement(OrganizationFilter, { options, value: options[0], onChange })
    const html = renderToStaticMarkup(createElement(I18nProvider, { language: 'en', children: filter }))
    expect(html).toContain('value="ΔΗΜΟΣ ΔΟΞΑΤΟΥ"')
    expect(html).toContain('Municipality of Doxato')
    expect(html).toContain('Other body · Green Fund')
    let tree: ReturnType<typeof create>
    act(() => { tree = create(createElement(I18nProvider, { language: 'en', children: filter })) })
    act(() => tree!.root.findByType('select').props.onChange({ target: { value: options[1] } }))
    expect(onChange).toHaveBeenCalledExactlyOnceWith(options[1])
    act(() => tree!.root.findByType('input').props.onChange({ target: { value: 'ΠΡΑΣΙΝΟ' } }))
    expect(tree!.root.findByType('select').findAllByType('option').some(option => option.props.value === options[1])).toBe(true)
    act(() => tree!.unmount())
    expect(options).toEqual(['ΔΗΜΟΣ ΔΟΞΑΤΟΥ', 'ΑΛΛΟΣ ΦΟΡΕΑΣ · ΠΡΑΣΙΝΟ ΤΑΜΕΙΟ'])
  })

  it('finds an English or Greek name with the same municipality and untouched key', () => {
    const municipalities = [{ municipality_key: '9001', dhmos: 'Δοξάτου', municipality_normalized_name: 'ΔΟΞΑΤΟΥ' }]
    const original = structuredClone(municipalities)
    const englishName = (source: string) => translatedOrganizationName(source) ?? source
    expect(rankMunicipalityMatches(municipalities, 'Doxato', englishName)[0]?.municipality.municipality_key).toBe('9001')
    expect(rankMunicipalityMatches(municipalities, 'ΔΟΞΑΤΟΥ', englishName)[0]?.municipality.municipality_key).toBe('9001')
    expect(rankMunicipalityMatches(municipalities, '9001', englishName)).toEqual([])
    expect(municipalities).toEqual(original)
  })
})
