import { describe, expect, it, vi } from 'vitest'
import { createAppI18n, resources } from './instance'
import { resolveSourceText } from './sourceText'

vi.mock('../lib/logger', () => ({ logWarn: vi.fn() }))

describe('bundled application translations', () => {
  it('initializes both languages synchronously with matching, nonempty dictionaries', () => {
    expect(Object.keys(resources.en)).toEqual(Object.keys(resources.el))
    for (const namespace of Object.keys(resources.el) as Array<keyof typeof resources.el>) {
      const greek = resources.el[namespace]
      const english = resources.en[namespace]
      expect(Object.keys(english).sort()).toEqual(Object.keys(greek).sort())
      for (const [key, value] of Object.entries(greek)) {
        const translated = (english as Record<string, string>)[key]
        expect(value.trim()).not.toBe('')
        expect(translated.trim()).not.toBe('')
        const variables = (text: string) => Array.from(text.matchAll(/{{\s*([^}, ]+)/g), match => match[1]).sort()
        expect(variables(translated)).toEqual(variables(value))
      }
    }
    for (const language of ['el', 'en'] as const) {
      const instance = createAppI18n(language)
      expect(instance.isInitialized).toBe(true)
      expect(instance.resolvedLanguage).toBe(language)
      expect(instance.options.saveMissing).toBe(false)
      expect(instance.services.backendConnector.backend).toBeNull()
      expect(instance.services.languageDetector).toBeUndefined()
    }
  })

  it.each([
    ['el', 0, '0 έγγραφα δεν έχουν ποσό'],
    ['el', 1, '1 έγγραφο δεν έχει ποσό'],
    ['el', 2, '2 έγγραφα δεν έχουν ποσό'],
    ['el', 1234, '1.234 έγγραφα δεν έχουν ποσό'],
    ['en', 0, '0 documents have no amount'],
    ['en', 1, '1 document has no amount'],
    ['en', 2, '2 documents have no amount'],
    ['en', 1234, '1,234 documents have no amount'],
  ] as const)('uses the %s plural and number format for %s documents', (language, count, expected) => {
    expect(createAppI18n(language).t('documentsWithoutAmount', { count })).toBe(expected)
  })

  it('falls back to Greek for an absent or empty English translation', () => {
    const instance = createAppI18n('en')
    instance.addResource('en', 'common', 'totalUnavailable', '')
    expect(instance.t('totalUnavailable')).toBe('Το σύνολο δεν είναι διαθέσιμο.')
    instance.removeResourceBundle('en', 'common')
    expect(instance.t('loadingData')).toBe('Φόρτωση δεδομένων')
  })

  it('shows a neutral placeholder instead of an internal translation key', () => {
    const instance = createAppI18n('en')
    // @ts-expect-error Deliberately exercise an invalid runtime key; normal callers are type checked.
    expect(instance.t('internal.missing.translation')).toBe('—')
  })

  it('isolates dictionaries and selected languages between instances', () => {
    const first = createAppI18n('en')
    const second = createAppI18n('el')
    first.addResource('en', 'navigation', 'home', 'Changed for this instance')
    expect(second.t('navigation:home')).toBe('Αρχική')
    expect(createAppI18n('en').t('navigation:home')).toBe('Home')
    expect(resources.en.navigation.home).toBe('Home')
  })
})

describe('source evidence and translations', () => {
  it.each(['el', 'en'] as const)('keeps the original Greek text intact in %s when no translation exists', (language) => {
    const source = Object.freeze({ originalGreek: '  Πρωτότυπος τίτλος εγγράφου  ' })
    expect(resolveSourceText(source, language)).toEqual({ text: source.originalGreek, language: 'el', isTranslation: false })
  })

  it('uses only an explicitly supplied English translation and labels its provenance', () => {
    const source = Object.freeze({ originalGreek: 'Πρωτότυπος τίτλος', englishTranslation: 'Translated title' })
    expect(resolveSourceText(source, 'en')).toEqual({ text: 'Translated title', language: 'en', isTranslation: true })
    expect(resolveSourceText(source, 'el')).toEqual({ text: 'Πρωτότυπος τίτλος', language: 'el', isTranslation: false })
  })

  it('treats a blank translation as unavailable without trimming the original', () => {
    expect(resolveSourceText({ originalGreek: ' Αρχικό ', englishTranslation: '  ' }, 'en'))
      .toEqual({ text: ' Αρχικό ', language: 'el', isTranslation: false })
  })
})
