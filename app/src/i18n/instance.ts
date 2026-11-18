import { createInstance } from 'i18next'
import { logWarn } from '../lib/logger'
import { APP_LANGUAGES, DEFAULT_LANGUAGE, EMPTY_VALUE, type AppLanguage } from './languages'
import { el } from './locales/el'
import { en } from './locales/en'

export const resources = { el, en }

export function createAppI18n(language: AppLanguage = DEFAULT_LANGUAGE) {
  const instance = createInstance()
  // All dictionaries are bundled. No detector, backend, network request or
  // global language mutation is needed. Copies keep separate providers isolated.
  void instance.init({
    resources: structuredClone(resources),
    lng: language,
    supportedLngs: [...APP_LANGUAGES],
    fallbackLng: DEFAULT_LANGUAGE,
    defaultNS: 'common',
    ns: Object.keys(el),
    initAsync: false,
    returnNull: false,
    returnEmptyString: false,
    saveMissing: false,
    interpolation: { escapeValue: false }, // React escapes the rendered text.
    react: { useSuspense: false },
    parseMissingKeyHandler: (key) => {
      logWarn('Missing application translation', { language, key })
      return EMPTY_VALUE
    },
    missingInterpolationHandler: () => EMPTY_VALUE,
  })
  return instance
}

// Shared components also render in isolated tests and embedded reports.
// Their explicit fallback stays Greek regardless of other provider instances.
export const defaultI18n = createAppI18n()
