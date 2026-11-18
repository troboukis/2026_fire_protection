export const APP_LANGUAGES = ['el', 'en'] as const
export type AppLanguage = typeof APP_LANGUAGES[number]
export const DEFAULT_LANGUAGE: AppLanguage = 'el'
export const APP_TIME_ZONE = 'Europe/Athens'
export const EMPTY_VALUE = '—'

export const LOCALES = {
  el: 'el-GR',
  en: 'en-GB',
} as const satisfies Record<AppLanguage, string>

export function isAppLanguage(value: unknown): value is AppLanguage {
  return value === 'el' || value === 'en'
}
