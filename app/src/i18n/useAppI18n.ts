import { useContext, useMemo } from 'react'
import { I18nContext, useTranslation } from 'react-i18next'
import { createFormatters } from './format'
import { defaultI18n } from './instance'
import { DEFAULT_LANGUAGE, isAppLanguage, LOCALES } from './languages'

export function useAppI18n() {
  const instance = useContext(I18nContext)?.i18n ?? defaultI18n
  const { t } = useTranslation(['common', 'navigation', 'layout', 'source', 'procurement', 'overview'] as const, { i18n: instance })
  const language = isAppLanguage(instance.resolvedLanguage) ? instance.resolvedLanguage : DEFAULT_LANGUAGE
  const format = useMemo(() => createFormatters(language), [language])
  return { language, locale: LOCALES[language], t, format }
}
